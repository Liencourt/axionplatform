"""
enrichment/processor.py
-----------------------
Valida e padroniza DataFrame de vendas antes do pipeline de enriquecimento.

Responsabilidades:
  1. Aceitar CSV (path) ou DataFrame já carregado
  2. Normalizar nomes de colunas (case-insensitive, aliases comuns)
  3. Validar colunas obrigatórias: data, quantidade
  4. Padronizar datas para datetime64[ns]
  5. Validar tipos numéricos (quantidade, preco)
  6. Se coluna 'cep' presente → resolver municipio/lat/lon via ViaCEP + Nominatim
  7. Retornar (DataFrame padronizado, ValidationReport)

Compatibilidade Cloud Run:
  - Sem estado em disco; cache em memória por processo (lru_cache)
  - Nominatim: 1 req/s, cacheado por município para minimizar chamadas
  - ViaCEP: cacheado por CEP limpo
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Union

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Constantes ─────────────────────────────────────────────────────────────

COLUNAS_OBRIGATORIAS = {"data", "quantidade"}
COLUNAS_OPCIONAIS = {"produto", "preco", "cep"}

# Aliases aceitos → nome canônico
ALIASES: dict[str, str] = {
    # data
    "date": "data", "dt": "data", "data_venda": "data", "data_pedido": "data",
    "data_emissao": "data", "competencia": "data",
    # quantidade
    "qty": "quantidade", "qtd": "quantidade", "volume": "quantidade",
    "qty_vendida": "quantidade", "quantidade_vendida": "quantidade",
    "qtde": "quantidade", "units": "quantidade",
    # produto
    "product": "produto", "sku": "produto", "codigo": "produto",
    "cod_produto": "produto", "codigo_produto": "produto", "item": "produto",
    "descricao": "produto", "nome_produto": "produto",
    # preco
    "price": "preco", "valor": "preco", "preco_unitario": "preco",
    "preco_venda": "preco", "vl_unitario": "preco", "vlr_unitario": "preco",
    # cep
    "zip": "cep", "zipcode": "cep", "zip_code": "cep", "postal_code": "cep",
    "cep_loja": "cep", "cep_filial": "cep",
}

VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "AxiomPlatform/1.0 (suporte@axiomplatform.com.br)"
}

_LAST_NOMINATIM_CALL: list[float] = [0.0]  # mutável para respeitar rate limit


# ── Relatório de Validação ──────────────────────────────────────────────────

@dataclass
class ValidationReport:
    colunas_encontradas: list[str] = field(default_factory=list)
    colunas_faltando: list[str] = field(default_factory=list)
    colunas_opcionais_presentes: list[str] = field(default_factory=list)
    total_linhas: int = 0
    linhas_validas: int = 0
    linhas_rejeitadas: int = 0
    erros: list[str] = field(default_factory=list)
    avisos: list[str] = field(default_factory=list)
    ceps_resolvidos: int = 0
    ceps_com_erro: int = 0
    valido: bool = True

    def to_dict(self) -> dict:
        return {
            "colunas_encontradas": self.colunas_encontradas,
            "colunas_faltando": self.colunas_faltando,
            "colunas_opcionais_presentes": self.colunas_opcionais_presentes,
            "total_linhas": self.total_linhas,
            "linhas_validas": self.linhas_validas,
            "linhas_rejeitadas": self.linhas_rejeitadas,
            "erros": self.erros,
            "avisos": self.avisos,
            "ceps_resolvidos": self.ceps_resolvidos,
            "ceps_com_erro": self.ceps_com_erro,
            "valido": self.valido,
        }


# ── API: ViaCEP ─────────────────────────────────────────────────────────────

def _limpar_cep(cep_raw: str) -> str | None:
    """Remove formatação e valida CEP brasileiro (8 dígitos)."""
    limpo = re.sub(r"\D", "", str(cep_raw))
    return limpo if len(limpo) == 8 else None


@lru_cache(maxsize=2048)
def _lookup_viacep(cep_limpo: str) -> dict | None:
    """
    Consulta ViaCEP e retorna dict com localidade, uf, ibge, etc.
    Cacheado por processo — efetivo para lotes do mesmo tenant.
    Retorna None em caso de CEP inválido ou erro de rede.
    """
    try:
        resp = requests.get(
            VIACEP_URL.format(cep=cep_limpo),
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            if "erro" not in data:
                return data
        logger.debug("ViaCEP: CEP %s não encontrado (status=%s)", cep_limpo, resp.status_code)
    except requests.RequestException as exc:
        logger.warning("ViaCEP falhou para %s: %s", cep_limpo, exc)
    return None


# ── API: Nominatim (OpenStreetMap) ──────────────────────────────────────────

@lru_cache(maxsize=512)
def _lookup_coordenadas(query: str) -> tuple[float | None, float | None]:
    """
    Geocoding via Nominatim (OSM) a partir de uma query de endereço.
    Rate limit: 1 req/s — respeitado com sleep entre chamadas.

    A query deve ser o mais completa possível para precisão máxima:
      - Endereço completo: "Rua X, 123, Bairro, Município, UF, Brasil"
      - Só município:      "Município, UF, Brasil"

    Para produção com volume alto, substitua por Google Maps Geocoding API
    ou pré-popule uma tabela de municípios IBGE com coordenadas.
    """
    # Respeitar rate limit de 1 req/s do Nominatim
    agora = time.monotonic()
    delta = agora - _LAST_NOMINATIM_CALL[0]
    if delta < 1.1:
        time.sleep(1.1 - delta)
    _LAST_NOMINATIM_CALL[0] = time.monotonic()

    try:
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
            "countrycodes": "br",
        }
        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            results = resp.json()
            if results:
                return float(results[0]["lat"]), float(results[0]["lon"])
        logger.debug("Nominatim: sem resultado para '%s'", query)
    except (requests.RequestException, KeyError, ValueError, IndexError) as exc:
        logger.warning("Nominatim falhou para '%s': %s", query, exc)
    return None, None


def _montar_query_nominatim(
    logradouro: str, numero: str | None, bairro: str, municipio: str, uf: str
) -> str:
    """
    Monta a query de geocoding do Nominatim na forma mais precisa possível.

    Prioridade:
      1. Logradouro + número + bairro + município/UF  (precisão de porta)
      2. Logradouro + bairro + município/UF           (precisão de rua)
      3. Município/UF                                  (fallback — comportamento anterior)
    """
    base = f"{municipio}, {uf}, Brasil"
    if logradouro and numero:
        return f"{logradouro}, {numero}, {bairro}, {base}" if bairro else f"{logradouro}, {numero}, {base}"
    if logradouro:
        return f"{logradouro}, {bairro}, {base}" if bairro else f"{logradouro}, {base}"
    return base


# ── Enriquecimento de CEP ────────────────────────────────────────────────────

def _enriquecer_ceps(
    df: pd.DataFrame,
    relatorio: ValidationReport,
    numero: str | None = None,
) -> pd.DataFrame:
    """
    Para cada CEP único no DataFrame, resolve municipio, bairro, lat/lon.
    Adiciona colunas: municipio, uf, codigo_ibge, bairro, lat, lon.
    Deduplica CEPs antes de chamar APIs para minimizar requisições.

    Parâmetros
    ----------
    df         : DataFrame com coluna 'cep'.
    relatorio  : ValidationReport atualizado in-place.
    numero     : Número do endereço (opcional, do cadastro de Empresa/Loja).
                 Quando fornecido, melhora a precisão do geocoding para nível de porta.
    """
    df = df.copy()

    # Inicializa colunas
    for col in ("municipio", "uf", "codigo_ibge", "bairro", "lat", "lon"):
        df[col] = None

    ceps_unicos = df["cep"].dropna().unique()
    cache_cep: dict[str, dict] = {}  # cep_limpo → {municipio, uf, bairro, logradouro, lat, lon, ...}

    for cep_raw in ceps_unicos:
        cep_limpo = _limpar_cep(str(cep_raw))
        if not cep_limpo:
            logger.debug("CEP inválido ignorado: %s", cep_raw)
            relatorio.ceps_com_erro += 1
            continue

        if cep_limpo in cache_cep:
            continue

        dados_viacep = _lookup_viacep(cep_limpo)
        if not dados_viacep:
            relatorio.ceps_com_erro += 1
            cache_cep[cep_limpo] = {}
            continue

        municipio  = dados_viacep.get("localidade", "")
        uf         = dados_viacep.get("uf", "")
        bairro     = dados_viacep.get("bairro", "") or ""
        logradouro = dados_viacep.get("logradouro", "") or ""

        if municipio:
            query = _montar_query_nominatim(logradouro, numero, bairro, municipio, uf)
            lat, lon = _lookup_coordenadas(query)
        else:
            lat, lon = None, None

        cache_cep[cep_limpo] = {
            "municipio":   municipio,
            "uf":          uf,
            "codigo_ibge": dados_viacep.get("ibge"),
            "bairro":      bairro or None,
            "lat":         lat,
            "lon":         lon,
        }
        relatorio.ceps_resolvidos += 1
        logger.debug(
            "CEP %s → %s/%s bairro=%s (%.4f, %.4f)",
            cep_limpo, municipio, uf, bairro or "?", lat or 0, lon or 0,
        )

    # Aplica cache ao DataFrame
    def _resolver_linha(cep_raw):
        cep_limpo = _limpar_cep(str(cep_raw)) if pd.notna(cep_raw) else None
        return cache_cep.get(cep_limpo, {}) if cep_limpo else {}

    dados_geo = df["cep"].apply(_resolver_linha)
    for col in ("municipio", "uf", "codigo_ibge", "bairro", "lat", "lon"):
        df[col] = dados_geo.apply(lambda d: d.get(col))

    return df


# ── Função Principal ─────────────────────────────────────────────────────────

def process_sales_data(
    file_path_or_df: Union[str, Path, pd.DataFrame],
    *,
    resolver_cep: bool = True,
    numero: str | None = None,
    encoding: str = "utf-8",
) -> tuple[pd.DataFrame, ValidationReport]:
    """
    Valida e padroniza dados de vendas para o pipeline de enriquecimento.

    Parâmetros
    ----------
    file_path_or_df : str | Path | pd.DataFrame
        Caminho para CSV ou DataFrame já carregado.
    resolver_cep : bool
        Se True e coluna 'cep' presente, chama ViaCEP + Nominatim.
        Defina False em testes unitários para não fazer chamadas reais.
    encoding : str
        Encoding para leitura de CSV (padrão: utf-8, fallback: latin-1).

    Retorna
    -------
    (DataFrame padronizado, ValidationReport)

    O DataFrame padronizado sempre terá:
      - data         → datetime64[ns]
      - quantidade   → float64
      - produto      → str (se presente)
      - preco        → float64 (se presente)
      - municipio, uf, codigo_ibge, lat, lon (se cep presente e resolver_cep=True)
    """
    relatorio = ValidationReport()

    # ── 1. Carregar dados ────────────────────────────────────────────────────
    if isinstance(file_path_or_df, pd.DataFrame):
        df = file_path_or_df.copy()
    else:
        caminho = Path(file_path_or_df)
        if not caminho.exists():
            relatorio.erros.append(f"Arquivo não encontrado: {caminho}")
            relatorio.valido = False
            return pd.DataFrame(), relatorio

        try:
            df = _ler_arquivo(caminho, encoding)
        except Exception as exc:
            relatorio.erros.append(f"Falha na leitura do arquivo: {exc}")
            relatorio.valido = False
            return pd.DataFrame(), relatorio

    relatorio.total_linhas = len(df)

    # ── 2. Normalizar nomes de colunas ───────────────────────────────────────
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in ALIASES.items() if k in df.columns})
    # Remove colunas completamente vazias
    df.dropna(axis=1, how="all", inplace=True)

    relatorio.colunas_encontradas = df.columns.tolist()

    # ── 3. Validar colunas obrigatórias ──────────────────────────────────────
    faltando = COLUNAS_OBRIGATORIAS - set(df.columns)
    if faltando:
        relatorio.colunas_faltando = sorted(faltando)
        relatorio.erros.append(
            f"Colunas obrigatórias ausentes: {', '.join(sorted(faltando))}. "
            "Necessário: data, quantidade"
        )
        relatorio.valido = False
        return df, relatorio

    opcionais_presentes = sorted(COLUNAS_OPCIONAIS & set(df.columns))
    relatorio.colunas_opcionais_presentes = opcionais_presentes

    # ── 4. Padronizar datas ──────────────────────────────────────────────────
    df, relatorio = _padronizar_datas(df, relatorio)
    if not relatorio.valido:
        return df, relatorio

    # ── 5. Padronizar tipos numéricos ────────────────────────────────────────
    df, relatorio = _padronizar_numericos(df, relatorio)

    # ── 6. Remover linhas inválidas (quantidade NaN) ─────────────────────────
    mask_valida = df["quantidade"].notna() & df["data"].notna()
    linhas_rejeitadas = (~mask_valida).sum()
    if linhas_rejeitadas > 0:
        relatorio.avisos.append(
            f"{linhas_rejeitadas} linha(s) removida(s) por data ou quantidade nula."
        )
        relatorio.linhas_rejeitadas = int(linhas_rejeitadas)
    df = df[mask_valida].reset_index(drop=True)
    relatorio.linhas_validas = len(df)

    # ── 7. Enriquecer CEPs ───────────────────────────────────────────────────
    if "cep" in df.columns and resolver_cep:
        df = _enriquecer_ceps(df, relatorio, numero=numero)
    elif "cep" in df.columns and not resolver_cep:
        relatorio.avisos.append("Resolução de CEP desabilitada (resolver_cep=False).")

    # ── 8. Aviso de amostra pequena ──────────────────────────────────────────
    if relatorio.linhas_validas < 30:
        relatorio.avisos.append(
            f"Apenas {relatorio.linhas_validas} registros válidos. "
            "Correlações estatísticas requerem mínimo de 30 observações."
        )

    return df, relatorio


# ── Helpers Internos ─────────────────────────────────────────────────────────

def _ler_arquivo(caminho: Path, encoding: str) -> pd.DataFrame:
    """Lê CSV ou Excel com fallback de encoding."""
    sufixo = caminho.suffix.lower()
    if sufixo == ".csv":
        try:
            return pd.read_csv(caminho, sep=None, engine="python", encoding=encoding)
        except UnicodeDecodeError:
            return pd.read_csv(caminho, sep=None, engine="python", encoding="latin-1")
    elif sufixo in (".xlsx", ".xls"):
        return pd.read_excel(caminho)
    else:
        # Tenta como CSV genérico
        try:
            return pd.read_csv(caminho, sep=None, engine="python", encoding=encoding)
        except UnicodeDecodeError:
            return pd.read_csv(caminho, sep=None, engine="python", encoding="latin-1")


def _padronizar_datas(df: pd.DataFrame, relatorio: ValidationReport) -> tuple[pd.DataFrame, ValidationReport]:
    """Converte coluna 'data' para datetime64[ns] com múltiplos formatos."""
    FORMATOS = [
        "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
        "%Y/%m/%d", "%d.%m.%Y", "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S",
    ]
    col = df["data"]

    # Tenta inferência automática do pandas primeiro
    try:
        df["data"] = pd.to_datetime(col, dayfirst=True)
        return df, relatorio
    except Exception:
        pass

    # Tenta formatos explícitos
    for fmt in FORMATOS:
        try:
            df["data"] = pd.to_datetime(col, format=fmt, errors="raise")
            return df, relatorio
        except (ValueError, TypeError):
            continue

    # Fallback com coerce (datas inválidas viram NaT)
    df["data"] = pd.to_datetime(col, errors="coerce", dayfirst=True)
    invalidas = df["data"].isna().sum()
    if invalidas > 0:
        relatorio.avisos.append(
            f"{invalidas} data(s) não reconhecida(s) e convertida(s) para nulo."
        )
    if df["data"].notna().sum() == 0:
        relatorio.erros.append("Nenhuma data válida encontrada na coluna 'data'.")
        relatorio.valido = False

    return df, relatorio


def _padronizar_numericos(df: pd.DataFrame, relatorio: ValidationReport) -> tuple[pd.DataFrame, ValidationReport]:
    """Converte quantidade e preco para float, tratando strings com vírgula decimal."""
    for col in ("quantidade", "preco"):
        if col not in df.columns:
            continue
        if df[col].dtype == object:
            # Trata formato brasileiro: "1.234,56" → 1234.56
            df[col] = (
                df[col].astype(str)
                .str.strip()
                .str.replace(r"\.", "", regex=True)   # remove separador de milhar
                .str.replace(",", ".", regex=False)    # vírgula → ponto decimal
            )
        df[col] = pd.to_numeric(df[col], errors="coerce")
        invalidos = df[col].isna().sum()
        if invalidos > 0:
            relatorio.avisos.append(
                f"{invalidos} valor(es) inválido(s) na coluna '{col}' convertido(s) para nulo."
            )
    return df, relatorio
