"""
enrichment/ibge.py
------------------
Integração com a API SIDRA do IBGE para dados socioeconômicos municipais.

Dados obtidos por município (código IBGE de 7 dígitos):
  populacao        int    Estimativa populacional mais recente
  pib_per_capita   float  PIB per capita em R$ (último ano disponível)
  classe_economica str    Segmento estimado: 'A', 'B', 'C', 'D', 'E'
  densidade_demo   float  Densidade demográfica (hab/km²) quando disponível

Endpoints IBGE SIDRA v3:
  Estimativas de população (tabela 6579, variável 9324):
    https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2023/variaveis/9324
    ?localidades=N6[{codigo}]

  PIB per capita (tabela 5938, variável 37, 2021 — último ano publicado):
    https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/2021/variaveis/37
    ?localidades=N6[{codigo}]

  Informações do município (nome, UF, área):
    https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{codigo}

Regras:
  - Cacheado por 30 dias (dados anuais/censitários não mudam)
  - Graceful Degradation: se IBGE falhar, retorna dict com None por campo
  - O código IBGE de 6 dígitos é aceito (expande para 7 com zero à direita se necessário)

Cloud Run:
  - Cache L1 (memória) + L2 (GCS), via _cache.py
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import requests

from ._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

# ── Constantes ─────────────────────────────────────────────────────────────

SIDRA_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"
LOCALIDADES_BASE = "https://servicodados.ibge.gov.br/api/v1/localidades"
HEADERS = {"User-Agent": "AxiomPlatform/1.0 (suporte@axiomplatform.com.br)"}
TIMEOUT = 10

TTL_IBGE = 30 * 24 * 3600  # 30 dias

# Faixas de PIB per capita anual (R$) → classe econômica estimada
# Referência: IBGE + critério FGV Social (2023)
_FAIXAS_CLASSE = [
    (80_000, "A"),
    (40_000, "B"),
    (20_000, "C"),
    (10_000, "D"),
    (0,      "E"),
]


# ── Parser de resposta SIDRA ───────────────────────────────────────────────

def _extrair_valor_sidra(resposta: list) -> Optional[float]:
    """
    Extrai o primeiro valor numérico de uma resposta SIDRA v3.
    Estrutura: [{resultados: [{series: [{serie: {ano: valor}}]}]}]
    """
    try:
        series = resposta[0]["resultados"][0]["series"]
        if not series:
            return None
        serie = series[0]["serie"]
        # Pega o primeiro valor não-nulo
        for val in serie.values():
            if val and val not in ("-", "...", "X"):
                return float(val)
    except (IndexError, KeyError, ValueError, TypeError) as exc:
        logger.debug("IBGE parse SIDRA falhou: %s", exc)
    return None


# ── Requisição SIDRA genérica ──────────────────────────────────────────────

def _sidra_get(tabela: int, periodo: int | str, variavel: int, codigo_ibge: str) -> Optional[float]:
    """Consulta um valor específico do SIDRA por município."""
    url = (
        f"{SIDRA_BASE}/{tabela}/periodos/{periodo}/variaveis/{variavel}"
        f"?localidades=N6[{codigo_ibge}]"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return _extrair_valor_sidra(resp.json())
    except requests.RequestException as exc:
        logger.warning("IBGE SIDRA %d/%d/%d falhou: %s", tabela, variavel, periodo, exc)
    return None


# ── Informações básicas do município ──────────────────────────────────────

def _info_municipio(codigo_ibge: str) -> dict:
    """Retorna nome, UF e área do município via endpoint de localidades."""
    url = f"{LOCALIDADES_BASE}/municipios/{codigo_ibge}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return {
            "nome_municipio": data.get("nome"),
            "uf": data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("sigla"),
            "regiao": data.get("microrregiao", {}).get("mesorregiao", {}).get("UF", {}).get("regiao", {}).get("nome"),
        }
    except Exception as exc:
        logger.debug("IBGE info município falhou (%s): %s", codigo_ibge, exc)
    return {}


# ── Função Principal ──────────────────────────────────────────────────────

def buscar_dados_municipio(codigo_ibge: str) -> dict:
    """
    Retorna dados socioeconômicos do município identificado pelo código IBGE.

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município com 6 ou 7 dígitos.
        Ex: '3550308' (São Paulo) ou '355030' (6 dígitos).

    Retorna
    -------
    dict com campos:
        codigo_ibge       str
        nome_municipio    str | None
        uf                str | None
        regiao            str | None
        populacao         int | None
        pib_per_capita    float | None   (R$ anuais, preços correntes)
        classe_economica  str | None     ('A'–'E')
        fonte_populacao   str            ano de referência
        fonte_pib         str            ano de referência
    """
    # Normaliza código (6 → 7 dígitos, remove traços/espaços)
    codigo = str(codigo_ibge).strip().replace("-", "").replace(" ", "")
    if len(codigo) == 6:
        codigo = codigo + "0"  # IBGE usa 7 dígitos; o 7º é dígito verificador

    cache_key = f"ibge_municipio_{codigo}"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.debug("IBGE cache hit: %s", codigo)
        return cached

    resultado: dict = {
        "codigo_ibge":      codigo,
        "nome_municipio":   None,
        "uf":               None,
        "regiao":           None,
        "populacao":        None,
        "pib_per_capita":   None,
        "classe_economica": None,
        "fonte_populacao":  "IBGE Estimativas 2023",
        "fonte_pib":        "IBGE PIB Municipal 2021",
    }

    # 1. Informações básicas
    info = _info_municipio(codigo)
    resultado.update(info)

    # 2. Estimativa populacional 2023 (tabela 6579, var 9324)
    pop = _sidra_get(6579, 2023, 9324, codigo)
    if pop is not None:
        resultado["populacao"] = int(pop)
    else:
        # Fallback: tabela 4714 (estimativas antigas)
        pop2 = _sidra_get(4714, 2022, 93, codigo)
        if pop2 is not None:
            resultado["populacao"] = int(pop2)
            resultado["fonte_populacao"] = "IBGE Estimativas 2022"

    # 3. PIB per capita 2021 (tabela 5938, var 37)
    pib = _sidra_get(5938, 2021, 37, codigo)
    if pib is not None:
        resultado["pib_per_capita"] = round(pib, 2)
        resultado["classe_economica"] = _classificar_classe(pib)

    cache_set(cache_key, resultado, TTL_IBGE)
    logger.info(
        "IBGE: %s (%s/%s) — pop=%s, pib_pc=R$%.0f, classe=%s",
        resultado.get("nome_municipio", codigo),
        resultado.get("uf", "?"),
        resultado.get("regiao", "?"),
        resultado.get("populacao"),
        resultado.get("pib_per_capita") or 0,
        resultado.get("classe_economica", "?"),
    )
    return resultado


def _classificar_classe(pib_per_capita_anual: float) -> str:
    """
    Classifica o município por PIB per capita em faixas A–E.
    Referência aproximada ao critério de renda da FGV Social.
    """
    for limite, classe in _FAIXAS_CLASSE:
        if pib_per_capita_anual >= limite:
            return classe
    return "E"


# ── Enriquecimento do DataFrame ───────────────────────────────────────────

def enrich_ibge(
    df: pd.DataFrame,
    codigo_ibge: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    bairro: Optional[str] = None,
) -> pd.DataFrame:
    """
    Adiciona colunas IBGE ao DataFrame (colunas constantes por empresa/loja).

    As colunas adicionadas têm o mesmo valor em todas as linhas — são
    características do mercado local da empresa, úteis para contextualizar
    correlações e calibrar elasticidades.

    Colunas sempre adicionadas:
      ibge_populacao       int | None
      ibge_pib_per_capita  float | None
      ibge_classe          str | None    ('A'–'E')
      ibge_municipio       str | None
      ibge_uf              str | None

    Colunas adicionadas quando lat/lon fornecidos:
      ibge_setor_codigo    str | None    (código do setor censitário, 15 dígitos)
      ibge_renda_per_capita float | None (renda mensal domiciliar per capita, Censo 2022)
      ibge_nivel_geo       str           ('setor' | 'municipio')

    Coluna adicionada quando bairro fornecido:
      ibge_bairro          str | None

    Parâmetros
    ----------
    df          : DataFrame de vendas (saída do processor + enrich_calendario)
    codigo_ibge : código IBGE do município da empresa/loja
    lat         : latitude da loja (opcional) — habilita lookup por setor censitário
    lon         : longitude da loja (opcional)
    bairro      : nome do bairro (opcional, vem do ViaCEP via processor)

    Retorna
    -------
    DataFrame com colunas IBGE adicionadas.
    """
    if not codigo_ibge:
        logger.info("enrich_ibge: código IBGE não informado. Pulando.")
        return df

    # ── Tentativa de enriquecimento por setor censitário ────────────────────
    nivel_geo = "municipio"
    setor_dados: dict = {}

    if lat is not None and lon is not None:
        try:
            from .ibge_geo import buscar_setor_por_coordenadas
            setor_dados = buscar_setor_por_coordenadas(lat, lon, codigo_ibge)
            if setor_dados.get("classe_economica"):
                nivel_geo = "setor"
        except Exception as exc:
            logger.warning(
                "enrich_ibge: lookup por setor falhou, usando município: %s", exc
            )

    # ── Dados municipais (sempre buscados — base de fallback e metadados) ───
    dados = buscar_dados_municipio(codigo_ibge)

    df = df.copy()
    df["ibge_populacao"]      = dados.get("populacao")
    df["ibge_pib_per_capita"] = dados.get("pib_per_capita")
    df["ibge_municipio"]      = dados.get("nome_municipio")
    df["ibge_uf"]             = dados.get("uf")

    # Classificação: setor tem precedência sobre município quando disponível
    if nivel_geo == "setor":
        df["ibge_classe"]          = setor_dados["classe_economica"]
        df["ibge_setor_codigo"]    = setor_dados.get("codigo_setor")
        df["ibge_renda_per_capita"] = setor_dados.get("renda_per_capita")
        df["ibge_nivel_geo"]       = "setor"
    else:
        df["ibge_classe"]          = dados.get("classe_economica")
        df["ibge_setor_codigo"]    = None
        df["ibge_renda_per_capita"] = None
        df["ibge_nivel_geo"]       = "municipio"

    if bairro:
        df["ibge_bairro"] = bairro

    logger.info(
        "enrich_ibge: %s/%s (nível=%s, classe=%s).",
        dados.get("nome_municipio", "?"),
        dados.get("uf", "?"),
        df["ibge_nivel_geo"].iloc[0],
        df["ibge_classe"].iloc[0],
    )
    return df


# ── Utilitário: múltiplos municípios ─────────────────────────────────────

def buscar_dados_multiplos_municipios(codigos_ibge: list[str]) -> dict[str, dict]:
    """
    Busca dados para múltiplos municípios de forma eficiente.
    Útil para cenários multi-loja onde cada filial fica em cidade diferente.

    Retorna
    -------
    dict mapeando código_ibge → dados do município
    """
    resultado = {}
    codigos_unicos = list({str(c).strip() for c in codigos_ibge if c})
    for codigo in codigos_unicos:
        resultado[codigo] = buscar_dados_municipio(codigo)
    return resultado
