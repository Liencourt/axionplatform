"""
enrichment/inmet.py
-------------------
Integração com a API do INMET (Instituto Nacional de Meteorologia).

Funcionalidades:
  1. Listar estações automáticas e encontrar a mais próxima de uma coordenada
  2. Buscar dados climáticos diários por estação e período
  3. Enriquecer DataFrame de vendas com variáveis de clima

Endpoints INMET:
  GET https://apitempo.inmet.gov.br/estacoes/T           → lista estações automáticas
  GET https://apitempo.inmet.gov.br/estacao/dados/{ini}/{fim}/{codigo} → dados horários

Variáveis produzidas (coluna no DataFrame):
  temp_max      float  temperatura máxima do dia (°C)
  temp_min      float  temperatura mínima do dia (°C)
  temp_media    float  temperatura média do dia (°C)
  precipitacao  float  precipitação acumulada (mm)
  umidade_media float  umidade relativa média (%)
  vento_max     float  rajada máxima de vento (m/s)

Regras de negócio:
  - Se INMET falhar, retorna DataFrame original sem crash (Graceful Degradation)
  - Dados climáticos cacheados por 24h (cache/_cache.py)
  - Lista de estações cacheada por 7 dias
  - Mínimo de 15 dias com dados para incluir o clima (evita correlações espúrias)

Cloud Run:
  - Sem dependências de disco; cache em memória L1 + GCS L2
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests

from ._cache import cache_get, cache_set

logger = logging.getLogger(__name__)

# ── Constantes ─────────────────────────────────────────────────────────────

INMET_BASE = "https://apitempo.inmet.gov.br"
HEADERS = {"User-Agent": "AxiomPlatform/1.0 (suporte@axiomplatform.com.br)"}
TIMEOUT = 15  # segundos

TTL_ESTACOES = 7 * 24 * 3600   # 7 dias
TTL_DADOS    = 24 * 3600        # 24 horas

# Mapeamento de campos INMET → nomes canônicos (a API mudou nomes ao longo do tempo)
_CAMPOS_TEMP_MAX  = ("TEM_MAX", "TEMP_MAX", "Tmax")
_CAMPOS_TEMP_MIN  = ("TEM_MIN", "TEMP_MIN", "Tmin")
_CAMPOS_TEMP_INS  = ("TEM_INS", "TEMP_INS", "Tins")
_CAMPOS_CHUVA     = ("CHUVA", "PRECIPITACAO_TOTAL", "Chuva")
_CAMPOS_UMIDADE   = ("UMD_INS", "UMID_INS", "Uins")
_CAMPOS_VENTO_RAJ = ("VEN_RAJ", "VENTO_RAJADA_MAXIMA", "Vmax")


# ── Helpers matemáticos ────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância Haversine em km entre dois pontos geográficos."""
    R = 6371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _campo(registro: dict, candidatos: tuple[str, ...]) -> Optional[float]:
    """Tenta extrair um campo numérico de um registro, testando múltiplos nomes."""
    for nome in candidatos:
        val = registro.get(nome)
        if val is not None and str(val).strip() not in ("", "null", "None", "-9999"):
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
    return None


# ── Estações INMET ─────────────────────────────────────────────────────────

def listar_estacoes(tipo: str = "T") -> list[dict]:
    """
    Retorna lista de estações INMET do tipo especificado.
    tipo="T" → automáticas (maior cobertura temporal)
    tipo="M" → convencionais (leituras manuais)
    Cacheado por 7 dias.
    """
    cache_key = f"inmet_estacoes_{tipo}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"{INMET_BASE}/estacoes/{tipo}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        estacoes = resp.json()
        if isinstance(estacoes, list) and estacoes:
            cache_set(cache_key, estacoes, TTL_ESTACOES)
            logger.info("INMET: %d estações carregadas (tipo=%s)", len(estacoes), tipo)
            return estacoes
    except requests.RequestException as exc:
        logger.warning("INMET listar_estacoes falhou: %s", exc)
    return []


def buscar_estacao_mais_proxima(
    lat: float,
    lon: float,
    raio_max_km: float = 150.0,
) -> Optional[dict]:
    """
    Retorna a estação automática INMET mais próxima de (lat, lon).

    Parâmetros
    ----------
    lat, lon      : coordenadas do ponto de interesse
    raio_max_km   : descarta estações mais distantes que este raio (padrão 150 km)

    Retorna
    -------
    dict com campos CD_ESTACAO, DC_NOME, SG_ESTADO, distancia_km, lat, lon
    ou None se nenhuma estação estiver dentro do raio.
    """
    estacoes = listar_estacoes("T")
    if not estacoes:
        return None

    melhor: Optional[dict] = None
    melhor_dist = float("inf")

    for est in estacoes:
        # Ignora estações desativadas
        if est.get("CD_SITUACAO", "").lower() not in ("operante", ""):
            continue

        try:
            e_lat = float(est.get("VL_LATITUDE") or est.get("latitude") or 0)
            e_lon = float(est.get("VL_LONGITUDE") or est.get("longitude") or 0)
        except (ValueError, TypeError):
            continue

        dist = _haversine(lat, lon, e_lat, e_lon)
        if dist < melhor_dist:
            melhor_dist = dist
            melhor = {**est, "distancia_km": round(dist, 1), "lat": e_lat, "lon": e_lon}

    if melhor and melhor_dist <= raio_max_km:
        logger.info(
            "INMET: estação mais próxima = %s (%s) — %.1f km",
            melhor.get("CD_ESTACAO"), melhor.get("DC_NOME"), melhor_dist,
        )
        return melhor

    logger.warning(
        "INMET: nenhuma estação dentro de %.0f km de (%.4f, %.4f)", raio_max_km, lat, lon
    )
    return None


# ── Dados Climáticos ──────────────────────────────────────────────────────

def buscar_dados_climaticos(
    codigo_estacao: str,
    data_inicio: date,
    data_fim: date,
) -> pd.DataFrame:
    """
    Busca dados climáticos diários do INMET para a estação e período dados.

    Retorna DataFrame indexado por 'data' com colunas:
      temp_max, temp_min, temp_media, precipitacao, umidade_media, vento_max

    Retorna DataFrame vazio em caso de erro (Graceful Degradation).
    """
    cache_key = f"inmet_{codigo_estacao}_{data_inicio}_{data_fim}"
    cached = cache_get(cache_key)
    if cached is not None:
        return pd.DataFrame(cached)

    url = (
        f"{INMET_BASE}/estacao/dados"
        f"/{data_inicio.strftime('%Y-%m-%d')}"
        f"/{data_fim.strftime('%Y-%m-%d')}"
        f"/{codigo_estacao}"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

        # 204 No Content = sem dados para o período
        if resp.status_code == 204:
            logger.warning("INMET: sem dados para %s no período %s–%s", codigo_estacao, data_inicio, data_fim)
            return pd.DataFrame()

        resp.raise_for_status()
        registros = resp.json()

        if not isinstance(registros, list) or not registros:
            return pd.DataFrame()

        df = _agregar_para_diario(registros)

        if not df.empty:
            cache_set(cache_key, df.to_dict("records"), TTL_DADOS)
            logger.info(
                "INMET: %d dias de clima para %s (%s–%s)",
                len(df), codigo_estacao, data_inicio, data_fim,
            )

        return df

    except requests.RequestException as exc:
        logger.warning("INMET buscar_dados_climaticos falhou (%s): %s", codigo_estacao, exc)
        return pd.DataFrame()
    except Exception as exc:
        logger.warning("INMET parse error (%s): %s", codigo_estacao, exc)
        return pd.DataFrame()


def _agregar_para_diario(registros: list[dict]) -> pd.DataFrame:
    """
    Agrega registros horários/sub-diários para uma linha por dia.
    INMET pode retornar múltiplas leituras por dia dependendo da estação.
    """
    rows = []
    for r in registros:
        dt_str = r.get("DT_MEDICAO") or r.get("DT_REGISTRO") or r.get("data")
        if not dt_str:
            continue
        try:
            dt = pd.to_datetime(str(dt_str).split("T")[0]).date()
        except Exception:
            continue

        rows.append({
            "data":         dt,
            "temp_max_raw": _campo(r, _CAMPOS_TEMP_MAX),
            "temp_min_raw": _campo(r, _CAMPOS_TEMP_MIN),
            "temp_ins_raw": _campo(r, _CAMPOS_TEMP_INS),
            "chuva_raw":    _campo(r, _CAMPOS_CHUVA),
            "umidade_raw":  _campo(r, _CAMPOS_UMIDADE),
            "vento_raj_raw":_campo(r, _CAMPOS_VENTO_RAJ),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["data"] = pd.to_datetime(df["data"])

    # Agrega por dia: max/min/sum/mean
    diario = (
        df.groupby("data")
        .agg(
            temp_max     =("temp_max_raw",  "max"),
            temp_min     =("temp_min_raw",  "min"),
            temp_media   =("temp_ins_raw",  "mean"),
            precipitacao =("chuva_raw",     "sum"),
            umidade_media=("umidade_raw",   "mean"),
            vento_max    =("vento_raj_raw", "max"),
        )
        .reset_index()
    )

    # Remove dias em que temp_max é nulo (leitura inválida)
    diario = diario[diario["temp_max"].notna()].copy()

    # Arredonda para 1 casa decimal
    for col in ("temp_max", "temp_min", "temp_media", "precipitacao", "umidade_media", "vento_max"):
        diario[col] = diario[col].round(1)

    return diario


# ── Enriquecimento do DataFrame ───────────────────────────────────────────

def enrich_clima(
    df: pd.DataFrame,
    codigo_estacao: str,
    coluna_data: str = "data",
    min_dias_cobertura: int = 15,
) -> pd.DataFrame:
    """
    Enriquece DataFrame de vendas com variáveis climáticas do INMET.

    Parâmetros
    ----------
    df                  : DataFrame com coluna de datas
    codigo_estacao      : código INMET (ex: 'A701')
    coluna_data         : nome da coluna de datas (padrão: 'data')
    min_dias_cobertura  : mínimo de dias com clima para incluir no enriquecimento.
                          Se < min_dias, retorna df sem colunas de clima.

    Retorna
    -------
    DataFrame com colunas de clima adicionadas (ou original se falhar).
    """
    if coluna_data not in df.columns:
        logger.warning("enrich_clima: coluna '%s' não encontrada.", coluna_data)
        return df

    datas = pd.to_datetime(df[coluna_data]).dropna()
    if datas.empty:
        return df

    data_inicio = datas.min().date()
    data_fim    = datas.max().date()

    # Divide em janelas de 365 dias para não exceder limites da API
    df_clima_parts = []
    cursor = data_inicio
    while cursor <= data_fim:
        fim_janela = min(cursor + timedelta(days=364), data_fim)
        parte = buscar_dados_climaticos(codigo_estacao, cursor, fim_janela)
        if not parte.empty:
            df_clima_parts.append(parte)
        cursor = fim_janela + timedelta(days=1)

    if not df_clima_parts:
        logger.info("enrich_clima: sem dados do INMET para %s. Retornando df original.", codigo_estacao)
        return df

    df_clima = pd.concat(df_clima_parts, ignore_index=True).drop_duplicates(subset=["data"])

    # Verifica cobertura mínima
    datas_vendas = pd.to_datetime(df[coluna_data]).dt.normalize()
    cobertura = datas_vendas.isin(df_clima["data"]).sum()
    if cobertura < min_dias_cobertura:
        logger.warning(
            "enrich_clima: cobertura insuficiente (%d/%d dias). Clima não adicionado.",
            cobertura, len(datas_vendas),
        )
        return df

    df = df.copy()
    df["_data_join"] = pd.to_datetime(df[coluna_data]).dt.normalize()
    df_clima["data"] = pd.to_datetime(df_clima["data"])

    df = df.merge(
        df_clima.rename(columns={"data": "_data_join"}),
        on="_data_join",
        how="left",
    ).drop(columns=["_data_join"])

    logger.info(
        "enrich_clima: %d/%d registros enriquecidos com clima (estação %s).",
        cobertura, len(df), codigo_estacao,
    )
    return df
