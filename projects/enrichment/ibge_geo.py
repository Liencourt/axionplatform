"""
enrichment/ibge_geo.py
-----------------------
Enriquecimento IBGE no nível de Setor Censitário (N10) — mais granular que o município.

Resolve a limitação do enriquecimento por município: cidades como Rio de Janeiro têm PIB
per capita alto como um todo, mas bairros como Zona Oeste têm perfil socioeconômico
completamente diferente.

Fluxo:
  1. buscar_malha_municipio(codigo_ibge)
       → GET IBGE Malha API: GeoJSON com polígonos dos setores censitários do município
       → Cacheado 90 dias (GCS L2 + memória L1)

  2. encontrar_setor(lat, lon, geojson)
       → Ponto-em-polígono com shapely
       → Retorna código do setor censitário (15 dígitos) ou None

  3. buscar_dados_setor(codigo_setor)
       → GET IBGE SIDRA: renda domiciliar per capita do Censo 2022 para o setor
       → Retorna classe econômica (A–E) usando os mesmos critérios de ibge.py
       → Cacheado 30 dias

  4. buscar_setor_por_coordenadas(lat, lon, codigo_ibge) — função pública principal
       → Orquestra 1→2→3 com graceful degradation em cada etapa

Dependência externa: shapely
  - Verificada com guard de import. Se ausente, nível geo="nenhum" e sem exceção.
  - Instalar: shapely==2.1.0 (wheel com GEOS embutido, compatível com Cloud Run)

IMPORTANTE sobre coordenadas GeoJSON:
  GeoJSON (RFC 7946) usa [longitude, latitude]. Shapely também.
  Sempre construir Point(lon, lat) — não Point(lat, lon).
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

from ._cache import cache_get, cache_set
from .ibge import _classificar_classe, _extrair_valor_sidra

logger = logging.getLogger(__name__)

# ── Shapely — import com guard ─────────────────────────────────────────────

try:
    from shapely.geometry import shape, Point
    _SHAPELY_DISPONIVEL = True
except ImportError:  # pragma: no cover
    _SHAPELY_DISPONIVEL = False
    logger.warning(
        "ibge_geo: shapely não instalado — enriquecimento por setor censitário desabilitado. "
        "Instale com: pip install shapely==2.1.0"
    )

# ── Constantes ─────────────────────────────────────────────────────────────

MALHA_BASE = "https://servicodados.ibge.gov.br/api/v3/malhas/municipios"
SIDRA_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"
HEADERS    = {"User-Agent": "AxiomPlatform/1.0 (suporte@axiomplatform.com.br)"}
TIMEOUT    = 20   # GeoJSON pode ser grande (SP ~8 MB)

TTL_MALHA = 90 * 24 * 3600  # 90 dias — fronteiras mudam no ciclo censitário
TTL_SETOR = 30 * 24 * 3600  # 30 dias — dados de renda estáveis

# Censo 2022 — rendimento domiciliar per capita médio por setor censitário
# Tabela 9921, variável 10605 — "Valor do rendimento nominal médio mensal per capita
# das pessoas em domicílios particulares permanentes" (Censo 2022, N10)
_TABELA_CENSO_RENDA  = 9921
_VAR_CENSO_RENDA     = 10605
_PERIODO_CENSO       = 2022


# ── Busca da Malha de Setores ──────────────────────────────────────────────

def buscar_malha_municipio(codigo_ibge: str) -> Optional[dict]:
    """
    Retorna o GeoJSON FeatureCollection com os polígonos dos setores censitários
    do município indicado.

    Cada Feature contém:
      - geometry: polígono (ou multipolígono) do setor
      - properties.codarea: código do setor com 15 dígitos

    Parâmetros
    ----------
    codigo_ibge : str
        Código IBGE do município (6 ou 7 dígitos).

    Retorna
    -------
    dict (GeoJSON) ou None em caso de falha.
    """
    codigo = str(codigo_ibge).strip()
    cache_key = f"ibge_malha_{codigo}"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.debug("ibge_geo: malha cache hit (%s)", codigo)
        return cached

    # resolucao=5 → polígonos simplificados (menor payload, suficiente para P-in-P)
    url = f"{MALHA_BASE}/{codigo}/setores?formato=application/json&resolucao=5"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        geojson = resp.json()
        cache_set(cache_key, geojson, TTL_MALHA)
        n_features = len(geojson.get("features", []))
        logger.info("ibge_geo: malha %s carregada — %d setores", codigo, n_features)
        return geojson
    except requests.RequestException as exc:
        logger.warning("ibge_geo: falha ao buscar malha %s: %s", codigo, exc)
    return None


# ── Ponto-em-Polígono ──────────────────────────────────────────────────────

def encontrar_setor(lat: float, lon: float, geojson: dict) -> Optional[str]:
    """
    Encontra o setor censitário que contém o ponto (lat, lon).

    ATENÇÃO: GeoJSON usa [longitude, latitude] — portanto construímos Point(lon, lat).

    Parâmetros
    ----------
    lat     : Latitude da loja/empresa.
    lon     : Longitude da loja/empresa.
    geojson : FeatureCollection retornado por buscar_malha_municipio().

    Retorna
    -------
    str com código do setor (15 dígitos) ou None se não encontrado.
    """
    if not _SHAPELY_DISPONIVEL:
        return None

    features = geojson.get("features", [])
    if not features:
        return None

    # GeoJSON: coordenadas são [longitude, latitude]
    ponto = Point(lon, lat)

    for feature in features:
        try:
            poligono = shape(feature["geometry"])
            if poligono.contains(ponto):
                return feature.get("properties", {}).get("codarea")
        except Exception as exc:
            logger.debug("ibge_geo: geometria inválida ignorada: %s", exc)
            continue

    return None


# ── Dados do Setor via SIDRA ───────────────────────────────────────────────

def buscar_dados_setor(codigo_setor: str) -> dict:
    """
    Busca renda domiciliar per capita do Censo 2022 para um setor censitário.

    Usa IBGE SIDRA nível N10 (setor censitário). Muitos setores rurais ou pequenos
    têm dados suprimidos — neste caso retorna dict com None nos campos de renda.

    Parâmetros
    ----------
    codigo_setor : str
        Código do setor censitário com 15 dígitos.

    Retorna
    -------
    dict com campos:
        codigo_setor       str
        renda_per_capita   float | None   (R$ mensais, Censo 2022)
        classe_economica   str | None     ('A'–'E')
        fonte              str
    """
    cache_key = f"ibge_setor_{codigo_setor}"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.debug("ibge_geo: setor cache hit (%s)", codigo_setor)
        return cached

    resultado: dict = {
        "codigo_setor":    codigo_setor,
        "renda_per_capita": None,
        "classe_economica": None,
        "fonte":            f"IBGE Censo {_PERIODO_CENSO}",
    }

    url = (
        f"{SIDRA_BASE}/{_TABELA_CENSO_RENDA}"
        f"/periodos/{_PERIODO_CENSO}"
        f"/variaveis/{_VAR_CENSO_RENDA}"
        f"?localidades=N10[{codigo_setor}]"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        renda_mensal = _extrair_valor_sidra(resp.json())
        if renda_mensal is not None:
            resultado["renda_per_capita"] = round(renda_mensal, 2)
            # Converte renda mensal → anual para usar a mesma escala de _classificar_classe
            resultado["classe_economica"] = _classificar_classe(renda_mensal * 12)
    except requests.RequestException as exc:
        logger.warning("ibge_geo: falha ao buscar setor %s: %s", codigo_setor, exc)

    cache_set(cache_key, resultado, TTL_SETOR)
    logger.info(
        "ibge_geo: setor %s — renda_pc=R$%.0f/mês, classe=%s",
        codigo_setor,
        resultado.get("renda_per_capita") or 0,
        resultado.get("classe_economica", "?"),
    )
    return resultado


# ── Função Principal ──────────────────────────────────────────────────────

def buscar_setor_por_coordenadas(
    lat: float,
    lon: float,
    codigo_ibge: str,
) -> dict:
    """
    Orquestra a lookup completa: coordenadas → setor censitário → dados de renda.

    Se qualquer etapa falhar (malha indisponível, shapely ausente, ponto fora dos
    polígonos, SIDRA sem dados), retorna dict com nivel_geo="nenhum" — sem exceção.

    Parâmetros
    ----------
    lat         : Latitude da loja/empresa.
    lon         : Longitude da loja/empresa.
    codigo_ibge : Código IBGE do município.

    Retorna
    -------
    dict com campos:
        nivel_geo          str     "setor" | "nenhum"
        codigo_setor       str | None
        renda_per_capita   float | None
        classe_economica   str | None
    """
    _fallback = {
        "nivel_geo":        "nenhum",
        "codigo_setor":     None,
        "renda_per_capita": None,
        "classe_economica": None,
    }

    if not _SHAPELY_DISPONIVEL:
        return _fallback

    if lat is None or lon is None or not codigo_ibge:
        return _fallback

    # 1. Malha de setores do município
    geojson = buscar_malha_municipio(codigo_ibge)
    if geojson is None:
        return _fallback

    # 2. Ponto-em-polígono
    codigo_setor = encontrar_setor(lat, lon, geojson)
    if not codigo_setor:
        logger.info(
            "ibge_geo: ponto (%.5f, %.5f) não encontrado em nenhum setor do município %s",
            lat, lon, codigo_ibge,
        )
        return _fallback

    # 3. Dados de renda do setor
    dados = buscar_dados_setor(codigo_setor)

    return {
        "nivel_geo":        "setor",
        "codigo_setor":     codigo_setor,
        "renda_per_capita": dados.get("renda_per_capita"),
        "classe_economica": dados.get("classe_economica"),
    }
