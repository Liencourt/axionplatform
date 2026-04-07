"""
enrichment/engine.py
---------------------
Fase 3 — Feature Engineering.

Transforma o DataFrame de vendas + variáveis externas em um conjunto
de features prontas para modelagem e correlação avançada.

Features criadas:
  Temporais:
    quantidade_lag1      Vendas do dia anterior
    quantidade_lag7      Vendas de 7 dias atrás (mesmo dia da semana anterior)
    quantidade_lag30     Vendas de 30 dias atrás
    quantidade_mm7       Média móvel de 7 dias (janela deslizante)
    quantidade_mm30      Média móvel de 30 dias

  Climáticas (se colunas disponíveis):
    temp_lag1            Temperatura máxima do dia anterior
    temp_lag7            Temperatura máxima de 7 dias atrás
    faixa_termica        Binning: frio / ameno / quente / muito_quente
    chuva_flag           Flag binária: precipitação > 2 mm

  Econômicas (calculadas):
    receita              quantidade × preco (se coluna preco disponível)
    ticket_medio         preco médio diário (se coluna preco disponível)

  Join completo:
    montar_dataset_completo()  Executa pipeline processor→calendario→clima→ibge→engine

Uso típico:
    from projects.enrichment.engine import montar_dataset_completo, criar_features

    df = montar_dataset_completo(
        df_vendas,
        codigo_ibge="3550308",
        codigo_estacao="A701",
    )
    correlacoes = calcular_correlacoes(df, coluna_alvo="quantidade")
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Binning de temperatura ────────────────────────────────────────────────────

# Limites das faixas em °C (inclusivo inferior)
_FAIXAS_TEMP = [
    (32.0, "muito_quente"),
    (26.0, "quente"),
    (18.0, "ameno"),
    (-99., "frio"),
]


def _classificar_temp(t: float) -> str:
    for limite, faixa in _FAIXAS_TEMP:
        if t >= limite:
            return faixa
    return "frio"


# ── Features de Vendas ────────────────────────────────────────────────────────

def criar_features_vendas(
    df: pd.DataFrame,
    coluna_qtd: str = "quantidade",
    coluna_data: str = "data",
) -> pd.DataFrame:
    """
    Adiciona lags e médias móveis sobre a coluna de quantidade.

    Parâmetros
    ----------
    df          : DataFrame com pelo menos coluna_qtd e coluna_data.
    coluna_qtd  : Nome da coluna de vendas (padrão: 'quantidade').
    coluna_data : Nome da coluna de data (padrão: 'data').

    Retorna
    -------
    Novo DataFrame (não modifica o original) com as colunas adicionadas.
    """
    if coluna_qtd not in df.columns:
        logger.warning("criar_features_vendas: coluna '%s' não encontrada.", coluna_qtd)
        return df

    df = df.copy()

    # Ordena por data para garantir ordem temporal correta
    if coluna_data in df.columns:
        df = df.sort_values(coluna_data).reset_index(drop=True)

    qtd = df[coluna_qtd]

    df[f"{coluna_qtd}_lag1"]  = qtd.shift(1)
    df[f"{coluna_qtd}_lag7"]  = qtd.shift(7)
    df[f"{coluna_qtd}_lag30"] = qtd.shift(30)
    df[f"{coluna_qtd}_mm7"]   = qtd.rolling(7,  min_periods=3).mean()
    df[f"{coluna_qtd}_mm30"]  = qtd.rolling(30, min_periods=10).mean()

    logger.debug(
        "criar_features_vendas: lags (1/7/30) e médias móveis (7/30) adicionados a '%s'.",
        coluna_qtd,
    )
    return df


# ── Features Climáticas ───────────────────────────────────────────────────────

def criar_features_clima(
    df: pd.DataFrame,
    coluna_temp_max: str = "temp_max",
    coluna_precipitacao: str = "precipitacao",
    limiar_chuva_mm: float = 2.0,
) -> pd.DataFrame:
    """
    Adiciona features derivadas das variáveis climáticas.

    Features adicionadas:
      temp_lag1       Temperatura máxima do dia anterior
      temp_lag7       Temperatura máxima de 7 dias atrás
      faixa_termica   Categórica: frio / ameno / quente / muito_quente
      chuva_flag      1 se precipitação > limiar, else 0

    Parâmetros
    ----------
    df                 : DataFrame com colunas climáticas (vindo de enrich_clima).
    coluna_temp_max    : Nome da coluna de temperatura máxima.
    coluna_precipitacao: Nome da coluna de precipitação diária.
    limiar_chuva_mm    : Threshold de chuva para flag binária (padrão: 2 mm).
    """
    df = df.copy()

    if coluna_temp_max in df.columns:
        temp = df[coluna_temp_max]
        df["temp_lag1"] = temp.shift(1)
        df["temp_lag7"] = temp.shift(7)

        # Binning de temperatura (ignora NaN)
        df["faixa_termica"] = np.where(
            temp.isna(),
            None,
            temp.map(_classificar_temp),
        )
        logger.debug("criar_features_clima: temp_lag1/7 e faixa_termica criados.")
    else:
        logger.debug(
            "criar_features_clima: coluna '%s' ausente — features de temperatura ignoradas.",
            coluna_temp_max,
        )

    if coluna_precipitacao in df.columns:
        df["chuva_flag"] = (df[coluna_precipitacao].fillna(0) > limiar_chuva_mm).astype(int)
        logger.debug("criar_features_clima: chuva_flag criado (limiar=%.1f mm).", limiar_chuva_mm)

    return df


# ── Features Econômicas ───────────────────────────────────────────────────────

def criar_features_economicas(
    df: pd.DataFrame,
    coluna_qtd: str = "quantidade",
    coluna_preco: str = "preco",
    coluna_data: str = "data",
) -> pd.DataFrame:
    """
    Adiciona features de receita e ticket médio diário.

    Features adicionadas (apenas se ambas as colunas existirem):
      receita      quantidade × preco por linha
      ticket_medio preco médio no dia (via groupby por data, reatribuído ao df)
    """
    df = df.copy()

    if coluna_qtd in df.columns and coluna_preco in df.columns:
        df["receita"] = df[coluna_qtd] * df[coluna_preco]

        if coluna_data in df.columns:
            ticket_dia = (
                df.groupby(coluna_data)[coluna_preco]
                .mean()
                .rename("ticket_medio")
                .reset_index()
            )
            df = df.merge(ticket_dia, on=coluna_data, how="left")
            logger.debug("criar_features_economicas: receita e ticket_medio adicionados.")

    return df


# ── Pipeline Completo ─────────────────────────────────────────────────────────

def criar_features(
    df: pd.DataFrame,
    coluna_qtd: str = "quantidade",
    coluna_data: str = "data",
    coluna_temp_max: str = "temp_max",
    coluna_precipitacao: str = "precipitacao",
    coluna_preco: str = "preco",
    limiar_chuva_mm: float = 2.0,
) -> pd.DataFrame:
    """
    Aplica todos os transformadores de features em sequência:
    vendas → clima → econômicas.

    É idempotente: colunas que já existem não são duplicadas.

    Parâmetros
    ----------
    df                : DataFrame enriquecido (processor + calendario + clima + ibge).
    coluna_qtd        : Coluna de quantidade de vendas.
    coluna_data       : Coluna de data.
    coluna_temp_max   : Coluna de temperatura máxima.
    coluna_precipitacao: Coluna de precipitação.
    coluna_preco      : Coluna de preço.
    limiar_chuva_mm   : Limiar para flag de chuva.

    Retorna
    -------
    DataFrame com todas as features adicionadas.
    """
    df = criar_features_vendas(df, coluna_qtd=coluna_qtd, coluna_data=coluna_data)
    df = criar_features_clima(df, coluna_temp_max=coluna_temp_max,
                              coluna_precipitacao=coluna_precipitacao,
                              limiar_chuva_mm=limiar_chuva_mm)
    df = criar_features_economicas(df, coluna_qtd=coluna_qtd,
                                   coluna_preco=coluna_preco,
                                   coluna_data=coluna_data)
    return df


def montar_dataset_completo(
    df_vendas: pd.DataFrame,
    codigo_ibge: Optional[str] = None,
    codigo_estacao: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    bairro: Optional[str] = None,
    coluna_data: str = "data",
    coluna_qtd: str = "quantidade",
    coluna_preco: str = "preco",
) -> pd.DataFrame:
    """
    Pipeline completo de enriquecimento + feature engineering.

    Etapas executadas em ordem:
      1. calendario  — feriados, sazonalidade, dias até eventos
      2. clima       — dados INMET (se codigo_estacao informado)
      3. ibge        — dados socioeconômicos por município; quando lat/lon fornecidos,
                       tenta classificação por setor censitário (mais precisa)
      4. features    — lags, médias móveis, binning, receita

    Parâmetros
    ----------
    df_vendas        : DataFrame já validado pelo processor.py.
    codigo_ibge      : Código IBGE do município (7 dígitos). Opcional.
    codigo_estacao   : Código da estação INMET (ex: 'A701'). Opcional.
    lat              : Latitude da loja/empresa. Opcional.
                       Quando fornecido com lon, habilita classificação por setor
                       censitário (bairro-level) em vez de município inteiro.
    lon              : Longitude da loja/empresa. Opcional.
    bairro           : Nome do bairro (do ViaCEP ou cadastro). Opcional.
    coluna_data      : Nome da coluna de data no DataFrame.
    coluna_qtd       : Nome da coluna de quantidade.
    coluna_preco     : Nome da coluna de preço.

    Retorna
    -------
    DataFrame enriquecido e com features prontas para modelagem.

    Notas
    -----
    - Todas as etapas de APIs externas têm graceful degradation:
      uma falha não interrompe o pipeline.
    - O DataFrame original não é modificado.
    """
    from .calendario import enrich_calendario
    from .inmet import enrich_clima
    from .ibge import enrich_ibge

    df = df_vendas.copy()

    # 1. Calendário
    try:
        df = enrich_calendario(df, coluna_data=coluna_data)
        logger.info("montar_dataset_completo: calendário adicionado.")
    except Exception as exc:
        logger.warning("montar_dataset_completo: calendário falhou: %s", exc)

    # 2. Clima (opcional)
    if codigo_estacao:
        try:
            if coluna_data in df.columns:
                datas = pd.to_datetime(df[coluna_data])
                data_inicio = datas.min().date()
                data_fim = datas.max().date()
                df = enrich_clima(
                    df,
                    codigo_estacao=codigo_estacao,
                    coluna_data=coluna_data,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                )
            logger.info("montar_dataset_completo: clima adicionado (estação %s).", codigo_estacao)
        except Exception as exc:
            logger.warning("montar_dataset_completo: clima falhou: %s", exc)

    # 3. IBGE (opcional) — lat/lon habilitam classificação por setor censitário
    if codigo_ibge:
        try:
            df = enrich_ibge(df, codigo_ibge=codigo_ibge, lat=lat, lon=lon, bairro=bairro)
            logger.info(
                "montar_dataset_completo: IBGE adicionado (município %s, nível=%s).",
                codigo_ibge,
                df["ibge_nivel_geo"].iloc[0] if "ibge_nivel_geo" in df.columns else "municipio",
            )
        except Exception as exc:
            logger.warning("montar_dataset_completo: IBGE falhou: %s", exc)

    # 4. Feature engineering
    try:
        df = criar_features(
            df,
            coluna_qtd=coluna_qtd,
            coluna_data=coluna_data,
            coluna_preco=coluna_preco,
        )
        logger.info("montar_dataset_completo: feature engineering concluído.")
    except Exception as exc:
        logger.warning("montar_dataset_completo: feature engineering falhou: %s", exc)

    return df
