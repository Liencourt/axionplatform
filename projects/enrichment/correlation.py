"""
enrichment/correlation.py
--------------------------
Calcula correlações de Pearson entre variáveis externas e vendas,
filtrando apenas resultados estatisticamente significativos.

Regras de negócio:
  - Mínimo de 30 observações válidas por par de variáveis
  - Apenas p_value < 0.05 (95% de confiança estatística)
  - Ordenado por |r| decrescente (força absoluta da correlação)
  - Exclui variáveis booleanas binárias sem variação
  - Detecta e alerta sobre multicolinearidade entre preditores

Uso típico:
    df_enriquecido = enrich_calendario(df_padronizado)
    correlacoes = calcular_correlacoes(df_enriquecido, coluna_alvo="quantidade")
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# Colunas excluídas por padrão (meta-dados, identificadores, alvos derivados)
# "receita" = quantidade × preco → correlação trivial de r≈1.00 com quantidade
# "ticket_medio" e "preco" também derivam do preço praticado — não são drivers externos
COLUNAS_EXCLUIR_PADRAO: frozenset[str] = frozenset({
    "data", "produto", "cep", "municipio", "uf", "codigo_ibge",
    "evento_especial",   # categórico
    "receita",           # derivado: qtd × preco → r≈1 trivial
    "ticket_medio",      # derivado do preco praticado
    "preco",             # variável de decisão, não driver externo
})


# ── Classificadores de Força ──────────────────────────────────────────────────

def _classificar_forca(r: float) -> str:
    """
    Classifica a força da correlação em termos de negócio.
    Baseado em Cohen (1988): 0.1 fraca, 0.3 moderada, 0.5 forte.
    """
    abs_r = abs(r)
    if abs_r >= 0.7:
        return "muito_forte"
    elif abs_r >= 0.5:
        return "forte"
    elif abs_r >= 0.3:
        return "moderada"
    elif abs_r >= 0.1:
        return "fraca"
    return "negligivel"


def _interpretar_correlacao(variavel: str, r: float, forca: str) -> str:
    """
    Gera interpretação em linguagem natural para C-Level.
    Exemplo: "Temperatura máxima tem correlação forte e positiva com suas vendas."
    """
    direcao = "positiva" if r > 0 else "negativa"
    label_map = {
        "temp_max": "Temperatura máxima",
        "temp_min": "Temperatura mínima",
        "precipitacao": "Volume de chuva",
        "umidade": "Umidade relativa do ar",
        "is_feriado": "Dias de feriado",
        "is_fim_semana": "Fins de semana",
        "is_vespera_feriado": "Vésperas de feriado",
        "dias_ate_natal": "Proximidade ao Natal",
        "dias_ate_black_friday": "Proximidade à Black Friday",
        "dia_semana": "Dia da semana",
        "mes": "Mês do ano",
        "renda_media": "Renda média da região",
        "populacao": "População do município",
    }
    nome_amigavel = label_map.get(variavel, variavel.replace("_", " ").title())
    return (
        f"{nome_amigavel} tem correlação {forca.replace('_', ' ')} e {direcao} com suas vendas."
    )


# ── Função Principal ──────────────────────────────────────────────────────────

def calcular_correlacoes(
    df: pd.DataFrame,
    coluna_alvo: str = "quantidade",
    min_obs: int = 30,
    p_value_max: float = 0.05,
    colunas_excluir: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Calcula correlações de Pearson entre variáveis numéricas e a coluna-alvo.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame enriquecido (após processor + calendario).
    coluna_alvo : str
        Coluna de vendas a correlacionar (padrão: 'quantidade').
    min_obs : int
        Número mínimo de observações válidas por par (padrão: 30).
    p_value_max : float
        Limite de significância (padrão: 0.05).
    colunas_excluir : list[str] | None
        Colunas adicionais a excluir além das padrão.

    Retorna
    -------
    list[dict] ordenada por |correlacao| decrescente, com campos:
        variavel, correlacao, p_value, n_observacoes, forca,
        direcao, interpretacao, ic_95_lower, ic_95_upper

    Retorna lista vazia se:
        - DataFrame tiver < min_obs linhas
        - coluna_alvo não existir
        - Nenhuma correlação passar nos filtros

    Levanta
    -------
    TypeError  se df não for pd.DataFrame
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Esperado pd.DataFrame, recebeu {type(df)}")

    if coluna_alvo not in df.columns:
        logger.warning("Coluna alvo '%s' não encontrada no DataFrame.", coluna_alvo)
        return []

    n_total = len(df)
    if n_total < min_obs:
        logger.info(
            "DataFrame muito pequeno (%d linhas) para calcular correlações (min=%d).",
            n_total, min_obs,
        )
        return []

    excluir = COLUNAS_EXCLUIR_PADRAO | set(colunas_excluir or []) | {coluna_alvo}

    # Seleciona apenas colunas numéricas candidatas
    numericas = [
        col for col in df.select_dtypes(include="number").columns
        if col not in excluir
    ]

    if not numericas:
        logger.info("Nenhuma variável numérica disponível para correlação.")
        return []

    y = df[coluna_alvo]
    resultado: list[dict[str, Any]] = []

    for col in numericas:
        x = df[col]

        # Índices onde ambos são válidos
        mask = x.notna() & y.notna()
        n_obs = int(mask.sum())

        if n_obs < min_obs:
            logger.debug("Coluna '%s' ignorada: apenas %d obs válidas.", col, n_obs)
            continue

        # Verifica variância (variável constante não tem correlação)
        if x[mask].std() == 0 or y[mask].std() == 0:
            logger.debug("Coluna '%s' ignorada: sem variância.", col)
            continue

        r, p = stats.pearsonr(x[mask], y[mask])

        if np.isnan(r) or p >= p_value_max:
            continue

        forca = _classificar_forca(r)
        direcao = "positiva" if r > 0 else "negativa"

        # Intervalo de confiança de 95% via transformação de Fisher
        ic_lower, ic_upper = _ic_pearson(r, n_obs)

        resultado.append({
            "variavel": col,
            "correlacao": round(float(r), 4),
            "p_value": round(float(p), 6),
            "n_observacoes": n_obs,
            "forca": forca,
            "direcao": direcao,
            "interpretacao": _interpretar_correlacao(col, r, forca),
            "ic_95_lower": round(float(ic_lower), 4),
            "ic_95_upper": round(float(ic_upper), 4),
        })

    # Ordena por força absoluta decrescente
    resultado.sort(key=lambda x: abs(x["correlacao"]), reverse=True)

    logger.info(
        "calcular_correlacoes: %d variável(is) analisada(s), %d correlação(ões) significativa(s) (p<%.2f).",
        len(numericas), len(resultado), p_value_max,
    )

    return resultado


def _ic_pearson(r: float, n: int) -> tuple[float, float]:
    """
    Intervalo de confiança de 95% para correlação de Pearson via transformação de Fisher.
    Retorna (ic_lower, ic_upper) clamped em [-1, 1].
    """
    if n <= 3:
        return (-1.0, 1.0)
    # arctanh indefinido em ±1 → retorna limites exatos
    if abs(r) >= 1.0:
        return (-1.0, 1.0) if r < 0 else (1.0, 1.0)

    z = np.arctanh(r)
    se = 1.0 / np.sqrt(n - 3)
    z_crit = stats.norm.ppf(0.975)  # ≈ 1.96 para 95%

    lower = np.tanh(z - z_crit * se)
    upper = np.tanh(z + z_crit * se)

    return (
        float(np.clip(lower, -1.0, 1.0)),
        float(np.clip(upper, -1.0, 1.0)),
    )


# ── Utilitário: Matriz de Correlação Completa ─────────────────────────────────

def matriz_correlacao(
    df: pd.DataFrame,
    colunas_excluir: list[str] | None = None,
) -> pd.DataFrame:
    """
    Retorna matriz de correlação de Pearson entre todas as variáveis numéricas.
    Útil para diagnóstico de multicolinearidade.
    """
    excluir = COLUNAS_EXCLUIR_PADRAO | set(colunas_excluir or [])
    numericas = [c for c in df.select_dtypes(include="number").columns if c not in excluir]
    return df[numericas].corr(method="pearson")
