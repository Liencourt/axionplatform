"""
trend_radar/processor/acceleration.py
----------------------------------------
Calcula a aceleração de menções a partir de múltiplos sinais coletados.

Conceito central:
  Aceleração (%) = (mencoes_recentes - mencoes_baseline) / max(mencoes_baseline, 1) × 100

Por que aceleração e não volume?
  Volume absoluto é enganoso: um produto popular sempre tem muitas menções.
  Aceleração detecta a MUDANÇA de ritmo — a onda que está chegando.

Composite score:
  Combina sinais de múltiplas fontes com pesos diferentes:
    google_trends: 0.5  (mais confiável, escala global)
    newsapi:       0.25
    rss:           0.15
    reddit:        0.10
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from ..collectors.base import SinalColetado

logger = logging.getLogger(__name__)

# Pesos por fonte para o composite score
PESOS_FONTE: dict[str, float] = {
    "google_trends": 0.50,
    "newsapi": 0.25,
    "rss": 0.15,
    "reddit": 0.10,
}


@dataclass
class ResultadoAceleracao:
    """
    Resultado consolidado de aceleração para um keyword.

    Campos
    ------
    keyword               : palavra-chave analisada
    aceleracao_pct        : aceleração composta (%)
    mencoes_recentes_total: soma de menções recentes (todas as fontes)
    mencoes_baseline_total: soma de menções baseline
    aceleracao_por_fonte  : dict {fonte: aceleracao_pct}
    sinais_usados         : número de fontes com dados válidos
    """
    keyword: str
    aceleracao_pct: float
    mencoes_recentes_total: float
    mencoes_baseline_total: float
    aceleracao_por_fonte: dict[str, float]
    sinais_usados: int


def _aceleracao_sinal(sinal: SinalColetado) -> float:
    """Calcula aceleração de um único sinal."""
    baseline = max(sinal.mencoes_baseline, 0.1)  # evita divisão por zero
    return (sinal.mencoes_recentes - sinal.mencoes_baseline) / baseline * 100.0


def calcular_aceleracao(sinais: list[SinalColetado]) -> ResultadoAceleracao | None:
    """
    Consolida múltiplos sinais num único score de aceleração.

    Retorna None se não houver sinais válidos (mencoes_recentes > 0 em pelo menos 1 fonte).
    """
    if not sinais:
        return None

    # Filtra sinais com dados mínimos
    validos = [s for s in sinais if s.mencoes_recentes > 0 or s.mencoes_baseline > 0]
    if not validos:
        return None

    keyword = validos[0].keyword
    aceleracao_por_fonte: dict[str, float] = {}
    peso_total = 0.0
    composite = 0.0
    mencoes_rec_total = 0.0
    mencoes_base_total = 0.0

    for sinal in validos:
        acc = _aceleracao_sinal(sinal)
        aceleracao_por_fonte[sinal.fonte] = round(acc, 1)

        peso = PESOS_FONTE.get(sinal.fonte, 0.1)
        composite += acc * peso
        peso_total += peso

        mencoes_rec_total += sinal.mencoes_recentes
        mencoes_base_total += sinal.mencoes_baseline

    # Normaliza pelo peso real usado (apenas fontes com dados)
    if peso_total > 0:
        composite = composite / peso_total

    logger.debug(
        "[acceleration] '%s': composite=%.1f%% | fontes=%s",
        keyword, composite,
        {k: f"{v:.0f}%" for k, v in aceleracao_por_fonte.items()},
    )

    return ResultadoAceleracao(
        keyword=keyword,
        aceleracao_pct=round(composite, 1),
        mencoes_recentes_total=round(mencoes_rec_total, 1),
        mencoes_baseline_total=round(mencoes_base_total, 1),
        aceleracao_por_fonte=aceleracao_por_fonte,
        sinais_usados=len(validos),
    )
