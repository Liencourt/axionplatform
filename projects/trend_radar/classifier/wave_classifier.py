"""
trend_radar/classifier/wave_classifier.py
-------------------------------------------
Classifica ondas de tendência em nível (baixo/moderado/alto/viral)
e sentido (positivo/negativo/neutro).

Escala de nível:
  < 50%   aceleração → BAIXO     (monitorar)
  50-100% aceleração → MODERADO  (oportunidade emergente)
  100-200%            → ALTO     (ação recomendada)
  > 200%              → VIRAL    (janela curta, agir agora)

Regra especial "negativo urgente":
  Se classificação = NEGATIVO e nível >= MODERADO → escala um nível acima.
  Crises de imagem têm janela mais curta que tendências positivas.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..processor.acceleration import ResultadoAceleracao
from ..processor.sentiment import ResultadoSentimento

NivelTendencia = Literal["baixo", "moderado", "alto", "viral"]
ClassifTendencia = Literal["positivo", "negativo", "neutro"]

# Limiar de aceleração (%) para cada nível
_LIMIARES = [
    (200.0, "viral"),
    (100.0, "alto"),
    (50.0,  "moderado"),
    (0.0,   "baixo"),
]

# Janela de oportunidade estimada (dias) por nível
JANELA_OPORTUNIDADE: dict[str, tuple[int, int]] = {
    "viral":    (7,  14),
    "alto":     (14, 21),
    "moderado": (21, 35),
    "baixo":    (30, 60),
}

# Recomendação de estoque por nível + sentido
_ACAO_ESTOQUE: dict[tuple[str, str], tuple[str, int]] = {
    # (nivel, classif): (acao_texto, pct_ajuste)
    ("viral",    "positivo"): ("Aumentar pedido urgente",   35),
    ("alto",     "positivo"): ("Aumentar pedido",           25),
    ("moderado", "positivo"): ("Aumentar pedido levemente", 12),
    ("baixo",    "positivo"): ("Monitorar estoque",          0),
    ("viral",    "negativo"): ("Liquidar estoque",          -30),
    ("alto",     "negativo"): ("Reduzir reposição",         -20),
    ("moderado", "negativo"): ("Monitorar com cautela",      0),
    ("baixo",    "negativo"): ("Monitorar",                   0),
    ("viral",    "neutro"):   ("Aumentar estoque",           15),
    ("alto",     "neutro"):   ("Aumentar levemente",         10),
    ("moderado", "neutro"):   ("Manter estoque",              0),
    ("baixo",    "neutro"):   ("Manter estoque",              0),
}


@dataclass
class ResultadoClassificacao:
    nivel: NivelTendencia
    classificacao: ClassifTendencia
    confianca: float
    janela_min_dias: int
    janela_max_dias: int
    emoji_nivel: str
    acao_estoque: str
    ajuste_estoque_pct: int


def _nivel_para_emoji(nivel: NivelTendencia, classif: ClassifTendencia) -> str:
    if classif == "negativo":
        return {"viral": "🚨", "alto": "⚠️", "moderado": "⚡", "baixo": "👁️"}[nivel]
    return {"viral": "🔥", "alto": "🚀", "moderado": "📈", "baixo": "👁️"}[nivel]


def classificar_onda(
    aceleracao: ResultadoAceleracao,
    sentimento: ResultadoSentimento,
) -> ResultadoClassificacao:
    """
    Combina aceleração + sentimento para classificar a onda.
    """
    acc = aceleracao.aceleracao_pct
    classif: ClassifTendencia = sentimento.classificacao  # type: ignore[assignment]

    # Determina nível base pela aceleração
    nivel: NivelTendencia = "baixo"
    for limiar, lbl in _LIMIARES:
        if acc >= limiar:
            nivel = lbl  # type: ignore[assignment]
            break

    # Urgência extra para sinais negativos (crises têm janela menor)
    if classif == "negativo":
        escalada = {"baixo": "moderado", "moderado": "alto", "alto": "viral", "viral": "viral"}
        nivel = escalada[nivel]  # type: ignore[assignment]

    janela = JANELA_OPORTUNIDADE[nivel]
    acao_sto, pct_sto = _ACAO_ESTOQUE.get((nivel, classif), ("Monitorar", 0))

    return ResultadoClassificacao(
        nivel=nivel,
        classificacao=classif,
        confianca=round(sentimento.confianca, 3),
        janela_min_dias=janela[0],
        janela_max_dias=janela[1],
        emoji_nivel=_nivel_para_emoji(nivel, classif),
        acao_estoque=acao_sto,
        ajuste_estoque_pct=pct_sto,
    )
