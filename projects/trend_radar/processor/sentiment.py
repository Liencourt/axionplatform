"""
trend_radar/processor/sentiment.py
-------------------------------------
Classificador de sentimento em Português Brasileiro baseado em palavras-chave.

Abordagem: keyword-based scoring sem dependência de modelo ML pesado.
  - Eficiente (µs por texto)
  - Sem dependência externa de GPU/transformers
  - Adaptado para o vocabulário de varejo e consumo BR

Score final:
  positivo_score = Σ pesos de keywords positivas encontradas
  negativo_score = Σ pesos de keywords negativas encontradas
  classificação: POSITIVO se positivo_score > negativo_score × 1.2
                 NEGATIVO se negativo_score > positivo_score × 1.2
                 NEUTRO   caso contrário (incerteza)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# ── Dicionários de keywords ───────────────────────────────────────────────────

# Peso 2.0: sinais fortes de tendência viral positiva
# Peso 1.0: sinais moderados de interesse crescente
KEYWORDS_POSITIVO: dict[str, float] = {
    # Viralidade / tendência
    "viral": 2.0, "viralizou": 2.0, "viralizando": 2.0,
    "tendência": 1.5, "tendencia": 1.5, "trend": 1.5, "trending": 2.0,
    "hype": 1.5, "febre": 1.5, "boom": 1.5, "explosão": 1.5, "explosao": 1.5,
    "hit": 1.0, "moda": 1.0, "popular": 1.0,
    # Saúde/bem-estar (comum em alimentos)
    "saudável": 1.0, "saudavel": 1.0, "nutritivo": 1.0, "orgânico": 1.0, "organico": 1.0,
    "natural": 0.5, "superalimento": 1.5, "fit": 0.8, "funcional": 0.8,
    # Receitas / culinária
    "receita": 1.0, "receitas": 1.0, "culinária": 0.8, "culinaria": 0.8,
    "delicioso": 0.8, "gostoso": 0.8, "sabor": 0.5,
    # Demanda e procura
    "procura": 1.0, "demanda": 1.0, "alta demanda": 2.0, "faltando": 1.5,
    "esgotado": 1.5, "esgotando": 1.5, "acabou": 1.0,
    "recomendado": 1.0, "sucesso": 1.0, "sensação": 1.0, "sensacao": 1.0,
    "vende muito": 2.0, "mais vendido": 2.0,
    # Redes sociais
    "tiktok": 0.5, "instagram": 0.5, "reels": 0.5, "shorts": 0.5,
}

# Peso 2.0: alertas críticos (recalls, contaminação)
# Peso 1.0: riscos moderados
KEYWORDS_NEGATIVO: dict[str, float] = {
    # Recalls e crises sanitárias
    "recall": 3.0, "recolhimento": 2.5, "recolher": 2.0,
    "contaminado": 3.0, "contaminação": 3.0, "contaminacao": 3.0,
    "adulterado": 2.5, "adulteração": 2.5, "adulteracao": 2.5,
    "irregularidade": 2.0, "irregular": 1.5,
    "anvisa": 2.0, "vigilância sanitária": 2.5, "vigilancia sanitaria": 2.5,
    "interdição": 2.5, "interdicao": 2.5, "proibido": 2.0, "apreensão": 2.0, "apreensao": 2.0,
    "retirado de circulação": 3.0, "retirado do mercado": 3.0,
    # Saúde/dano
    "intoxicação": 3.0, "intoxicacao": 3.0, "intoxicado": 2.5,
    "morte": 2.5, "morreu": 2.5, "óbito": 2.5, "obito": 2.5,
    "acidente": 1.5, "perigo": 2.0, "perigoso": 2.0, "risco": 1.5,
    "nocivo": 2.0, "tóxico": 2.5, "toxico": 2.5,
    "alergia": 1.5, "alérgico": 1.5, "alergico": 1.5,
    # Crises de imagem
    "escândalo": 2.0, "escandalo": 2.0, "denúncia": 2.0, "denuncia": 2.0,
    "fraude": 2.5, "golpe": 2.0, "enganando": 2.0,
    "recall": 3.0, "crise": 1.5, "problema": 1.0,
    # Queda de demanda
    "boicote": 2.5, "boicotando": 2.5, "não compro": 2.0, "nao compro": 2.0,
    "péssimo": 1.5, "pessimo": 1.5, "horrível": 1.5, "horrivel": 1.5,
}


@dataclass
class ResultadoSentimento:
    classificacao: Literal["positivo", "negativo", "neutro"]
    confianca: float          # 0.0 – 1.0
    score_positivo: float
    score_negativo: float
    keywords_detectadas: list[str]


def analisar_sentimento(textos: list[str]) -> ResultadoSentimento:
    """
    Analisa sentimento de uma lista de textos (títulos de notícias, posts, etc.)
    e retorna classificação consolidada.

    Parâmetros
    ----------
    textos : list[str]
        Lista de títulos ou frases a analisar.

    Retorna
    -------
    ResultadoSentimento com classificação e confiança.
    """
    if not textos:
        return ResultadoSentimento(
            classificacao="neutro", confianca=0.5,
            score_positivo=0.0, score_negativo=0.0, keywords_detectadas=[],
        )

    texto_completo = " ".join(textos).lower()
    score_pos = 0.0
    score_neg = 0.0
    kws_detectadas: list[str] = []

    for kw, peso in KEYWORDS_POSITIVO.items():
        if kw in texto_completo:
            score_pos += peso
            kws_detectadas.append(f"+{kw}")

    for kw, peso in KEYWORDS_NEGATIVO.items():
        if kw in texto_completo:
            score_neg += peso
            kws_detectadas.append(f"-{kw}")

    total = score_pos + score_neg
    if total == 0:
        return ResultadoSentimento(
            classificacao="neutro", confianca=0.5,
            score_positivo=0.0, score_negativo=0.0, keywords_detectadas=[],
        )

    # Limiar: requer 20% de margem para classificar (caso contrário = neutro)
    if score_pos > score_neg * 1.2:
        classificacao: Literal["positivo", "negativo", "neutro"] = "positivo"
        confianca = min(score_pos / total, 0.98)
    elif score_neg > score_pos * 1.2:
        classificacao = "negativo"
        confianca = min(score_neg / total, 0.98)
    else:
        classificacao = "neutro"
        confianca = 0.5

    return ResultadoSentimento(
        classificacao=classificacao,
        confianca=round(confianca, 3),
        score_positivo=round(score_pos, 2),
        score_negativo=round(score_neg, 2),
        keywords_detectadas=list(set(kws_detectadas))[:15],
    )
