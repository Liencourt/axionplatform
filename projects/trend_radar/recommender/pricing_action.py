"""
trend_radar/recommender/pricing_action.py
-------------------------------------------
Conecta o sinal de tendência a SKUs específicos da empresa
e gera recomendações prescritivas de pricing e estoque.

Fluxo:
  1. Busca produtos no catálogo da empresa que coincidam com o keyword
  2. Para cada SKU encontrado, busca a elasticidade em ResultadoPrecificacao
  3. Gera ação de preço baseada em:
     - Classificação do sinal (positivo/negativo/neutro)
     - Nível da onda (viral/alto/moderado/baixo)
     - Elasticidade do produto (quando disponível)

Matching de SKU:
  - Tokenização do keyword (split por espaço + stopwords PT)
  - Busca case-insensitive no nome_produto
  - Retorna SKUs com score de similaridade ≥ 0.3
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Stopwords para tokenização de keywords
_STOPWORDS = frozenset({
    "de", "da", "do", "das", "dos", "com", "sem", "para", "por",
    "em", "no", "na", "nos", "nas", "a", "o", "e", "ou", "ao",
    "kg", "ml", "g", "l", "un", "und", "cx", "pct", "lt",
})

# Ações de preço por (nivel, classif)
_ACAO_PRECO: dict[tuple[str, str], tuple[str, float]] = {
    # (nivel, classif): (acao_texto, pct_ajuste)
    # Positivo: manter ou reduzir levemente para capturar volume
    ("viral",    "positivo"): ("Manter ou reduzir 3% para capturar volume", -3.0),
    ("alto",     "positivo"): ("Manter preço — demanda alta sustenta margem",  0.0),
    ("moderado", "positivo"): ("Manter preço e monitorar demanda",             0.0),
    ("baixo",    "positivo"): ("Manter preço",                                 0.0),
    # Negativo: reduzir para liquidar / minimizar risco
    ("viral",    "negativo"): ("Reduzir 10–15% para liquidar estoque",       -12.0),
    ("alto",     "negativo"): ("Reduzir 5–8% para acelerar saída",            -6.0),
    ("moderado", "negativo"): ("Monitorar — reduzir se demanda cair",          0.0),
    ("baixo",    "negativo"): ("Manter preço, aumentar monitoramento",         0.0),
    # Neutro: oportunidade de volume
    ("viral",    "neutro"):   ("Considerar redução de 2% para volume",        -2.0),
    ("alto",     "neutro"):   ("Manter preço",                                 0.0),
    ("moderado", "neutro"):   ("Manter preço",                                 0.0),
    ("baixo",    "neutro"):   ("Manter preço",                                 0.0),
}

# Recomendação de gôndola por nível
_ACAO_GONDOLA: dict[str, str] = {
    "viral":    "Mover para ponta de gôndola imediatamente",
    "alto":     "Mover para ponta de gôndola por 2 semanas",
    "moderado": "Aumentar frente de gôndola",
    "baixo":    "Manter posição atual",
}


@dataclass
class SKURelacionado:
    codigo_produto: str
    nome_produto: str
    similaridade: float    # 0–1
    elasticidade: float | None  # da tabela ResultadoPrecificacao


@dataclass
class RecomendacaoCompleta:
    skus_relacionados: list[SKURelacionado]
    acao_preco_texto: str
    ajuste_preco_pct: float     # negativo = redução
    acao_estoque_texto: str
    ajuste_estoque_pct: int
    acao_gondola: str
    janela_min_dias: int
    janela_max_dias: int


def _tokenizar(texto: str) -> set[str]:
    tokens = set(texto.lower().split())
    return tokens - _STOPWORDS


def _similaridade(kw_tokens: set[str], nome_produto: str) -> float:
    """
    Jaccard-like similarity: interseção / união.
    Retorna 0–1.
    """
    prod_tokens = _tokenizar(nome_produto)
    if not kw_tokens or not prod_tokens:
        return 0.0
    inter = kw_tokens & prod_tokens
    return len(inter) / len(kw_tokens | prod_tokens)


def buscar_skus_relacionados(
    keyword: str,
    empresa,  # accounts.Empresa instance
    min_similaridade: float = 0.25,
    limite: int = 5,
) -> list[SKURelacionado]:
    """
    Busca SKUs no catálogo da empresa que coincidam com o keyword.
    Enriquece com elasticidade de ResultadoPrecificacao quando disponível.
    """
    from projects.models import VendaHistoricaDW, ResultadoPrecificacao  # evita import circular

    kw_tokens = _tokenizar(keyword)
    if not kw_tokens:
        return []

    # Catálogo distinto da empresa
    produtos = (
        VendaHistoricaDW.objects
        .filter(empresa=empresa)
        .exclude(nome_produto__isnull=True)
        .exclude(nome_produto="")
        .values("codigo_produto", "nome_produto")
        .distinct()
    )

    # Mapa de elasticidade por código
    elast_map: dict[str, float] = {
        r["codigo_produto"]: r["elasticidade"]
        for r in ResultadoPrecificacao.objects
        .filter(projeto__empresa=empresa)
        .values("codigo_produto", "elasticidade")
    }

    candidatos: list[SKURelacionado] = []
    vistos: set[str] = set()

    for p in produtos:
        cod = p["codigo_produto"]
        nome = p["nome_produto"] or ""
        if cod in vistos:
            continue
        vistos.add(cod)

        sim = _similaridade(kw_tokens, nome)
        if sim >= min_similaridade:
            candidatos.append(SKURelacionado(
                codigo_produto=cod,
                nome_produto=nome,
                similaridade=round(sim, 3),
                elasticidade=elast_map.get(cod),
            ))

    candidatos.sort(key=lambda x: x.similaridade, reverse=True)
    return candidatos[:limite]


def gerar_recomendacao(
    keyword: str,
    empresa,
    nivel: str,
    classificacao: str,
    janela_min: int,
    janela_max: int,
    acao_estoque: str,
    ajuste_estoque_pct: int,
) -> RecomendacaoCompleta:
    """
    Gera a recomendação prescritiva completa para um sinal.
    """
    skus = buscar_skus_relacionados(keyword, empresa)

    acao_preco, ajuste_preco = _ACAO_PRECO.get(
        (nivel, classificacao),
        ("Manter preço e monitorar", 0.0),
    )
    gondola = _ACAO_GONDOLA.get(nivel, "Manter posição atual")

    # Ajuste de preço via elasticidade: se produto é muito inelástico, não sugerimos redução
    if skus and skus[0].elasticidade is not None:
        elast = skus[0].elasticidade
        # Elasticidade > -0.3: produto muito inelástico — redução não traz volume extra
        if elast > -0.3 and ajuste_preco < 0:
            acao_preco = "Manter preço — produto inelástico (redução não gera volume)"
            ajuste_preco = 0.0

    return RecomendacaoCompleta(
        skus_relacionados=skus,
        acao_preco_texto=acao_preco,
        ajuste_preco_pct=round(ajuste_preco, 1),
        acao_estoque_texto=acao_estoque,
        ajuste_estoque_pct=ajuste_estoque_pct,
        acao_gondola=gondola,
        janela_min_dias=janela_min,
        janela_max_dias=janela_max,
    )
