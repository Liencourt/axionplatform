"""
enrichment/insights.py
-----------------------
Fase 3 — Geração de Insights em Linguagem Natural para C-Level.

Traduz correlações estatísticas em recomendações acionáveis usando
templates calibrados por tipo de variável e direção do efeito.

Categorias de insight:
  CLIMA       Temperatura, chuva, umidade → decisões de estoque/promoção
  CALENDARIO  Feriados, fim de semana, sazonalidade → planejamento de campanhas
  TENDENCIA   Lags, médias móveis → momentum e previsão
  ECONOMICO   PIB per capita, receita, ticket médio → segmentação de mercado

Cada insight retorna:
  categoria    str      Uma das categorias acima
  titulo       str      Título executivo (<= 80 chars)
  descricao    str      Explicação em 1-2 frases
  acao         str      Recomendação acionável direta
  prioridade   str      'alta' | 'media' | 'baixa'  (baseada em |r|)
  correlacao   float    Valor de r para referência
  variavel     str      Nome da variável original

Uso típico:
    correlacoes = calcular_correlacoes(df_enriquecido, coluna_alvo="quantidade")
    insights = gerar_insights(correlacoes, contexto={"ibge_classe": "B"})

    for i in insights:
        print(i["titulo"])
        print(i["acao"])
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Mapeamento de Variáveis → Categoria ──────────────────────────────────────

_CATEGORIA_MAP: dict[str, str] = {
    # Clima
    "temp_max":           "CLIMA",
    "temp_min":           "CLIMA",
    "temp_media":         "CLIMA",
    "temp_lag1":          "CLIMA",
    "temp_lag7":          "CLIMA",
    "faixa_termica":      "CLIMA",
    "precipitacao":       "CLIMA",
    "chuva_flag":         "CLIMA",
    "umidade_media":      "CLIMA",
    "vento_max":          "CLIMA",
    # Calendário
    "is_feriado":         "CALENDARIO",
    "is_vespera_feriado": "CALENDARIO",
    "is_fim_semana":      "CALENDARIO",
    "dia_semana":         "CALENDARIO",
    "mes":                "CALENDARIO",
    "trimestre":          "CALENDARIO",
    "semana_ano":         "CALENDARIO",
    "dias_ate_natal":     "CALENDARIO",
    "dias_ate_black_friday": "CALENDARIO",
    # Tendência
    "quantidade_lag1":    "TENDENCIA",
    "quantidade_lag7":    "TENDENCIA",
    "quantidade_lag30":   "TENDENCIA",
    "quantidade_mm7":     "TENDENCIA",
    "quantidade_mm30":    "TENDENCIA",
    # Econômico
    "ibge_populacao":     "ECONOMICO",
    "ibge_pib_per_capita":"ECONOMICO",
    "receita":            "ECONOMICO",
    "ticket_medio":       "ECONOMICO",
    "preco":              "ECONOMICO",
}


def _get_categoria(variavel: str) -> str:
    return _CATEGORIA_MAP.get(variavel, "GERAL")


def _get_prioridade(abs_r: float) -> str:
    if abs_r >= 0.5:
        return "alta"
    elif abs_r >= 0.3:
        return "media"
    return "baixa"


# ── Templates de Insight ──────────────────────────────────────────────────────

def _insight_clima_temperatura(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    abs_r = abs(r)

    if var in ("temp_lag1", "temp_lag7"):
        dias = "1 dia" if "lag1" in var else "7 dias"
        titulo = f"Temperatura de {dias} atrás influencia suas vendas"
        descricao = (
            f"Quanto maior a temperatura de {dias} atrás, "
            f"{'mais' if positivo else 'menos'} você vende hoje "
            f"(r={r:+.2f}). O efeito é defasado — os clientes planejam com antecedência."
        )
        acao = (
            f"Ative promoções ou reforce estoques {'no calor' if positivo else 'no frio'} "
            f"prevendo impacto {'positivo' if positivo else 'negativo'} nas vendas seguintes."
        )
    else:
        titulo = (
            f"Temperatura {'aumenta' if positivo else 'reduz'} suas vendas"
        )
        descricao = (
            f"Dias {'mais quentes' if positivo else 'mais frios'} correlacionam "
            f"{'positivamente' if positivo else 'negativamente'} com o volume de vendas "
            f"(r={r:+.2f}, força {'forte' if abs_r >= 0.5 else 'moderada'})."
        )
        if positivo:
            acao = (
                "Aumente estoques e ative campanhas de marketing em dias de previsão de calor. "
                "Considere promoções sazonais de verão para capturar o pico de demanda."
            )
        else:
            acao = (
                "Reforce promoções e liquidações em dias frios para sustentar a demanda. "
                "Avalie bundles ou produtos quentes para compensar a queda natural."
            )
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_clima_chuva(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    if var == "chuva_flag":
        titulo = f"Dias de chuva {'impulsionam' if positivo else 'reduzem'} as vendas"
        descricao = (
            f"Sua operação {'se beneficia' if positivo else 'sofre'} em dias chuvosos "
            f"(r={r:+.2f}). {'Canal digital/delivery se sobressai.' if positivo else 'Menor fluxo presencial.'}"
        )
    else:
        titulo = f"Volume de chuva {'aumenta' if positivo else 'diminui'} as vendas"
        descricao = (
            f"Quanto {'mais' if positivo else 'menos'} chuva, {'mais' if positivo else 'menos'} vendas "
            f"(r={r:+.2f})."
        )
    acao = (
        f"{'Ative canais de delivery e comunicação digital em dias de chuva.' if positivo else 'Planeje ações de tráfego (frete grátis, cupons) para dias chuvosos previstos.'}"
    )
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_calendario_feriado(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    if var == "is_feriado":
        titulo = f"Feriados {'aumentam' if positivo else 'reduzem'} as vendas"
        descricao = (
            f"Dias de feriado mostram {'vendas acima' if positivo else 'vendas abaixo'} da média "
            f"(r={r:+.2f}). "
            f"{'Clientes aproveitam o tempo livre para compras.' if positivo else 'Operação reduzida ou menor demanda.'}"
        )
        acao = (
            f"{'Garanta operação completa e estoques elevados em feriados nacionais.' if positivo else 'Considere promoções-relâmpago antes de feriados para antecipar demanda.'}"
        )
    elif var == "is_vespera_feriado":
        titulo = f"Vésperas de feriado {'impulsionam' if positivo else 'reduzem'} as vendas"
        descricao = (
            f"O dia anterior a feriados tem {'maior' if positivo else 'menor'} volume (r={r:+.2f}). "
            f"{'Clientes antecipam compras.' if positivo else 'Movimento antecipado para o feriado em si.'}"
        )
        acao = (
            f"{'Programe campanhas de urgência (últimas unidades, oferta expira hoje) nas vésperas.' if positivo else 'Reforce estoques no feriado seguinte para absorver a demanda deslocada.'}"
        )
    else:  # is_fim_semana
        titulo = f"Fins de semana {'aumentam' if positivo else 'reduzem'} as vendas"
        descricao = (
            f"Sábados e domingos {'superam' if positivo else 'ficam abaixo de'} os dias úteis "
            f"em volume de vendas (r={r:+.2f})."
        )
        acao = (
            f"{'Concentre investimento em mídia paga e promoções nos finais de semana.' if positivo else 'Crie incentivos para compras nos dias úteis (frete diferenciado, pontos em dobro).'}"
        )
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_calendario_sazonalidade(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    if var == "dias_ate_natal":
        titulo = "Proximidade ao Natal impacta as vendas"
        descricao = (
            f"Conforme o Natal se aproxima, as vendas {'sobem' if not positivo else 'caem'} "
            f"(r={r:+.2f}). "
            f"{'Efeito de antecipação de compras de presentes.' if not positivo else 'Possível fadiga de consumo pós-pico.'}"
        )
        acao = (
            "Inicie campanhas de Natal com 30-45 dias de antecedência para capturar o pico. "
            "Reserve estoque estratégico dos top-SKUs para dezembro."
        )
    elif var == "dias_ate_black_friday":
        titulo = "Black Friday concentra demanda expressiva"
        descricao = (
            f"A aproximação da Black Friday {'eleva' if not positivo else 'reduz'} as vendas "
            f"(r={r:+.2f}). "
            f"{'Clientes postergam compras esperando as ofertas.' if positivo else 'Demanda antecipada cresce antes do evento.'}"
        )
        acao = (
            "Planeje campanhas de Black Friday com pelo menos 3 semanas de antecedência. "
            "Use comunicação de urgência ('últimas horas') para converter indecisos."
        )
    elif var == "mes":
        titulo = "Sazonalidade mensal clara nas vendas"
        descricao = f"O mês do ano influencia significativamente as vendas (r={r:+.2f})."
        acao = (
            "Construa um calendário anual de estoques e investimento em marketing "
            "alinhado com os meses de pico e vale identificados."
        )
    else:
        titulo = "Padrão sazonal detectado"
        descricao = f"A variável '{var}' mostra correlação com sazonalidade (r={r:+.2f})."
        acao = "Revise os dados para identificar o padrão específico e ajuste o planejamento."
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_tendencia(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    if "lag" in var:
        dias = var.split("lag")[-1]
        titulo = f"Vendas de {dias} dias atrás preveem as de hoje"
        descricao = (
            f"O volume vendido há {dias} dias {'aumenta' if positivo else 'reduz'} as vendas do dia "
            f"(r={r:+.2f}). {'Momentum positivo.' if positivo else 'Ciclo de reposição ou rotatividade.'}"
        )
        acao = (
            f"Use o volume de {dias} dias atrás como sinal de alerta precoce para reposição de estoque. "
            f"{'Aproveite momentos de alta para upsell.' if positivo else 'Prepare-se para compensar queda cíclica.'}"
        )
    else:  # média móvel
        janela = var.split("mm")[-1]
        titulo = f"Tendência de {janela} dias molda as vendas atuais"
        descricao = (
            f"A média de vendas dos últimos {janela} dias está {'positivamente' if positivo else 'negativamente'} "
            f"correlacionada com o dia atual (r={r:+.2f})."
        )
        acao = (
            f"Monitore a média móvel de {janela} dias como KPI de tendência. "
            f"{'Expanda estoque quando a tendência for ascendente.' if positivo else 'Acione promoções quando a média cair por 3 dias consecutivos.'}"
        )
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_economico(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    classe = contexto.get("ibge_classe", "")
    if var == "ibge_pib_per_capita":
        titulo = "PIB per capita da região explica o ticket médio"
        descricao = (
            f"O poder aquisitivo local {'favorece' if positivo else 'limita'} as vendas "
            f"(r={r:+.2f}). "
            f"{'Mercado com capacidade de absorver produtos premium.' if positivo else 'Sensibilidade a preço elevada — priorize custo-benefício.'}"
        )
        acao = (
            f"{'Invista em SKUs premium e serviços de valor agregado para este mercado.' if positivo else 'Priorize embalagens econômicas e preços de entrada para aumentar volume.'}"
            + (f" Mercado classificado como '{classe}'." if classe else "")
        )
    elif var == "ticket_medio":
        titulo = f"Ticket médio {'aumenta' if positivo else 'reduz'} o volume"
        descricao = (
            f"Preços {'mais altos' if positivo else 'mais baixos'} correlacionam com "
            f"{'mais' if positivo else 'menos'} volume (r={r:+.2f}). "
            f"{'Efeito prestígio — valor percebido elevado.' if positivo else 'Elástico ao preço — reduções geram volume.'}"
        )
        acao = (
            f"{'Teste precificação premium em produtos-âncora para aumentar receita.' if positivo else 'Use promoções de preço e descontos progressivos para elevar volume.'}"
        )
    elif var == "preco":
        titulo = f"Preço {'aumenta' if positivo else 'reduz'} as vendas"
        descricao = (
            f"Existe correlação {'positiva' if positivo else 'negativa'} entre preço e quantidade "
            f"vendida (r={r:+.2f}). "
            f"{'Sinal de demanda inelástica ou produto premium.' if positivo else 'Produto elástico — consumidor sensível a preço.'}"
        )
        acao = (
            f"{'Avalie aumentos de preço graduais para maximizar receita sem perda de volume.' if positivo else 'Monitore a elasticidade por faixa de preço para encontrar o ponto ótimo.'}"
        )
    else:
        titulo = "Variável econômica influencia as vendas"
        descricao = f"'{var}' mostra correlação de r={r:+.2f} com as vendas."
        acao = "Analise o padrão para definir estratégia de precificação e segmentação."
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


def _insight_geral(var: str, r: float, contexto: dict) -> dict:
    positivo = r > 0
    nome = var.replace("_", " ").title()
    titulo = f"{nome} {'aumenta' if positivo else 'reduz'} as vendas (r={r:+.2f})"
    descricao = (
        f"'{nome}' apresenta correlação {'positiva' if positivo else 'negativa'} "
        f"estatisticamente significativa com as vendas."
    )
    acao = "Investigue esta variável com a equipe comercial para definir plano de ação."
    return {"titulo": titulo, "descricao": descricao, "acao": acao}


# ── Dispatcher ────────────────────────────────────────────────────────────────

def _gerar_insight_individual(corr: dict, contexto: dict) -> dict:
    """Gera insight para uma correlação individual."""
    var = corr["variavel"]
    r = corr["correlacao"]
    categoria = _get_categoria(var)
    abs_r = abs(r)

    # Despacha para template correto
    if categoria == "CLIMA":
        if "chuva" in var or "precipitacao" in var:
            conteudo = _insight_clima_chuva(var, r, contexto)
        else:
            conteudo = _insight_clima_temperatura(var, r, contexto)
    elif categoria == "CALENDARIO":
        if var in ("is_feriado", "is_vespera_feriado", "is_fim_semana"):
            conteudo = _insight_calendario_feriado(var, r, contexto)
        else:
            conteudo = _insight_calendario_sazonalidade(var, r, contexto)
    elif categoria == "TENDENCIA":
        conteudo = _insight_tendencia(var, r, contexto)
    elif categoria == "ECONOMICO":
        conteudo = _insight_economico(var, r, contexto)
    else:
        conteudo = _insight_geral(var, r, contexto)

    return {
        "categoria":   categoria,
        "variavel":    var,
        "correlacao":  r,
        "prioridade":  _get_prioridade(abs_r),
        "titulo":      conteudo["titulo"],
        "descricao":   conteudo["descricao"],
        "acao":        conteudo["acao"],
        "ic_95_lower": corr.get("ic_95_lower"),
        "ic_95_upper": corr.get("ic_95_upper"),
        "n_observacoes": corr.get("n_observacoes"),
        "p_value":     corr.get("p_value"),
    }


# ── Função Principal ──────────────────────────────────────────────────────────

def gerar_insights(
    correlacoes: list[dict[str, Any]],
    contexto: dict[str, Any] | None = None,
    max_insights: int = 10,
) -> list[dict[str, Any]]:
    """
    Transforma correlações estatísticas em insights acionáveis para C-Level.

    Parâmetros
    ----------
    correlacoes : list[dict]
        Saída de `calcular_correlacoes()` — já ordenada por |r| decrescente.
    contexto : dict | None
        Dados opcionais de contexto para personalizar os insights:
          ibge_classe      str   Classe econômica do município ('A'–'E')
          ibge_municipio   str   Nome do município
          ibge_uf          str   UF
          nome_empresa     str   Nome da empresa
          segmento         str   Segmento de varejo (ex: 'alimentar', 'moda')
    max_insights : int
        Número máximo de insights a retornar (padrão: 10).

    Retorna
    -------
    list[dict] com campos:
        categoria, variavel, correlacao, prioridade,
        titulo, descricao, acao, ic_95_lower, ic_95_upper,
        n_observacoes, p_value

    Ordenado por prioridade (alta → media → baixa) e depois por |r|.
    """
    if not correlacoes:
        logger.info("gerar_insights: lista de correlações vazia — sem insights.")
        return []

    ctx = contexto or {}
    insights: list[dict] = []

    for corr in correlacoes:
        try:
            insight = _gerar_insight_individual(corr, ctx)
            insights.append(insight)
        except Exception as exc:
            logger.warning(
                "gerar_insights: falha ao gerar insight para '%s': %s",
                corr.get("variavel", "?"), exc,
            )

    # Ordena: alta → media → baixa, depois por |r| decrescente
    _ordem_prioridade = {"alta": 0, "media": 1, "baixa": 2}
    insights.sort(key=lambda x: (
        _ordem_prioridade.get(x["prioridade"], 9),
        -abs(x["correlacao"]),
    ))

    resultado = insights[:max_insights]

    logger.info(
        "gerar_insights: %d correlações → %d insights gerados (%d retornados).",
        len(correlacoes), len(insights), len(resultado),
    )
    return resultado


def resumo_executivo(
    insights: list[dict[str, Any]],
    contexto: dict[str, Any] | None = None,
) -> str:
    """
    Gera um parágrafo de resumo executivo a partir dos insights.

    Parâmetros
    ----------
    insights : list[dict]
        Saída de `gerar_insights()`.
    contexto : dict | None
        Mesmo contexto passado para `gerar_insights()`.

    Retorna
    -------
    str com parágrafo em português para apresentação executiva.
    """
    if not insights:
        return "Não foram encontradas correlações estatisticamente significativas nos dados analisados."

    ctx = contexto or {}
    empresa = ctx.get("nome_empresa", "sua empresa")
    municipio = ctx.get("ibge_municipio", "")
    uf = ctx.get("ibge_uf", "")
    classe = ctx.get("ibge_classe", "")

    local = f" em {municipio}/{uf}" if municipio and uf else ""
    mercado = f" no mercado classe {classe}" if classe else ""

    n_total = len(insights)
    alta = [i for i in insights if i["prioridade"] == "alta"]
    categorias = list({i["categoria"] for i in insights})

    linhas = [
        f"A análise de dados de {empresa}{local}{mercado} "
        f"identificou {n_total} driver{'s' if n_total > 1 else ''} "
        f"estatisticamente significativo{'s' if n_total > 1 else ''} para as vendas."
    ]

    if alta:
        top = alta[0]
        linhas.append(
            f"O fator de maior impacto é {top['titulo'].lower()}: {top['descricao']}"
        )
        linhas.append(f"Ação recomendada: {top['acao']}")

    if len(alta) > 1:
        outros_titulos = " | ".join(i["titulo"] for i in alta[1:3])
        linhas.append(f"Outros fatores de alta prioridade: {outros_titulos}.")

    cats_pt = {"CLIMA": "climáticos", "CALENDARIO": "calendário", "TENDENCIA": "tendência", "ECONOMICO": "econômicos", "GERAL": "gerais"}
    cats_str = ", ".join(cats_pt.get(c, c.lower()) for c in categorias)
    linhas.append(f"Fatores analisados: {cats_str}.")

    return " ".join(linhas)
