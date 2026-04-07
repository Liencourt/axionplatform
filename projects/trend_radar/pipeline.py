"""
trend_radar/pipeline.py
------------------------
Orquestrador principal do Axiom Trend Radar.

Fluxo por keyword:
  1. Coleta sinais de todas as fontes ativas (Google Trends, NewsAPI, RSS, Reddit)
  2. Calcula aceleração composta
  3. Analisa sentimento dos títulos coletados
  4. Classifica a onda (nivel + sentido)
  5. Busca SKUs relacionados no catálogo da empresa
  6. Gera recomendação prescritiva
  7. Persiste TendenciaDetectada se aceleração > limiar configurado

Palavras-chave:
  - Se RadarConfig.usar_catalogo_automatico = True, usa os principais
    termos extraídos dos nomes de produto da empresa.
  - RadarConfig.palavras_chave adiciona termos customizados.
  - Máximo de 30 keywords por scan no total.
  - Google Trends (SerpAPI): máximo 10 keywords/scan para preservar quota free (250/mês).
  - Palavras-chave manuais têm prioridade sobre as geradas automaticamente do catálogo.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import asdict
from typing import Any

logger = logging.getLogger(__name__)

# Stopwords para extração de keywords do catálogo
_STOPWORDS_CATALOGO = frozenset({
    "de", "da", "do", "das", "dos", "com", "sem", "para", "por", "ao",
    "em", "no", "na", "nos", "nas", "a", "o", "e", "ou",
    "kg", "ml", "g", "l", "un", "und", "cx", "pct", "lt", "gr",
    "tipo", "sabor", "natural", "original", "classico", "clássico",
    "tradicional", "especial", "premium", "extra", "light", "zero",
    "100", "200", "250", "300", "400", "500", "600", "750", "1000",
})

MAX_KEYWORDS = 30


def _gerar_keywords_catalogo(empresa, max_kw: int = 20) -> list[str]:
    """
    Extrai keywords relevantes dos nomes de produto do catálogo da empresa.
    Retorna nomes compostos mais longos (≥ 2 tokens) com prioridade sobre palavras simples.
    Retorna [] se a query demorar mais de 10s (DB lento).
    """
    from projects.models import VendaHistoricaDW

    def _query():
        return list(
            VendaHistoricaDW.objects
            .filter(empresa=empresa)
            .exclude(nome_produto__isnull=True)
            .exclude(nome_produto="")
            .values_list("nome_produto", flat=True)
            .distinct()[:200]
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            nomes = executor.submit(_query).result(timeout=10)
    except FuturesTimeout:
        logger.warning("[pipeline] Timeout (10s) na query do catálogo — empresa %s. Usando apenas keywords manuais.", empresa.id)
        return []
    except Exception as exc:
        logger.warning("[pipeline] Erro ao consultar catálogo da empresa %s: %s", empresa.id, exc)
        return []

    bigramas: dict[str, int] = {}
    palavras: dict[str, int] = {}

    for nome in nomes:
        if not nome:
            continue
        tokens = [t.lower().strip(".,()[]") for t in nome.split()]
        tokens = [t for t in tokens if t and t not in _STOPWORDS_CATALOGO and len(t) > 2]

        # Bigramas (nomes compostos têm melhor especificidade no Google Trends)
        for i in range(len(tokens) - 1):
            bg = f"{tokens[i]} {tokens[i+1]}"
            bigramas[bg] = bigramas.get(bg, 0) + 1

        for t in tokens:
            palavras[t] = palavras.get(t, 0) + 1

    # Prioriza bigramas mais frequentes, completa com palavras simples
    top_bi = sorted(bigramas.items(), key=lambda x: -x[1])[:max_kw // 2]
    top_un = sorted(palavras.items(), key=lambda x: -x[1])[:max_kw // 2]

    result = [k for k, _ in top_bi] + [k for k, _ in top_un]
    # Remove duplicatas preservando ordem
    seen: set[str] = set()
    dedup: list[str] = []
    for kw in result:
        if kw not in seen:
            seen.add(kw)
            dedup.append(kw)

    return dedup[:max_kw]


def _construir_coletores(config) -> list[Any]:
    """
    Instancia os coletores ativos com base na RadarConfig da empresa.
    Google Trends usa SerpAPI (evita banimento de IP do pytrends).
    """
    from django.conf import settings
    from .collectors.serpapi import SerpApiCollector
    from .collectors.newsapi import NewsApiCollector, RSSCollector
    from .collectors.reddit import RedditCollector

    coletores: list[Any] = []
    fontes = set(config.fontes_ativas or ["google_trends", "rss"])

    if "google_trends" in fontes:
        serpapi_key = getattr(settings, "SERPAPI_KEY", "")
        if serpapi_key:
            coletores.append(SerpApiCollector(api_key=serpapi_key))
        else:
            logger.info("[pipeline] Google Trends ignorado — SERPAPI_KEY não configurada.")

    if "newsapi" in fontes:
        api_key = getattr(config, "newsapi_key", None) or getattr(settings, "NEWSAPI_KEY", None)
        if api_key:
            coletores.append(NewsApiCollector(api_key=api_key))
        else:
            logger.info("[pipeline] NewsAPI ignorada — sem chave configurada.")

    if "rss" in fontes:
        coletores.append(RSSCollector())

    if "reddit" in fontes:
        client_id = getattr(config, "reddit_client_id", None) or getattr(settings, "REDDIT_CLIENT_ID", None)
        client_secret = getattr(config, "reddit_client_secret", None) or getattr(settings, "REDDIT_CLIENT_SECRET", None)
        if client_id and client_secret:
            coletores.append(RedditCollector(client_id=client_id, client_secret=client_secret))
        else:
            logger.info("[pipeline] Reddit ignorado — sem credenciais configuradas.")

    return coletores


def _sinal_para_dict(sinal: Any) -> dict:
    """Serializa SinalColetado para JSON-safe dict."""
    return {
        "fonte": sinal.fonte,
        "mencoes_recentes": sinal.mencoes_recentes,
        "mencoes_baseline": sinal.mencoes_baseline,
        "titulos": sinal.titulos[:5],
    }


def executar_scan(empresa, projeto=None) -> list[Any]:
    """
    Executa o scan completo de tendências para uma empresa/projeto.

    Retorna
    -------
    list[TendenciaDetectada] — objetos salvos no banco.
    """
    from projects.models import TendenciaDetectada, RadarConfig
    from .processor.acceleration import calcular_aceleracao
    from .processor.sentiment import analisar_sentimento
    from .classifier.wave_classifier import classificar_onda
    from .recommender.pricing_action import gerar_recomendacao

    # 1. Configuração
    config, _ = RadarConfig.objects.get_or_create(
        empresa=empresa,
        defaults={
            "fontes_ativas": ["google_trends", "rss"],
            "usar_catalogo_automatico": True,
            "limiar_aceleracao": 50.0,
        },
    )

    # 2. Keywords — manuais têm prioridade (consomem quota SerpAPI primeiro)
    manuais: list[str] = list(config.palavras_chave or [])
    keywords: list[str] = list(manuais)
    if config.usar_catalogo_automatico:
        # Catálogo complementa, mas não ultrapassa o limite total
        vagas = MAX_KEYWORDS - len(keywords)
        if vagas > 0:
            auto_kws = _gerar_keywords_catalogo(empresa, max_kw=min(vagas, 15))
            for kw in auto_kws:
                if kw not in keywords:
                    keywords.append(kw)
    keywords = keywords[:MAX_KEYWORDS]

    logger.info(
        "[pipeline] Keywords: %d manuais + %d automáticas = %d total",
        len(manuais), len(keywords) - len(manuais), len(keywords),
    )

    if not keywords:
        logger.warning("[pipeline] Nenhuma keyword para escanear na empresa %s.", empresa.id)
        return []

    # 3. Coletores
    coletores = _construir_coletores(config)
    if not coletores:
        logger.warning("[pipeline] Nenhum coletor ativo para empresa %s.", empresa.id)
        return []

    logger.info(
        "[pipeline] Iniciando scan — empresa=%s | %d keywords | %d coletores",
        empresa.id, len(keywords), len(coletores),
    )

    # 4. Pré-aquece o cache dos coletores que têm feeds (RSS)
    # Faz o download dos feeds UMA VEZ aqui, com timeout real via requests.
    # Sem isso, o 1º coletar_seguro(kw) de cada keyword ficaria tentando baixar
    # os 5 feeds (até 40s) dentro de um thread com timeout=20s — nunca completando.
    for coletor in coletores:
        if hasattr(coletor, '_carregar_feeds'):
            logger.info("[pipeline] Pré-aquecendo cache RSS (%d feeds)...", len(getattr(coletor, 'feeds', [])))
            try:
                coletor._carregar_feeds()
                logger.info("[pipeline] Cache RSS pronto.")
            except Exception as exc:
                logger.warning("[pipeline] Erro ao pré-aquecer RSS: %s — feeds serão ignorados.", exc)

    # 5. Arquiva tendências anteriores deste projeto/empresa (não-arquivadas)
    # Mantemos histórico mas marcamos como "do scan anterior"
    _arquivo_qs = TendenciaDetectada.objects.filter(empresa=empresa, arquivado=False)
    if projeto is not None:
        _arquivo_qs = _arquivo_qs.filter(projeto=projeto)
    _arquivo_qs.update(arquivado=True)

    detectadas: list[Any] = []

    for i, keyword in enumerate(keywords, 1):
        logger.info("[pipeline] Keyword %d/%d: '%s'", i, len(keywords), keyword)

        # Coleta sinais de todas as fontes
        sinais = [c.coletar_seguro(keyword) for c in coletores]
        sinais = [s for s in sinais if s is not None]

        if not sinais:
            logger.info("[pipeline] '%s' — sem sinais coletados.", keyword)
            continue

        # Aceleração composta
        resultado_acc = calcular_aceleracao(sinais)
        if resultado_acc is None:
            continue

        # Filtra pelo limiar da empresa
        if resultado_acc.aceleracao_pct < config.limiar_aceleracao:
            logger.debug(
                "[pipeline] '%s' — aceleração %.1f%% abaixo do limiar %.1f%%.",
                keyword, resultado_acc.aceleracao_pct, config.limiar_aceleracao,
            )
            continue

        # Sentimento a partir dos títulos coletados
        todos_titulos = [t for s in sinais for t in s.titulos]
        resultado_sent = analisar_sentimento(todos_titulos)

        # Classificação da onda
        classificacao = classificar_onda(resultado_acc, resultado_sent)

        # Recomendação prescritiva
        recomendacao = gerar_recomendacao(
            keyword=keyword,
            empresa=empresa,
            nivel=classificacao.nivel,
            classificacao=classificacao.classificacao,
            janela_min=classificacao.janela_min_dias,
            janela_max=classificacao.janela_max_dias,
            acao_estoque=classificacao.acao_estoque,
            ajuste_estoque_pct=classificacao.ajuste_estoque_pct,
        )

        # Serializa fontes para JSON
        fontes_json = [_sinal_para_dict(s) for s in sinais]

        # SKUs para JSON
        skus_json = [
            {
                "codigo_produto": s.codigo_produto,
                "nome_produto": s.nome_produto,
                "similaridade": s.similaridade,
                "elasticidade": s.elasticidade,
            }
            for s in recomendacao.skus_relacionados
        ]

        # Recomendação para JSON
        rec_json = {
            "preco_acao": recomendacao.acao_preco_texto,
            "preco_pct": recomendacao.ajuste_preco_pct,
            "estoque_acao": recomendacao.acao_estoque_texto,
            "estoque_pct": recomendacao.ajuste_estoque_pct,
            "gondola": recomendacao.acao_gondola,
            "janela_min": recomendacao.janela_min_dias,
            "janela_max": recomendacao.janela_max_dias,
        }

        tendencia = TendenciaDetectada.objects.create(
            empresa=empresa,
            projeto=projeto,
            palavra_chave=keyword,
            nivel=classificacao.nivel,
            aceleracao_pct=resultado_acc.aceleracao_pct,
            mencoes_recentes=resultado_acc.mencoes_recentes_total,
            mencoes_baseline=resultado_acc.mencoes_baseline_total,
            aceleracao_por_fonte=resultado_acc.aceleracao_por_fonte,
            classificacao=classificacao.classificacao,
            confianca=classificacao.confianca,
            fontes=fontes_json,
            skus_relacionados=skus_json,
            recomendacao=rec_json,
        )
        detectadas.append(tendencia)
        logger.info(
            "[pipeline] Tendência detectada: '%s' | %s %s | acc=%.0f%%",
            keyword, classificacao.emoji_nivel, classificacao.nivel.upper(),
            resultado_acc.aceleracao_pct,
        )

    logger.info(
        "[pipeline] Scan concluído — %d/%d keywords geraram alertas.",
        len(detectadas), len(keywords),
    )
    return detectadas
