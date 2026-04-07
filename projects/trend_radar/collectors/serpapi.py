"""
trend_radar/collectors/serpapi.py
-----------------------------------
Coletor de tendências via SerpAPI Google Trends.
Usa requests direto com timeout explícito — evita hangs infinitos.

Free tier SerpAPI: 250 buscas/mês
  → Limite de MAX_KEYWORDS_POR_SCAN por scan para preservar quota
  → Com 10 keywords/scan × 4 scans/mês = 40 buscas (suporta ~6 clientes no free tier)
"""
from __future__ import annotations

import logging

import requests

from .base import BaseCollector, SinalColetado

logger = logging.getLogger(__name__)

SERPAPI_ENDPOINT = "https://serpapi.com/search"
MAX_KEYWORDS_POR_SCAN = 10
REQUEST_TIMEOUT = 12  # segundos — falha rápida se SerpAPI demorar


class SerpApiCollector(BaseCollector):
    """
    Coleta interesse relativo via SerpAPI Google Trends.
    Faz chamada HTTP direta com timeout — não usa a lib serpapi que pode travar.
    """

    nome = "google_trends"
    timeout_segundos = 15  # timeout do BaseCollector (backup extra)

    def __init__(
        self,
        api_key: str = "",
        geo: str = "BR",
        max_keywords: int = MAX_KEYWORDS_POR_SCAN,
    ):
        self.api_key = api_key
        self.geo = geo
        self.max_keywords = max_keywords
        self._scan_count = 0

    def coletar(self, keyword: str) -> SinalColetado | None:
        if not self.api_key:
            logger.warning("[serpapi] SERPAPI_KEY não configurada.")
            return None

        if self._scan_count >= self.max_keywords:
            logger.debug("[serpapi] Limite de %d keywords/scan — pulando '%s'.", self.max_keywords, keyword)
            return None
        self._scan_count += 1

        logger.info("[serpapi] Buscando '%s' (%d/%d)...", keyword, self._scan_count, self.max_keywords)

        try:
            resp = requests.get(
                SERPAPI_ENDPOINT,
                params={
                    "engine": "google_trends",
                    "q": keyword,
                    "geo": self.geo,
                    "date": "now 7-d",
                    "api_key": self.api_key,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json()
        except requests.Timeout:
            logger.warning("[serpapi] Timeout (%ds) para '%s' — pulando.", REQUEST_TIMEOUT, keyword)
            return None
        except Exception as exc:
            logger.warning("[serpapi] Erro ao buscar '%s': %s", keyword, exc)
            return None

        # Verifica erro de API (ex: saldo esgotado, chave inválida)
        if "error" in result:
            logger.warning("[serpapi] Erro da API para '%s': %s", keyword, result["error"])
            return None

        # Extrai série temporal
        timeline = (result.get("interest_over_time") or {}).get("timeline_data") or []

        if len(timeline) < 6:
            logger.debug("[serpapi] Dados insuficientes para '%s' (%d pontos).", keyword, len(timeline))
            return None

        valores: list[float] = []
        for ponto in timeline:
            vals = ponto.get("values") or []
            if vals:
                v = vals[0].get("extracted_value") or vals[0].get("value") or 0
                try:
                    valores.append(float(str(v).replace("<1", "0.5")))
                except (ValueError, TypeError):
                    valores.append(0.0)

        if len(valores) < 6:
            return None

        mid = len(valores) // 2
        baseline = sum(valores[:mid]) / mid
        recente = sum(valores[mid:]) / (len(valores) - mid)

        # Queries relacionadas (best-effort)
        titulos: list[str] = []
        try:
            top_queries = (result.get("related_queries") or {}).get("top") or []
            titulos = [q.get("query", "") for q in top_queries[:5] if q.get("query")]
        except Exception:
            pass

        logger.info(
            "[serpapi] '%s' — baseline=%.1f recente=%.1f acc=%.0f%%",
            keyword, baseline, recente,
            (recente - baseline) / max(baseline, 1) * 100,
        )

        return SinalColetado(
            fonte=self.nome,
            keyword=keyword,
            mencoes_recentes=recente,
            mencoes_baseline=baseline,
            titulos=titulos,
            dados_extras={"geo": self.geo, "n_pontos": len(valores)},
        )
