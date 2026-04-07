"""
trend_radar/collectors/google_trends.py
-----------------------------------------
Coletor de tendências do Google Trends via pytrends.

Estratégia de aceleração:
  - Busca interesse horário dos últimos 7 dias (timeframe='now 7-d')
  - Divide a série ao meio: primeira metade = baseline, segunda = recente
  - Aceleração = (média_recente - média_baseline) / max(média_baseline, 1) × 100

Limitações conhecidas:
  - Máximo de 5 keywords por requisição (limitação da API)
  - Sujeito a rate limiting — usa backoff automático
  - Valores de interesse são relativos (0–100), não absolutos
"""
from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseCollector, SinalColetado

logger = logging.getLogger(__name__)

# Tenta importar pytrends; se não estiver instalado, o coletor é desabilitado
try:
    from pytrends.request import TrendReq
    _PYTRENDS_OK = True
except ImportError:
    _PYTRENDS_OK = False
    logger.info("pytrends não instalado — GoogleTrendsCollector desabilitado.")


class GoogleTrendsCollector(BaseCollector):
    """
    Coleta interesse relativo do Google Trends para um keyword (Brasil).

    Parâmetros
    ----------
    timeframe : str
        Janela de busca (padrão: 'now 7-d' = últimos 7 dias com granularidade horária).
    geo : str
        País de interesse (padrão: 'BR').
    retries : int
        Número máximo de tentativas em caso de rate limit.
    """

    nome = "google_trends"
    timeout_segundos = 15  # pytrends pode travar — timeout agressivo
    _scan_count = 0         # contador de keywords processadas neste scan

    def __init__(
        self,
        timeframe: str = "now 7-d",
        geo: str = "BR",
        retries: int = 2,
        max_keywords: int = 5,
    ):
        self.timeframe = timeframe
        self.geo = geo
        self.retries = retries
        self.max_keywords = max_keywords
        self._pytrends: Any = None
        self._scan_count = 0

    def _get_client(self) -> Any:
        if self._pytrends is None:
            self._pytrends = TrendReq(hl="pt-BR", tz=180, timeout=(10, 25), retries=2, backoff_factor=0.5)
        return self._pytrends

    def coletar(self, keyword: str) -> SinalColetado | None:
        if not _PYTRENDS_OK:
            return None

        # Limita keywords por scan para evitar bloqueio de IP
        if self._scan_count >= self.max_keywords:
            logger.debug("[google_trends] Limite de %d keywords atingido — pulando '%s'.", self.max_keywords, keyword)
            return None
        self._scan_count += 1

        for tentativa in range(self.retries):
            try:
                pt = self._get_client()
                pt.build_payload([keyword], cat=0, timeframe=self.timeframe, geo=self.geo)
                df = pt.interest_over_time()

                if df is None or df.empty or keyword not in df.columns:
                    logger.debug("[google_trends] Sem dados para '%s'.", keyword)
                    return None

                serie = df[keyword].dropna()
                if len(serie) < 6:
                    return None

                # Divide ao meio: primeira = baseline, segunda = recente
                mid = len(serie) // 2
                baseline_vals = serie.iloc[:mid]
                recente_vals = serie.iloc[mid:]

                baseline = float(baseline_vals.mean())
                recente = float(recente_vals.mean())

                # Títulos: top consultas relacionadas (opcional, best-effort)
                titulos: list[str] = []
                try:
                    top = pt.related_queries().get(keyword, {}).get("top")
                    if top is not None and not top.empty:
                        titulos = top["query"].head(5).tolist()
                except Exception:
                    pass

                # Série temporal como extra para exibição no frontend
                serie_json = {
                    str(ts): int(v)
                    for ts, v in zip(serie.index.astype(str), serie.values)
                }

                return SinalColetado(
                    fonte=self.nome,
                    keyword=keyword,
                    mencoes_recentes=recente,
                    mencoes_baseline=baseline,
                    titulos=titulos,
                    dados_extras={"serie_temporal": serie_json, "geo": self.geo},
                )

            except Exception as exc:
                wait = 2 ** tentativa
                logger.warning(
                    "[google_trends] Tentativa %d/%d falhou para '%s': %s. Aguardando %ds.",
                    tentativa + 1, self.retries, keyword, exc, wait,
                )
                if tentativa < self.retries - 1:
                    time.sleep(min(wait, 3))  # máximo 3s entre retries
                else:
                    raise

        return None
