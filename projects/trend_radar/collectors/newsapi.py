"""
trend_radar/collectors/newsapi.py
-----------------------------------
Coletor de notícias via NewsAPI.org e feeds RSS.

Fontes RSS padrão (varejo brasileiro):
  - SuperHiper Magazine
  - ABRAS (Associação Brasileira de Supermercados)
  - G1 Economia
  - Valor Econômico Feed

Estratégia de aceleração:
  - Conta artigos mencionando o keyword nas últimas 72h vs. 72–144h anteriores
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from .base import BaseCollector, SinalColetado

logger = logging.getLogger(__name__)

try:
    from newsapi import NewsApiClient
    _NEWSAPI_OK = True
except ImportError:
    _NEWSAPI_OK = False

try:
    import feedparser
    _FEEDPARSER_OK = True
except ImportError:
    _FEEDPARSER_OK = False


# Feeds RSS de varejo/consumo brasileiros
RSS_FEEDS_VAREJO = [
    "https://superhiper.abras.com.br/feed/",
    "https://www.abras.com.br/rss.xml",
    "https://g1.globo.com/rss/g1/economia/",
    "https://feeds.folha.uol.com.br/mercado/rss091.xml",
    "https://valor.globo.com/rss/home.xml",
]


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


class NewsApiCollector(BaseCollector):
    """
    Coleta artigos de notícias via NewsAPI.

    Parâmetros
    ----------
    api_key : str
        Chave da NewsAPI (newsapi.org).
    janela_horas : int
        Janela de busca em horas (padrão: 144 = 6 dias → divide em 2 blocos de 72h).
    """

    nome = "newsapi"

    def __init__(self, api_key: str, janela_horas: int = 144):
        self.api_key = api_key
        self.janela_horas = janela_horas

    def coletar(self, keyword: str) -> SinalColetado | None:
        if not _NEWSAPI_OK or not self.api_key:
            return None

        client = NewsApiClient(api_key=self.api_key)
        now = _now_utc()
        mid = now - timedelta(hours=self.janela_horas // 2)      # 72h atrás
        inicio = now - timedelta(hours=self.janela_horas)         # 144h atrás

        def _buscar(from_dt: datetime, to_dt: datetime) -> tuple[int, list[str]]:
            try:
                resp = client.get_everything(
                    q=keyword,
                    language="pt",
                    from_param=from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    to=to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    sort_by="publishedAt",
                    page_size=20,
                )
                articles = resp.get("articles", [])
                titulos = [a.get("title", "") for a in articles if a.get("title")]
                return resp.get("totalResults", 0), titulos
            except Exception as exc:
                logger.debug("[newsapi] Erro ao buscar '%s': %s", keyword, exc)
                return 0, []

        n_recente, titulos_recente = _buscar(mid, now)
        n_baseline, _ = _buscar(inicio, mid)

        if n_recente == 0 and n_baseline == 0:
            return None

        return SinalColetado(
            fonte=self.nome,
            keyword=keyword,
            mencoes_recentes=float(n_recente),
            mencoes_baseline=float(n_baseline),
            titulos=titulos_recente[:10],
            dados_extras={"n_total_recente": n_recente, "n_total_baseline": n_baseline},
        )


class RSSCollector(BaseCollector):
    """
    Coleta artigos de feeds RSS de varejo/consumo brasileiro.
    Não requer chave de API.

    Otimização: feeds são baixados UMA VEZ por instância (cache em memória).
    Todas as keywords são verificadas contra o cache — sem requisições repetidas.
    """

    nome = "rss"

    def __init__(self, feeds: list[str] | None = None, janela_horas: int = 144):
        self.feeds = feeds or RSS_FEEDS_VAREJO
        self.janela_horas = janela_horas
        self._cache: list[Any] | None = None  # cache de feeds já baixados

    def _carregar_feeds(self) -> list[Any]:
        """Baixa todos os feeds via requests (timeout real de 8s) e armazena em cache."""
        if self._cache is not None:
            return self._cache

        resultado: list[Any] = []
        for url in self.feeds:
            try:
                resp = requests.get(url, timeout=8, headers={"User-Agent": "AxiomPlatform/1.0"})
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)  # parse em memória, sem I/O adicional
                resultado.append(feed)
                logger.debug("[rss] Feed carregado: %s (%d entradas)", url, len(feed.entries))
            except requests.Timeout:
                logger.warning("[rss] Timeout (8s) no feed '%s' — ignorando.", url)
            except Exception as exc:
                logger.debug("[rss] Erro ao carregar feed '%s': %s", url, exc)

        self._cache = resultado
        return self._cache

    def _parse_data(self, entry: Any) -> datetime | None:
        """Tenta extrair a data de publicação de uma entrada RSS."""
        for campo in ("published", "updated", "created"):
            val = getattr(entry, campo, None)
            if val:
                try:
                    return parsedate_to_datetime(val).replace(tzinfo=timezone.utc)
                except Exception:
                    pass
        return None

    def coletar(self, keyword: str) -> SinalColetado | None:
        if not _FEEDPARSER_OK:
            return None

        now = _now_utc()
        mid = now - timedelta(hours=self.janela_horas // 2)
        inicio = now - timedelta(hours=self.janela_horas)
        kw_lower = keyword.lower()

        n_recente = 0
        n_baseline = 0
        titulos: list[str] = []

        for feed in self._carregar_feeds():
            try:
                for entry in feed.entries:
                    titulo = getattr(entry, "title", "") or ""
                    resumo = getattr(entry, "summary", "") or ""
                    texto = (titulo + " " + resumo).lower()

                    if kw_lower not in texto:
                        continue

                    pub = self._parse_data(entry)
                    if pub is None:
                        continue

                    if pub >= mid:
                        n_recente += 1
                        if titulo:
                            titulos.append(titulo)
                    elif pub >= inicio:
                        n_baseline += 1

            except Exception as exc:
                logger.debug("[rss] Erro ao processar entradas: %s", exc)

        if n_recente == 0 and n_baseline == 0:
            return None

        return SinalColetado(
            fonte=self.nome,
            keyword=keyword,
            mencoes_recentes=float(n_recente),
            mencoes_baseline=float(n_baseline),
            titulos=titulos[:10],
        )
