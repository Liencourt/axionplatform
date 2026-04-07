"""
trend_radar/collectors/reddit.py
----------------------------------
Coletor de menções no Reddit BR via PRAW.

Subreddits monitorados por padrão:
  brasil, mercadolivre, financaspessoais, culinaria, receitasdecozinha,
  veganismobr, saudeebemestar

Estratégia:
  - Busca posts e comentários por keyword nos últimos 7 dias (Pushshift fallback)
  - Divide em janelas de 72h para cálculo de aceleração
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from .base import BaseCollector, SinalColetado

logger = logging.getLogger(__name__)

try:
    import praw
    _PRAW_OK = True
except ImportError:
    _PRAW_OK = False

SUBREDDITS_BR = [
    "brasil", "mercadolivre", "financaspessoais",
    "culinaria", "receitasdecozinha", "veganismobr", "saudeebemestar",
]


class RedditCollector(BaseCollector):
    """
    Coleta menções de keywords no Reddit brasileiro.

    Parâmetros
    ----------
    client_id, client_secret : str
        Credenciais de app Reddit (reddit.com/prefs/apps).
    user_agent : str
        Identificador do app (padrão: 'axiom-trend-radar/1.0').
    subreddits : list[str]
        Lista de subreddits a monitorar.
    """

    nome = "reddit"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str = "axiom-trend-radar/1.0",
        subreddits: list[str] | None = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.subreddits = subreddits or SUBREDDITS_BR
        self._reddit = None

    def _get_client(self):
        if self._reddit is None:
            self._reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent,
            )
        return self._reddit

    def coletar(self, keyword: str) -> SinalColetado | None:
        if not _PRAW_OK or not self.client_id or not self.client_secret:
            return None

        reddit = self._get_client()
        now = datetime.now(tz=timezone.utc)
        mid_ts = (now - timedelta(hours=72)).timestamp()
        inicio_ts = (now - timedelta(hours=144)).timestamp()

        n_recente = 0
        n_baseline = 0
        titulos: list[str] = []
        subreddit_str = "+".join(self.subreddits)

        try:
            sub = reddit.subreddit(subreddit_str)
            resultados = sub.search(keyword, sort="new", time_filter="week", limit=100)

            for post in resultados:
                ts = post.created_utc
                titulo = post.title or ""

                if ts >= mid_ts:
                    n_recente += 1
                    if titulo:
                        titulos.append(titulo)
                elif ts >= inicio_ts:
                    n_baseline += 1

        except Exception as exc:
            logger.warning("[reddit] Erro ao buscar '%s': %s", keyword, exc)
            return None

        if n_recente == 0 and n_baseline == 0:
            return None

        return SinalColetado(
            fonte=self.nome,
            keyword=keyword,
            mencoes_recentes=float(n_recente),
            mencoes_baseline=float(n_baseline),
            titulos=titulos[:10],
            dados_extras={"subreddits": self.subreddits},
        )
