"""
trend_radar/collectors/base.py
--------------------------------
Contrato base para todos os coletores de sinais externos.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SinalColetado:
    """
    Unidade atômica de sinal coletada de uma fonte.

    Campos
    ------
    fonte         : identificador da fonte ('google_trends', 'newsapi', 'reddit', 'rss')
    keyword       : palavra-chave monitorada
    mencoes_recentes : contagem/score nas últimas 72h (ou equivalente)
    mencoes_baseline : contagem/score nas 72h anteriores (baseline de comparação)
    titulos       : amostra de títulos/textos coletados (para análise de sentimento)
    dados_extras  : payload adicional (série temporal, scores por hora, etc.)
    coletado_em   : timestamp da coleta
    """
    fonte: str
    keyword: str
    mencoes_recentes: float
    mencoes_baseline: float
    titulos: list[str] = field(default_factory=list)
    dados_extras: dict[str, Any] = field(default_factory=dict)
    coletado_em: datetime = field(default_factory=datetime.utcnow)


class BaseCollector(ABC):
    """
    Classe base para coletores.
    Subclasses implementam `coletar(keyword) -> SinalColetado | None`.
    """

    nome: str = "base"
    timeout_segundos: int = 20  # tempo máximo por keyword antes de desistir

    def coletar_seguro(self, keyword: str) -> SinalColetado | None:
        """Wrapper com timeout e tratamento de exceção — nunca propaga para o pipeline."""
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.coletar, keyword)
                return future.result(timeout=self.timeout_segundos)
        except FuturesTimeout:
            logger.warning("[%s] Timeout (%ds) ao coletar '%s' — pulando.", self.nome, self.timeout_segundos, keyword)
            return None
        except Exception as exc:
            logger.warning("[%s] Falha ao coletar '%s': %s", self.nome, keyword, exc)
            return None

    @abstractmethod
    def coletar(self, keyword: str) -> SinalColetado | None:
        """Coleta sinal para um keyword. Retorna None se não houver dados."""
        ...
