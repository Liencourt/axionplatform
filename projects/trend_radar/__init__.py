"""
Axiom Trend Radar
-----------------
Detecção de ondas de tendência e crises de imagem que afetam a demanda de SKUs.

Camadas:
  1. Collectors  — Google Trends, NewsAPI, RSS, Reddit
  2. Processor   — Aceleração de menções + sentimento PT-BR
  3. Classifier  — Nível da onda + sentido (positivo/negativo/neutro)
  4. Recommender — Ação prescritiva de pricing e estoque por SKU

Ponto de entrada:
  from projects.trend_radar import executar_scan
  tendencias = executar_scan(empresa)
"""
from .pipeline import executar_scan

__all__ = ["executar_scan"]
