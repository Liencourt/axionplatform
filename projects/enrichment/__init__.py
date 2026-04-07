"""
Axiom Data Enrichment Module
=============================
Enriquece dados de vendas com variáveis externas (clima, calendário, IBGE)
e descobre correlações estatísticas automaticamente.

Fases:
  Fase 1 (core): processor, calendario, correlation
  Fase 2 (APIs): inmet, ibge, ibge_geo
  Fase 3 (features): engine, insights
"""
from .processor import process_sales_data, ValidationReport
from .calendario import enrich_calendario
from .correlation import calcular_correlacoes
from .inmet import buscar_estacao_mais_proxima, buscar_dados_climaticos, enrich_clima
from .ibge import buscar_dados_municipio, enrich_ibge
from .engine import criar_features, criar_features_vendas, criar_features_clima, montar_dataset_completo
from .insights import gerar_insights, resumo_executivo

# ibge_geo requer shapely — importado com guard para graceful degradation
try:
    from .ibge_geo import buscar_setor_por_coordenadas
    __all_geo__ = ["buscar_setor_por_coordenadas"]
except ImportError:  # pragma: no cover
    __all_geo__ = []

__all__ = [
    "process_sales_data",
    "ValidationReport",
    "enrich_calendario",
    "calcular_correlacoes",
    "buscar_estacao_mais_proxima",
    "buscar_dados_climaticos",
    "enrich_clima",
    "buscar_dados_municipio",
    "enrich_ibge",
    "criar_features",
    "criar_features_vendas",
    "criar_features_clima",
    "montar_dataset_completo",
    "gerar_insights",
    "resumo_executivo",
] + __all_geo__
