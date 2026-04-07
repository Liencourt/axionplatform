from django.urls import path
from . import views
from . import api_public

urlpatterns = [

    # ==========================================
    # AXIOM PUBLIC REST API v1 (B2B)
    # Autenticação: header X-Axiom-API-Key: <uuid>
    # ==========================================
    path('api/v1/elasticidade/<int:projeto_id>/', api_public.api_v1_elasticidade, name='api_v1_elasticidade'),
    path('api/v1/simular-preco/', api_public.api_v1_simular_preco, name='api_v1_simular_preco'),
    path('api/v1/otimizar-margem/', api_public.api_v1_otimizar_margem, name='api_v1_otimizar_margem'),
    # Documentação interativa (Swagger UI)
    path('api/v1/docs/', api_public.api_v1_docs, name='api_v1_docs'),
    path('api/v1/schema/', api_public.api_v1_schema, name='api_v1_schema'),

    # ── MÓDULO DE INGESTÃO (PUSH API) ──────────────────────────────────────
    path('api/v1/lojas/',                api_public.api_v1_lojas,               name='api_v1_lojas'),
    path('api/v1/projetos/',             api_public.api_v1_projetos,            name='api_v1_projetos'),
    path('api/v1/ingestao/vendas/',      api_public.api_v1_ingestao_vendas,     name='api_v1_ingestao_vendas'),
    path('api/v1/ingestao/faturamento/', api_public.api_v1_ingestao_faturamento, name='api_v1_ingestao_faturamento'),

    
    path('novo-estudo/', views.iniciar_projeto_upload, name='iniciar_projeto_upload'),
    
    
    path('processar/', views.processar_modelo_dinamico, name='processar_modelo_dinamico'),
    
   
    path('projeto/<int:projeto_id>/dashboard/', views.dashboard_resultado, name='dashboard_resultado'),
    
    
    path('projeto/<int:projeto_id>/exportar/', views.exportar_resultados_erp, name='exportar_erp'),

    path('simulador/<int:resultado_id>/', views.simulador_produto, name='simulador_produto'),

    path('api/recalcular-modelo/', views.api_recalcular_modelo, name='api_recalcular_modelo'),

    path('meus-projetos/', views.lista_projetos, name='lista_projetos'),

    path('projeto/<int:projeto_id>/excluir/', views.excluir_projeto, name='excluir_projeto'),

    path('configuracoes/', views.configuracoes_conta, name='configuracoes_conta'),

    path('resultado/<int:resultado_id>/salvar-preco/', views.salvar_preco_simulado, name='salvar_preco_simulado'),

    path('forecast/painel/<int:resultado_id>/', views.painel_forecast, name='painel_forecast'),
    
    path('forecast/gerar/<int:resultado_id>/', views.gerar_forecast_action, name='gerar_forecast_action'),
    path('api/simular-preco/', views.api_simular_preco, name='api_simular_preco'),
    # MÓDULO AXIOM MACRO (PREVISÃO DE FATURAMENTO GLOBAL)
    path('forecast-corporativo/', views.painel_macro_forecast, name='painel_macro_forecast'),
    path('forecast-corporativo/gerar/', views.gerar_macro_forecast_action, name='gerar_macro_forecast_action'),

    path('forecast-corporativo/upload/', views.upload_macro_financeiro, name='upload_macro_financeiro'),

    # ==========================================
    # ASSINATURAS E PAGAMENTOS (STRIPE)
    # ==========================================
    path('assinatura/checkout/', views.criar_checkout_stripe, name='criar_checkout_stripe'),
    path('assinatura/sucesso/', views.sucesso_pagamento, name='sucesso_pagamento'),
    path('assinatura/cancelado/', views.cancelado_pagamento, name='cancelado_pagamento'),
    path('stripe/webhook/', views.stripe_webhook, name='stripe_webhook'),

    # CALENDÁRIO DE EVENTOS
    path('calendario/', views.painel_calendario, name='painel_calendario'),
    path('calendario/deletar/<int:evento_id>/', views.deletar_evento, name='deletar_evento'),

    # Classificação de Produtos
    path('projeto/<int:projeto_id>/portfolio/', views.painel_portfolio, name='painel_portfolio'),

    # ==========================================
    # AXIOM MARGIN COMMAND (O Dashboard do CEO)
    # ==========================================
    path('projeto/<int:projeto_id>/margin-command/', views.painel_margin_command, name='painel_margin_command'),
    path('api/otimizar-margem/', views.api_otimizar_margem_global, name='api_otimizar_margem_global'),

    # ==========================================
    # ENRIQUECIMENTO & CORRELAÇÕES
    # ==========================================
    path('projeto/<int:projeto_id>/correlacoes/', views.painel_correlacoes, name='painel_correlacoes'),
    path('projeto/<int:projeto_id>/correlacoes/rodar/', views.rodar_analise_correlacoes, name='rodar_analise_correlacoes'),

    # ==========================================
    # AXIOM REPUTATION — Sentimento Google
    # ==========================================
    path('projeto/<int:projeto_id>/reputacao/', views.reputacao_dashboard, name='reputacao_dashboard'),
    path('projeto/<int:projeto_id>/reputacao/buscar/', views.reputacao_buscar_lugar, name='reputacao_buscar_lugar'),
    path('projeto/<int:projeto_id>/reputacao/confirmar/', views.reputacao_confirmar_lugar, name='reputacao_confirmar_lugar'),
    path('projeto/<int:projeto_id>/reputacao/analisar/', views.reputacao_analisar, name='reputacao_analisar'),
    path('projeto/<int:projeto_id>/reputacao/trocar/', views.reputacao_trocar_lugar, name='reputacao_trocar_lugar'),

    # ==========================================
    # AXIOM TREND RADAR
    # ==========================================
    path('projeto/<int:projeto_id>/trend-radar/', views.trend_radar_dashboard, name='trend_radar_dashboard'),
    path('projeto/<int:projeto_id>/trend-radar/scan/', views.rodar_scan_radar, name='rodar_scan_radar'),
    path('projeto/<int:projeto_id>/trend-radar/arquivar/<int:tendencia_id>/', views.arquivar_tendencia, name='arquivar_tendencia'),
    path('projeto/<int:projeto_id>/trend-radar/configurar/', views.salvar_radar_config, name='salvar_radar_config'),

]