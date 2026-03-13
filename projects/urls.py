from django.urls import path
from . import views

urlpatterns = [
    
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
    
]