from django.urls import path
from . import views

urlpatterns = [
    
    path('', views.iniciar_projeto_upload, name='iniciar_projeto_upload'),
    
    
    path('processar/', views.processar_modelo_dinamico, name='processar_modelo_dinamico'),
    
   
    path('projeto/<int:projeto_id>/dashboard/', views.dashboard_resultado, name='dashboard_resultado'),
    
    
    path('projeto/<int:projeto_id>/exportar/', views.exportar_resultados_erp, name='exportar_erp'),

    path('simulador/<int:resultado_id>/', views.simulador_produto, name='simulador_produto'),

    path('meus-projetos/', views.lista_projetos, name='lista_projetos'),

    path('projeto/<int:projeto_id>/excluir/', views.excluir_projeto, name='excluir_projeto'),

    path('configuracoes/', views.configuracoes_conta, name='configuracoes_conta'),

    path('resultado/<int:resultado_id>/salvar-preco/', views.salvar_preco_simulado, name='salvar_preco_simulado'),

    path('forecast/<str:sku>/', views.painel_forecast, name='painel_forecast'),
    
    path('forecast/<str:sku>/gerar/', views.gerar_forecast_action, name='gerar_forecast_action'),

    # MÓDULO AXIOM MACRO (PREVISÃO DE FATURAMENTO GLOBAL)
    path('forecast-corporativo/', views.painel_macro_forecast, name='painel_macro_forecast'),
    path('forecast-corporativo/gerar/', views.gerar_macro_forecast_action, name='gerar_macro_forecast_action'),
]