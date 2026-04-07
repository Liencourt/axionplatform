from django.urls import path
from . import views

urlpatterns = [
    path('encartes/', views.lista_extracoes, name='lista_extracoes'),
    path('encartes/upload/', views.upload_encarte, name='upload_encarte'),
    path('encartes/<int:extracao_id>/', views.detalhe_extracao, name='detalhe_extracao'),
    path('encartes/<int:extracao_id>/status/', views.status_extracao, name='status_extracao'),
    path('encartes/<int:extracao_id>/excluir/', views.excluir_extracao, name='excluir_extracao'),
    path('encartes/<int:extracao_id>/exportar/', views.exportar_extracao_excel, name='exportar_extracao_excel'),
    path('encartes/concorrentes/', views.lista_concorrentes, name='lista_concorrentes'),
    path('encartes/concorrentes/<int:concorrente_id>/excluir/', views.excluir_concorrente, name='excluir_concorrente'),
]
