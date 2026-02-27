from django.urls import path
from . import views

app_name = 'accounts' 

urlpatterns = [
    
    path('configuracoes/', views.configuracoes_empresa, name='configuracoes_empresa'),
    
    # Futuramente você pode adicionar aqui rotas padrão do Django como:
    # path('login/', views.login_view, name='login'),
    # path('logout/', views.logout_view, name='logout'),
]