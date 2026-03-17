
from django.contrib import admin
from django.urls import path,include
from django.views.generic import TemplateView
from django.contrib.auth import views as auth_views

from config.view import gerar_url_upload

urlpatterns = [
    path('admin/', admin.site.urls),

    path('api/gerar-url-upload/', gerar_url_upload, name='gerar_url_upload'),
    
   
    path('accounts/', include('accounts.urls')),

    path('', TemplateView.as_view(template_name='landing.html'), name='landing_page'),
    
    # As Rotas de Autenticação Nativas do Django
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(next_page='landing_page'), name='logout'),
    

    path('', include('projects.urls')), 
]

