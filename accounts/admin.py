from django.contrib import admin
from .models import Empresa, UsuarioEmpresa

# Register your models here.
admin.site.register(Empresa)
admin.site.register(UsuarioEmpresa)