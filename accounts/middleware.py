from django.utils.deprecation import MiddlewareMixin
from django.core.exceptions import PermissionDenied

class TenantMiddleware(MiddlewareMixin):
    """
    Middleware Multi-Tenant: Intercepta todas as requisições, identifica a empresa 
    do usuário logado e anexa ao objeto 'request'. Se o usuário não tiver empresa, bloqueia.
    """
    def process_request(self, request):
        # Só faz a checagem se o usuário estiver logado e não for da área administrativa pura
        if request.user.is_authenticated and not request.path.startswith('/admin/'):
            try:
                # Tenta buscar a empresa atrelada ao perfil do usuário
                empresa = request.user.usuarioempresa.empresa
                
                # "Grampeia" a empresa na requisição para as views usarem
                request.empresa = empresa
                
            except AttributeError:
                # Se o usuário não tem o perfil 'usuarioempresa' configurado, acesso negado!
                raise PermissionDenied("Seu usuário não está vinculado a nenhuma Empresa (Tenant).")