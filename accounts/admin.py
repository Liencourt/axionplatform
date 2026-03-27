from django.contrib import admin
from .models import Empresa, UsuarioEmpresa


@admin.register(Empresa)
class EmpresaAdmin(admin.ModelAdmin):
    list_display = ('nome', 'cnpj', 'ativo', 'is_active_subscriber', 'api_key', 'criado_em')
    list_filter = ('ativo', 'is_active_subscriber')
    search_fields = ('nome', 'cnpj', 'email_contato')
    readonly_fields = ('api_key', 'criado_em')

    fieldsets = (
        ('Dados da Empresa', {
            'fields': ('nome', 'cnpj', 'email_contato', 'telefone', 'responsavel_tecnico', 'ativo')
        }),
        ('Configurações de Pricing', {
            'fields': ('margem_minima_padrao', 'limite_variacao_preco')
        }),
        ('Assinatura Stripe', {
            'fields': ('is_active_subscriber', 'stripe_customer_id', 'stripe_subscription_id'),
            'classes': ('collapse',)
        }),
        ('Integração API (B2B)', {
            'fields': ('api_key',),
            'description': 'Chave para integração via header X-Axiom-API-Key. Somente leitura — gerada automaticamente.'
        }),
    )


@admin.register(UsuarioEmpresa)
class UsuarioEmpresaAdmin(admin.ModelAdmin):
    list_display = ('usuario', 'empresa', 'cargo')
    search_fields = ('usuario__username', 'empresa__nome')
