from django.contrib import admin
from .models import ProjetoPrecificacao, ResultadoPrecificacao, VendaHistoricaDW

@admin.register(ProjetoPrecificacao)
class ProjetoPrecificacaoAdmin(admin.ModelAdmin):
    # O que aparece nas colunas
    list_display = ('nome', 'empresa', 'id')
    # Barra de pesquisa
    search_fields = ('nome', 'empresa__nome')
    # Filtro lateral
    list_filter = ('empresa',)

@admin.register(ResultadoPrecificacao)
class ResultadoPrecificacaoAdmin(admin.ModelAdmin):
    list_display = ('codigo_produto', 'projeto', 'elasticidade', 'preco_atual', 'preco_sugerido','revisado_pelo_usuario')
    search_fields = ('codigo_produto', 'nome_produto', 'projeto__nome')
    list_filter = ('projeto__empresa', 'projeto')
    # Permite ordenar clicando na coluna
    ordering = ('codigo_produto',)

@admin.register(VendaHistoricaDW)
class VendaHistoricaDWAdmin(admin.ModelAdmin):
    list_display = ('codigo_produto', 'nome_produto', 'data_venda', 'quantidade', 'preco_praticado', 'empresa')
    search_fields = ('codigo_produto', 'nome_produto')
    list_filter = ('empresa', 'data_venda')
    # Mostra até 100 vendas por página para facilitar a leitura
    list_per_page = 100
    # Ordena das vendas mais recentes para as mais antigas
    ordering = ('-data_venda',)