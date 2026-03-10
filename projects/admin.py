from django.contrib import admin
from .models import (ProjetoPrecificacao, ResultadoPrecificacao, 
                     Loja,EventoCalendario,VendaHistoricaDW,FaturamentoEmpresaDW)


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

@admin.register(FaturamentoEmpresaDW)
class FaturamentoEmpresaDWAdmin(admin.ModelAdmin):
    list_display = ('empresa', 'data_faturamento', 'faturamento_total')
    list_filter = ('empresa', 'data_faturamento')
    search_fields = ('empresa__nome',)
    date_hierarchy = 'data_faturamento'

# Registra a Loja para você poder editar os nomes se quiser
@admin.register(Loja)
class LojaAdmin(admin.ModelAdmin):
    list_display = ('nome', 'empresa', 'ativo', 'criado_em')
    list_filter = ('empresa', 'ativo')

# Registra o Calendário
@admin.register(EventoCalendario)
class EventoCalendarioAdmin(admin.ModelAdmin):
    list_display = ('nome', 'data_inicio', 'data_fim', 'empresa', 'loja')
    list_filter = ('empresa', 'loja')