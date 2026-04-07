from django.contrib import admin
from .models import Concorrente, ExtraçãoEncarte, ProdutoEncarte, PrecoEncarte


class PrecoInline(admin.TabularInline):
    model = PrecoEncarte
    extra = 0
    readonly_fields = ('valor', 'tipo', 'canal', 'condicao')


class ProdutoInline(admin.TabularInline):
    model = ProdutoEncarte
    extra = 0
    readonly_fields = ('nome', 'marca', 'categoria', 'quantidade', 'pagina')
    show_change_link = True


@admin.register(Concorrente)
class ConcorrenteAdmin(admin.ModelAdmin):
    list_display = ('nome', 'empresa', 'programa_fidelidade', 'ativo', 'criado_em')
    list_filter = ('ativo', 'empresa')
    search_fields = ('nome',)


@admin.register(ExtraçãoEncarte)
class ExtraçãoEncarteAdmin(admin.ModelAdmin):
    list_display = ('concorrente', 'vigencia_inicio', 'vigencia_fim', 'status', 'total_produtos', 'data_extracao')
    list_filter = ('status', 'concorrente', 'empresa')
    readonly_fields = ('data_extracao', 'total_paginas', 'total_produtos', 'total_precos', 'precos_clube', 'precos_promocional', 'avisos', 'erro_mensagem')
    inlines = [ProdutoInline]


@admin.register(ProdutoEncarte)
class ProdutoEncarteAdmin(admin.ModelAdmin):
    list_display = ('nome', 'marca', 'categoria', 'quantidade', 'pagina')
    list_filter = ('categoria', 'extracao__concorrente')
    search_fields = ('nome', 'marca')
    inlines = [PrecoInline]
