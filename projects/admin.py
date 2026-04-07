from django.contrib import admin
from django.utils.html import format_html
from .models import (ProjetoPrecificacao, ResultadoPrecificacao,
                     Loja, EventoCalendario, VendaHistoricaDW,
                     FaturamentoEmpresaDW, CorrelacaoAnalise,
                     RadarConfig, TendenciaDetectada,
                     ReputacaoConfig, AnaliseReputacao)


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


@admin.register(CorrelacaoAnalise)
class CorrelacaoAnaliseAdmin(admin.ModelAdmin):
    list_display = (
        'projeto', 'empresa', 'status_badge', 'n_registros',
        'n_correlacoes', 'n_insights', 'estacao_resumo',
        'ibge_resumo', 'criado_em',
    )
    list_filter  = ('status', 'empresa', 'ibge_classe')
    search_fields = ('projeto__nome', 'empresa__nome', 'ibge_municipio', 'estacao_codigo')
    ordering      = ('-criado_em',)
    readonly_fields = (
        'projeto', 'empresa', 'criado_em', 'status',
        'n_registros', 'estacao_codigo', 'estacao_nome', 'distancia_estacao_km',
        'ibge_municipio', 'ibge_classe',
        'correlacoes_formatadas', 'insights_formatados', 'resumo_executivo',
    )
    fieldsets = (
        ('Identificação', {
            'fields': ('projeto', 'empresa', 'criado_em', 'status', 'n_registros'),
        }),
        ('Estação INMET', {
            'fields': ('estacao_codigo', 'estacao_nome', 'distancia_estacao_km'),
        }),
        ('Dados IBGE', {
            'fields': ('ibge_municipio', 'ibge_classe'),
        }),
        ('Resultados', {
            'fields': ('resumo_executivo', 'correlacoes_formatadas', 'insights_formatados'),
        }),
    )

    # ── Colunas calculadas ────────────────────────────────────────────────

    @admin.display(description='Status')
    def status_badge(self, obj):
        cores = {
            'concluido':       ('success', '✔ Concluído'),
            'sem_dados':       ('warning', '⚠ Sem dados'),
            'sem_localizacao': ('secondary', '📍 Sem localização'),
            'erro':            ('danger',  '✖ Erro'),
        }
        cor, label = cores.get(obj.status, ('secondary', obj.status))
        return format_html(
            '<span style="padding:2px 8px;border-radius:4px;font-size:.8rem;'
            'font-weight:600;background:var(--bs-{}-bg,#eee);color:var(--bs-{});">{}</span>',
            cor, cor, label,
        )

    @admin.display(description='Correlações')
    def n_correlacoes(self, obj):
        n = len(obj.correlacoes) if obj.correlacoes else 0
        return n or '—'

    @admin.display(description='Insights')
    def n_insights(self, obj):
        n = len(obj.insights) if obj.insights else 0
        return n or '—'

    @admin.display(description='Estação INMET')
    def estacao_resumo(self, obj):
        if not obj.estacao_codigo:
            return '—'
        dist = f' · {obj.distancia_estacao_km:.0f} km' if obj.distancia_estacao_km else ''
        return f'{obj.estacao_codigo}{dist}'

    @admin.display(description='Município / Classe')
    def ibge_resumo(self, obj):
        if not obj.ibge_municipio:
            return '—'
        classe = f' (Classe {obj.ibge_classe})' if obj.ibge_classe else ''
        return f'{obj.ibge_municipio}{classe}'

    # ── Campos de detalhe legíveis ────────────────────────────────────────

    @admin.display(description='Correlações (detalhes)')
    def correlacoes_formatadas(self, obj):
        if not obj.correlacoes:
            return '—'
        linhas = []
        for c in obj.correlacoes:
            sinal = '▲' if c.get('correlacao', 0) >= 0 else '▼'
            linhas.append(
                f"{sinal} <strong>{c.get('variavel','')}</strong> "
                f"r={c.get('correlacao',0):.3f} · p={c.get('p_value',0):.4f} "
                f"· {c.get('forca','')}"
            )
        return format_html('<br>'.join(linhas))

    @admin.display(description='Insights (detalhes)')
    def insights_formatados(self, obj):
        if not obj.insights:
            return '—'
        linhas = []
        for i in obj.insights:
            prio_cores = {'alta': '#dc3545', 'media': '#fd7e14', 'baixa': '#6c757d'}
            cor = prio_cores.get(i.get('prioridade', ''), '#6c757d')
            linhas.append(
                f'<div style="margin-bottom:10px;padding:8px 10px;'
                f'border-left:3px solid {cor};background:#f9f9f9;">'
                f'<strong>{i.get("titulo","")}</strong><br>'
                f'<span style="color:#555;font-size:.9em">{i.get("acao","")}</span>'
                f'</div>'
            )
        return format_html(''.join(linhas))


# ── Axiom Trend Radar ─────────────────────────────────────────────────────────

@admin.register(RadarConfig)
class RadarConfigAdmin(admin.ModelAdmin):
    list_display  = ('empresa', 'limiar_aceleracao', 'usar_catalogo_automatico', 'fontes_resumo', 'ativo', 'atualizado_em')
    list_filter   = ('ativo', 'usar_catalogo_automatico')
    search_fields = ('empresa__nome',)

    @admin.display(description='Fontes Ativas')
    def fontes_resumo(self, obj):
        return ', '.join(obj.fontes_ativas) if obj.fontes_ativas else '—'


@admin.register(TendenciaDetectada)
class TendenciaDetectadaAdmin(admin.ModelAdmin):
    list_display  = ('palavra_chave', 'empresa', 'nivel_badge', 'classif_badge',
                     'aceleracao_formatada', 'n_skus', 'visualizado', 'arquivado', 'criado_em')
    list_filter   = ('nivel', 'classificacao', 'arquivado', 'visualizado', 'empresa')
    search_fields = ('palavra_chave', 'empresa__nome')
    ordering      = ('-criado_em',)
    readonly_fields = (
        'empresa', 'palavra_chave', 'nivel', 'aceleracao_pct', 'classificacao',
        'confianca', 'criado_em', 'fontes_formatadas', 'skus_formatados', 'recomendacao_formatada',
    )
    fieldsets = (
        ('Sinal', {
            'fields': ('empresa', 'palavra_chave', 'nivel', 'aceleracao_pct', 'classificacao', 'confianca', 'criado_em'),
        }),
        ('Fontes de Dados', {'fields': ('fontes_formatadas',)}),
        ('SKUs Relacionados', {'fields': ('skus_formatados',)}),
        ('Recomendação Prescritiva', {'fields': ('recomendacao_formatada',)}),
    )

    @admin.display(description='Nível')
    def nivel_badge(self, obj):
        cores = {'viral': '#ef4444', 'alto': '#f97316', 'moderado': '#f59e0b', 'baixo': '#6b7280'}
        cor = cores.get(obj.nivel, '#6b7280')
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;border-radius:4px;font-size:.8rem;font-weight:700;">{} {}</span>',
            cor, obj.emoji_nivel, obj.get_nivel_display(),
        )

    @admin.display(description='Classificação')
    def classif_badge(self, obj):
        cores = {'positivo': '#10b981', 'negativo': '#ef4444', 'neutro': '#6b7280'}
        cor = cores.get(obj.classificacao, '#6b7280')
        return format_html(
            '<span style="color:{}; font-weight:600;">{}</span>',
            cor, obj.get_classificacao_display(),
        )

    @admin.display(description='Aceleração')
    def aceleracao_formatada(self, obj):
        return obj.aceleracao_formatada

    @admin.display(description='SKUs')
    def n_skus(self, obj):
        return len(obj.skus_relacionados) if obj.skus_relacionados else '—'

    @admin.display(description='Fontes (detalhe)')
    def fontes_formatadas(self, obj):
        if not obj.fontes:
            return '—'
        linhas = []
        for f in obj.fontes:
            linhas.append(
                f'<strong>{f.get("fonte","")}</strong>: '
                f'recente={f.get("mencoes_recentes",0):.0f} / '
                f'baseline={f.get("mencoes_baseline",0):.0f}'
            )
        return format_html('<br>'.join(linhas))

    @admin.display(description='SKUs Relacionados')
    def skus_formatados(self, obj):
        if not obj.skus_relacionados:
            return '—'
        linhas = []
        for s in obj.skus_relacionados:
            elast = f' | elast={s["elasticidade"]:.2f}' if s.get('elasticidade') else ''
            linhas.append(
                f'<strong>{s.get("codigo_produto","")}</strong> — '
                f'{s.get("nome_produto","")} '
                f'(sim={s.get("similaridade",0):.0%}{elast})'
            )
        return format_html('<br>'.join(linhas))

    @admin.display(description='Recomendação')
    def recomendacao_formatada(self, obj):
        r = obj.recomendacao
        if not r:
            return '—'
        return format_html(
            '<strong>Preço:</strong> {} ({:+.0f}%)<br>'
            '<strong>Estoque:</strong> {} ({:+.0f}%)<br>'
            '<strong>Gôndola:</strong> {}<br>'
            '<strong>Janela:</strong> {} – {} dias',
            r.get('preco_acao', '—'), r.get('preco_pct', 0),
            r.get('estoque_acao', '—'), r.get('estoque_pct', 0),
            r.get('gondola', '—'),
            r.get('janela_min', '?'), r.get('janela_max', '?'),
        )


# ── Axiom Reputation ──────────────────────────────────────────────────────────

@admin.register(ReputacaoConfig)
class ReputacaoConfigAdmin(admin.ModelAdmin):
    list_display  = ('empresa', 'google_place_nome', 'google_place_endereco', 'configurado_em')
    search_fields = ('empresa__nome', 'google_place_nome')
    readonly_fields = ('configurado_em', 'atualizado_em')


@admin.register(AnaliseReputacao)
class AnaliseReputacaoAdmin(admin.ModelAdmin):
    list_display  = ('empresa', 'lugar_nome', 'score_badge', 'sentimento_geral',
                     'rating_geral', 'total_avaliacoes', 'n_reviews', 'custo_display', 'criado_em')
    list_filter   = ('sentimento_geral', 'status', 'empresa')
    search_fields = ('empresa__nome', 'config__google_place_nome')
    ordering      = ('-criado_em',)
    readonly_fields = (
        'empresa', 'config', 'criado_em', 'status',
        'rating_geral', 'total_avaliacoes', 'sentimento_geral', 'score_sentimento',
        'temas_positivos', 'temas_negativos', 'resumo_executivo',
        'reviews_formatadas', 'tokens_input', 'tokens_output', 'custo_display',
    )
    fieldsets = (
        ('Identificação', {
            'fields': ('empresa', 'config', 'criado_em', 'status'),
        }),
        ('Google', {
            'fields': ('rating_geral', 'total_avaliacoes'),
        }),
        ('Sentimento (Claude Haiku)', {
            'fields': ('sentimento_geral', 'score_sentimento', 'resumo_executivo',
                       'temas_positivos', 'temas_negativos'),
        }),
        ('Reviews', {
            'fields': ('reviews_formatadas',),
        }),
        ('Auditoria de Custo', {
            'fields': ('tokens_input', 'tokens_output', 'custo_display'),
        }),
    )

    @admin.display(description='Local')
    def lugar_nome(self, obj):
        return obj.config.google_place_nome if obj.config else '—'

    @admin.display(description='Score')
    def score_badge(self, obj):
        cor = obj.cor_score
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 10px;border-radius:12px;font-weight:700;">{}/100</span>',
            cor, obj.score_sentimento,
        )

    @admin.display(description='Reviews')
    def n_reviews(self, obj):
        return len(obj.reviews) if obj.reviews else 0

    @admin.display(description='Custo API')
    def custo_display(self, obj):
        return f"${obj.custo_usd:.5f}"

    @admin.display(description='Avaliações (detalhe)')
    def reviews_formatadas(self, obj):
        if not obj.reviews:
            return '—'
        linhas = []
        for rv in obj.reviews:
            estrelas = '★' * int(rv.get('rating', 0)) + '☆' * (5 - int(rv.get('rating', 0)))
            sent = rv.get('sentimento', '')
            cor  = {'positivo': '#10b981', 'negativo': '#ef4444'}.get(sent, '#6b7280')
            linhas.append(
                f'<div style="margin-bottom:10px;padding:8px;border-left:3px solid {cor};background:#f9f9f9;">'
                f'<strong>{rv.get("autor","")}</strong> {estrelas} — {rv.get("data_relativa","")}<br>'
                f'<span style="color:#555;font-size:.9em">{rv.get("texto","")[:200]}</span>'
                f'</div>'
            )
        return format_html(''.join(linhas))