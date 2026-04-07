from django.db import models
from django.core.validators import MinValueValidator
from accounts.models import Empresa
import logging

  
class Loja(models.Model):
    """
    Permite que uma Empresa tenha múltiplas filiais (Ex: Grupo Pão de Açúcar -> Loja Leblon)
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE, related_name='lojas')
    nome = models.CharField(max_length=255, help_text="Ex: Filial RJ-Centro")
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    # Localização da filial — usados para enriquecimento IBGE por setor censitário
    cep = models.CharField(max_length=9, blank=True, null=True)
    numero = models.CharField(
        max_length=20, blank=True, null=True,
        help_text="Número do endereço da filial (ex: 2701). Melhora a precisão do geocoding.",
    )
    bairro = models.CharField(
        max_length=150, blank=True, null=True,
        help_text="Bairro conforme ViaCEP / cadastro manual",
    )
    lat = models.FloatField(blank=True, null=True, help_text="Latitude (geocoding automático)")
    lon = models.FloatField(blank=True, null=True, help_text="Longitude (geocoding automático)")
    codigo_ibge = models.CharField(
        max_length=10, blank=True, null=True,
        help_text="Código IBGE do município da filial (sobrepõe o da Empresa quando preenchido)",
    )

    def __str__(self):
        return f"{self.nome} ({self.empresa.nome})"
    

class EventoCalendario(models.Model):
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE, related_name='eventos')
    loja = models.ForeignKey(Loja, on_delete=models.CASCADE, null=True, blank=True, related_name='eventos')
    nome = models.CharField(max_length=255, help_text="Ex: Black Week, Aniversário")
    
    # NOVOS CAMPOS:
    data_inicio = models.DateField()
    data_fim = models.DateField()

    class Meta:
        verbose_name = "Evento de Calendário"
        verbose_name_plural = "Eventos de Calendário"
        unique_together = ('empresa', 'loja', 'nome', 'data_inicio', 'data_fim')

    def __str__(self):
        escopo = self.loja.nome if self.loja else "Global"
        return f"{self.nome} | {self.data_inicio.strftime('%d/%m/%Y')} a {self.data_fim.strftime('%d/%m/%Y')} ({escopo})"
    


class ProjetoPrecificacao(models.Model):
    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE, related_name='projetos')
    loja = models.ForeignKey(Loja, on_delete=models.CASCADE, null=True, blank=True, related_name='projetos_pricing')
    nome = models.CharField(max_length=255)
    
    # Motor AutoML: Guarda as variáveis que o cliente arrastou na tela
    configuracao_variaveis = models.JSONField(default=dict)
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.nome} ({self.empresa.nome})"

class ResultadoPrecificacao(models.Model):
    projeto = models.ForeignKey('ProjetoPrecificacao', on_delete=models.CASCADE, related_name='resultados')
    loja = models.ForeignKey('Loja', on_delete=models.CASCADE, null=True, blank=True, related_name='resultados_precificacao')
    codigo_produto = models.CharField(max_length=100)
    
    # Diagnóstico Científico
    elasticidade = models.FloatField()
    elasticidade_ic_lower = models.FloatField(null=True, blank=True)
    elasticidade_ic_upper = models.FloatField(null=True, blank=True)
    elasticidade_p_value = models.FloatField(null=True, blank=True)
    r_squared = models.FloatField(null=True, blank=True)
    shapiro_p_value = models.FloatField(null=True, blank=True)
    detalhes_variaveis = models.JSONField(default=dict)

    # Dados Financeiros
    custo_unitario = models.FloatField(validators=[MinValueValidator(0.0)])
    preco_atual = models.FloatField(validators=[MinValueValidator(0.0)])
    
    # Preenchidos automaticamente
    preco_sugerido = models.FloatField(blank=True, null=True)
    revisado_pelo_usuario = models.BooleanField(default=False)
    margem_projetada = models.FloatField(blank=True, null=True)

    class Meta:
        # A IA agora pode salvar uma elasticidade de Itaipu e uma do Leblon para o mesmo projeto/produto
        unique_together = ('projeto', 'loja', 'codigo_produto')

    def save(self, *args, **kwargs):
        try:
            empresa = self.projeto.empresa
            fator_piso = 1.0 + (empresa.margem_minima_padrao / 100.0)    
            fator_choque = empresa.limite_variacao_preco / 100.0         
            
            if self.elasticidade < -1.0:
                preco_teorico = self.custo_unitario * (self.elasticidade / (1 + self.elasticidade))
            else:
                preco_teorico = self.custo_unitario * 1.45  
            
            limite_inferior_choque = self.preco_atual * (1.0 - fator_choque)
            limite_superior_choque = self.preco_atual * (1.0 + fator_choque)
            
            preco_com_choque = min(max(preco_teorico, limite_inferior_choque), limite_superior_choque)

            piso_margem_calculado = self.custo_unitario * fator_piso
            self.preco_sugerido = max(preco_com_choque, piso_margem_calculado)
            
            self.margem_projetada = ((self.preco_sugerido - self.custo_unitario) / self.preco_sugerido) * 100.0

        except Exception as e:
            empresa = self.projeto.empresa
            fator_piso = 1.0 + (empresa.margem_minima_padrao / 100.0)
            self.preco_sugerido = self.custo_unitario * fator_piso
            self.margem_projetada = empresa.margem_minima_padrao
            
        super().save(*args, **kwargs)

class VendaHistoricaDW(models.Model):
    """
    Data Warehouse Multitenant: Armazena o histórico cru de vendas (OBT).
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE, related_name='vendas_dw')
    loja = models.ForeignKey('Loja', on_delete=models.CASCADE, null=True, blank=True, related_name='vendas_dw')
    projeto = models.ForeignKey('ProjetoPrecificacao', on_delete=models.SET_NULL, null=True, blank=True)
    
    codigo_produto = models.CharField(max_length=100)
    nome_produto = models.CharField(max_length=255, null=True, blank=True) 
    data_venda = models.DateField()
    quantidade = models.FloatField()
    preco_praticado = models.FloatField()
    custo_unitario = models.FloatField(null=True, blank=True)
    
    # Covariáveis Extras (Clima, Feriado, etc)
    variaveis_extras = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Venda Histórica (DW)"
        verbose_name_plural = "Vendas Históricas (DW)"
        # CORREÇÃO DA BOMBA: Agora a trava considera a Loja!
        unique_together = ('empresa', 'loja', 'codigo_produto', 'data_venda')
        indexes = [
            models.Index(fields=['empresa', 'loja', 'codigo_produto', 'data_venda']),
        ]

    def __str__(self):
        loja_nome = self.loja.nome if self.loja else "Global"
        return f"{self.codigo_produto} ({loja_nome}) | {self.data_venda}"
    

class PrevisaoDemanda(models.Model):
    """
    Guarda o resultado do Motor de Árvores de Decisão (Axiom Forecast - Micro)
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE)
    loja = models.ForeignKey('Loja', on_delete=models.CASCADE, null=True, blank=True, related_name='previsoes_demanda')
    codigo_produto = models.CharField(max_length=100)
    data_geracao = models.DateTimeField(auto_now_add=True)
    
    # Previsão Futura e Histórico
    dados_previsao = models.JSONField(default=dict, help_text="Datas futuras e quantidades previstas")
    
    # A Mágica do SHAP
    explicabilidade_shap = models.JSONField(default=dict, help_text="Impacto percentual de cada variável")
    
    # Métricas de Qualidade do Modelo
    acuracia_r2 = models.FloatField(null=True, blank=True)
    erro_medio = models.FloatField(null=True, blank=True)

    class Meta:
        verbose_name = "Previsão de Demanda"
        verbose_name_plural = "Previsões de Demanda"
        # Garante que não temos previsões duplicadas no mesmo dia para a mesma loja e produto
        unique_together = ('empresa', 'loja', 'codigo_produto', 'data_geracao')

    def __str__(self):
        loja_nome = self.loja.nome if self.loja else "Global"
        return f"Forecast: {self.codigo_produto} ({loja_nome}) | {self.data_geracao.strftime('%d/%m/%Y')}"


class PrevisaoFaturamentoMacro(models.Model):
    """
    Guarda o resultado do Motor Macro (Facebook Prophet) para a Empresa toda.
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE)
    loja = models.ForeignKey(Loja, on_delete=models.CASCADE, null=True, blank=True, related_name='previsoes_macro')
    data_geracao = models.DateTimeField(auto_now_add=True)
    
    # JSON completo retornado pelo Prophet (com intervalos de confiança)
    dados_forecast = models.JSONField(default=dict, help_text="Previsão de Receita (yhat, yhat_lower, yhat_upper)")
    
    # JSON com as componentes separadas para desenharmos os gráficos de sazonalidade
    componentes_sazonalidade = models.JSONField(default=dict, help_text="Tendência, Sazonalidade Semanal e Anual")
    
    faturamento_projetado_total = models.FloatField(default=0, help_text="Soma da receita prevista no período")

    class Meta:
        verbose_name = "Previsão de Faturamento"
        verbose_name_plural = "Previsões de Faturamento"

    def __str__(self):
        return f"Macro Forecast: {self.empresa.nome} | Gerado em {self.data_geracao.strftime('%d/%m/%Y')}"

class FaturamentoEmpresaDW(models.Model):
    """
    Data Warehouse Macro: Guarda apenas a linha do tempo de faturamento global (Data + Valor).
    Arquivo minúsculo, otimizado para o Prophet.
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE)
    loja = models.ForeignKey(Loja, on_delete=models.CASCADE, null=True, blank=True, related_name='faturamentos_macro')
    data_faturamento = models.DateField()
    faturamento_total = models.FloatField()

    class Meta:
        verbose_name = "Faturamento Macro"
        verbose_name_plural = "Faturamentos Macro"
        # Garante que não teremos duas linhas para o mesmo dia na mesma empresa
        unique_together = ('empresa', 'loja', 'data_faturamento') 

    def __str__(self):
        return f"{self.empresa.nome} | {self.data_faturamento} | R$ {self.faturamento_total}"


class CorrelacaoAnalise(models.Model):
    """
    Persiste o resultado de uma rodada de Enriquecimento + Correlações para um projeto.
    Evita refazer as chamadas de API a cada visita à tela.
    """
    STATUS_CHOICES = [
        ('concluido',        'Concluído'),
        ('sem_dados',        'Sem dados suficientes'),
        ('sem_localizacao',  'Empresa sem localização cadastrada'),
        ('erro',             'Erro durante análise'),
    ]

    projeto  = models.ForeignKey(ProjetoPrecificacao, on_delete=models.CASCADE,
                                  related_name='correlacoes')
    empresa  = models.ForeignKey(Empresa, on_delete=models.CASCADE,
                                  related_name='correlacoes')
    criado_em = models.DateTimeField(auto_now_add=True)
    status   = models.CharField(max_length=20, choices=STATUS_CHOICES, default='concluido')

    # Resultados
    correlacoes      = models.JSONField(default=list)
    insights         = models.JSONField(default=list)
    resumo_executivo = models.TextField(blank=True)

    # Metadados da análise
    n_registros          = models.IntegerField(default=0)
    estacao_codigo       = models.CharField(max_length=20, blank=True, null=True)
    estacao_nome         = models.CharField(max_length=100, blank=True, null=True)
    distancia_estacao_km = models.FloatField(null=True, blank=True)
    ibge_municipio       = models.CharField(max_length=150, blank=True, null=True)
    ibge_classe          = models.CharField(max_length=2, blank=True, null=True)
    # Snapshot completo dos dados IBGE no momento da análise
    ibge_dados           = models.JSONField(default=dict, blank=True)
    # Granularidade geográfica atingida pelo enriquecimento
    ibge_bairro       = models.CharField(max_length=150, blank=True, null=True)
    ibge_setor_codigo = models.CharField(max_length=15, blank=True, null=True)
    ibge_nivel_geo    = models.CharField(
        max_length=20, blank=True, null=True,
        help_text="'setor', 'municipio' ou 'nenhum' — indica a precisão do enriquecimento IBGE",
    )

    class Meta:
        verbose_name = "Análise de Correlações"
        verbose_name_plural = "Análises de Correlações"
        ordering = ['-criado_em']

    def __str__(self):
        return f"Correlações — {self.projeto.nome} | {self.criado_em.strftime('%d/%m/%Y %H:%M')}"


# ══════════════════════════════════════════════════════════════════════════════
# AXIOM TREND RADAR
# ══════════════════════════════════════════════════════════════════════════════

class RadarConfig(models.Model):
    """
    Configuração de monitoramento do Trend Radar por empresa.
    Criada automaticamente na primeira execução do scan.
    """
    empresa = models.OneToOneField(
        'accounts.Empresa', on_delete=models.CASCADE, related_name='radar_config'
    )
    # Palavras-chave customizadas (além das geradas automaticamente do catálogo)
    palavras_chave = models.JSONField(
        default=list,
        help_text="Lista de keywords customizados para monitorar (ex: ['azeite', 'leite condensado'])",
    )
    # Geração automática a partir dos nomes de produto do catálogo
    usar_catalogo_automatico = models.BooleanField(default=True)
    # Fontes ativas
    fontes_ativas = models.JSONField(
        default=list,
        help_text="Fontes ativas: google_trends, newsapi, rss, reddit",
    )
    # Limiar de aceleração (%) para gerar alerta
    limiar_aceleracao = models.FloatField(
        default=50.0,
        help_text="Aceleração mínima (%) para registrar uma tendência (padrão: 50%)",
    )
    # Credenciais opcionais por empresa (sobrepõe settings.py)
    newsapi_key = models.CharField(max_length=100, blank=True, null=True)
    reddit_client_id = models.CharField(max_length=100, blank=True, null=True)
    reddit_client_secret = models.CharField(max_length=200, blank=True, null=True)

    ativo = models.BooleanField(default=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Configuração do Radar"
        verbose_name_plural = "Configurações do Radar"

    def __str__(self):
        return f"RadarConfig — {self.empresa.nome}"


class TendenciaDetectada(models.Model):
    """
    Onda de tendência detectada pelo Trend Radar para uma empresa.
    Gerada pelo pipeline.executar_scan().
    """
    NIVEL_CHOICES = [
        ('baixo',    'Baixo'),
        ('moderado', 'Moderado'),
        ('alto',     'Alto'),
        ('viral',    'Viral'),
    ]
    CLASSIF_CHOICES = [
        ('positivo', 'Positivo'),
        ('negativo', 'Negativo'),
        ('neutro',   'Neutro'),
    ]

    empresa = models.ForeignKey(
        'accounts.Empresa', on_delete=models.CASCADE, related_name='tendencias'
    )
    projeto = models.ForeignKey(
        'ProjetoPrecificacao',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='tendencias',
    )
    palavra_chave = models.CharField(max_length=200)

    # Métricas de sinal
    nivel = models.CharField(max_length=20, choices=NIVEL_CHOICES)
    aceleracao_pct = models.FloatField(help_text="Aceleração composta (%) das menções")
    mencoes_recentes = models.FloatField(default=0)
    mencoes_baseline = models.FloatField(default=0)
    aceleracao_por_fonte = models.JSONField(
        default=dict,
        help_text="{'google_trends': 280.0, 'newsapi': 150.0, ...}",
    )

    # Classificação
    classificacao = models.CharField(max_length=20, choices=CLASSIF_CHOICES)
    confianca = models.FloatField(default=0.5, help_text="Confiança da classificação (0–1)")

    # Dados das fontes coletadas
    fontes = models.JSONField(
        default=list,
        help_text="[{fonte, mencoes_recentes, mencoes_baseline, titulos}]",
    )

    # SKUs relacionados do catálogo da empresa
    skus_relacionados = models.JSONField(
        default=list,
        help_text="[{codigo_produto, nome_produto, similaridade, elasticidade}]",
    )

    # Recomendação prescritiva
    recomendacao = models.JSONField(
        default=dict,
        help_text="{preco_acao, preco_pct, estoque_acao, estoque_pct, gondola, janela_min, janela_max}",
    )

    # Status
    visualizado = models.BooleanField(default=False)
    arquivado = models.BooleanField(default=False)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Tendência Detectada"
        verbose_name_plural = "Tendências Detectadas"
        ordering = ['-criado_em', '-aceleracao_pct']
        indexes = [
            models.Index(fields=['empresa', 'arquivado', '-criado_em']),
        ]

    def __str__(self):
        return f"{self.get_nivel_display().upper()} | {self.palavra_chave} ({self.empresa.nome})"

    @property
    def emoji_nivel(self) -> str:
        if self.classificacao == "negativo":
            return {"viral": "🚨", "alto": "⚠️", "moderado": "⚡", "baixo": "👁️"}.get(self.nivel, "👁️")
        return {"viral": "🔥", "alto": "🚀", "moderado": "📈", "baixo": "👁️"}.get(self.nivel, "👁️")

    @property
    def aceleracao_formatada(self) -> str:
        return f"+{self.aceleracao_pct:.0f}%"


# ══════════════════════════════════════════════════════════════════════════════
# AXIOM REPUTATION — Análise de Sentimento de Avaliações do Google
# ══════════════════════════════════════════════════════════════════════════════

class ReputacaoConfig(models.Model):
    """
    Configuração do lugar monitorado por empresa.
    Uma empresa monitora um único Place ID do Google (pode trocar a qualquer momento).
    """
    empresa = models.OneToOneField(
        'accounts.Empresa', on_delete=models.CASCADE, related_name='reputacao_config'
    )
    # Termo de busca usado pelo usuário
    nome_busca = models.CharField(max_length=255, help_text="Nome digitado pelo usuário na busca")

    # Dados do lugar confirmado pelo usuário
    google_place_id   = models.CharField(max_length=255, unique=True)
    google_place_nome = models.CharField(max_length=255)
    google_place_endereco = models.CharField(max_length=500, blank=True)
    google_place_url  = models.URLField(blank=True, help_text="Link do Google Maps")
    google_place_foto = models.URLField(blank=True, help_text="URL da foto do lugar")

    configurado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em  = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Configuração de Reputação"
        verbose_name_plural = "Configurações de Reputação"

    def __str__(self):
        return f"Reputação — {self.google_place_nome} ({self.empresa.nome})"


class AnaliseReputacao(models.Model):
    """
    Resultado de uma análise de sentimento para um lugar no Google.
    Múltiplos registros por empresa (histórico de scans).
    Cooldown: 7 dias entre scans.
    """
    STATUS_CHOICES = [
        ('concluido',    'Concluído'),
        ('sem_reviews',  'Sem avaliações textuais'),
        ('erro',         'Erro'),
    ]
    SENTIMENTO_CHOICES = [
        ('positivo', 'Positivo'),
        ('negativo', 'Negativo'),
        ('neutro',   'Neutro'),
    ]

    empresa = models.ForeignKey(
        'accounts.Empresa', on_delete=models.CASCADE, related_name='analises_reputacao'
    )
    projeto = models.ForeignKey(
        'ProjetoPrecificacao',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='analises_reputacao',
    )
    config = models.ForeignKey(
        ReputacaoConfig, on_delete=models.CASCADE, related_name='analises'
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    status    = models.CharField(max_length=20, choices=STATUS_CHOICES, default='concluido')

    # Snapshot dos dados do Google no momento do scan
    rating_geral      = models.FloatField(null=True, blank=True)
    total_avaliacoes  = models.IntegerField(default=0)

    # Resultado da análise de sentimento (Claude Haiku)
    sentimento_geral  = models.CharField(max_length=20, choices=SENTIMENTO_CHOICES, blank=True)
    score_sentimento  = models.IntegerField(
        default=0, help_text="Índice de satisfação 0–100"
    )
    temas_positivos   = models.JSONField(default=list)
    temas_negativos   = models.JSONField(default=list)
    resumo_executivo  = models.TextField(blank=True)

    # Reviews coletadas (JSON completo com sentimento individual)
    reviews = models.JSONField(
        default=list,
        help_text="[{autor, rating, texto, data_iso, data_relativa, sentimento, temas}]",
    )

    # Auditoria de custo da API
    tokens_input  = models.IntegerField(default=0)
    tokens_output = models.IntegerField(default=0)

    class Meta:
        verbose_name = "Análise de Reputação"
        verbose_name_plural = "Análises de Reputação"
        ordering = ['-criado_em']
        indexes = [
            models.Index(fields=['empresa', '-criado_em']),
        ]

    def __str__(self):
        return (
            f"Reputação — {self.config.google_place_nome} | "
            f"{self.criado_em.strftime('%d/%m/%Y')} | score={self.score_sentimento}"
        )

    @property
    def custo_usd(self) -> float:
        return (self.tokens_input * 0.80 + self.tokens_output * 4.00) / 1_000_000

    @property
    def cor_score(self) -> str:
        """Cor CSS baseada no score de satisfação."""
        if self.score_sentimento >= 70:
            return "#10b981"   # verde
        if self.score_sentimento >= 50:
            return "#f59e0b"   # âmbar
        return "#ef4444"       # vermelho

    @property
    def label_score(self) -> str:
        s = self.score_sentimento
        if s >= 85: return "Excelente"
        if s >= 70: return "Bom"
        if s >= 50: return "Regular"
        if s >= 30: return "Ruim"
        return "Crítico"
