from django.db import models
from accounts.models import Empresa


class Concorrente(models.Model):
    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE, related_name='concorrentes')
    nome = models.CharField(max_length=255, help_text="Ex: Supermercados Guanabara")
    programa_fidelidade = models.CharField(
        max_length=100, blank=True, null=True,
        help_text="Ex: Guana Clube, Clube Extra — ou vazio se não houver",
    )
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Concorrente'
        verbose_name_plural = 'Concorrentes'
        unique_together = ('empresa', 'nome')
        ordering = ['nome']

    def __str__(self):
        return f"{self.nome} ({self.empresa.nome})"


class ExtraçãoEncarte(models.Model):
    STATUS_PENDENTE = 'pendente'
    STATUS_PROCESSANDO = 'processando'
    STATUS_CONCLUIDO = 'concluido'
    STATUS_ERRO = 'erro'

    STATUS_CHOICES = [
        (STATUS_PENDENTE, 'Pendente'),
        (STATUS_PROCESSANDO, 'Processando'),
        (STATUS_CONCLUIDO, 'Concluído'),
        (STATUS_ERRO, 'Erro'),
    ]

    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE, related_name='extracoes_encarte')
    concorrente = models.ForeignKey(Concorrente, on_delete=models.CASCADE, related_name='extracoes')
    arquivo_pdf = models.FileField(upload_to='encartes/pdfs/')
    vigencia_inicio = models.DateField(help_text="Início da validade das ofertas")
    vigencia_fim = models.DateField(help_text="Fim da validade das ofertas")
    data_extracao = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDENTE)

    # Métricas da extração (preenchidas após processamento)
    total_paginas = models.IntegerField(default=0)
    total_produtos = models.IntegerField(default=0)
    total_precos = models.IntegerField(default=0)
    precos_clube = models.IntegerField(default=0)
    precos_promocional = models.IntegerField(default=0)
    modelo_extracao = models.CharField(max_length=100, default='claude-opus-4-5')
    avisos = models.JSONField(default=list)
    erro_mensagem = models.TextField(blank=True, null=True)

    class Meta:
        verbose_name = 'Extração de Encarte'
        verbose_name_plural = 'Extrações de Encarte'
        ordering = ['-data_extracao']

    def __str__(self):
        return f"{self.concorrente.nome} | {self.vigencia_inicio.strftime('%d/%m/%Y')} a {self.vigencia_fim.strftime('%d/%m/%Y')}"

    @property
    def vigencia_str(self):
        return f"{self.vigencia_inicio.strftime('%d/%m/%Y')} a {self.vigencia_fim.strftime('%d/%m/%Y')}"

    @property
    def status_badge_class(self):
        return {
            self.STATUS_PENDENTE: 'secondary',
            self.STATUS_PROCESSANDO: 'warning',
            self.STATUS_CONCLUIDO: 'success',
            self.STATUS_ERRO: 'danger',
        }.get(self.status, 'secondary')


class ProdutoEncarte(models.Model):
    extracao = models.ForeignKey(ExtraçãoEncarte, on_delete=models.CASCADE, related_name='produtos')
    pagina = models.IntegerField(default=1)
    nome = models.CharField(max_length=500)
    marca = models.CharField(max_length=255, blank=True, null=True)
    categoria = models.CharField(max_length=100, blank=True, null=True)
    quantidade = models.CharField(max_length=100, blank=True, null=True, help_text="Ex: 5kg, 500ml, pack c/12 473ml")
    ean = models.CharField(max_length=20, blank=True, null=True)
    validade_oferta = models.CharField(max_length=100, blank=True, null=True)
    condicao_especial = models.CharField(max_length=500, blank=True, null=True, help_text="Ex: leve 3 pague 2")

    class Meta:
        verbose_name = 'Produto do Encarte'
        verbose_name_plural = 'Produtos do Encarte'
        ordering = ['pagina', 'nome']

    def __str__(self):
        return f"{self.nome} ({self.extracao.concorrente.nome})"

    @property
    def preco_principal(self):
        """Retorna o menor preço disponível (clube > promocional > normal)."""
        precos = self.precos.all()
        for tipo in ('clube', 'promocional', 'condicional', 'normal'):
            p = precos.filter(tipo=tipo).first()
            if p:
                return p
        return precos.first()


class PrecoEncarte(models.Model):
    TIPO_NORMAL = 'normal'
    TIPO_PROMOCIONAL = 'promocional'
    TIPO_CLUBE = 'clube'
    TIPO_CONDICIONAL = 'condicional'

    TIPO_CHOICES = [
        (TIPO_NORMAL, 'Normal'),
        (TIPO_PROMOCIONAL, 'Promocional'),
        (TIPO_CLUBE, 'Clube'),
        (TIPO_CONDICIONAL, 'Condicional'),
    ]

    produto = models.ForeignKey(ProdutoEncarte, on_delete=models.CASCADE, related_name='precos')
    valor = models.DecimalField(max_digits=10, decimal_places=2)
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_NORMAL)
    canal = models.CharField(max_length=50, blank=True, null=True, help_text="Ex: loja, app, online")
    condicao = models.CharField(max_length=500, blank=True, null=True, help_text="Ex: Guana Clube, embalagem 400ml")

    class Meta:
        verbose_name = 'Preço do Encarte'
        verbose_name_plural = 'Preços do Encarte'
        ordering = ['tipo', 'valor']

    def __str__(self):
        return f"R$ {self.valor} ({self.tipo}) — {self.produto.nome}"

    @property
    def tipo_badge_class(self):
        return {
            self.TIPO_NORMAL: 'secondary',
            self.TIPO_PROMOCIONAL: 'warning',
            self.TIPO_CLUBE: 'primary',
            self.TIPO_CONDICIONAL: 'info',
        }.get(self.tipo, 'secondary')
