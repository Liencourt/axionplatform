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
    projeto = models.ForeignKey(ProjetoPrecificacao, on_delete=models.CASCADE, related_name='resultados')
    codigo_produto = models.CharField(max_length=100)
    
    # Diagnóstico Científico
    elasticidade = models.FloatField()
    r_squared = models.FloatField(null=True, blank=True)
    shapiro_p_value = models.FloatField(null=True, blank=True)
    detalhes_variaveis = models.JSONField(default=dict)

    # Dados Financeiros
    custo_unitario = models.FloatField(validators=[MinValueValidator(0.0)])
    preco_atual = models.FloatField(validators=[MinValueValidator(0.0)])
    
    # Preenchidos automaticamente pelo método save()
    preco_sugerido = models.FloatField(blank=True, null=True)
    revisado_pelo_usuario = models.BooleanField(default=False)
    margem_projetada = models.FloatField(blank=True, null=True)

    class Meta:
        unique_together = ('projeto', 'codigo_produto')

    def save(self, *args, **kwargs):
        try:
            # Puxando as regras dinâmicas da Empresa logada
            empresa = self.projeto.empresa
            fator_piso = 1.0 + (empresa.margem_minima_padrao / 100.0)    # Ex: 1.18
            fator_choque = empresa.limite_variacao_preco / 100.0         # Ex: 0.20
            
            # 1. Cálculo Base (Markup Ótimo / Índice de Lerner)
            if self.elasticidade < -1.0:
                preco_teorico = self.custo_unitario * (self.elasticidade / (1 + self.elasticidade))
            else:
                preco_teorico = self.custo_unitario * 1.45  # Margem alvo em caso inelástico
            
            # 2. Trava Anti-Choque Dinâmica
            limite_inferior_choque = self.preco_atual * (1.0 - fator_choque)
            limite_superior_choque = self.preco_atual * (1.0 + fator_choque)
            
            preco_com_choque = min(max(preco_teorico, limite_inferior_choque), limite_superior_choque)

            # 3. Piso de Margem Dinâmico
            piso_margem_calculado = self.custo_unitario * fator_piso
            self.preco_sugerido = max(preco_com_choque, piso_margem_calculado)
            
            # 4. Calcula a margem final projetada (%)
            self.margem_projetada = ((self.preco_sugerido - self.custo_unitario) / self.preco_sugerido) * 100.0

        except Exception as e:
            # Failsafe: Se der erro, protege com o piso configurado
            empresa = self.projeto.empresa
            fator_piso = 1.0 + (empresa.margem_minima_padrao / 100.0)
            self.preco_sugerido = self.custo_unitario * fator_piso
            self.margem_projetada = empresa.margem_minima_padrao
            
        super().save(*args, **kwargs)

class VendaHistoricaDW(models.Model):
    """
    Data Warehouse Multitenant: Armazena o histórico cru de vendas de cada cliente (OBT).
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE, related_name='vendas_dw')
    loja = models.ForeignKey(Loja, on_delete=models.CASCADE, null=True, blank=True, related_name='vendas_dw')
    projeto = models.ForeignKey(ProjetoPrecificacao, on_delete=models.SET_NULL, null=True, blank=True)
    
    
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
        # A trava de segurança contra duplicidade!
        unique_together = ('empresa', 'codigo_produto', 'data_venda')
        indexes = [
            models.Index(fields=['empresa', 'codigo_produto', 'data_venda']),
        ]

    def __str__(self):
        nome = self.nome_produto if self.nome_produto else "Sem Nome"
        return f"{self.codigo_produto} - {nome} | {self.data_venda}"


class PrevisaoDemanda(models.Model):
    """
    Guarda o resultado do Motor de Árvores de Decisão (Axiom Forecast)
    """
    empresa = models.ForeignKey('accounts.Empresa', on_delete=models.CASCADE)
    codigo_produto = models.CharField(max_length=100)
    data_geracao = models.DateTimeField(auto_now_add=True)
    
    # Previsão Futura e Histórico (JSON com datas e valores)
    dados_previsao = models.JSONField(default=dict, help_text="Datas futuras e quantidades previstas")
    
    # A Mágica do SHAP: O peso de cada variável na decisão da IA
    explicabilidade_shap = models.JSONField(default=dict, help_text="Impacto percentual de cada variável")
    
    # Métricas de Qualidade do Modelo
    acuracia_r2 = models.FloatField(null=True, blank=True)
    erro_medio = models.FloatField(null=True, blank=True)

    class Meta:
        verbose_name = "Previsão de Demanda"
        verbose_name_plural = "Previsões de Demanda"

    def __str__(self):
        return f"Forecast: {self.codigo_produto} | Gerado em {self.data_geracao.strftime('%d/%m/%Y')}"


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
  