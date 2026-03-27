import uuid
from django.db import models
from django.contrib.auth.models import User

class Empresa(models.Model):
    nome = models.CharField(max_length=255)
    cnpj = models.CharField(max_length=18, unique=True)
    
    
    email_contato = models.EmailField(help_text="E-mail principal para faturamento e alertas do sistema", blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True, null=True)
    responsavel_tecnico = models.CharField(max_length=100, help_text="Nome do gestor de Pricing", blank=True, null=True)
 
    
    ativo = models.BooleanField(default=True)
    margem_minima_padrao = models.FloatField(default=18.0)
    limite_variacao_preco = models.FloatField(default=20.0)
    criado_em = models.DateTimeField(auto_now_add=True)

    # ==========================================
    # ASSINATURA E INTEGRAÇÃO STRIPE
    # ==========================================
    stripe_customer_id = models.CharField(
        max_length=100, 
        blank=True, 
        null=True, 
        help_text="ID do cliente no Stripe (ex: cus_12345)"
    )
    stripe_subscription_id = models.CharField(
        max_length=100, 
        blank=True, 
        null=True, 
        help_text="ID da assinatura ativa (ex: sub_12345)"
    )
    is_active_subscriber = models.BooleanField(
        default=False,
        help_text="Verdadeiro se a empresa tem uma assinatura paga e ativa."
    )

    # ==========================================
    # INTEGRAÇÃO B2B — API KEY
    # ==========================================
    api_key = models.UUIDField(
        default=uuid.uuid4,
        unique=True,
        editable=False,
        help_text="Chave de autenticação para integração via API REST (B2B). Enviar no header X-Axiom-API-Key."
    )

    def __str__(self):
        return self.nome

class UsuarioEmpresa(models.Model):
    """Extensão do usuário padrão do Django para vinculá-lo a uma Empresa"""
    usuario = models.OneToOneField(User, on_delete=models.CASCADE)
    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE, related_name='usuarios')
    cargo = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f"{self.usuario.username} - {self.empresa.nome}"

