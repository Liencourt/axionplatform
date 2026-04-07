import re
from django import forms
from django.contrib.auth.models import User


class RegistroSaaSForm(forms.Form):
    nome_completo = forms.CharField(
        max_length=150,
        widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: João Silva'}),
    )
    email = forms.EmailField(
        widget=forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'joao@suaempresa.com'}),
    )
    nome_empresa = forms.CharField(
        max_length=255,
        widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Razão Social ou Nome Fantasia'}),
    )
    cnpj = forms.CharField(
        max_length=18,
        label='CNPJ',
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '00.000.000/0001-00',
            'id': 'id_cnpj',
            'maxlength': '18',
        }),
    )
    # Localização — preenchida automaticamente via ViaCEP no frontend
    cep = forms.CharField(
        max_length=9,
        label='CEP',
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '00000-000',
            'id': 'id_cep',
            'maxlength': '9',
        }),
    )
    numero = forms.CharField(
        max_length=20,
        label='Número',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Ex: 2701',
            'id': 'id_numero',
            'maxlength': '20',
        }),
        help_text='Número do endereço. Melhora a precisão da localização.',
    )
    municipio = forms.CharField(
        max_length=150,
        required=False,
        widget=forms.HiddenInput(attrs={'id': 'id_municipio'}),
    )
    uf = forms.CharField(
        max_length=2,
        required=False,
        widget=forms.HiddenInput(attrs={'id': 'id_uf'}),
    )
    codigo_ibge = forms.CharField(
        max_length=10,
        required=False,
        widget=forms.HiddenInput(attrs={'id': 'id_codigo_ibge'}),
    )
    senha = forms.CharField(
        label='Senha',
        widget=forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Mínimo 8 caracteres'}),
    )
    confirmar_senha = forms.CharField(
        label='Confirmar Senha',
        widget=forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Repita a senha'}),
    )

    def clean_email(self):
        email = self.cleaned_data.get('email')
        if User.objects.filter(username=email).exists():
            raise forms.ValidationError("Este e-mail já está cadastrado na plataforma.")
        return email

    def clean_cnpj(self):
        cnpj_digits = re.sub(r'\D', '', self.cleaned_data.get('cnpj', ''))
        if len(cnpj_digits) != 14:
            raise forms.ValidationError("CNPJ inválido. Informe os 14 dígitos.")
        formatted = f"{cnpj_digits[:2]}.{cnpj_digits[2:5]}.{cnpj_digits[5:8]}/{cnpj_digits[8:12]}-{cnpj_digits[12:]}"
        from .models import Empresa
        # Checa tanto formato com pontuação quanto só dígitos (compatibilidade com registros antigos)
        if Empresa.objects.filter(cnpj__in=[cnpj_digits, formatted]).exists():
            raise forms.ValidationError("Este CNPJ já possui uma conta na plataforma.")
        return formatted

    def clean_cep(self):
        cep = re.sub(r'\D', '', self.cleaned_data.get('cep', ''))
        if len(cep) != 8:
            raise forms.ValidationError("CEP inválido. Informe os 8 dígitos.")
        return f"{cep[:5]}-{cep[5:]}"

    def clean(self):
        cleaned_data = super().clean()
        senha = cleaned_data.get("senha")
        confirmar = cleaned_data.get("confirmar_senha")
        if senha and confirmar and senha != confirmar:
            raise forms.ValidationError("As senhas não coincidem.")
        if senha and len(senha) < 8:
            self.add_error('senha', "A senha deve ter pelo menos 8 caracteres.")
        return cleaned_data