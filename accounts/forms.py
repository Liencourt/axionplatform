from django import forms
from django.contrib.auth.models import User

class RegistroSaaSForm(forms.Form):
    nome_completo = forms.CharField(max_length=150, widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: João Silva'}))
    email = forms.EmailField(widget=forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'joao@suaempresa.com'}))
    nome_empresa = forms.CharField(max_length=255, widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Nome da sua Empresa'}))
    senha = forms.CharField(label="Palavra-passe", widget=forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Crie uma palavra-passe forte'}))
    confirmar_senha = forms.CharField(label="Confirmar Palavra-passe", widget=forms.PasswordInput(attrs={'class': 'form-control', 'placeholder': 'Repita a palavra-passe'}))

    def clean_email(self):
        email = self.cleaned_data.get('email')
        # Garante que não existem duas contas com o mesmo email
        if User.objects.filter(username=email).exists():
            raise forms.ValidationError("Este email já está registado na plataforma.")
        return email

    def clean(self):
        cleaned_data = super().clean()
        senha = cleaned_data.get("senha")
        confirmar_senha = cleaned_data.get("confirmar_senha")

        # Validação de segurança básica
        if senha and confirmar_senha and senha != confirmar_senha:
            raise forms.ValidationError("As palavras-passe não coincidem.")
        return cleaned_data