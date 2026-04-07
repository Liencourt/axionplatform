from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from .models import Empresa, UsuarioEmpresa
from .forms import RegistroSaaSForm
from django.db import transaction
from django.contrib.auth import login
from django.contrib.auth.models import User


@login_required
def configuracoes_empresa(request):
    """Permite ao administrador do Tenant configurar as réguas de negócio do motor Axiom"""
    empresa = request.user.usuarioempresa.empresa
    
    if request.method == 'POST':
        try:
            nova_margem = float(request.POST.get('margem_minima', 18.0))
            novo_choque = float(request.POST.get('limite_choque', 20.0))
            
            # Validação de segurança básica para o usuário não colocar números bizarros
            if nova_margem < 0 or novo_choque < 0:
                messages.error(request, "Os valores não podem ser negativos.")
            else:
                empresa.margem_minima_padrao = nova_margem
                empresa.limite_variacao_preco = novo_choque
                empresa.save()
                messages.success(request, "Regras de negócio da empresa atualizadas com sucesso!")
                
        except ValueError:
            messages.error(request, "Por favor, insira apenas números válidos.")
            
        return redirect('configuracoes_empresa')

    return render(request, 'accounts/configuracoes.html', {'empresa': empresa})

def sign_up(request):
    # Se o utilizador já estiver autenticado, manda-o para a plataforma
    if request.user.is_authenticated:
        return redirect('painel_calendario') # Pode mudar para a rota do seu Dashboard Principal

    if request.method == 'POST':
        form = RegistroSaaSForm(request.POST)
        if form.is_valid():
            try:
                # ==========================================
                # O SEGREDO DO SAAS: TRANSAÇÃO ATÓMICA
                # ==========================================
                with transaction.atomic():
                    # 1. Cria o Utilizador (usamos o email como username)
                    email = form.cleaned_data['email']
                    senha = form.cleaned_data['senha']
                    nome_completo = form.cleaned_data['nome_completo']
                    
                    partes_nome = nome_completo.split()
                    primeiro_nome = partes_nome[0]
                    ultimo_nome = " ".join(partes_nome[1:]) if len(partes_nome) > 1 else ""
                    
                    user = User.objects.create_user(
                        username=email,
                        email=email,
                        password=senha,
                        first_name=primeiro_nome,
                        last_name=ultimo_nome
                    )
                    
                    # 2. Cria o Tenant (A Empresa isolada)
                    empresa = Empresa.objects.create(
                        nome=form.cleaned_data['nome_empresa'],
                        cnpj=form.cleaned_data['cnpj'],
                        cep=form.cleaned_data.get('cep') or '',
                        numero=form.cleaned_data.get('numero') or None,
                        municipio=form.cleaned_data.get('municipio') or '',
                        uf=form.cleaned_data.get('uf') or '',
                        codigo_ibge=form.cleaned_data.get('codigo_ibge') or '',
                        email_contato=email,
                    )
                    
                    # 3. Cria o Vínculo VIP (A ponte entre os dois)
                    UsuarioEmpresa.objects.create(usuario=user, empresa=empresa)
                    
                # 4. Faz o Login Automático (Fora da transação, pois o banco já guardou os dados)
                login(request, user)
                messages.success(request, f"Bem-vindo(a) à Axiom, {user.first_name}! O seu ambiente empresarial foi criado com sucesso.")
                
                # Redireciona o cliente recém-criado direto para dentro da plataforma
                return redirect('painel_calendario') # Ajuste para a rota do Dashboard se preferir
                
            except Exception as e:
                messages.error(request, f"Ocorreu um erro inesperado ao criar a sua conta. Tente novamente.")
                print(f"[AXIOM ERRO] Falha no Sign-up: {e}")
    else:
        form = RegistroSaaSForm()
        
    return render(request, 'accounts/signup.html', {'form': form})