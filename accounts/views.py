from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages

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