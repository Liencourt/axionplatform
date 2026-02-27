import pandas as pd
import logging
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.db.models import Count
import tempfile
import os
from django.contrib import messages
import json
import numpy as np
import statsmodels.formula.api as smf
from scipy.stats import shapiro
from .models import ProjetoPrecificacao, ResultadoPrecificacao, VendaHistoricaDW
from django.shortcuts import get_object_or_404
import csv
from django.http import HttpResponse
from django.http import JsonResponse
import json
from .services import treinar_previsao_xgboost,treinar_previsao_macro_empresa
from .models import PrevisaoDemanda,PrevisaoFaturamentoMacro

logger = logging.getLogger(__name__)
@login_required
def iniciar_projeto_upload(request):
    """Passo 1: Recebe o arquivo e extrai as colunas (Versão sem engolir erros de HTML)"""
    if not hasattr(request.user, 'usuarioempresa'):
        messages.error(request, "Seu usuário não está vinculado a uma Empresa.")
        return redirect('admin:index')

    if request.method == 'POST' and request.FILES.get('arquivo_dados'):
        arquivo = request.FILES['arquivo_dados']
        nome_projeto = request.POST.get('nome_projeto', 'Novo Projeto')
        
        contexto = None # Inicializa vazio
        
        try:
            # 1. Salva o arquivo fisicamente
            extensao = '.csv' if arquivo.name.endswith('.csv') else '.xlsx'
            fd, caminho_temp = tempfile.mkstemp(suffix=extensao)
            
            with os.fdopen(fd, 'wb') as f:
                for chunk in arquivo.chunks():
                    f.write(chunk)
            
            request.session['caminho_arquivo_temp'] = caminho_temp

            # 2. Pandas lê o arquivo
            if extensao == '.csv':
                try:
                    df = pd.read_csv(caminho_temp, sep=None, engine='python', encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(caminho_temp, sep=None, engine='python', encoding='latin-1')
            else:
                df = pd.read_excel(caminho_temp)

            df.dropna(axis=1, how='all', inplace=True) 

            # 3. Mapeia as colunas
            colunas_numericas = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            colunas_categoricas = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()

            contexto = {
                'nome_projeto': nome_projeto,
                'colunas_numericas': colunas_numericas,
                'colunas_categoricas': colunas_categoricas,
            }
            
        except Exception as e:
            logger.error(f"Erro fatal ao processar upload: {e}")
            messages.error(request, f"Falha na leitura do arquivo (CSV/Excel): {e}")
            
        # FORA DO TRY/EXCEPT!
        # Se o contexto foi criado com sucesso, renderiza a próxima tela.
        # Se a próxima tela tiver erro de código, o Django vai nos avisar de forma clara!
        if contexto:
            return render(request, 'projects/construtor_hipoteses.html', contexto)
            
    return render(request, 'projects/upload_dados.html')


def tratar_nan(valor):
    """Utilitário de Arquitetura: Limpa sujeiras matemáticas antes de ir para o JSON do Banco"""
    if pd.isna(valor) or np.isinf(valor):
        return 0.0
    return float(valor)

@login_required
def processar_modelo_dinamico(request):
    if request.method == 'POST':
        try:
            nome_projeto = request.POST.get('nome_projeto')
            config_json = request.POST.get('configuracao_variaveis')
            config = json.loads(config_json)
            
            sku_col = config.get('sku_col')
            data_col = config.get('data_col')
            target_col = config.get('target')
            preco_col = config.get('preco')
            custo_col = config.get('custo_col')
            variaveis_extras = config.get('variaveis_extras', [])

            caminho_arquivo = request.session.get('caminho_arquivo_temp')
            
            if not caminho_arquivo:
                messages.error(request, "A sessão expirou. Faça o upload novamente.")
                return redirect('iniciar_projeto_upload')
                
            # 1. Leitura Robusta
            if caminho_arquivo.endswith('.csv'):
                try:
                    df = pd.read_csv(caminho_arquivo, sep=None, engine='python', encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(caminho_arquivo, sep=None, engine='python', encoding='latin-1')
            else:
                df = pd.read_excel(caminho_arquivo)
            
            # 2. Blindagem e Limpeza
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
            df[preco_col] = pd.to_numeric(df[preco_col], errors='coerce')
            df[custo_col] = pd.to_numeric(df[custo_col], errors='coerce')
            df.dropna(subset=[sku_col, data_col, target_col, preco_col], inplace=True)
            
            # Formata a data para garantir que o banco de dados entenda
            df[data_col] = pd.to_datetime(df[data_col], errors='coerce')
            df.dropna(subset=[data_col], inplace=True)

            empresa_cliente = request.user.usuarioempresa.empresa
            projeto = ProjetoPrecificacao.objects.create(
                empresa=empresa_cliente,
                nome=nome_projeto,
                configuracao_variaveis=config
            )

            # ==============================================================
            # PASSO A: INGESTÃO NO DATA WAREHOUSE (DW)
            # ==============================================================
            lista_vendas_dw = []
            nomes_variaveis_extras = [v['nome'] for v in variaveis_extras]

            for index, row in df.iterrows():
                # Monta o JSON das covariáveis dinâmicas para essa linha
                dict_extras = {}
                for var in nomes_variaveis_extras:
                    if var in row and pd.notna(row[var]):
                        dict_extras[var] = row[var]

                lista_vendas_dw.append(VendaHistoricaDW(
                    empresa=empresa_cliente,
                    projeto=projeto,
                    codigo_produto=str(row[sku_col]),
                    nome_produto=str(row[sku_col]), # Temporariamente usando o SKU como Nome
                    data_venda=row[data_col].date(),
                    quantidade=float(row[target_col]),
                    preco_praticado=float(row[preco_col]),
                    custo_unitario=float(row[custo_col]) if pd.notna(row[custo_col]) else None,
                    variaveis_extras=dict_extras
                ))

            # bulk_create é 100x mais rápido que salvar um por um
            # ignore_conflicts=True evita o erro se o cliente subir dados repetidos
            VendaHistoricaDW.objects.bulk_create(lista_vendas_dw, ignore_conflicts=True)

            # ==============================================================
            # PASSO B: ENGENHARIA DE DATAS (Feature Engineering)
            # ==============================================================
            df_model = df[(df[target_col] > 0) & (df[preco_col] > 0)].copy()
            df_model['log_y'] = np.log(df_model[target_col])
            df_model['log_p'] = np.log(df_model[preco_col])

            mapa_dias = {0: 'Segunda', 1: 'Terca', 2: 'Quarta', 3: 'Quinta', 4: 'Sexta', 5: 'Sabado', 6: 'Domingo'}
            df_model['dia_semana_auto'] = df_model[data_col].dt.dayofweek.map(mapa_dias)

            df_model = df_model.sort_values(by=[sku_col, data_col])

            # Constrói a fórmula estatística
            termos_formula = ["log_p", "C(dia_semana_auto)"] # Dia da semana injetado automaticamente!
            for var in variaveis_extras:
                nome = var['nome']
                termos_formula.append(f"C({nome})" if var['tipo'] == 'cat' else nome)

            formula_final = f"log_y ~ {' + '.join(termos_formula)}"
            
            # ==============================================================
            # PASSO C: TREINAMENTO DO MODELO (AutoML)
            # ==============================================================
            produtos_processados = 0
            
            for sku, df_sku in df_model.groupby(sku_col):
                if len(df_sku) < 10:
                    continue
                
                try:
                    modelo = smf.ols(formula_final, data=df_sku).fit()
                    stat_shapiro, p_shapiro = shapiro(modelo.resid)
                    
                    detalhes_vars = {}
                    for termo in modelo.pvalues.index:
                        if termo != 'Intercept' and termo != 'log_p': 
                            detalhes_vars[termo] = {
                                "p_valor": tratar_nan(modelo.pvalues[termo]),
                                "coeficiente": tratar_nan(modelo.params[termo]),
                                "status": "Relevante" if modelo.pvalues[termo] < 0.05 else "Ruído" 
                            }

                    ResultadoPrecificacao.objects.create(
                        projeto=projeto,
                        codigo_produto=str(sku),
                        elasticidade=tratar_nan(modelo.params.get('log_p', 0)),
                        r_squared=tratar_nan(modelo.rsquared),
                        shapiro_p_value=tratar_nan(p_shapiro),
                        detalhes_variaveis=detalhes_vars,
                        custo_unitario=df_sku[custo_col].mean(),
                        preco_atual=df_sku[preco_col].mean()
                    )
                    produtos_processados += 1
                    
                except Exception as e:
                    print(f"Erro estatístico no SKU {sku}: {e}")

            if produtos_processados == 0:
                messages.error(request, "Nenhum resultado gerado. Verifique os dados.")
                return redirect('iniciar_projeto_upload')

            messages.success(request, f"Sucesso! Dados salvos no DW e {produtos_processados} produtos processados.")
            return redirect('dashboard_resultado', projeto_id=projeto.id)

        except Exception as e:
            messages.error(request, f"Erro crítico: {e}")
            return redirect('iniciar_projeto_upload')

    return redirect('iniciar_projeto_upload')


@login_required
def dashboard_resultado(request, projeto_id):
    """
    Passo 3: Exibe o resultado agregado no formato 'SaaS Executivo'.
    """
    empresa_cliente = request.user.usuarioempresa.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa_cliente)
    
    resultados_db = projeto.resultados.all()
    
    if not resultados_db.exists():
        messages.error(request, "Nenhum resultado pôde ser gerado para este projeto. Verifique os dados.")
        return redirect('iniciar_projeto_upload')

    # Contadores para os Cards Superiores
    total_analisados = resultados_db.count()
    elasticos_count = 0
    inelasticos_count = 0
    
    # Processamento da Lógica de Negócio para a Tabela
    resultados_processados = []
    
    for res in resultados_db:
        # Lógica 1: Comportamento
        if res.elasticidade < -1.0:
            comportamento = "Sensível"
            cor_comp = "danger"  # Vermelho
            elasticos_count += 1
        else:
            comportamento = "Fiel"
            cor_comp = "success" # Verde
            inelasticos_count += 1
            
        # Lógica 2: Confiança (Ajustada para a realidade do Varejo)
        r2 = res.r_squared if res.r_squared else 0
        if r2 >= 0.50:
            confianca = "Alta"
        elif r2 >= 0.30:
            confianca = "Média"
        else:
            confianca = "Baixa"
            
        resultados_processados.append({
            'id': res.id,
            'sku': res.codigo_produto,
            'elasticidade': res.elasticidade,
            'comportamento': comportamento,
            'cor_comp': cor_comp,
            'confianca': confianca,
            'preco_atual': res.preco_atual,
            'preco_sugerido': res.preco_sugerido,
        })

    contexto = {
        'projeto': projeto,
        'total_analisados': total_analisados,
        'elasticos_count': elasticos_count,
        'inelasticos_count': inelasticos_count,
        'resultados': resultados_processados,
    }
    
    return render(request, 'projects/painel.html', contexto)

@login_required
def exportar_resultados_erp(request, projeto_id):
    """
    Gera o CSV final com os preços aprovados pelo cliente no Simulador.
    """
    empresa = request.user.usuarioempresa.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)
    resultados = projeto.resultados.all()

    # Prepara a resposta forçando o download como arquivo CSV
    response = HttpResponse(
        content_type='text/csv',
        headers={'Content-Disposition': f'attachment; filename="Axiom_Precos_{projeto.id}.csv"'},
    )
    
    # Adiciona o BOM para o Excel do Windows abrir com acentos corretos
    response.write(u'\ufeff'.encode('utf8')) 
    
    # O Ponto-e-Vírgula é o padrão para ERPs brasileiros
    writer = csv.writer(response, delimiter=';')
    
    # Cabeçalho da Tabela
    writer.writerow(['Codigo_SKU', 'Preco_Base_Atual', 'Preco_Aprovado_ERP', 'Indice_Elasticidade', 'Comportamento'])

    for res in resultados:
        comp = "Sensível" if res.elasticidade < -1.0 else "Fiel"

        
        if res.revisado_pelo_usuario and res.preco_sugerido:
            preco_final = res.preco_sugerido
            status = "Aprovado"
        else:
            preco_final = res.preco_atual
            status = "Pendente (Mantido Preço Base)"
        
        # Garante que os números vão sair com vírgula para não quebrar no ERP
        preco_atual_br = str(round(res.preco_atual, 2)).replace('.', ',')
        preco_final_br = str(round(preco_final, 2)).replace('.', ',')
        elasticidade_br = str(round(res.elasticidade, 3)).replace('.', ',')

        writer.writerow([
            res.codigo_produto,
            preco_atual_br,
            preco_final_br,
            status,
            elasticidade_br,
            comp
        ])

    return response

@login_required
def simulador_produto(request, resultado_id):
    """
    Simulador Avançado: Lê o Data Warehouse para plotar o histórico real (Bolhas)
    e projeta cenários usando a regressão AutoML.
    """
    empresa = request.user.usuarioempresa.empresa
    resultado = get_object_or_404(ResultadoPrecificacao, id=resultado_id, projeto__empresa=empresa)
    
    # Busca o histórico deste SKU no DW
    vendas = VendaHistoricaDW.objects.filter(
        projeto=resultado.projeto,
        codigo_produto=resultado.codigo_produto
    ).values('data_venda', 'quantidade', 'preco_praticado')

    dados_grafico = {}
    demanda_base_diaria = 100.0 # Valor padrão de segurança

    if vendas.exists():
        # Transforma os dados do banco num DataFrame do Pandas rapidinho
        df = pd.DataFrame.from_records(vendas)
        df['preco_praticado'] = df['preco_praticado'].astype(float).round(2)
        df['quantidade'] = df['quantidade'].astype(float)

        # AGORA É REAL: Puxa a média real de vendas diárias desse SKU!
        demanda_base_diaria = df['quantidade'].mean()

        # Filtro de Outliers (A Matemática que você já usava no BigQuery)
        Q1 = df['quantidade'].quantile(0.25)
        Q3 = df['quantidade'].quantile(0.75)
        IQR = Q3 - Q1
        teto_maximo = Q3 + (1.5 * IQR)

        df_normal = df[df['quantidade'] <= teto_maximo]
        df_outlier = df[df['quantidade'] > teto_maximo]

        # Agrupa para formar as "Bolhas" (Preço -> Média de Qtd -> Frequência)
        df_grouped = df_normal.groupby('preco_praticado').agg(
            qtd_media=('quantidade', 'mean'),
            frequencia=('data_venda', 'count')
        ).reset_index()

        # Calcula o tamanho da bolha visual no gráfico (mínimo 10, máximo 35)
        df_grouped['tamanho'] = np.log1p(df_grouped['frequencia']) * 12
        df_grouped['tamanho'] = df_grouped['tamanho'].clip(lower=10, upper=35)

        # Empacota para o Javascript
        dados_grafico = {
            'x_normal': df_grouped['preco_praticado'].tolist(),
            'y_normal': df_grouped['qtd_media'].tolist(),
            'sizes': df_grouped['tamanho'].tolist(),
            'freqs': df_grouped['frequencia'].tolist(),
            'x_outlier': df_outlier['preco_praticado'].tolist(),
            'y_outlier': df_outlier['quantidade'].tolist()
        }
    
    contexto = {
        'resultado': resultado,
        'demanda_base': demanda_base_diaria,
        'margem_minima': empresa.margem_minima_padrao,
        'limite_choque': empresa.limite_variacao_preco,
        'detalhes_json': json.dumps(resultado.detalhes_variaveis),
        'historico_json': json.dumps(dados_grafico)
    }
    
    return render(request, 'projects/simulador.html', contexto)

@login_required
def lista_projetos(request):
    """
    Hub de Projetos: Lista todos os estudos de precificação da empresa do usuário logado.
    """
    empresa = request.user.usuarioempresa.empresa
    
    # Busca os projetos e já conta quantos 'resultados' (SKUs) cada um tem
    projetos = ProjetoPrecificacao.objects.filter(empresa=empresa).annotate(
        total_skus=Count('resultados')
    ).order_by('-id') # Ordena do mais recente para o mais antigo
    
    contexto = {
        'projetos': projetos
    }
    return render(request, 'projects/lista_projetos.html', contexto)

@login_required
def excluir_projeto(request, projeto_id):
    """
    Permite ao cliente apagar um estudo antigo para limpar o painel.
    """
    empresa = request.user.usuarioempresa.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)
    
    if request.method == 'POST':
        projeto.delete()
        messages.success(request, f"O projeto '{projeto.nome}' foi excluído com sucesso.")
        
    return redirect('lista_projetos')

@login_required
def configuracoes_conta(request):
    """
    Painel de Gestão da Conta: Permite ao usuário editar o próprio perfil
    e as regras de negócio da empresa (Margem e Limites).
    """
    usuario = request.user
    empresa = usuario.usuarioempresa.empresa

    if request.method == 'POST':
        # Verifica qual dos dois formulários foi enviado
        if 'btn_salvar_perfil' in request.POST:
            usuario.first_name = request.POST.get('first_name', '')
            usuario.last_name = request.POST.get('last_name', '')
            usuario.email = request.POST.get('email', usuario.email)
            usuario.save()
            messages.success(request, "Seus dados pessoais foram atualizados com sucesso.")
            
        elif 'btn_salvar_empresa' in request.POST:
            empresa.nome = request.POST.get('nome_empresa', empresa.nome)
            try:
                # Converte os valores financeiros, tratando possíveis vírgulas
                margem_str = request.POST.get('margem_minima', str(empresa.margem_minima_padrao)).replace(',', '.')
                limite_str = request.POST.get('limite_variacao', str(empresa.limite_variacao_preco)).replace(',', '.')
                
                empresa.margem_minima_padrao = float(margem_str)
                empresa.limite_variacao_preco = float(limite_str)
                empresa.save()
                messages.success(request, "As regras de negócio da empresa foram atualizadas!")
            except ValueError:
                messages.error(request, "Erro: Digite apenas números válidos na margem e limite.")
                
        return redirect('configuracoes_conta')

    contexto = {
        'usuario': usuario,
        'empresa': empresa
    }
    return render(request, 'projects/configuracoes.html', contexto)

@login_required
def salvar_preco_simulado(request, resultado_id):
    """
    Recebe o preço final escolhido pelo usuário no Simulador via AJAX
    e FORÇA a atualização direta no banco de dados.
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            # Trata vírgulas caso alguma escape do front-end
            preco_limpo = str(data.get('preco')).replace(',', '.')
            novo_preco = float(preco_limpo)
            
            empresa = request.user.usuarioempresa.empresa
            
            # Força o UPDATE direto no SQL, superando qualquer problema de cache
            linhas_afetadas = ResultadoPrecificacao.objects.filter(
                id=resultado_id, 
                projeto__empresa=empresa
            ).update(preco_sugerido=novo_preco,
                     revisado_pelo_usuario=True)
            
            if linhas_afetadas > 0:
                print(f"SUCESSO: Preço do SKU atualizado para {novo_preco}")
                return JsonResponse({'status': 'success', 'message': 'Gravado com sucesso!'})
            else:
                return JsonResponse({'status': 'error', 'message': 'Produto não encontrado ou sem permissão.'}, status=404)
                
        except Exception as e:
            print(f"ERRO AO SALVAR PREÇO: {e}")
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
            
    return JsonResponse({'status': 'invalid method'}, status=405)

@login_required
def painel_forecast(request, sku):
    """
    Carrega a tela do Axiom Forecast para um SKU específico.
    Se a IA já rodou, carrega os gráficos. Se não, mostra o botão para gerar.
    """
    empresa = request.user.usuarioempresa.empresa
    
    # Pega a previsão mais recente gerada para este SKU
    previsao = PrevisaoDemanda.objects.filter(empresa=empresa, codigo_produto=sku).last()
    
    # Busca o nome do produto no Data Warehouse para ficar bonito na tela
    produto_info = VendaHistoricaDW.objects.filter(empresa=empresa, codigo_produto=sku).first()
    nome_produto = produto_info.nome_produto if produto_info else sku

    contexto = {
        'sku': sku,
        'nome_produto': nome_produto,
        'previsao': previsao,
    }
    return render(request, 'projects/forecast.html', contexto)

@login_required
def gerar_forecast_action(request, sku):
    """
    Ação do botão: Chama o Motor do XGBoost no services.py e salva no banco.
    """
    empresa = request.user.usuarioempresa.empresa
    
    # Chama o motor que está no seu services.py
    sucesso, mensagem = treinar_previsao_xgboost(empresa, sku, dias_futuros=30)
    
    if sucesso:
        messages.success(request, "Motor Preditivo executado! Previsão de 30 dias gerada com sucesso.")
    else:
        messages.error(request, f"Erro na IA: {mensagem}")
        
    return redirect('painel_forecast', sku=sku)

@login_required
def painel_macro_forecast(request):
    """
    O Dashboard do CFO: Mostra a projeção de faturamento da empresa inteira.
    """
    empresa = request.user.usuarioempresa.empresa
    
    # Pega a última previsão gerada para a empresa
    previsao = PrevisaoFaturamentoMacro.objects.filter(empresa=empresa).last()

    contexto = {
        'previsao': previsao,
    }
    return render(request, 'projects/macro_forecast.html', contexto)

@login_required
def gerar_macro_forecast_action(request):
    """
    Dispara o treinamento do Facebook Prophet para prever a receita global.
    """
    empresa = request.user.usuarioempresa.empresa
    
    sucesso, mensagem = treinar_previsao_macro_empresa(empresa, dias_futuros=90)
    
    if sucesso:
        messages.success(request, mensagem)
    else:
        messages.error(request, f"Atenção: {mensagem}")
        
    return redirect('painel_macro_forecast')