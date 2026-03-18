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
from .models import(ProjetoPrecificacao, ResultadoPrecificacao,
                     VendaHistoricaDW,Loja,PrevisaoDemanda,
                     PrevisaoFaturamentoMacro,FaturamentoEmpresaDW,EventoCalendario)
from .forms import EventoCalendarioForm
from django.shortcuts import get_object_or_404
import csv
from django.http import HttpResponse
from django.http import JsonResponse
from .services import treinar_previsao_xgboost,treinar_previsao_macro_empresa
import stripe
from django.conf import settings
from django.urls import reverse
from accounts.models import Empresa
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg,Sum, F, FloatField,Max
from django.db.models.functions import Coalesce
import math
from scipy.stats.mstats import winsorize
from datetime import timedelta
from google.cloud import storage


logger = logging.getLogger(__name__)

@login_required
def iniciar_projeto_upload(request):
    """Passo 1: Recebe o caminho do arquivo no GCS e extrai as colunas"""

    if request.method == 'POST' and request.POST.get('caminho_gcs'):
        caminho_gcs = request.POST.get('caminho_gcs')
        nome_projeto = request.POST.get('nome_projeto', 'Novo Projeto')
        
        print(f"-> 1. POST Recebido! Caminho GCS: {caminho_gcs}")
        contexto = None 
        
        try:
            # Puxa o nome do bucket igual fizemos na outra função!
            bucket_name = os.getenv('BUCKET_NAME', 'axiom-platform-datasets')
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(caminho_gcs)
            
            print(f"-> 2. Conectou no Bucket '{bucket_name}'. Baixando arquivo...")
            
            extensao = '.csv' if caminho_gcs.lower().endswith('.csv') else '.xlsx'
            fd, caminho_temp = tempfile.mkstemp(suffix=extensao)
            
            with os.fdopen(fd, 'wb') as f:
                blob.download_to_file(f)
            
            print(f"-> 3. Download concluído. Lendo com Pandas...")
            request.session['caminho_arquivo_temp'] = caminho_temp

            if extensao == '.csv':
                try:
                    df = pd.read_csv(caminho_temp, sep=None, engine='python', encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(caminho_temp, sep=None, engine='python', encoding='latin-1')
            else:
                df = pd.read_excel(caminho_temp)

            df.dropna(axis=1, how='all', inplace=True) 
            print(f"-> 4. Sucesso! Colunas encontradas: {df.columns.tolist()}")

            colunas_numericas = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            colunas_categoricas = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()

            contexto = {
                'nome_projeto': nome_projeto,
                'colunas_numericas': colunas_numericas,
                'colunas_categoricas': colunas_categoricas,
            }
            print("-> 5. Tudo certo! Redirecionando para o Construtor de Hipóteses...")
            
        except Exception as e:
            # Esse print vai gritar o erro exato no log do Cloud Run!
            print(f"-> ERRO FATAL NO BACKEND: {str(e)}")
            messages.error(request, f"Falha na leitura do arquivo: {e}")
            
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
            
            # --- NOVAS COLUNAS MAPEADAS ---
            loja_col = config.get('loja_col') # Pode vir vazio se for projeto global
            nome_produto_col = config.get('nome_produto_col') # Pode vir vazio
            
            # Colunas Clássicas
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
            
            df[data_col] = pd.to_datetime(df[data_col], errors='coerce')
            df.dropna(subset=[data_col], inplace=True)

            empresa_cliente = request.empresa
            projeto = ProjetoPrecificacao.objects.create(
                id=None,
                empresa=empresa_cliente,
                nome=nome_projeto,
                configuracao_variaveis=config
            )

            # ==============================================================
            # ARQUITETURA DE ALTA PERFORMANCE: CACHE E CRIAÇÃO AUTOMÁTICA DE LOJAS
            # ==============================================================
            dict_lojas = {}
            if loja_col and loja_col in df.columns:
                lojas_unicas = df[loja_col].dropna().unique()
                for nome_loja in lojas_unicas:
                    nome_str = str(nome_loja).strip()
                    # A Mágica: Se a loja não existir para este cliente, o Django cria agora!
                    loja_obj, created = Loja.objects.get_or_create(
                        empresa=empresa_cliente,
                        nome=nome_str,
                        defaults={'ativo': True} # Pode adicionar outros campos default da sua Loja aqui
                    )
                    dict_lojas[nome_str] = loja_obj # Guarda na memória para usar no laço abaixo

            # ==============================================================
            # PASSO A: INGESTÃO NO DATA WAREHOUSE (DW)
            # ==============================================================
            
            # 1. PREPARAÇÃO SUPER BLINDADA
            df_limpo = df.copy()
            # Troca tudo que é NaN/NaT por None de forma segura nativa do Python
            df_limpo = df_limpo.astype(object).where(pd.notnull(df_limpo), None)
            
            lista_vendas_dw = []
            nomes_variaveis_extras = [v['nome'] for v in variaveis_extras]

            for row in df_limpo.to_dict('records'):
                # Resolve a Loja
                loja_obj = None
                if loja_col and row.get(loja_col) is not None:
                    loja_obj = dict_lojas.get(str(row.get(loja_col)).strip())

                # Resolve o Nome do Produto
                nome_prod = str(row.get(sku_col))
                if nome_produto_col and row.get(nome_produto_col) is not None:
                    nome_prod = str(row.get(nome_produto_col))

                # Purificação Extrema do JSON para evitar erro de Numpy
                dict_extras = {}
                for var in nomes_variaveis_extras:
                    val = row.get(var)
                    if val is not None:
                        # Extrai o valor puro do Python (mata o np.float64, np.int64)
                        if hasattr(val, 'item'):
                            dict_extras[var] = val.item()
                        else:
                            dict_extras[var] = val

                # Extrai a Data nativa (Contorna o bug do pd.to_datetime)
                data_val = row.get(data_col)
                if hasattr(data_val, 'date'):
                    # Se já for um Timestamp, apenas extrai a data silenciosamente
                    data_final = data_val.date()
                else:
                    # Só usa o conversor do Pandas em último caso
                    data_final = pd.to_datetime(str(data_val)).date()

                # Tratamento do Custo
                custo_val = row.get(custo_col)
                custo_final = float(custo_val) if custo_val is not None else None

                lista_vendas_dw.append(VendaHistoricaDW(
                    empresa=empresa_cliente,
                    loja=loja_obj,
                    projeto=projeto,
                    codigo_produto=str(row.get(sku_col)),
                    nome_produto=nome_prod,
                    data_venda=data_final,
                    quantidade=float(row.get(target_col)),
                    preco_praticado=float(row.get(preco_col)),
                    custo_unitario=custo_final,
                    variaveis_extras=dict_extras
                ))

            # Grava no banco em massa voando!
            VendaHistoricaDW.objects.bulk_create(lista_vendas_dw, ignore_conflicts=True)

            # ==============================================================
            # PASSO B: ENGENHARIA DE DATAS (Feature Engineering)
            # ==============================================================
            df_model = df[(df[target_col] > 0) & (df[preco_col] > 0)].copy()
            df_model['log_y'] = np.log(df_model[target_col])
            df_model['log_p'] = np.log(df_model[preco_col])

            mapa_dias = {0: 'Segunda', 1: 'Terca', 2: 'Quarta', 3: 'Quinta', 4: 'Sexta', 5: 'Sabado', 6: 'Domingo'}
            df_model['dia_semana_auto'] = df_model[data_col].dt.dayofweek.map(mapa_dias)

            # Construção da Fórmula
            termos_formula = ["log_p", "C(dia_semana_auto)"]
            for var in variaveis_extras:
                nome = var['nome']
                termos_formula.append(f"C({nome})" if var['tipo'] == 'cat' else nome)

            formula_final = f"log_y ~ {' + '.join(termos_formula)}"

            # ==============================================================
            # PASSO C: TREINAMENTO DO MODELO ISOLADO (MULTI-LOJA) - V8
            # ==============================================================
            produtos_processados = 0
            
            # Define as colunas de agrupamento
            colunas_agrupamento = [sku_col]
            if loja_col and loja_col in df_model.columns:
                colunas_agrupamento = [loja_col, sku_col]
                # Não fazemos o sort aqui, deixamos para fazer isoladamente por SKU!

            for keys, df_sku in df_model.groupby(colunas_agrupamento):
                # Desempacota a chave
                if isinstance(keys, tuple):
                    loja_val, sku = keys
                    loja_obj = dict_lojas.get(str(loja_val).strip())
                else:
                    sku = keys
                    loja_obj = None

                # 1. ORDENAÇÃO TEMPORAL (Vital para o Lag de Preço)
                df_sku = df_sku.sort_values(data_col).copy()

                # 2. QUEBRA DE CAUSALIDADE REVERSA (Lag de Preço)
                df_sku['log_p_lag1'] = df_sku['log_p'].shift(1)
                df_sku.dropna(subset=['log_p_lag1'], inplace=True)

                # 3. TRAVA DE GRAUS DE LIBERDADE (Degrees of Freedom)
                n_params = len(termos_formula) + 7 # Variáveis Extras + 7 Dias da Semana
                min_obs = max(30, n_params * 5)
                
                if len(df_sku) < min_obs:
                    continue # Pula se não tiver maturidade estatística
                
                # Trava de Variação Mínima
                if df_sku['log_p'].nunique() <= 1 or df_sku['log_y'].nunique() <= 1:
                    continue

                try:
                    # 4. BLINDAGEM CONTRA OUTLIERS (Winsorize 2% das pontas do volume de vendas)
                    df_sku['log_y'] = winsorize(df_sku['log_y'], limits=[0.02, 0.02])
                    
                    # 5. AJUSTE DA FÓRMULA (Injetando a variável instrumental)
                    formula_iv = formula_final.replace('log_p', 'log_p + log_p_lag1')
                    
                    # 6. TREINAMENTO
                    modelo = smf.ols(formula_iv, data=df_sku).fit()
                    stat_shapiro, p_shapiro = shapiro(modelo.resid)
                    
                    # 7. EXTRAÇÃO DO INTERVALO DE CONFIANÇA E P-VALUE
                    conf_int = modelo.conf_int()
                    elasticidade_lower = tratar_nan(conf_int.loc['log_p', 0]) if 'log_p' in conf_int.index else 0
                    elasticidade_upper = tratar_nan(conf_int.loc['log_p', 1]) if 'log_p' in conf_int.index else 0
                    elasticidade_pvalue = tratar_nan(modelo.pvalues.get('log_p', 1.0))
                    
                    detalhes_vars = {}
                    for termo in modelo.pvalues.index:
                        # Ignoramos as variáveis base na explicação
                        if termo not in ['Intercept', 'log_p', 'log_p_lag1']: 
                            detalhes_vars[termo] = {
                                "p_valor": tratar_nan(modelo.pvalues[termo]),
                                "coeficiente": tratar_nan(modelo.params[termo]),
                                "status": "Relevante" if modelo.pvalues[termo] < 0.05 else "Ruído" 
                            }

                    # 8. PREÇO E CUSTO BASE SEGUROS (Mediana Recente - 30 dias)
                    df_recente = df_sku.tail(30)
                    preco_base_seguro = df_recente[preco_col].median()
                    if pd.isna(preco_base_seguro):
                        preco_base_seguro = df_sku[preco_col].iloc[-1]
                        
                    custo_base_seguro = df_recente[custo_col].median()
                    if pd.isna(custo_base_seguro):
                        custo_base_seguro = df_sku[custo_col].iloc[-1]

                    # SALVA O RESULTADO NO BANCO!
                    ResultadoPrecificacao.objects.create(
                        id=None,
                        projeto=projeto,
                        loja=loja_obj,
                        codigo_produto=str(sku),
                        elasticidade=tratar_nan(modelo.params.get('log_p', 0)),
                        
                        # OS NOVOS CAMPOS!
                        elasticidade_ic_lower=elasticidade_lower,
                        elasticidade_ic_upper=elasticidade_upper,
                        elasticidade_p_value=elasticidade_pvalue,
                        
                        r_squared=tratar_nan(modelo.rsquared),
                        shapiro_p_value=tratar_nan(p_shapiro),
                        detalhes_variaveis=detalhes_vars,
                        
                        # OS PREÇOS SEGUROS
                        custo_unitario=custo_base_seguro,
                        preco_atual=preco_base_seguro
                    )
                    produtos_processados += 1
                    
                except Exception as e:
                    print(f"[AXIOM OLS ERRO] Falha no SKU {sku} (Loja: {loja_obj}): {e}")

            if produtos_processados == 0:
                messages.error(request, "Nenhum resultado gerado. Os produtos não atingiram a volumetria mínima exigida (>30 dias) ou não possuem variação de preço.")
                return redirect('iniciar_projeto_upload')
            
            # ======== GARANTA QUE ESSAS DUAS LINHAS ESTÃO AQUI ========
            messages.success(request, f"Sucesso! Dados salvos no DW e {produtos_processados} precificações geradas.")
            return redirect('dashboard_resultado', projeto_id=projeto.id)

        except Exception as e:
            messages.error(request, f"Erro crítico na Ingestão: {e}")
            print(f"[AXIOM CRITICO] {e}")
            return redirect('iniciar_projeto_upload')

    return redirect('iniciar_projeto_upload')




@login_required
def dashboard_resultado(request, projeto_id):
    """
    Passo 3: Exibe o resultado agregado no formato 'SaaS Executivo'.
    """
    empresa_cliente = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa_cliente)
    
    resultados_db = projeto.resultados.all()
    
    if not resultados_db.exists():
        messages.error(request, "Nenhum resultado pôde ser gerado para este projeto. Verifique os dados.")
        return redirect('iniciar_projeto_upload')

    # Contadores para os Cards Superiores
    total_analisados = resultados_db.count()
    elasticos_count = 0
    inelasticos_count = 0

    mapeamento_nomes = dict(
        VendaHistoricaDW.objects.filter(projeto=projeto)
        .values_list('codigo_produto', 'nome_produto')
        .distinct()
    )
    
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
            
        # Lógica 2: Confiança 
        r2 = res.r_squared if res.r_squared else 0
        p_val = res.elasticidade_p_value if res.elasticidade_p_value is not None else 1.0
        
        # A nova trava do PM:
        if r2 >= 0.50 and p_val < 0.05:
            confianca = "Alta"
            badge_cor = "success"
        elif r2 >= 0.30 and p_val < 0.10:
            confianca = "Média"
            badge_cor = "warning"
        else:
            confianca = "Baixa (Use com cautela)"
            badge_cor = "danger"
            
        resultados_processados.append({
            'id': res.id,
            'sku': res.codigo_produto,
            'nome_produto': mapeamento_nomes.get(res.codigo_produto, res.codigo_produto),
            'loja': res.loja.nome if res.loja else 'Global',
            'elasticidade': res.elasticidade,
            'comportamento': comportamento,
            'cor_comp': cor_comp,
            'confianca': confianca,
            'badge_cor': badge_cor, # <- Mandamos a cor pro HTML!
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
    empresa = request.empresa
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
    empresa = request.empresa
    resultado = get_object_or_404(ResultadoPrecificacao, id=resultado_id, projeto__empresa=empresa)
    
    # CORREÇÃO ARQUITETURAL: Agora as bolhas do gráfico puxam as vendas APENAS desta loja específica!
    vendas = VendaHistoricaDW.objects.filter(
        projeto=resultado.projeto,
        codigo_produto=resultado.codigo_produto,
        loja=resultado.loja # <-- O FILTRO MÁGICO AQUI
    ).values('data_venda', 'quantidade', 'preco_praticado')

    dados_grafico = {}
    demanda_base_diaria = 100.0 

    if vendas.exists():
        df = pd.DataFrame.from_records(vendas)
        df['preco_praticado'] = df['preco_praticado'].astype(float).round(2)
        df['quantidade'] = df['quantidade'].astype(float)
        
        # Converte para datetime para podermos filtrar
        df['data_venda'] = pd.to_datetime(df['data_venda'])
        
        # ==========================================
        # DEMANDA BASE RECENTE (Sincronizado com a API)
        # ==========================================
        ultima_data = df['data_venda'].max()
        data_corte = ultima_data - pd.Timedelta(days=30)
        df_recente = df[df['data_venda'] >= data_corte]
        
        if not df_recente.empty and df_recente['quantidade'].mean() > 0:
            demanda_base_diaria = df_recente['quantidade'].mean()
        else:
            demanda_base_diaria = df['quantidade'].mean()

        Q1 = df['quantidade'].quantile(0.25)
        Q3 = df['quantidade'].quantile(0.75)
        IQR = Q3 - Q1
        teto_maximo = Q3 + (1.5 * IQR)

        df_normal = df[df['quantidade'] <= teto_maximo]
        df_outlier = df[df['quantidade'] > teto_maximo]

        df_grouped = df_normal.groupby('preco_praticado').agg(
            qtd_media=('quantidade', 'mean'),
            frequencia=('data_venda', 'count')
        ).reset_index()

        df_grouped['tamanho'] = np.log1p(df_grouped['frequencia']) * 12
        df_grouped['tamanho'] = df_grouped['tamanho'].clip(lower=10, upper=35)

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
    empresa = request.empresa
    
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
    empresa = request.empresa
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
            
            empresa = request.empresa
            
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
def painel_forecast(request, resultado_id):
    """
    Carrega a tela do Axiom Forecast para um SKU e Loja específicos.
    """
    empresa = request.empresa
    
    # 1. Pega o Resultado exato (que já sabe a loja e o SKU)
    resultado = get_object_or_404(ResultadoPrecificacao, id=resultado_id, projeto__empresa=empresa)
    sku = resultado.codigo_produto
    loja = resultado.loja
    
    # 2. Busca a previsão mais recente PARA ESTA LOJA específica
    previsao = PrevisaoDemanda.objects.filter(empresa=empresa, codigo_produto=sku, loja=loja).last()
    
    # 3. Busca o nome bonito do produto
    produto_info = VendaHistoricaDW.objects.filter(empresa=empresa, codigo_produto=sku, loja=loja).first()
    nome_produto = produto_info.nome_produto if produto_info else sku

    contexto = {
        'resultado_id': resultado.id,
        'sku': sku,
        'loja_nome': loja.nome if loja else 'Global',
        'nome_produto': nome_produto,
        'previsao': previsao,
    }
    
    return render(request, 'projects/forecast.html', contexto)

@login_required
def gerar_forecast_action(request, resultado_id):
    """
    Ação do botão: Chama o Motor do XGBoost no services.py e salva no banco.
    """
    empresa = request.empresa
    resultado = get_object_or_404(ResultadoPrecificacao, id=resultado_id, projeto__empresa=empresa)
    
    # Chama o motor passando a LOJA para que o XGBoost isole os dados!
    sucesso, mensagem = treinar_previsao_xgboost(empresa, resultado.codigo_produto, loja=resultado.loja, dias_futuros=30)
    
    if sucesso:
        messages.success(request, f"Motor Preditivo executado para a filial {resultado.loja.nome if resultado.loja else 'Global'}!")
    else:
        messages.error(request, f"Erro na IA: {mensagem}")
        
    return redirect('painel_forecast', resultado_id=resultado.id)

@login_required
def painel_macro_forecast(request):
    """
    O Dashboard do CFO: Mostra a projeção de faturamento da empresa inteira ou filtrada por Loja.
    """
    empresa = request.empresa

    # ==========================================
    # A BARREIRA VIP (PAYWALL)
    # ==========================================
    if not empresa.is_active_subscriber:
        messages.warning(request, "🔒 Este módulo é exclusivo do plano Axiom Premium. Faça o upgrade para desbloquear o Motor Preditivo.")
        return redirect('configuracoes_conta')     
   
    # 1. Busca todas as lojas ativas do cliente para montar o Dropdown na tela
    lojas = Loja.objects.filter(empresa=empresa, ativo=True).order_by('nome')
    
    # 2. Verifica se o cliente escolheu alguma loja no filtro (vem pela URL ex: ?loja_id=5)
    loja_selecionada_id = request.GET.get('loja_id')

    if loja_selecionada_id:
        # Se ele escolheu uma loja, puxa a última previsão gerada PARA ESSA LOJA
        previsao = PrevisaoFaturamentoMacro.objects.filter(empresa=empresa, loja_id=loja_selecionada_id).last()
    else:
        # Se ele acabou de entrar na tela, puxa a última previsão gerada no geral
        previsao = PrevisaoFaturamentoMacro.objects.filter(empresa=empresa).last()
        # Se achou uma previsão, descobre de qual loja ela é para deixar o Dropdown marcado certinho
        if previsao and previsao.loja:
            loja_selecionada_id = str(previsao.loja.id)

    contexto = {
        'previsao': previsao,
        'lojas': lojas,
        'loja_selecionada_id': str(loja_selecionada_id) if loja_selecionada_id else None,
    }
    
    return render(request, 'projects/macro_forecast.html', contexto)


@login_required
def gerar_macro_forecast_action(request):
   
    """
    Dispara o treinamento do Facebook Prophet para prever a receita de uma loja específica.
    """
    empresa = request.empresa

    # Pega o ID da loja que o usuário selecionou no Front-end (via POST)
    loja_id = request.POST.get('loja_id')

    if not loja_id:
        messages.error(request, "Por favor, selecione uma filial para gerar a previsão.")
        return redirect('painel_macro_forecast')

    sucesso, mensagem = treinar_previsao_macro_empresa(empresa, loja_id, dias_futuros=90)

    if sucesso:
        messages.success(request, mensagem)
    else:
        messages.error(request, f"Atenção: {mensagem}")

    # Devolve o cliente para o painel, já filtrado na loja que ele rodou
    return redirect(f"{reverse('painel_macro_forecast')}?loja_id={loja_id}")


@login_required
def upload_macro_financeiro(request):
    """
    Ingestão Fast-Track: CFO sobe o CSV leve, a gente limpa a sujeira do Excel BR e salva.
    """
    empresa = request.empresa

    if not empresa.is_active_subscriber:
        messages.warning(request, "🔒 Funcionalidade Premium: A ingestão de caixa para IA requer uma assinatura ativa.")
        return redirect('configuracoes_conta')
    
    if request.method == 'POST' and request.FILES.get('arquivo_macro'):
        arquivo = request.FILES['arquivo_macro']
        try:
            extensao = '.csv' if arquivo.name.lower().endswith('.csv') else '.xlsx'
            
            # 1. LEITURA DO ARQUIVO (A Máscara de Texto - Solução do Erro de Bytes)
            if extensao == '.csv':
                import io
                conteudo_bytes = arquivo.read() # Lê como binário
                
                # Tenta decodificar para texto humano (String)
                try:
                    conteudo_texto = conteudo_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    conteudo_texto = conteudo_bytes.decode('latin-1')
                
                # Entrega para o Pandas como um arquivo de texto virtual!
                df = pd.read_csv(io.StringIO(conteudo_texto), sep=None, engine='python')
            else:
                df = pd.read_excel(arquivo)
                
            # Limpa colunas e linhas totalmente vazias (fantasmas do Excel)
            df.dropna(axis=1, how='all', inplace=True) 
            df.dropna(axis=0, how='all', inplace=True)
            
            if len(df.columns) < 2:
                messages.error(request, "O arquivo precisa ter pelo menos duas colunas (Data e Valor).")
                return redirect('upload_macro_financeiro')

            # ==========================================
            # INTELIGÊNCIA DE AUTO-MAPPING (Versão Multi-Lojas)
            # ==========================================
            col_data = df.columns[0]
            col_valor = df.columns[1] 
            
            # A MÁGICA: Tenta pegar a 3ª coluna. Se não tiver, vira None.
            col_loja = df.columns[2] if len(df.columns) >= 3 else None
            
            # 1. Limpeza Extrema da Data (Parser Bulletproof BR/USA)
            df_data_limpa = df[col_data].astype(str).str.strip()
            datas_parsed = pd.to_datetime(df_data_limpa, format='%Y-%m-%d', errors='coerce')
            
            mask = datas_parsed.isna()
            if mask.any():
                datas_parsed.loc[mask] = pd.to_datetime(df_data_limpa[mask], format='%d/%m/%Y', errors='coerce')
                
            mask = datas_parsed.isna()
            if mask.any():
                datas_parsed.loc[mask] = pd.to_datetime(df_data_limpa[mask], errors='coerce', dayfirst=True)
                
            df[col_data] = datas_parsed

            linhas_antes = len(df)
            
            # 2. Limpeza Bruta do Dinheiro (O Exterminador de \xa0)
            if df[col_valor].dtype == 'object':
                df[col_valor] = df[col_valor].astype(str)
                df[col_valor] = df[col_valor].str.replace(r'[R$\s]', '', regex=True, case=False)
                df[col_valor] = df[col_valor].apply(
                    lambda x: str(x).replace('.', '').replace(',', '.') if ',' in str(x) else x
                )
            
            df[col_valor] = pd.to_numeric(df[col_valor], errors='coerce')
            
            # 3. Limpeza da Coluna de Loja (Se existir)
            if col_loja:
                df[col_loja] = df[col_loja].astype(str).str.strip()
                # Se a pessoa deixou a célula em branco na terceira coluna, chamamos de Matriz
                df[col_loja] = df[col_loja].replace(['nan', 'None', '', 'NaT'], 'Matriz / Global')
            else:
                # Se não tem 3ª coluna, criamos uma virtual para o código funcionar igual
                col_loja = 'loja_virtual'
                df[col_loja] = 'Matriz / Global'
            
            # Exclui linhas onde a data ou valor falharam
            df.dropna(subset=[col_data, col_valor], inplace=True)
            linhas_perdidas = linhas_antes - len(df)
            
            if df.empty:
                messages.error(request, "Falha crítica: As colunas foram lidas, mas nenhum valor financeiro válido sobreviveu.")
                return redirect('upload_macro_financeiro')
                
            # Agrupa os faturamentos que caírem no mesmo dia E NA MESMA LOJA
            df_agrupado = df.groupby([col_loja, col_data])[col_valor].sum().reset_index()
            
            # ==========================================
            # AUTO-DISCOVERY: CRIANDO AS LOJAS NO BANCO
            # ==========================================
            nomes_lojas = df_agrupado[col_loja].unique()
            dicionario_lojas = {}
            
            for nome_loja in nomes_lojas:
                # O get_or_create busca a loja, se não achar, ele cria silenciosamente!
                loja_obj, created = Loja.objects.get_or_create(
                    empresa=empresa,
                    nome=nome_loja,
                    defaults={'ativo': True}
                )
                dicionario_lojas[nome_loja] = loja_obj

            # Prepara a lista para salvar no Banco (Bulk Create veloz)
            lista_faturamento = [
                FaturamentoEmpresaDW(
                    empresa=empresa,
                    loja=dicionario_lojas[row[col_loja]], # Amarra a linha com o ID real da Loja!
                    data_faturamento=row[col_data].date(),
                    faturamento_total=float(row[col_valor])
                )
                for index, row in df_agrupado.iterrows()
            ]
            
            # FLUSH AND FILL (Limpa o velho, bota o novo)
            FaturamentoEmpresaDW.objects.filter(empresa=empresa).delete()
            FaturamentoEmpresaDW.objects.bulk_create(lista_faturamento, batch_size=5000)
            # ==========================================
            # NOVO: FEEDBACK TRANSPARENTE PARA O USUÁRIO
            # ==========================================
            if linhas_perdidas > 0:
                messages.warning(request, f"Atenção: {linhas_perdidas} linhas do seu arquivo continham formatos irreconhecíveis e foram ignoradas (Verifique as datas e valores a partir de Março/2025).")
            
            messages.success(request, f"Mágico! {len(lista_faturamento)} dias de faturamento carregados e processados.")
            return redirect('painel_macro_forecast')
            
        except Exception as e:
            # AGORA O ERRO VAI DENUNCIAR EXATAMENTE O QUE QUEBROU
            import logging
            logging.error(f"Erro no Upload Macro: {e}")
            messages.error(request, f"Erro interno ao ler o arquivo: {str(e)}")
            return redirect('upload_macro_financeiro')
            
    return render(request, 'projects/upload_macro.html')


stripe.api_key = settings.STRIPE_SECRET_KEY

@login_required
def criar_checkout_stripe(request):
    """
    Gera a sessão de pagamento (Checkout) segura na Stripe e redireciona o cliente.
    """
    empresa = request.empresa
    
    # COLE AQUI O SEU PRICE_ID GERADO NO PASSO 1
    STRIPE_PRICE_ID = 'prod_U4l0CZfksGcAto' 
    
    # Monta as URLs de Sucesso e Cancelamento (para a Stripe saber pra onde devolver o cliente)
    dominio = request.build_absolute_uri('/')[:-1] # Pega o "http://127.0.0.1:8000" automático
    url_sucesso = dominio + reverse('sucesso_pagamento')
    url_cancelado = dominio + reverse('cancelado_pagamento')

    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'], # Pode adicionar 'boleto' ou 'pix' depois lá na Stripe
            line_items=[
                {
                    'price': STRIPE_PRICE_ID,
                    'quantity': 1,
                },
            ],
            mode='subscription', # Modo de Assinatura Mensal Recorrente
            success_url=url_sucesso + '?session_id={CHECKOUT_SESSION_ID}',
            cancel_url=url_cancelado,
            
            # O PULO DO GATO: Mandamos o ID da empresa escondido. 
            # Quando a Stripe confirmar o pagamento, ela devolve esse ID e nós sabemos quem pagou!
            client_reference_id=str(empresa.id),
            customer_email=request.user.email, # Já preenche o e-mail do cliente na tela da Stripe
        )
        return redirect(checkout_session.url, code=303)
        
    except Exception as e:
        messages.error(request, f"Erro ao conectar com o servidor de pagamentos: {str(e)}")
        return redirect('configuracoes_conta')

@login_required
def sucesso_pagamento(request):
    # Tela para onde o cliente cai após passar o cartão com sucesso
    messages.success(request, "🎉 Pagamento aprovado! Bem-vindo ao Axiom Premium.")
    return redirect('configuracoes_conta')

@login_required
def cancelado_pagamento(request):
    # Tela caso ele desista de digitar o cartão e clique em "Voltar"
    messages.warning(request, "O processo de assinatura foi cancelado.")
    return redirect('configuracoes_conta')

# ==========================================
# WEBHOOK DA STRIPE (O Ouvinte Automático)
# ==========================================
@csrf_exempt
def stripe_webhook(request):
    """
    Rota invisível que a Stripe chama automaticamente quando um pagamento é aprovado.
    """
    payload = request.body
    sig_header = request.META.get('HTTP_STRIPE_SIGNATURE')
    event = None

    try:
        # A Stripe exige que a gente valide a assinatura para provar que a mensagem é real e não de um hacker
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.STRIPE_WEBHOOK_SECRET
        )
    except ValueError as e:
        return HttpResponse(status=400) # Payload inválido
    except stripe.error.SignatureVerificationError as e:
        return HttpResponse(status=400) # Assinatura falsa (Hacker)

    # Verifica qual foi o evento que a Stripe mandou
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        
        # Lembra que nós mandamos o ID da empresa escondido no client_reference_id? Nós pegamos ele de volta!
        empresa_id = session.get('client_reference_id')
        stripe_customer_id = session.get('customer')
        stripe_subscription_id = session.get('subscription')

        if empresa_id:
            try:
                # Encontra a empresa no banco de dados
                empresa = Empresa.objects.get(id=empresa_id)
                
                # MÁGICA: Ativa a assinatura do cliente e salva as chaves dele!
                empresa.is_active_subscriber = True
                empresa.stripe_customer_id = stripe_customer_id
                empresa.stripe_subscription_id = stripe_subscription_id
                empresa.save()
                
                print(f"SUCESSO NO WEBHOOK: A empresa {empresa.nome} agora é Premium!")
            except Empresa.DoesNotExist:
                print(f"ERRO NO WEBHOOK: Empresa ID {empresa_id} não encontrada.")

    # Responde 200 OK para a Stripe parar de mandar a mensagem
    return HttpResponse(status=200)

# ==========================================
# API DE SIMULAÇÃO EM TEMPO REAL (XGBOOST)
# ==========================================
@login_required
def api_simular_preco(request):
    """
    Recebe um novo preço do Front-end, consulta o modelo matemático do banco,
    e devolve a quantidade prevista e o lucro projetado em milissegundos.
    """
    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            # CORREÇÃO ARQUITETURAL: Recebemos o ID exato do resultado, e não mais o SKU genérico
            resultado_id = dados.get('resultado_id') 

            elasticidade_customizada = dados.get('elasticidade_customizada')
            
            preco_limpo = str(dados.get('novo_preco')).replace(',', '.')
            custo_limpo = str(dados.get('custo')).replace(',', '.')
            
            novo_preco = float(preco_limpo)
            custo = float(custo_limpo) 
            
            # 1. Busca os parâmetros reais de Elasticidade no Banco (Usando o ID único)
            resultado = ResultadoPrecificacao.objects.get(id=resultado_id, projeto__empresa=request.empresa)
            
            ultima_data = VendaHistoricaDW.objects.filter(
                projeto_id=resultado.projeto_id, 
                codigo_produto=resultado.codigo_produto,
                loja=resultado.loja
            ).aggregate(ultima=Max('data_venda'))['ultima']

            demanda_base_diaria = 0.0

            if ultima_data:
                # Cortamos exatamente 30 dias para trás a partir da última venda
                data_corte = ultima_data - timedelta(days=30)
                
                media_recente = VendaHistoricaDW.objects.filter(
                    projeto_id=resultado.projeto_id, 
                    codigo_produto=resultado.codigo_produto,
                    loja=resultado.loja,
                    data_venda__gte=data_corte
                ).aggregate(media=Avg('quantidade'))['media']
                
                # Se por acaso os últimos 30 dias estiverem vazios, fazemos o fallback para a média geral
                if media_recente:
                    demanda_base_diaria = float(media_recente)
                else:
                    media_geral = VendaHistoricaDW.objects.filter(
                        projeto_id=resultado.projeto_id, 
                        codigo_produto=resultado.codigo_produto,
                        loja=resultado.loja
                    ).aggregate(media=Avg('quantidade'))['media']
                    demanda_base_diaria = float(media_geral) if media_geral else 0.0
            
            # 3. O Motor Matemático de Previsão
            preco_atual = resultado.preco_atual
            elasticidade = resultado.elasticidade

            if elasticidade_customizada is not None:
                elasticidade = float(elasticidade_customizada)
            else:
                elasticidade = resultado.elasticidade
            
            razao_preco = novo_preco / preco_atual if preco_atual > 0 else 1
            if razao_preco <= 0: razao_preco = 1
                
            quantidade_diaria_prevista = demanda_base_diaria * math.pow(razao_preco, elasticidade)
            
            if quantidade_diaria_prevista < 0:
                quantidade_diaria_prevista = 0
                
            # 4. Matemática Financeira Executiva
            faturamento_diario = quantidade_diaria_prevista * novo_preco
            lucro_diario = (novo_preco - custo) * quantidade_diaria_prevista
            margem_projetada = ((novo_preco - custo) / novo_preco) * 100 if novo_preco > 0 else 0

            return JsonResponse({
                'status': 'sucesso',
                'quantidade_prevista': round(quantidade_diaria_prevista, 2), 
                'faturamento_projetado': round(faturamento_diario, 2),
                'lucro_projetado': round(lucro_diario, 2),
                'margem_projetada': round(margem_projetada, 2)
            })
            
        except Exception as e:
            print(f"ERRO NA API: {e}")
            return JsonResponse({'status': 'erro', 'mensagem': str(e)}, status=400)
            
    return JsonResponse({'status': 'invalido'}, status=405)

@login_required
def painel_calendario(request):
    empresa = request.empresa

    # Se o usuário enviou o formulário (Clicou em Salvar)
    if request.method == 'POST':
        form = EventoCalendarioForm(request.POST, empresa=empresa)
        if form.is_valid():
            evento = form.save(commit=False)
            evento.empresa = empresa # Amarra o evento à empresa do usuário logado
            evento.save()
            messages.success(request, f"Evento '{evento.nome}' adicionado com sucesso!")
            return redirect('painel_calendario')
        else:
            messages.error(request, "Erro ao salvar o evento. Verifique as datas.")
    else:
        # Se ele só está abrindo a página, carrega o form vazio
        form = EventoCalendarioForm(empresa=empresa)

    # Busca todos os eventos futuros e passados da empresa para montar a tabela
    eventos = EventoCalendario.objects.filter(empresa=empresa).order_by('-data_inicio')

    contexto = {
        'form': form,
        'eventos': eventos,
    }
    return render(request, 'projects/calendario.html', contexto)


@login_required
def deletar_evento(request, evento_id):
    empresa = request.empresa
    # get_object_or_404 garante que ele só pode deletar um evento da PRÓPRIA empresa
    evento = get_object_or_404(EventoCalendario, id=evento_id, empresa=empresa)
    nome = evento.nome
    evento.delete()
    messages.warning(request, f"O evento '{nome}' foi removido do calendário.")
    return redirect('painel_calendario')

@login_required
def api_recalcular_modelo(request):
    """
    Recalcula a Regressão OLS em tempo real, ligando ou desligando o filtro de Outliers (Winsorize).
    """
    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            resultado_id = dados.get('resultado_id')
            filtrar_outliers = dados.get('filtrar_outliers', True) # Padrão é True (Limpo)

            resultado = ResultadoPrecificacao.objects.get(id=resultado_id, projeto__empresa=request.empresa)
            projeto = resultado.projeto

            # 1. Busca os dados crus do banco
            vendas = VendaHistoricaDW.objects.filter(
                projeto=projeto, codigo_produto=resultado.codigo_produto, loja=resultado.loja
            ).order_by('data_venda')

            df = pd.DataFrame(list(vendas.values('data_venda', 'quantidade', 'preco_praticado', 'variaveis_extras')))
            
            # 2. Reconstrói as variáveis extras
            config = projeto.configuracao_variaveis
            variaveis_extras_config = config.get('variaveis_extras', [])
            
            for var in variaveis_extras_config:
                nome = var['nome']
                df[nome] = df['variaveis_extras'].apply(lambda x: x.get(nome) if isinstance(x, dict) else None)

            # 3. Engenharia de Features (Igual ao treinamento original)
            df_model = df[(df['quantidade'] > 0) & (df['preco_praticado'] > 0)].copy()
            df_model['log_y'] = np.log(df_model['quantidade'].astype(float))
            df_model['log_p'] = np.log(df_model['preco_praticado'].astype(float))
            
            mapa_dias = {0: 'Segunda', 1: 'Terca', 2: 'Quarta', 3: 'Quinta', 4: 'Sexta', 5: 'Sabado', 6: 'Domingo'}
            df_model['dia_semana_auto'] = pd.to_datetime(df_model['data_venda']).dt.dayofweek.map(mapa_dias)

            termos_formula = ["log_p", "C(dia_semana_auto)"]
            for var in variaveis_extras_config:
                nome = var['nome']
                termos_formula.append(f"C({nome})" if var['tipo'] == 'cat' else nome)

            df_model = df_model.sort_values('data_venda')
            df_model['log_p_lag1'] = df_model['log_p'].shift(1)
            df_model.dropna(subset=['log_p_lag1'], inplace=True)

            # 4. A CHAVE MESTRA: O BOTÃO DO USUÁRIO
            if filtrar_outliers:
                df_model['log_y'] = winsorize(df_model['log_y'], limits=[0.02, 0.02])

            formula_iv = f"log_y ~ {' + '.join(termos_formula)}".replace('log_p', 'log_p + log_p_lag1')

            # 5. Treina o Modelo Instantâneo
            modelo = smf.ols(formula_iv, data=df_model).fit()
            stat_shapiro, p_shapiro = shapiro(modelo.resid)

            detalhes_vars = {}
            for termo in modelo.pvalues.index:
                if termo not in ['Intercept', 'log_p', 'log_p_lag1']:
                    detalhes_vars[termo] = {
                        "p_valor": float(modelo.pvalues[termo]) if not pd.isna(modelo.pvalues[termo]) else 1.0,
                        "coeficiente": float(modelo.params[termo]) if not pd.isna(modelo.params[termo]) else 0.0,
                        "status": "Relevante" if modelo.pvalues[termo] < 0.05 else "Ruído"
                    }

            return JsonResponse({
                'status': 'sucesso',
                'elasticidade': float(modelo.params.get('log_p', 0)),
                'r_squared': float(modelo.rsquared),
                'shapiro_p_value': float(p_shapiro),
                'detalhes_variaveis': detalhes_vars
            })

        except Exception as e:
            return JsonResponse({'status': 'erro', 'mensagem': str(e)}, status=400)
        
@login_required
def painel_portfolio(request, projeto_id):
    """
    Motor de Portfolio Analytics:
    Usa Database Pushdown para agregar milhões de linhas no SQL, 
    e o Pandas na memória para calcular Curva ABC e Margens.
    """
    empresa = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)

    # ==========================================
    # 1. DATABASE PUSHDOWN (SQL BRUTO EM C/C++)
    # ==========================================
    # Em vez de carregar milhões de vendas, pedimos ao banco para agrupar tudo por SKU.
    # O comando 'F' multiplica as colunas linha a linha no banco de dados e o 'Sum' soma o total.
    agrupamento = VendaHistoricaDW.objects.filter(projeto=projeto).values(
        'codigo_produto', 'nome_produto'
    ).annotate(
        volume_total=Coalesce(Sum('quantidade'), 0.0, output_field=FloatField()),
        receita_total=Coalesce(Sum(F('quantidade') * F('preco_praticado')), 0.0, output_field=FloatField()),
        custo_total=Coalesce(Sum(F('quantidade') * F('custo_unitario')), 0.0, output_field=FloatField())
    )

    # ==========================================
    # 2. PANDAS (TRABALHO LEVE NA MEMÓRIA)
    # ==========================================
    # O banco devolve apenas 1 linha por produto. O Pandas engole isso em 0.01 segundos.
    df_vendas = pd.DataFrame(list(agrupamento))

    if df_vendas.empty:
        messages.warning(request, "Não há dados de vendas suficientes para gerar o portfólio.")
        return redirect('dashboard_resultado', projeto_id=projeto.id)
    
    # ==========================================
    # NOVO: MERGE DA ELASTICIDADE 
    # ==========================================
    # Buscamos a elasticidade de todos os produtos do projeto
    elasticidades = ResultadoPrecificacao.objects.filter(projeto=projeto).values('codigo_produto', 'elasticidade')
    df_elasticidades = pd.DataFrame(list(elasticidades))

    # Se tivermos cálculo de elasticidade pronto, mesclamos com as vendas
    if not df_elasticidades.empty:
        df = pd.merge(df_vendas, df_elasticidades, on='codigo_produto', how='left')
        # Se um produto for novo e ainda não tiver elasticidade, colocamos -1.5 (média de mercado) para não dar erro
        df['elasticidade'] = df['elasticidade'].fillna(-1.5)
    else:
        df = df_vendas
        df['elasticidade'] = -1.5 # Fallback caso não exista modelo treinado ainda

    # 3. CÁLCULO DE MARGEM (%)
    # Evita divisão por zero usando o np.where
    df['margem_lucro'] = np.where(
        df['receita_total'] > 0,
        ((df['receita_total'] - df['custo_total']) / df['receita_total']) * 100,
        0
    )

    
    df['preco_medio'] = np.where(df['volume_total'] > 0, df['receita_total'] / df['volume_total'], 0).round(2)
    df['custo_medio'] = np.where(df['volume_total'] > 0, df['custo_total'] / df['volume_total'], 0).round(2)

    # ==========================================
    # 4. A CURVA ABC (Princípio de Pareto)
    # ==========================================
    # Ordena quem dá mais dinheiro para quem dá menos
    df = df.sort_values(by='receita_total', ascending=False).reset_index(drop=True)
    
    # Soma acumulada (ex: se o 1º vende 50k e o 2º vende 30k, a linha 2 terá 80k)
    df['receita_acumulada'] = df['receita_total'].cumsum()
    df['percentual_acumulado'] = df['receita_acumulada'] / df['receita_total'].sum()

    def classificar_abc(pct):
        if pct <= 0.80: return 'A' # Primeiros 80% do faturamento
        elif pct <= 0.95: return 'B' # Próximos 15%
        else: return 'C'             # Últimos 5% (Cauda Longa)

    df['curva_abc'] = df['percentual_acumulado'].apply(classificar_abc)

    # ==========================================
    # 5. EXPORTAÇÃO LIMPA PARA O JAVASCRIPT (FRONT-END)
    # ==========================================
    df['receita_total'] = df['receita_total'].round(2)
    df['margem_lucro'] = df['margem_lucro'].round(2)
    df['volume_total'] = df['volume_total'].round(2)
    df['elasticidade'] = df['elasticidade'].round(2)

    # Transformamos o DataFrame em um JSON super leve para o navegador mastigar e desenhar a Matriz BCG!
    colunas_exportacao = ['codigo_produto', 'nome_produto', 'volume_total', 'receita_total', 'margem_lucro', 'curva_abc','preco_medio', 'custo_medio','elasticidade']
    produtos_json = df[colunas_exportacao].to_dict(orient='records')

    contexto = {
        'projeto': projeto,
        'produtos_json': json.dumps(produtos_json)
    }
    
    return render(request, 'projects/portfolio.html', contexto)