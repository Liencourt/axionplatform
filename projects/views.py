import pandas as pd
import logging
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
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
                     PrevisaoFaturamentoMacro,FaturamentoEmpresaDW,EventoCalendario,
                     CorrelacaoAnalise, TendenciaDetectada, RadarConfig,
                     ReputacaoConfig, AnaliseReputacao)
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
from django.db.models import Avg, Sum, F, FloatField, Max, Min


logger = logging.getLogger(__name__)

@login_required
def iniciar_projeto_upload(request):
    """Passo 1: Recebe o caminho do arquivo (GCS ou local) e extrai as colunas."""

    if request.method == 'POST' and request.POST.get('caminho_gcs'):
        caminho_gcs = request.POST.get('caminho_gcs')
        nome_projeto = request.POST.get('nome_projeto', 'Novo Projeto')

        print(f"-> 1. POST Recebido! Caminho: {caminho_gcs}")
        contexto = None

        try:
            extensao = '.csv' if caminho_gcs.lower().endswith('.csv') else '.xlsx'

            # ── Modo local (DEBUG sem GCS) ──────────────────────────────────
            caminho_local = os.path.join(settings.BASE_DIR, 'media', caminho_gcs)
            usar_gcs = os.getenv('USE_GCS', 'false').lower() == 'true' or not settings.DEBUG

            if not usar_gcs and os.path.exists(caminho_local):
                print(f"-> 2. [LOCAL] Lendo arquivo em {caminho_local}")
                caminho_temp = caminho_local
            else:
                # ── Modo produção: baixa do GCS ─────────────────────────────
                bucket_name = os.getenv('BUCKET_NAME', 'axiom-platform-datasets')
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(caminho_gcs)

                print(f"-> 2. [GCS] Conectou no Bucket '{bucket_name}'. Baixando...")

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
            # loja_col é opcional: None ou "" significa projeto sem multi-loja
            _loja_raw = config.get('loja_col', '')
            loja_col = _loja_raw if _loja_raw else None
            nome_produto_col = config.get('nome_produto_col') or None
            
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
            VendaHistoricaDW.objects.bulk_create(lista_vendas_dw, batch_size=2000, ignore_conflicts=True)

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
            # loja_col é ignorado se: não foi selecionado, não existe no df,
            # ou foi acidentalmente mapeado para a mesma coluna do sku.
            usar_loja = (
                loja_col
                and loja_col in df_model.columns
                and loja_col != sku_col
            )
            colunas_agrupamento = [loja_col, sku_col] if usar_loja else [sku_col]

            for keys, df_sku in df_model.groupby(colunas_agrupamento):
                # Desempacota a chave de forma segura para qualquer combinação
                # de colunas, inclusive quando loja_col == sku_col (pandas pode
                # retornar 1-tuple nesses casos).
                if isinstance(keys, tuple) and len(keys) == 2:
                    loja_val, sku = keys
                    loja_obj = dict_lojas.get(str(loja_val).strip())
                elif isinstance(keys, tuple) and len(keys) == 1:
                    sku = keys[0]
                    loja_obj = None
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
            import traceback
            messages.error(request, f"Erro crítico na Ingestão: {e}")
            print(f"[AXIOM CRITICO] {e}")
            traceback.print_exc()   # imprime o stack trace completo no log do Cloud Run
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

    _qs_nomes = VendaHistoricaDW.objects.filter(projeto=projeto)
    if not _qs_nomes.exists():
        _qs_nomes = VendaHistoricaDW.objects.filter(empresa=empresa_cliente)
    mapeamento_nomes = dict(
        _qs_nomes.values_list('codigo_produto', 'nome_produto').distinct()
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
        dados_grafico['box_precos'] = df['preco_praticado'].tolist()

        dados_grafico = {
            'x_normal': df_grouped['preco_praticado'].tolist(),
            'y_normal': df_grouped['qtd_media'].tolist(),
            'sizes': df_grouped['tamanho'].tolist(),
            'freqs': df_grouped['frequencia'].tolist(),
            'x_outlier': df_outlier['preco_praticado'].tolist(),
            'y_outlier': df_outlier['quantidade'].tolist(),
            'box_precos': df['preco_praticado'].tolist() 
        }
    
    #==========================================
    # NOVO: MOTOR DOS 3 CENÁRIOS (A MÁGICA DA IA)
    # ==========================================
    preco_atual = resultado.preco_atual
    custo = resultado.custo_unitario if resultado.custo_unitario and resultado.custo_unitario > 0 else 0.01
    # Teto obrigatório: aumentar preço NUNCA aumenta volume na vida real (CLAUDE.md §2)
    elasticidade = min(resultado.elasticidade, 0.0)

    cenarios = []
    
    # Função auxiliar rápida para calcular as métricas de um preço
    def calcular_cenario(p_novo):
        razao = p_novo / preco_atual if preco_atual > 0 else 1
        if razao <= 0: razao = 1
        vol = demanda_base_diaria * math.pow(razao, elasticidade)
        lucro = (p_novo - custo) * vol
        margem = ((p_novo - custo) / p_novo) * 100 if p_novo > 0 else 0
        return {'preco': round(p_novo, 2), 'volume': round(vol, 2), 'lucro': round(lucro, 2), 'margem': round(margem, 1)}

    # 1. CENÁRIO: LUCRO ÓTIMO (O Algoritmo testa 1.000 preços para achar o topo)
    precos_teste = np.linspace(custo * 1.05, preco_atual * 2.0, 1000)
    melhor_lucro = -float('inf')
    preco_otimo = preco_atual

    for pt in precos_teste:
        luc_t = calcular_cenario(pt)['lucro']
        if luc_t > melhor_lucro:
            melhor_lucro = luc_t
            preco_otimo = pt

    cenarios.append({
        'nome': 'Lucro Ótimo (IA)',
        'icone': 'fas fa-brain', 'cor': 'primary',
        'desc': 'O ponto matemático exato que maximiza o dinheiro em caixa.',
        **calcular_cenario(preco_otimo)
    })

    # 2. CENÁRIO: CONSERVADOR (Quick Win seguro)
    # Se inelástico, sobe 3%. Se elástico, baixa 2%.
    ajuste = 1.03 if elasticidade >= -1.0 else 0.98
    cenarios.append({
        'nome': 'Ajuste Conservador',
        'icone': 'fas fa-shield-alt', 'cor': 'success',
        'desc': 'Micro-ajuste focado em ganhar margem sem assustar o cliente.',
        **calcular_cenario(preco_atual * ajuste)
    })

    # 3. CENÁRIO: MARKET SHARE (Agressivo)
    # Reduz preço em 8% para disparar volume, mas trava se chegar perto do custo
    preco_agressivo = preco_atual * 0.92
    if preco_agressivo <= custo:
        preco_agressivo = custo * 1.10 # Trava em 10% de margem mínima
        
    cenarios.append({
        'nome': 'Defesa de Volume',
        'icone': 'fas fa-fire', 'cor': 'danger',
        'desc': 'Redução agressiva para esmagar concorrência e girar estoque.',
        **calcular_cenario(preco_agressivo)
    })

    
    nome_produto = (
        VendaHistoricaDW.objects
        .filter(projeto=resultado.projeto, codigo_produto=resultado.codigo_produto)
        .exclude(nome_produto__isnull=True).exclude(nome_produto='')
        .values_list('nome_produto', flat=True)
        .first()
    )

    contexto = {
        'resultado': resultado,
        'nome_produto': nome_produto or '',
        'demanda_base': demanda_base_diaria,
        'margem_minima': empresa.margem_minima_padrao,
        'limite_choque': empresa.limite_variacao_preco,
        'detalhes_json': json.dumps(resultado.detalhes_variaveis),
        'historico_json': json.dumps(dados_grafico),
        'cenarios': cenarios
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
                margem_str = request.POST.get('margem_minima', str(empresa.margem_minima_padrao)).replace(',', '.')
                limite_str = request.POST.get('limite_variacao', str(empresa.limite_variacao_preco)).replace(',', '.')
                empresa.margem_minima_padrao = float(margem_str)
                empresa.limite_variacao_preco = float(limite_str)
                empresa.save()
                messages.success(request, "As regras de negócio da empresa foram atualizadas!")
            except ValueError:
                messages.error(request, "Erro: Digite apenas números válidos na margem e limite.")

        elif 'btn_alterar_senha' in request.POST:
            form_senha = PasswordChangeForm(request.user, request.POST)
            if form_senha.is_valid():
                user = form_senha.save()
                update_session_auth_hash(request, user)
                messages.success(request, "Senha alterada com sucesso! Você continua conectado.")
            else:
                for field, errs in form_senha.errors.items():
                    for err in errs:
                        messages.error(request, err)

        return redirect('configuracoes_conta')

    num_projetos = ProjetoPrecificacao.objects.filter(empresa=empresa).count()
    contexto = {
        'usuario': usuario,
        'empresa': empresa,
        'num_projetos': num_projetos,
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
            # Teto obrigatório: aumentar preço NUNCA aumenta volume na vida real (CLAUDE.md §2)
            elasticidade = min(elasticidade, 0.0)

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
    # RESOLUÇÃO DO ESCOPO DE DADOS
    # ==========================================
    # Prioridade: dados vinculados ao projeto. Se projeto=NULL (ex: projeto recriado após
    # importação, ou SET_NULL disparado por deleção), usa todos os dados da empresa como fallback.
    _qs_projeto = VendaHistoricaDW.objects.filter(projeto=projeto)
    if _qs_projeto.exists():
        _qs_vendas = _qs_projeto
        _escopo = 'projeto'
    else:
        _qs_vendas = VendaHistoricaDW.objects.filter(empresa=empresa)
        _escopo = 'empresa'
        if _qs_vendas.exists():
            logger.info("painel_portfolio: dados sem vínculo de projeto — usando escopo da empresa %s.", empresa.id)

    # ==========================================
    # BUSCAR O PERÍODO ANALISADO
    # ==========================================
    datas_venda = _qs_vendas.aggregate(
        primeira=Min('data_venda'),
        ultima=Max('data_venda')
    )

    data_inicial_str = datas_venda['primeira'].strftime('%d/%m/%Y') if datas_venda['primeira'] else '--/--/----'
    data_final_str = datas_venda['ultima'].strftime('%d/%m/%Y') if datas_venda['ultima'] else '--/--/----'

    # ==========================================
    # 1. DATABASE PUSHDOWN (SQL BRUTO EM C/C++)
    # ==========================================
    agrupamento = _qs_vendas.values(
        'codigo_produto', 'nome_produto'
    ).annotate(
        volume_total=Coalesce(Sum('quantidade'), 0.0, output_field=FloatField()),
        receita_total=Coalesce(Sum(F('quantidade') * F('preco_praticado')), 0.0, output_field=FloatField()),
        custo_total=Coalesce(Sum(F('quantidade') * F('custo_unitario')), 0.0, output_field=FloatField())
    )

    # ==========================================
    # 2. PANDAS (TRABALHO LEVE NA MEMÓRIA)
    # ==========================================
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
        df['elasticidade'] = np.where(df['elasticidade'].isna(), -1.5, df['elasticidade'])
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
        'produtos_json': json.dumps(produtos_json),
        'data_inicial': data_inicial_str, 
        'data_final': data_final_str      
    }
    
    return render(request, 'projects/portfolio.html', contexto)

def extrair_dados_agrupados_do_dw(projeto):
    """
    Helper Function: Busca milhões de linhas no banco, agrupa por SKU,
    calcula a Curva ABC e junta com as Elasticidades.

    Escopo: dados vinculados ao projeto. Fallback para empresa inteira quando
    projeto=NULL (ex: projeto recriado após importação, SET_NULL por deleção).
    """
    # 1. DATABASE PUSHDOWN (SQL Bruto e Rápido)
    _qs = VendaHistoricaDW.objects.filter(projeto=projeto)
    if not _qs.exists():
        _qs = VendaHistoricaDW.objects.filter(empresa=projeto.empresa)
        if _qs.exists():
            logger.info(
                "extrair_dados_agrupados_do_dw: projeto %s sem dados vinculados — "
                "usando escopo da empresa %s.", projeto.id, projeto.empresa_id
            )

    agrupamento = _qs.values(
        'codigo_produto', 'nome_produto'
    ).annotate(
        volume_total=Coalesce(Sum('quantidade'), 0.0, output_field=FloatField()),
        receita_total=Coalesce(Sum(F('quantidade') * F('preco_praticado')), 0.0, output_field=FloatField()),
        custo_total=Coalesce(Sum(F('quantidade') * F('custo_unitario')), 0.0, output_field=FloatField())
    )

    df_vendas = pd.DataFrame(list(agrupamento))
    if df_vendas.empty:
        return []

    # 2. Busca Elasticidades e Custos/Preços Base (Do Resultado do AutoML)
    resultados = ResultadoPrecificacao.objects.filter(projeto=projeto).values(
        'codigo_produto'
    ).annotate(
        elasticidade=Avg('elasticidade'),
        preco_atual=Avg('preco_atual'),
        custo_unitario=Avg('custo_unitario')
    )
    df_resultados = pd.DataFrame(list(resultados))

    # 3. Merge (Junta as vendas reais com a inteligência do modelo)
    if not df_resultados.empty:
        df = pd.merge(df_vendas, df_resultados, on='codigo_produto', how='left')
        # Fallback de mercado para produtos novos sem IA treinada
        df['elasticidade'] = np.where(df['elasticidade'].isna(), -1.5, df['elasticidade'])
    else:
        df = df_vendas
        df['elasticidade'] = -1.5

    # Proteção: Se a IA não gerou preco_atual/custo_unitario, calcula a média ponderada
    if 'preco_atual' not in df.columns:
        df['preco_atual'] = np.nan
    if 'custo_unitario' not in df.columns:
        df['custo_unitario'] = np.nan

    df['preco_atual'] = np.where(
        df['preco_atual'].isna(), 
        np.where(df['volume_total'] > 0, df['receita_total'] / df['volume_total'], 0), 
        df['preco_atual']
    )

    df['custo_unitario'] = np.where(
        df['custo_unitario'].isna(), 
        np.where(df['volume_total'] > 0, df['custo_total'] / df['volume_total'], 0), 
        df['custo_unitario']
    )

    # 4. CÁLCULO DA CURVA ABC (O mesmo que fizemos na Matriz BCG)
    df = df.sort_values(by='receita_total', ascending=False).reset_index(drop=True)
    df['receita_acumulada'] = df['receita_total'].cumsum()
    df['percentual_acumulado'] = df['receita_acumulada'] / df['receita_total'].sum()

    def classificar_abc(pct):
        if pct <= 0.80: return 'A'
        elif pct <= 0.95: return 'B'
        else: return 'C'

    df['curva_abc'] = df['percentual_acumulado'].apply(classificar_abc)

    # 5. Formatação do Dicionário para o Algoritmo Greedy do CEO
    skus_data = []
    for _, row in df.iterrows():
        # Ignora lixo matemático (produtos sem volume ou sem preço)
        if row['volume_total'] > 0 and row['preco_atual'] > 0:
            skus_data.append({
                'codigo_produto': row['codigo_produto'],
                'nome_produto': row['nome_produto'],
                'preco': float(row['preco_atual']),
                'volume': float(row['volume_total']),
                'custo_unit': float(row['custo_unitario']),
                'elasticidade': float(row['elasticidade']),
                'curva_abc': row['curva_abc']
            })

    return skus_data


@login_required
def api_otimizar_margem_global(request):
    """
    AXIOM MARGIN COMMAND — Motor de Otimização de Margem v2
    
    Recebe a meta de margem do CFO e devolve o plano de ação cirúrgico,
    alterando apenas os SKUs estritamente necessários para atingir a meta.

    Correções aplicadas em relação à v1:
      - Estado simulado separado do estado original (preco_sim / vol_sim)
      - Greedy adaptativo: para assim que a meta é atingida, sem sobre-ajustar
      - Reordenação por impacto marginal a cada rodada (SKU mais valioso primeiro)
      - Passo de aumento proporcional ao delta de receita necessário (não fixo em 1%)
      - Documentação explícita de que a margem calculada é margem de contribuição
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.'}, status=405)

    try:
        dados = json.loads(request.body)
        projeto_id = dados.get('projeto_id')
        meta_margem_alvo = float(dados.get('meta_margem')) / 100.0  # Ex: 30 → 0.30

        limite_teto_input = float(dados.get('limite_teto', 5.0)) / 100.0

        projeto = ProjetoPrecificacao.objects.get(id=projeto_id, empresa=request.empresa)

        # ── 1. EXTRAÇÃO DOS DADOS ──────────────────────────────────────────────
        skus_data = extrair_dados_agrupados_do_dw(projeto)

        receita_atual_global = 0.0
        custo_variavel_global = 0.0  # ⚠️ Margem de contribuição (sem fixos rateados)

        skus_para_otimizar = []

        # ── 2. CLASSIFICAÇÃO DOS BALDES ────────────────────────────────────────
        for p in skus_data:
            receita_sku  = p['preco'] * p['volume']
            custo_sku    = p['custo_unit'] * p['volume']

            receita_atual_global    += receita_sku
            custo_variavel_global   += custo_sku

            elasticidade  = min(p['elasticidade'], 0.0) 
            margem_atual  = (p['preco'] - p['custo_unit']) / p['preco'] if p['preco'] > 0 else 0

            is_inelastico = elasticidade >= -1.0          # Demanda pouco sensível a preço
            is_elastico   = elasticidade <= -1.5          # KVI — não tocar
            is_sangria    = margem_atual < 0.10 and p['curva_abc'] == 'C'

            # Limite máximo de aumento permitido por grupo de risco
            if is_elastico:
                continue  # KVI: ignora completamente

            if is_sangria:
                limite_aumento = 1.15   # Mantemos os 15% fixos para SANGRIA (Afinal, produto que dá prejuízo precisa de remédio amargo)
                motivo = 'Estancar Sangria'
            elif is_inelastico:
                # A MÁGICA AQUI: O limite agora é 1.0 + o que o CFO digitou
                limite_aumento = 1.0 + limite_teto_input   
                motivo = 'Ouro Oculto'
            else:
                continue  # Elástico moderado: conservador, não otimiza

            skus_para_otimizar.append({
                # ── Referência imutável (origem) ──────────────────────────────
                'sku'         : p['codigo_produto'],
                'nome'        : p['nome_produto'],
                'preco_orig'  : p['preco'],           # NUNCA alterado após este ponto
                'vol_orig'    : p['volume'],           # NUNCA alterado após este ponto
                'custo_unit'  : p['custo_unit'],
                'elasticidade': elasticidade,
                'limite_preco': p['preco'] * limite_aumento,
                'motivo'      : motivo,

                # ── Estado simulado (mutável a cada rodada) ───────────────────
                'preco_sim'   : p['preco'],
                'vol_sim'     : p['volume'],
            })

        if receita_atual_global == 0:
            return JsonResponse({'status': 'erro', 'mensagem': 'Receita global zerada.'}, status=400)

        margem_atual_global = (receita_atual_global - custo_variavel_global) / receita_atual_global

        if margem_atual_global >= meta_margem_alvo:
            return JsonResponse({
                'status': 'sucesso',
                'atingiu_meta': True,
                'mensagem': 'Meta já atingida. Nenhuma alteração necessária.',
                'kpis': _montar_kpis(
                    margem_atual_global, margem_atual_global,
                    receita_atual_global, custo_variavel_global,
                    receita_atual_global, custo_variavel_global,
                    skus_alterados=0
                ),
                'plano_execucao': []
            })

        # ── 3. MOTOR HEURÍSTICO ADAPTATIVO ────────────────────────────────────
        #
        # Estratégia:
        #   a) A cada rodada calcula quanto de receita extra ainda falta.
        #   b) Distribui esse delta entre os SKUs proporcionalmente ao seu
        #      impacto marginal (lucro_contrib = vol_sim × (preco_sim - custo)).
        #   c) Para cada SKU, calcula o passo de aumento necessário — mas
        #      respeita o teto (limite_preco) e nunca passa de +2% por rodada
        #      para manter realismo.
        #   d) Aplica elasticidade para ajustar o volume simulado.
        #   e) Para assim que margem_simulada >= meta.
        #
        receita_sim = receita_atual_global
        custo_sim   = custo_variavel_global
        atingiu_meta = False

        MAX_RODADAS  = 20    # Mais rodadas, passos menores → mais preciso
        PASSO_MAX    = 0.02  # Cada SKU sobe no máximo 2% por rodada

        for rodada in range(1, MAX_RODADAS + 1):

            margem_sim = (receita_sim - custo_sim) / receita_sim
            if margem_sim >= meta_margem_alvo:
                atingiu_meta = True
                break

            # Filtra apenas SKUs que ainda têm espaço para subir
            skus_ativos = [s for s in skus_para_otimizar if s['preco_sim'] < s['limite_preco']]
            if not skus_ativos:
                break  # Esgotamos o espaço de manobra

            # ── Reordena por impacto marginal decrescente ──────────────────────
            # Impacto marginal ≈ lucro de contribuição atual do SKU
            # SKUs com maior lucro potencial vêm primeiro
            skus_ativos.sort(
                key=lambda s: s['vol_sim'] * (s['preco_sim'] - s['custo_unit']),
                reverse=True
            )

            # ── Receita alvo desta rodada ──────────────────────────────────────
            receita_alvo   = custo_sim / (1.0 - meta_margem_alvo)
            delta_necessario = receita_alvo - receita_sim

            # Soma total do impacto marginal (para distribuir o delta proporcionalmente)
            impacto_total = sum(
                s['vol_sim'] * (s['preco_sim'] - s['custo_unit'])
                for s in skus_ativos
            )

            for item in skus_ativos:
                if item['preco_sim'] >= item['limite_preco']:
                    continue

                # ── Passo proporcional ao peso do SKU no impacto total ─────────
                if impacto_total > 0:
                    peso = (item['vol_sim'] * (item['preco_sim'] - item['custo_unit'])) / impacto_total
                else:
                    peso = 1.0 / len(skus_ativos)

                # Quanto este SKU precisa subir para contribuir com sua parte do delta
                receita_sku_atual = item['preco_sim'] * item['vol_sim']
                receita_sku_alvo  = receita_sku_atual + (delta_necessario * peso)

                # Passo de preço necessário (sem considerar elasticidade ainda)
                passo_necessario = (receita_sku_alvo / receita_sku_atual) - 1.0 if receita_sku_atual > 0 else 0.0

                # Aplica teto de segurança por rodada
                passo_aplicado = min(passo_necessario, PASSO_MAX)
                passo_aplicado = max(passo_aplicado, 0.0)  # Nunca reduzir

                novo_preco = item['preco_sim'] * (1.0 + passo_aplicado)

                # Respeita o teto absoluto do grupo
                novo_preco = min(novo_preco, item['limite_preco'])

                if novo_preco <= item['preco_sim']:
                    continue  # Nada a fazer neste SKU nesta rodada

                # ── Remove cenário antigo do bolo global ───────────────────────
                receita_sim -= item['preco_sim'] * item['vol_sim']
                custo_sim   -= item['custo_unit'] * item['vol_sim']

                # ── Elasticidade: novo volume com base no preço ORIGINAL ────────
                # Usamos preco_orig como âncora para evitar acumulação de erro
                razao    = novo_preco / item['preco_orig']
                novo_vol = item['vol_orig'] * math.pow(razao, item['elasticidade'])
                novo_vol = max(novo_vol, 0.0)  # Volume nunca negativo

                # ── Atualiza estado simulado (NÃO toca em preco_orig / vol_orig) ─
                item['preco_sim'] = novo_preco
                item['vol_sim']   = novo_vol

                # ── Injeta novo cenário no bolo global ────────────────────────
                receita_sim += novo_preco * novo_vol
                custo_sim   += item['custo_unit'] * novo_vol

                # Verifica meta intra-rodada para parar o mais cedo possível
                if receita_sim > 0:
                    margem_intra = (receita_sim - custo_sim) / receita_sim
                    if margem_intra >= meta_margem_alvo:
                        atingiu_meta = True
                        break

            if atingiu_meta:
                break

        margem_simulada = (receita_sim - custo_sim) / receita_sim

        # ── 4. PLANO DE AÇÃO — só SKUs que realmente mudaram ──────────────────
        plano_de_acao = []
        for item in skus_para_otimizar:
            delta_pct = (item['preco_sim'] / item['preco_orig']) - 1.0
            if delta_pct > 0.0001:  # Filtra ruído de ponto flutuante
                plano_de_acao.append({
                    'sku'          : item['sku'],
                    'produto'      : item['nome'],
                    'preco_atual'  : round(item['preco_orig'], 2),
                    'preco_novo'   : round(item['preco_sim'], 2),
                    'aumento_pct'  : round(delta_pct * 100, 1),
                    'vol_projetado': round(item['vol_sim'], 0),
                    'estrategia'   : item['motivo'],
                })

        # Ordena o plano por maior aumento percentual (mais urgentes primeiro)
        plano_de_acao.sort(key=lambda x: x['aumento_pct'], reverse=True)

        # ── 5. RESPOSTA FINAL ──────────────────────────────────────────────────
        return JsonResponse({
            'status'          : 'sucesso',
            'atingiu_meta'    : atingiu_meta,
            'aviso_margem'    : (
                'Margem calculada é de contribuição variável. '
                'Custos fixos não estão incluídos.'
            ),
            'kpis'            : _montar_kpis(
                margem_atual_global, margem_simulada,
                receita_atual_global, custo_variavel_global,
                receita_sim, custo_sim,
                skus_alterados=len(plano_de_acao)
            ),
            'plano_execucao'  : plano_de_acao,
        })

    except ProjetoPrecificacao.DoesNotExist:
        return JsonResponse({'status': 'erro', 'mensagem': 'Projeto não encontrado.'}, status=404)
    except (ValueError, KeyError) as e:
        return JsonResponse({'status': 'erro', 'mensagem': f'Parâmetro inválido: {e}'}, status=400)
    except Exception as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e)}, status=500)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _montar_kpis(
    margem_atual, margem_proj,
    receita_atual, custo_atual,
    receita_proj, custo_proj,
    skus_alterados
):
    """Monta o dict de KPIs padronizado para o front-end."""
    lucro_atual  = receita_atual - custo_atual
    lucro_proj   = receita_proj  - custo_proj
    return {
        'margem_atual_pct'      : round(margem_atual * 100, 2),
        'margem_projetada_pct'  : round(margem_proj  * 100, 2),
        'ganho_margem_pp'       : round((margem_proj - margem_atual) * 100, 2),
        'lucro_atual_reais'     : round(lucro_atual, 2),
        'lucro_projetado_reais' : round(lucro_proj,  2),
        'ganho_lucro_reais'     : round(lucro_proj - lucro_atual, 2),
        'receita_atual_reais'   : round(receita_atual, 2),
        'receita_projetada_reais': round(receita_proj, 2),
        'skus_alterados'        : skus_alterados,
    }

@login_required
def painel_margin_command(request, projeto_id):
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=request.empresa)
    return render(request, 'projects/margin_command.html', {'projeto': projeto})


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO DE ENRIQUECIMENTO & CORRELAÇÕES (Fase 3)
# ══════════════════════════════════════════════════════════════════════════════

@login_required
def painel_correlacoes(request, projeto_id):
    """Exibe o painel de Enriquecimento & Correlações para um projeto."""
    from projects.enrichment.ibge import buscar_dados_municipio as _ibge_municipio

    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=request.empresa)
    analise = CorrelacaoAnalise.objects.filter(projeto=projeto).first()

    # Dados IBGE: usa snapshot salvo na análise; se vazio (análise antiga), busca ao vivo (cacheado)
    ibge_dados = {}
    if analise and analise.ibge_dados:
        ibge_dados = analise.ibge_dados
    elif request.empresa.codigo_ibge:
        try:
            ibge_dados = _ibge_municipio(request.empresa.codigo_ibge)
        except Exception:
            pass

    return render(request, 'projects/correlacoes.html', {
        'projeto': projeto,
        'analise': analise,
        'ibge_dados': ibge_dados,
    })


@login_required
def rodar_analise_correlacoes(request, projeto_id):
    """
    Executa o pipeline completo de enriquecimento e correlações para o projeto.
    Salva o resultado em CorrelacaoAnalise e redireciona para o painel.
    """
    if request.method != 'POST':
        return redirect('painel_correlacoes', projeto_id=projeto_id)

    from projects.enrichment import (
        montar_dataset_completo,
        calcular_correlacoes,
        gerar_insights,
        resumo_executivo,
    )
    from projects.enrichment.inmet import buscar_estacao_mais_proxima
    from projects.enrichment.ibge import buscar_dados_municipio

    projeto  = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=request.empresa)
    empresa  = request.empresa

    # Coordenadas efetivas: prioriza campos da loja quando disponíveis
    loja = projeto.loja
    _lat         = loja.lat         if (loja and loja.lat)         else empresa.lat
    _lon         = loja.lon         if (loja and loja.lon)         else empresa.lon
    _codigo_ibge = loja.codigo_ibge if (loja and loja.codigo_ibge) else empresa.codigo_ibge
    _bairro      = loja.bairro      if (loja and loja.bairro)      else None

    logger.info("[correlacoes] Iniciando análise — projeto=%s empresa=%s", projeto_id, empresa.id)

    # ── 1. Busca dados do DW ────────────────────────────────────────────────
    # Fallback: se não houver dados vinculados ao projeto (projeto=NULL por SET_NULL),
    # usa todos os dados da empresa.
    _qs_base = VendaHistoricaDW.objects.filter(empresa=empresa, projeto=projeto)
    if not _qs_base.exists():
        _qs_base = VendaHistoricaDW.objects.filter(empresa=empresa)
        if _qs_base.exists():
            logger.info("[correlacoes] Dados sem vínculo de projeto — usando escopo da empresa.")

    vendas_qs = _qs_base.values('data_venda', 'quantidade', 'preco_praticado')

    if not vendas_qs.exists():
        messages.warning(
            request,
            "Sem dados de vendas para analisar. "
            "Importe o histórico de vendas da empresa para usar este módulo."
        )
        CorrelacaoAnalise.objects.create(
            projeto=projeto, empresa=empresa, status='sem_dados',
        )
        logger.info("[correlacoes] Sem dados para empresa=%s — abortando.", empresa.id)
        return redirect('painel_correlacoes', projeto_id=projeto_id)

    df_raw = pd.DataFrame(list(vendas_qs))
    df_raw = df_raw.rename(columns={'data_venda': 'data', 'preco_praticado': 'preco'})

    # Agrega para nível diário (soma quantidade, média de preço)
    df_diario = (
        df_raw.groupby('data')
        .agg(quantidade=('quantidade', 'sum'), preco=('preco', 'mean'))
        .reset_index()
    )

    if len(df_diario) < 30:
        CorrelacaoAnalise.objects.create(
            projeto=projeto, empresa=empresa, status='sem_dados',
            n_registros=len(df_diario),
        )
        messages.warning(
            request,
            f"Dados insuficientes: {len(df_diario)} dias (mínimo 30). "
            "Adicione mais histórico de vendas e tente novamente."
        )
        return redirect('painel_correlacoes', projeto_id=projeto_id)

    # ── 2. Identifica estação INMET via lat/lon da Empresa ──────────────────
    estacao_codigo = None
    estacao_nome   = None
    distancia_km   = None

    if _lat and _lon:
        logger.info("[correlacoes] Buscando estação INMET próxima a (%.4f, %.4f)...", _lat, _lon)
        try:
            estacao = buscar_estacao_mais_proxima(_lat, _lon)
            if estacao:
                estacao_codigo = estacao.get('CD_ESTACAO')
                estacao_nome   = estacao.get('DC_NOME', '')
                distancia_km   = estacao.get('distancia_km')
                logger.info("[correlacoes] Estação INMET: %s (%s) — %.1f km", estacao_codigo, estacao_nome, distancia_km or 0)
            else:
                logger.info("[correlacoes] Nenhuma estação INMET próxima encontrada.")
        except Exception as exc:
            logger.warning("[correlacoes] Erro ao buscar estação INMET: %s", exc)
    else:
        logger.info("[correlacoes] Loja/Empresa sem lat/lon — dados de clima ignorados.")

    # ── 3. Pipeline de enriquecimento + features ────────────────────────────
    logger.info("[correlacoes] Iniciando montar_dataset_completo (ibge=%s, estacao=%s, bairro=%s)...",
                _codigo_ibge or None, estacao_codigo, _bairro or "N/A")
    try:
        df_enriquecido = montar_dataset_completo(
            df_diario,
            codigo_ibge=_codigo_ibge or None,
            codigo_estacao=estacao_codigo,
            lat=_lat,
            lon=_lon,
            bairro=_bairro,
        )
        logger.info("[correlacoes] Dataset montado: %d linhas, %d colunas.", len(df_enriquecido), len(df_enriquecido.columns))
    except Exception as exc:
        import traceback; traceback.print_exc()
        CorrelacaoAnalise.objects.create(
            projeto=projeto, empresa=empresa, status='erro',
            n_registros=len(df_diario),
        )
        messages.error(request, f"Erro no enriquecimento dos dados: {exc}")
        return redirect('painel_correlacoes', projeto_id=projeto_id)

    # ── 4. Correlações ──────────────────────────────────────────────────────
    logger.info("[correlacoes] Calculando correlações...")
    correlacoes = calcular_correlacoes(df_enriquecido, coluna_alvo='quantidade')
    logger.info("[correlacoes] %d correlações calculadas.", len(correlacoes))

    # ── 5. Insights ─────────────────────────────────────────────────────────
    logger.info("[correlacoes] Gerando insights...")
    contexto = {
        'nome_empresa':    empresa.nome,
        'ibge_municipio':  empresa.municipio or '',
        'ibge_uf':         empresa.uf or '',
        'ibge_classe':     df_enriquecido['ibge_classe'].iloc[0]
                           if 'ibge_classe' in df_enriquecido.columns else '',
        'ibge_nivel_geo':  df_enriquecido['ibge_nivel_geo'].iloc[0]
                           if 'ibge_nivel_geo' in df_enriquecido.columns else 'municipio',
        'ibge_bairro':     df_enriquecido['ibge_bairro'].iloc[0]
                           if 'ibge_bairro' in df_enriquecido.columns else '',
    }
    insights = gerar_insights(correlacoes, contexto=contexto)
    resumo   = resumo_executivo(insights, contexto=contexto)

    # ── 6. Busca snapshot completo IBGE (cacheado 30 dias, sem custo extra) ──
    ibge_dados = {}
    if _codigo_ibge:
        try:
            ibge_dados = buscar_dados_municipio(_codigo_ibge)
        except Exception as exc:
            logger.warning("[correlacoes] Erro ao buscar dados IBGE completos: %s", exc)

    # ── 7. Persiste ─────────────────────────────────────────────────────────
    CorrelacaoAnalise.objects.create(
        projeto=projeto,
        empresa=empresa,
        status='concluido',
        correlacoes=correlacoes,
        insights=insights,
        resumo_executivo=resumo,
        n_registros=len(df_diario),
        estacao_codigo=estacao_codigo,
        estacao_nome=estacao_nome,
        distancia_estacao_km=distancia_km,
        ibge_municipio=contexto.get('ibge_municipio') or None,
        ibge_classe=contexto.get('ibge_classe') or None,
        ibge_dados=ibge_dados,
        ibge_bairro=contexto.get('ibge_bairro') or None,
        ibge_setor_codigo=df_enriquecido['ibge_setor_codigo'].iloc[0]
                          if 'ibge_setor_codigo' in df_enriquecido.columns else None,
        ibge_nivel_geo=contexto.get('ibge_nivel_geo') or 'nenhum',
    )

    n_insights = len(insights)
    messages.success(
        request,
        f"Análise concluída: {n_insights} driver{'s' if n_insights != 1 else ''} "
        f"identificado{'s' if n_insights != 1 else ''} em {len(df_diario)} dias de dados."
    )
    return redirect('painel_correlacoes', projeto_id=projeto_id)


# ══════════════════════════════════════════════════════════════════════════════
# AXIOM TREND RADAR
# ══════════════════════════════════════════════════════════════════════════════

@login_required
def trend_radar_dashboard(request, projeto_id):
    """
    Dashboard principal do Trend Radar.
    Exibe tendências ativas (não arquivadas) do projeto.
    """
    empresa = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)

    # Busca tendências ativas (do último scan) do projeto
    tendencias_qs = TendenciaDetectada.objects.filter(
        empresa=empresa, projeto=projeto, arquivado=False,
    )

    # Ordena: negativo primeiro (crise), depois por nível e aceleração
    nivel_order = {'viral': 0, 'alto': 1, 'moderado': 2, 'baixo': 3}
    classif_order = {'negativo': 0, 'neutro': 1, 'positivo': 2}
    tendencias = sorted(
        tendencias_qs,
        key=lambda t: (classif_order.get(t.classificacao, 1), nivel_order.get(t.nivel, 3), -t.aceleracao_pct),
    )

    # Marca todas como visualizadas
    tendencias_qs.filter(visualizado=False).update(visualizado=True)

    config = RadarConfig.objects.filter(empresa=empresa).first()

    n_viral    = sum(1 for t in tendencias if t.nivel == 'viral')
    n_alto     = sum(1 for t in tendencias if t.nivel == 'alto')
    n_negativo = sum(1 for t in tendencias if t.classificacao == 'negativo')

    context = {
        'projeto': projeto,
        'tendencias': tendencias,
        'config': config,
        'n_viral': n_viral,
        'n_alto': n_alto,
        'n_negativo': n_negativo,
        'tem_alertas': n_viral + n_alto + n_negativo > 0,
    }
    return render(request, 'projects/trend_radar.html', context)


@login_required
def rodar_scan_radar(request, projeto_id):
    """Executa o pipeline completo de scan e redireciona ao dashboard."""
    if request.method != 'POST':
        return redirect('trend_radar_dashboard', projeto_id=projeto_id)

    from projects.trend_radar import executar_scan

    empresa = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)

    # ── Validação prévia: evita entrar no pipeline sem keywords ──────────────
    config = RadarConfig.objects.filter(empresa=empresa).first()
    manuais = list((config.palavras_chave if config else None) or [])
    if not manuais and (not config or config.usar_catalogo_automatico):
        tem_catalogo = (
            VendaHistoricaDW.objects
            .filter(empresa=empresa)
            .exclude(nome_produto__isnull=True)
            .exclude(nome_produto='')
            .exists()
        )
        if not tem_catalogo:
            messages.error(
                request,
                "Scan bloqueado: nenhuma palavra-chave configurada e o catálogo de produtos está vazio. "
                "Adicione palavras-chave manualmente (ex: 'cerveja', 'chocolate') em Configurações do Radar, "
                "ou importe o histórico de vendas da empresa para gerar keywords automaticamente."
            )
            return redirect('trend_radar_dashboard', projeto_id=projeto_id)

    try:
        tendencias = executar_scan(empresa, projeto=projeto)
        n = len(tendencias)
        if n > 0:
            messages.success(
                request,
                f"Scan concluído: {n} tendência{'s' if n != 1 else ''} detectada{'s' if n != 1 else ''} no seu mercado."
            )
        else:
            messages.info(
                request,
                "Scan concluído. Nenhuma tendência significativa — mercado estável."
            )
    except Exception as exc:
        logger.exception("Erro no scan do Trend Radar para empresa %s: %s", empresa.id, exc)
        messages.error(request, f"Erro durante o scan: {exc}")

    return redirect('trend_radar_dashboard', projeto_id=projeto_id)


@login_required
def arquivar_tendencia(request, projeto_id, tendencia_id):
    """Arquiva uma tendência específica (via POST)."""
    if request.method != 'POST':
        return redirect('trend_radar_dashboard', projeto_id=projeto_id)
    TendenciaDetectada.objects.filter(id=tendencia_id, empresa=request.empresa).update(arquivado=True)
    return redirect('trend_radar_dashboard', projeto_id=projeto_id)


@login_required
def salvar_radar_config(request, projeto_id):
    """Salva configurações do Radar."""
    if request.method != 'POST':
        return redirect('trend_radar_dashboard', projeto_id=projeto_id)

    empresa = request.empresa
    config, _ = RadarConfig.objects.get_or_create(
        empresa=empresa, defaults={'fontes_ativas': ['google_trends', 'rss']},
    )

    kws_raw = request.POST.get('palavras_chave', '')
    config.palavras_chave = [k.strip() for k in kws_raw.splitlines() if k.strip()]
    config.usar_catalogo_automatico = request.POST.get('usar_catalogo_automatico') == 'on'
    limiar_raw = request.POST.get('limiar_aceleracao', '').strip()
    config.limiar_aceleracao = float(limiar_raw) if limiar_raw else 50.0

    fontes = [f for f in ['google_trends', 'newsapi', 'rss', 'reddit'] if request.POST.get(f'fonte_{f}')]
    if fontes:
        config.fontes_ativas = fontes

    nk = request.POST.get('newsapi_key', '').strip()
    if nk:
        config.newsapi_key = nk

    config.save()
    messages.success(request, "Configurações do Radar salvas.")
    return redirect('trend_radar_dashboard', projeto_id=projeto_id)


# ══════════════════════════════════════════════════════════════════════════════
# AXIOM REPUTATION — Análise de Sentimento de Avaliações do Google
# ══════════════════════════════════════════════════════════════════════════════

_REPUTACAO_COOLDOWN_DIAS = 7


def _dias_para_proximo_scan(ultima_analise) -> int | None:
    """Retorna dias restantes para o próximo scan, ou None se já liberado."""
    if ultima_analise is None:
        return None
    from django.utils import timezone
    proxima = ultima_analise.criado_em + timedelta(days=_REPUTACAO_COOLDOWN_DIAS)
    diff = proxima - timezone.now()
    if diff.total_seconds() <= 0:
        return None
    return max(1, diff.days + 1)


@login_required
def reputacao_dashboard(request, projeto_id):
    """
    Dashboard principal do módulo Reputation.

    Estados do template:
      1. Candidatos na sessão → exibe lista para seleção
      2. Sem config → exibe formulário de busca
      3. Config definida → exibe última análise + botão de scan + histórico
    """
    empresa = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)
    config  = ReputacaoConfig.objects.filter(empresa=empresa).first()
    ultima_analise = (
        AnaliseReputacao.objects.filter(empresa=empresa, projeto=projeto).first()
        if config else None
    )

    # Histórico para o gráfico (até 12 scans anteriores)
    historico = []
    if config:
        historico = list(
            AnaliseReputacao.objects
            .filter(empresa=empresa, projeto=projeto, status='concluido')
            .order_by('criado_em')
            .values('criado_em', 'score_sentimento', 'rating_geral')[:12]
        )

    dias_bloqueado = _dias_para_proximo_scan(ultima_analise)
    candidatos = request.session.pop('reputacao_candidatos', None)

    context = {
        'projeto':         projeto,
        'config':          config,
        'ultima_analise':  ultima_analise,
        'historico_json':  json.dumps([
            {
                'data':  h['criado_em'].strftime('%d/%m/%Y'),
                'score': h['score_sentimento'],
                'rating': h['rating_geral'],
            }
            for h in historico
        ]),
        'dias_bloqueado':  dias_bloqueado,
        'candidatos':      candidatos,  # list[dict] ou None
    }
    return render(request, 'projects/reputacao.html', context)


@login_required
def reputacao_buscar_lugar(request, projeto_id):
    """POST: busca empresas pelo nome e armazena candidatos na sessão."""
    if request.method != 'POST':
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    from django.conf import settings
    from projects.reputation import buscar_empresas

    nome = request.POST.get('nome_empresa', '').strip()
    if not nome:
        messages.error(request, "Digite o nome da empresa para buscar.")
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    api_key = getattr(settings, 'GOOGLE_PLACES_API_KEY', '')
    try:
        candidatos = buscar_empresas(nome, api_key)
    except Exception as exc:
        messages.error(request, f"Erro ao buscar no Google: {exc}")
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    if not candidatos:
        messages.warning(request, f'Nenhuma empresa encontrada para "{nome}". Tente um nome diferente.')
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    # Armazena na sessão (temporário, para a tela de seleção)
    request.session['reputacao_candidatos'] = candidatos
    return redirect('reputacao_dashboard', projeto_id=projeto_id)


@login_required
def reputacao_confirmar_lugar(request, projeto_id):
    """POST: confirma a seleção de um Place ID e salva em ReputacaoConfig."""
    if request.method != 'POST':
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    from django.conf import settings
    from projects.reputation import buscar_detalhes_lugar

    place_id = request.POST.get('place_id', '').strip()
    if not place_id:
        messages.error(request, "Seleção inválida.")
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    api_key = getattr(settings, 'GOOGLE_PLACES_API_KEY', '')
    try:
        detalhes = buscar_detalhes_lugar(place_id, api_key)
    except Exception as exc:
        messages.error(request, f"Erro ao confirmar local: {exc}")
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    empresa = request.empresa

    # Cria ou atualiza config (uma empresa pode trocar o local monitorado)
    ReputacaoConfig.objects.filter(empresa=empresa).delete()
    ReputacaoConfig.objects.create(
        empresa=empresa,
        nome_busca=request.POST.get('nome_busca', detalhes['nome']),
        google_place_id=place_id,
        google_place_nome=detalhes['nome'],
        google_place_endereco=detalhes.get('endereco', ''),
        google_place_url=detalhes.get('url_maps', ''),
        google_place_foto=detalhes.get('foto_url', '') or '',
    )

    messages.success(
        request,
        f'"{detalhes["nome"]}" configurado com sucesso. Clique em "Rodar Análise" para gerar o primeiro relatório.'
    )
    return redirect('reputacao_dashboard', projeto_id=projeto_id)


@login_required
def reputacao_analisar(request, projeto_id):
    """
    POST: executa a análise completa (Google Reviews + Claude Haiku).
    Respeita cooldown de 7 dias.
    """
    if request.method != 'POST':
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    from django.conf import settings
    from projects.reputation import buscar_detalhes_lugar, analisar_sentimento_reviews

    empresa = request.empresa
    projeto = get_object_or_404(ProjetoPrecificacao, id=projeto_id, empresa=empresa)
    config  = ReputacaoConfig.objects.filter(empresa=empresa).first()

    if not config:
        messages.error(request, "Configure um local antes de rodar a análise.")
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    # Verifica cooldown
    ultima = AnaliseReputacao.objects.filter(empresa=empresa, projeto=projeto).first()
    dias   = _dias_para_proximo_scan(ultima)
    if dias:
        messages.warning(
            request,
            f"Análise disponível em {dias} dia{'s' if dias != 1 else ''}. "
            f"O cooldown de {_REPUTACAO_COOLDOWN_DIAS} dias evita cobranças desnecessárias de API."
        )
        return redirect('reputacao_dashboard', projeto_id=projeto_id)

    places_key    = getattr(settings, 'GOOGLE_PLACES_API_KEY', '')
    anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '')

    try:
        # 1. Busca reviews no Google
        detalhes = buscar_detalhes_lugar(config.google_place_id, places_key)
        reviews  = detalhes.get('reviews', [])
        rating   = detalhes.get('rating')
        total    = detalhes.get('total_avaliacoes', 0)

        if not reviews:
            AnaliseReputacao.objects.create(
                empresa=empresa, projeto=projeto, config=config,
                status='sem_reviews',
                rating_geral=rating, total_avaliacoes=total,
            )
            messages.warning(request, "Nenhuma avaliação disponível para este local no momento.")
            return redirect('reputacao_dashboard', projeto_id=projeto_id)

        # 2. Análise de sentimento via Claude Haiku
        resultado = analisar_sentimento_reviews(
            reviews=reviews,
            nome_empresa=config.google_place_nome,
            rating_geral=rating,
            total_avaliacoes=total,
            api_key=anthropic_key,
        )

        # 3. Enriquece as reviews com sentimento individual do Claude
        por_review_map = {r['indice']: r for r in resultado.get('por_review', [])}
        reviews_enriquecidas = []
        for i, rv in enumerate(reviews):
            rv_extra = por_review_map.get(i, {})
            reviews_enriquecidas.append({
                **rv,
                'sentimento': rv_extra.get('sentimento', 'neutro'),
                'temas':      rv_extra.get('temas', []),
            })

        # 4. Persiste
        AnaliseReputacao.objects.create(
            empresa=empresa,
            projeto=projeto,
            config=config,
            status='concluido',
            rating_geral=rating,
            total_avaliacoes=total,
            sentimento_geral=resultado['sentimento_geral'],
            score_sentimento=resultado['score'],
            temas_positivos=resultado['temas_positivos'],
            temas_negativos=resultado['temas_negativos'],
            resumo_executivo=resultado.get('resumo_executivo', ''),
            reviews=reviews_enriquecidas,
            tokens_input=resultado.get('tokens_input', 0),
            tokens_output=resultado.get('tokens_output', 0),
        )

        messages.success(
            request,
            f"Análise concluída — Score: {resultado['score']}/100 "
            f"({resultado['sentimento_geral'].capitalize()}) com base em {len(reviews)} avaliação(ões)."
        )

    except Exception as exc:
        logger.exception("Erro na análise de reputação para empresa %s: %s", empresa.id, exc)
        AnaliseReputacao.objects.create(
            empresa=empresa, projeto=projeto, config=config, status='erro',
        )
        messages.error(request, f"Erro durante a análise: {exc}")

    return redirect('reputacao_dashboard', projeto_id=projeto_id)


@login_required
def reputacao_trocar_lugar(request, projeto_id):
    """POST: remove a configuração atual e volta para a tela de busca."""
    if request.method != 'POST':
        return redirect('reputacao_dashboard', projeto_id=projeto_id)
    ReputacaoConfig.objects.filter(empresa=request.empresa).delete()
    messages.info(request, "Local removido. Busque um novo estabelecimento para monitorar.")
    return redirect('reputacao_dashboard', projeto_id=projeto_id)