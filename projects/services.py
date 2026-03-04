import pandas as pd
import numpy as np
import xgboost as xgb
import shap
from datetime import timedelta
from .models import VendaHistoricaDW, PrevisaoDemanda,PrevisaoFaturamentoMacro,FaturamentoEmpresaDW
from prophet import Prophet
from prophet.make_holidays import make_holidays_df



def treinar_previsao_xgboost(empresa, codigo_produto, dias_futuros=30):
    """
    O Motor Micro: Lê o DW, constrói a árvore de decisão, gera o forecast e o SHAP.
    """
    # 1. Busca os dados brutos no Data Warehouse
    vendas = VendaHistoricaDW.objects.filter(
        empresa=empresa, codigo_produto=codigo_produto
    ).values('data_venda', 'quantidade', 'preco_praticado')

    if not vendas:
        return False, "Sem dados suficientes no DW para este produto."

    df = pd.DataFrame.from_records(vendas)
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    
    # Agrupa por dia (caso existam várias vendas no mesmo dia)
    df = df.groupby('data_venda').agg({'quantidade': 'sum', 'preco_praticado': 'mean'}).reset_index()
    df.set_index('data_venda', inplace=True)
    
    # Preenche os dias que a loja não vendeu com zero
    idx = pd.date_range(df.index.min(), df.index.max())
    df = df.reindex(idx, fill_value=0)
    
    # ==========================================
    # ENGENHARIA DE RECURSOS (Traduzindo o Tempo para a IA)
    # ==========================================
    df['dia_semana'] = df.index.dayofweek
    df['mes'] = df.index.month
    df['fim_de_semana'] = df['dia_semana'].apply(lambda x: 1 if x >= 5 else 0)
    
    # Lags (O que aconteceu ontem? E há 7 dias?)
    df['venda_ontem'] = df['quantidade'].shift(1)
    df['venda_semana_passada'] = df['quantidade'].shift(7)
    df['media_movel_7d'] = df['quantidade'].shift(1).rolling(window=7).mean()
    
    # Remove as primeiras linhas que ficaram com "NaN" devido aos Lags
    df.dropna(inplace=True)

    if len(df) < 14:
        return False, "Histórico muito curto para treinar médias móveis (mínimo 14 dias)."

    # Separa quem é X (Variáveis) e y (Alvo)
    y = df['quantidade']
    X = df.drop(columns=['quantidade'])

    # ==========================================
    # TREINAMENTO DO XGBOOST
    # ==========================================
    modelo = xgb.XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
    modelo.fit(X, y)
    
    r2_score = modelo.score(X, y) # Pega a aderência do modelo (Acurácia base)

    # ==========================================
    # IA EXPLICÁVEL (SHAP)
    # ==========================================
    explainer = shap.TreeExplainer(modelo)
    shap_values = explainer.shap_values(X)
    
    # Calcula a importância média (absoluta) de cada variável e converte para dicionário
    importancia = np.abs(shap_values).mean(axis=0)
    pesos_shap = {coluna: float(peso) for coluna, peso in zip(X.columns, importancia)}
    
    # Ordena para pegar os mais importantes primeiro
    pesos_shap_ordenados = dict(sorted(pesos_shap.items(), key=lambda item: item[1], reverse=True))

    # ==========================================
    # PREVISÃO DO FUTURO (Os próximos X dias)
    # ==========================================
    ultima_data = df.index.max()
    datas_futuras = [ultima_data + timedelta(days=i) for i in range(1, dias_futuros + 1)]
    
    # Montamos um DataFrame fictício pro futuro para a IA prever
    # (Em um ambiente real complexo, projetaríamos os Lags dinamicamente dia a dia)
    df_futuro = pd.DataFrame(index=datas_futuras)
    df_futuro['preco_praticado'] = df['preco_praticado'].iloc[-1] # Assume o último preço
    df_futuro['dia_semana'] = df_futuro.index.dayofweek
    df_futuro['mes'] = df_futuro.index.month
    df_futuro['fim_de_semana'] = df_futuro['dia_semana'].apply(lambda x: 1 if x >= 5 else 0)
    df_futuro['venda_ontem'] = df['quantidade'].iloc[-1]
    df_futuro['venda_semana_passada'] = df['quantidade'].iloc[-7:].mean() # Aproximação
    df_futuro['media_movel_7d'] = df['media_movel_7d'].iloc[-1]
    
    # Garante a mesma ordem de colunas
    df_futuro = df_futuro[X.columns]
    
    previsoes = modelo.predict(df_futuro)
    previsoes = np.maximum(previsoes, 0) # Venda não pode ser negativa

    # ==========================================
    # SALVANDO NO BANCO DE DADOS
    # ==========================================
    dados_json = {
        'datas': [d.strftime('%Y-%m-%d') for d in datas_futuras],
        'valores_previstos': [round(float(v), 2) for v in previsoes],
        'datas_historicas': [d.strftime('%Y-%m-%d') for d in df.index[-30:]], # Manda os últimos 30 dias pra emendar no gráfico
        'valores_historicos': [float(v) for v in df['quantidade'].iloc[-30:]]
    }

    PrevisaoDemanda.objects.create(
        empresa=empresa,
        codigo_produto=codigo_produto,
        dados_previsao=dados_json,
        explicabilidade_shap=pesos_shap_ordenados,
        acuracia_r2=r2_score
    )

    return True, "Previsão gerada com XGBoost e explicada pelo SHAP com sucesso!"

def treinar_previsao_macro_empresa(empresa, dias_futuros=90):
    """
    O Motor Macro: Lê a tabela leve do CFO, treina o Prophet e gera a previsão.
    """
    # 1. Busca os dados na tabela NOVA e LEVE
    faturamentos = FaturamentoEmpresaDW.objects.filter(empresa=empresa).values('data_faturamento', 'faturamento_total')

    if not faturamentos:
        return False, "Nenhum histórico macro financeiro encontrado. Por favor, faça o upload da planilha financeira."

    # 2. Transforma em Pandas (Já vem pronto, não precisa multiplicar quantidade x preço)
    df = pd.DataFrame.from_records(faturamentos)
    df['data_faturamento'] = pd.to_datetime(df['data_faturamento']).dt.tz_localize(None)
    
    # 3. Renomeia as colunas para o padrão de ferro do Prophet ('ds' para Data e 'y' para Valor)
    df.rename(columns={'data_faturamento': 'ds', 'faturamento_total': 'y'}, inplace=True)
    df_agrupado = df.sort_values('ds')
    
    if len(df_agrupado) < 30:
        return False, "O Prophet precisa de pelo menos 30 dias de histórico para achar padrões confiáveis."
    
    ano_inicio = df_agrupado['ds'].dt.year.min()
    ano_fim = df_agrupado['ds'].dt.year.max()

    lista_anos = list(range(ano_inicio, ano_fim + 2))

    df_holidays = make_holidays_df(
        year_list=lista_anos, 
        country='BR', 
        #province='RJ' # <--- Opcional: No futuro, puxar o 'estado' da tabela Empresa
    )
    
    # ==========================================
    # TREINAMENTO DO FACEBOOK PROPHET
    # ==========================================
    modelo =  Prophet(
    scaling='minmax',      
    holidays=df_holidays,
    growth='linear',
    n_changepoints=38,
    # Deixe o Prophet achar os changepoints sozinho (padrão é 25)
    changepoint_range=0.95, # Lê até os últimos 10% para captar tendências recentes
    
    yearly_seasonality=True,  # Entende Janeiro x Dezembro
    weekly_seasonality=True,  # Entende Segunda x Sábado
    daily_seasonality=False,  # Correto, pois é dado diário e não por hora
    
    seasonality_mode='additive', 
    
    seasonality_prior_scale=10.0,
    holidays_prior_scale=15.0,
    
    changepoint_prior_scale=0.18, # Subimos do padrão (0.05) para dar flexibilidade, mas sem o Overfitting do 0.90
    
    interval_width=0.80, # Ótimo, dá uma "sombra" azul de 90% de confiança para o CFO
    uncertainty_samples=1000 # 1000 já é mais que suficiente e deixa o servidor mais rápido
)


    
    # Treina a IA
    modelo.fit(df_agrupado)

    # ==========================================
    # PREVISÃO DO FUTURO (Cria um calendário em branco + 90 dias)
    # ==========================================
    futuro = modelo.make_future_dataframe(periods=dias_futuros)
    forecast = modelo.predict(futuro)

    # Separa os dados de Saída (Histórico e Previsão) e garante que o faturamento não seja negativo
    forecast['yhat'] = np.maximum(forecast['yhat'], 0)
    forecast['yhat_lower'] = np.maximum(forecast['yhat_lower'], 0)
    forecast['yhat_upper'] = np.maximum(forecast['yhat_upper'], 0)

    # Calcula a soma total do dinheiro que vai entrar nesses 90 dias
    apenas_futuro = forecast.tail(dias_futuros)
    soma_projetada = apenas_futuro['yhat'].sum()


    # ==========================================
    # NOVO: CÁLCULO DE ACURÁCIA (MAPE E COMPARATIVO)
    # ==========================================
    # Junta os dados reais com os dados que a IA "achou" que seriam para o mesmo período
    df_comparativo = pd.merge(df_agrupado, forecast[['ds', 'yhat']], on='ds', how='inner')
    
    # Previne divisão por zero (caso tenha dia com venda R$ 0,00)
    df_comparativo['y_safe'] = df_comparativo['y'].replace(0, np.nan)
    
    # Calcula o MAPE (Erro Percentual Absoluto Médio)
    mape = (np.abs((df_comparativo['y_safe'] - df_comparativo['yhat']) / df_comparativo['y_safe']).mean()) * 100
    if np.isnan(mape) or np.isinf(mape):
        mape = 0.0

    # Calcula os totais do histórico (O que aconteceu vs O que a IA teria previsto)
    total_real = df_comparativo['y'].sum()
    total_previsto = df_comparativo['yhat'].sum()
    diferenca = total_real - total_previsto

    # ==========================================
    # PREPARAR OS JSONS PARA O DASHBOARD (Frontend)
    # ==========================================
    # Os valores que compõem o gráfico da linha do tempo com a sombra azul de confiança
    dados_json = {
        'datas': forecast['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'previsao': [round(x, 2) for x in forecast['yhat']],
        'limite_inferior': [round(x, 2) for x in forecast['yhat_lower']],
        'limite_superior': [round(x, 2) for x in forecast['yhat_upper']],
        'datas_reais': df_agrupado['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'valores_reais': [round(x, 2) for x in df_agrupado['y']],
        # ENVIANDO A ACURÁCIA PARA A TELA:
        'mape': round(mape, 2),
        'total_real': round(total_real, 2),
        'total_previsto': round(total_previsto, 2),
        'diferenca': round(diferenca, 2)
    }

    # Os valores que compõem a explicação da Diretoria (Por que a IA previu isso?)
    componentes = {
        'datas': forecast['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'tendencia': [round(x, 2) for x in forecast['trend']],
        # Prevenindo erro caso a pessoa tenha menos de 1 ano de dados e a IA não gere as colunas sazonais
        'sazonalidade_semanal': [round(x, 2) for x in forecast['weekly']] if 'weekly' in forecast.columns else [],
        'sazonalidade_anual': [round(x, 2) for x in forecast['yearly']] if 'yearly' in forecast.columns else []
    }

    # ==========================================
    # SALVAR NO BANCO
    # ==========================================
    PrevisaoFaturamentoMacro.objects.create(
        empresa=empresa,
        dados_forecast=dados_json,
        componentes_sazonalidade=componentes,
        faturamento_projetado_total=soma_projetada
    )

    return True, f"Faturamento Macro de 90 dias projetado com sucesso usando Facebook Prophet!"