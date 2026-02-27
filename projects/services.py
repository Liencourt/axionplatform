import pandas as pd
import numpy as np
import xgboost as xgb
import shap
from datetime import timedelta
from .models import VendaHistoricaDW, PrevisaoDemanda,PrevisaoFaturamentoMacro
from prophet import Prophet


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
    O Motor Macro: Lê todo o DW da empresa, agrupa o faturamento (Qtd * Preco) por dia,
    treina o Facebook Prophet (com feriados do Brasil) e gera a previsão corporativa.
    """
    # 1. Busca todos os dados da empresa no DW
    vendas = VendaHistoricaDW.objects.filter(empresa=empresa).values('data_venda', 'quantidade', 'preco_praticado')

    if not vendas:
        return False, "Nenhum dado encontrado no Data Warehouse da empresa."

    # 2. Transforma em Pandas e calcula o Faturamento
    df = pd.DataFrame.from_records(vendas)
    df['data_venda'] = pd.to_datetime(df['data_venda']).dt.tz_localize(None) # Remove fuso horário para o Prophet não chorar
    df['faturamento'] = df['quantidade'] * df['preco_praticado']
    
    # 3. Agrupa por Dia (Obrigatoriedade do Prophet)
    df_agrupado = df.groupby('data_venda')['faturamento'].sum().reset_index()
    
    # 4. Renomeia as colunas para o padrão de ferro do Prophet ('ds' para Data e 'y' para Valor)
    df_agrupado.rename(columns={'data_venda': 'ds', 'faturamento': 'y'}, inplace=True)
    df_agrupado.sort_values('ds', inplace=True)
    
    if len(df_agrupado) < 30:
        return False, "O Prophet precisa de pelo menos 30 dias de histórico para achar padrões confiáveis."

    # ==========================================
    # TREINAMENTO DO FACEBOOK PROPHET
    # ==========================================
    modelo = Prophet(
        daily_seasonality=False, # Não queremos prever por hora, só por dia
        yearly_seasonality=True, # Queremos entender se o verão é melhor que o inverno
        weekly_seasonality=True, # Queremos entender se sexta vende mais que segunda
        seasonality_mode='multiplicative' # Geralmente o varejo funciona em porcentagem (ex: 20% a mais na sexta)
    )
    
    # A MÁGICA: Adicionamos os feriados do Brasil nativamente!
    modelo.add_country_holidays(country_name='BR')
    
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
    # PREPARAR OS JSONS PARA O DASHBOARD (Frontend)
    # ==========================================
    # Os valores que compõem o gráfico da linha do tempo com a sombra azul de confiança
    dados_json = {
        'datas': forecast['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'previsao': [round(x, 2) for x in forecast['yhat']],
        'limite_inferior': [round(x, 2) for x in forecast['yhat_lower']],
        'limite_superior': [round(x, 2) for x in forecast['yhat_upper']],
        'datas_reais': df_agrupado['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'valores_reais': [round(x, 2) for x in df_agrupado['y']]
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