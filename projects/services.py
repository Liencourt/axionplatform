import pandas as pd
import numpy as np
import xgboost as xgb
import shap
from datetime import timedelta
from .models import VendaHistoricaDW, PrevisaoDemanda,PrevisaoFaturamentoMacro,FaturamentoEmpresaDW
from prophet import Prophet
from prophet.make_holidays import make_holidays_df
import pandas as pd
from django.db.models import Q
from .models import EventoCalendario
from prophet.diagnostics import cross_validation, performance_metrics
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score as sklearn_r2

def treinar_previsao_xgboost(empresa, codigo_produto, loja=None, dias_futuros=30):
    """
    O Motor Micro V8: Com Train/Test Split Honesto e Loop Autoregressivo no Futuro.
    """
    # 1. Busca os dados brutos no Data Warehouse
    query = VendaHistoricaDW.objects.filter(empresa=empresa, codigo_produto=codigo_produto)
    
    if loja:
        query = query.filter(loja=loja)
    else:
        query = query.filter(loja__isnull=True)
        
    vendas = query.values('data_venda', 'quantidade', 'preco_praticado')

    if not vendas:
        nome_loja = loja.nome if loja else 'Global'
        return False, f"Sem dados suficientes no DW para este produto na filial {nome_loja}."

    df = pd.DataFrame.from_records(vendas)
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    
    df = df.groupby('data_venda').agg({'quantidade': 'sum', 'preco_praticado': 'mean'}).reset_index()
    df.set_index('data_venda', inplace=True)
    df = df.sort_index() # Segurança temporal
    
    idx = pd.date_range(df.index.min(), df.index.max())
    df = df.reindex(idx, fill_value=0)
    
    # ==========================================
    # ENGENHARIA DE RECURSOS (Feature Engineering)
    # ==========================================
    df['dia_semana'] = df.index.dayofweek
    df['mes'] = df.index.month
    df['fim_de_semana'] = df['dia_semana'].apply(lambda x: 1 if x >= 5 else 0)
    
    # Lags (Essenciais para o XGBoost entender o ritmo das vendas)
    df['venda_ontem'] = df['quantidade'].shift(1)
    df['venda_semana_passada'] = df['quantidade'].shift(7)
    df['media_movel_7d'] = df['quantidade'].shift(1).rolling(window=7).mean()
    
    df.dropna(inplace=True)

    if len(df) < 20:
        nome_loja = loja.nome if loja else 'Global'
        return False, f"Histórico muito curto para aplicar o Train/Test Split na filial {nome_loja} (mínimo 20 dias ativos)."

    # Separa quem é X (Variáveis) e y (Alvo)
    y = df['quantidade']
    X = df.drop(columns=['quantidade'])

   # ==========================================
    # TRAIN / TEST SPLIT (WAPE)
    # ==========================================
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    modelo = xgb.XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
    modelo.fit(X_train, y_train) 
    
    y_pred_test = modelo.predict(X_test)
    y_pred_test = np.maximum(y_pred_test, 0) # Corta previsões negativas matematicas
    
    # ==========================================
    # O CÁLCULO DE ACURÁCIA DO VAREJO (WAPE)
    # ==========================================
    soma_erros_absolutos = np.abs(y_test - y_pred_test).sum()
    soma_vendas_reais = y_test.sum()
    
    if soma_vendas_reais > 0:
        wape = soma_erros_absolutos / soma_vendas_reais
        acuracia_varejo = max(0.0, 100.0 - (wape * 100)) # Ex: Se errou 15%, a acurácia é 85%
    else:
        acuracia_varejo = 0.0
        
    print(f"[AXIOM DEBUG] XGBoost SKU {codigo_produto} | Acurácia WAPE (Teste): {acuracia_varejo:.2f}%")

    # Como já sabemos a acurácia real, RETREINAMOS a IA com 100% dos dados.
    modelo.fit(X, y)

    # ==========================================
    # IA EXPLICÁVEL (SHAP)
    # ==========================================
    explainer = shap.TreeExplainer(modelo)
    shap_values = explainer.shap_values(X)
    
    importancia = np.abs(shap_values).mean(axis=0)
    pesos_shap = {coluna: float(peso) for coluna, peso in zip(X.columns, importancia)}
    pesos_shap_ordenados = dict(sorted(pesos_shap.items(), key=lambda item: item[1], reverse=True))

    # ==========================================
    # PREVISÃO DO FUTURO (LOOP AUTOREGRESSIVO)
    # ==========================================
    ultima_data = df.index.max()
    ultimo_preco = df['preco_praticado'].iloc[-1]
    
    # Memória viva da IA para calcular a média móvel do futuro
    historico_qtd = list(df['quantidade'].values) 
    
    previsoes = []
    datas_futuras = []

    for i in range(1, dias_futuros + 1):
        data_atual = ultima_data + timedelta(days=i)
        datas_futuras.append(data_atual)
        
        # A Mágica: O "ontem" do futuro usa as previsões que a IA acabou de fazer no loop anterior!
        venda_ontem = historico_qtd[-1]
        venda_semana_passada = historico_qtd[-7] if len(historico_qtd) >= 7 else historico_qtd[-1]
        media_movel_7d = np.mean(historico_qtd[-7:]) if len(historico_qtd) >= 7 else venda_ontem
        
        linha_futura = pd.DataFrame([{
            'preco_praticado': ultimo_preco,
            'dia_semana': data_atual.dayofweek,
            'mes': data_atual.month,
            'fim_de_semana': 1 if data_atual.dayofweek >= 5 else 0,
            'venda_ontem': venda_ontem,
            'venda_semana_passada': venda_semana_passada,
            'media_movel_7d': media_movel_7d
        }])
        
        linha_futura = linha_futura[X.columns]
        
        # A IA prevê apenas ESTE DIA
        pred_dia = modelo.predict(linha_futura)[0]
        pred_dia = max(0.0, float(pred_dia)) 
        
        previsoes.append(pred_dia)
        
        # Guardamos a previsão de hoje no histórico, para a IA usar como o "venda_ontem" de amanhã
        historico_qtd.append(pred_dia)

    # ==========================================
    # SALVANDO NO BANCO DE DADOS
    # ==========================================
    dados_json = {
        'datas': [d.strftime('%Y-%m-%d') for d in datas_futuras],
        'valores_previstos': [round(v, 2) for v in previsoes],
        'datas_historicas': [d.strftime('%Y-%m-%d') for d in df.index[-30:]],
        'valores_historicos': [round(float(v), 2) for v in df['quantidade'].iloc[-30:]]
    }

    PrevisaoDemanda.objects.create(
        empresa=empresa,
        loja=loja,
        codigo_produto=codigo_produto,
        dados_previsao=dados_json,
        explicabilidade_shap=pesos_shap_ordenados,
        acuracia_r2=acuracia_varejo # Salva a acurácia honesta cortada no Train/Test
    )

    return True, "Previsão Autoregressiva gerada com XGBoost e explicada pelo SHAP com sucesso!"

def treinar_previsao_macro_empresa(empresa, loja_id, dias_futuros=90):
    
    # 1. FILTRO BLINDADO
    faturamentos = FaturamentoEmpresaDW.objects.filter(
        empresa=empresa, 
        loja_id=loja_id
    ).values('data_faturamento', 'faturamento_total')

    if not faturamentos.exists():
        return False, "Nenhum dado de venda foi encontrado para esta filial. Por favor, verifique a base de dados."

    # 2. PREPARAÇÃO DO DATAFRAME PRINCIPAL
    df_agrupado = pd.DataFrame(list(faturamentos))
    df_agrupado = df_agrupado.rename(columns={'data_faturamento': 'ds', 'faturamento_total': 'y'})
    df_agrupado['ds'] = pd.to_datetime(df_agrupado['ds'])
    
    # Ordenar por data é vital para séries temporais e cross-validation
    df_agrupado = df_agrupado.sort_values(by='ds').reset_index(drop=True)

    # ==========================================
    # AUDITORIA DE ESCALA (A sua sacada brilhante)
    # ==========================================
    y = df_agrupado['y']
    fator_minmax = y.max() - y.min()
    fator_absmax = y.abs().max()
    distorcao = abs(fator_minmax - fator_absmax) / fator_absmax * 100 if fator_absmax != 0 else 0
    
    print(f"\n[AXIOM AUDITORIA] Loja ID: {loja_id}")
    print(f"Fator minmax: {fator_minmax:.2f}")
    print(f"Fator absmax: {fator_absmax:.2f}")
    print(f"Distorção relativa (Picos): {distorcao:.1f}%")

    # 3. FERIADOS NACIONAIS E CORPORATIVOS
    lista_anos = df_agrupado['ds'].dt.year.unique().tolist()
    df_holidays = make_holidays_df(year_list=lista_anos, country='BR', province='RJ')

    eventos_db = EventoCalendario.objects.filter(
        Q(empresa=empresa) & (Q(loja__isnull=True) | Q(loja_id=loja_id))
    ).values('nome', 'data_inicio', 'data_fim')

    if eventos_db.exists():
        lista_feriados_expandida = []
        for evento in eventos_db:
            datas_periodo = pd.date_range(start=evento['data_inicio'], end=evento['data_fim'])
            for data in datas_periodo:
                lista_feriados_expandida.append({
                    'holiday': evento['nome'],
                    'ds': data
                })
        
        if lista_feriados_expandida:
            df_custom_holidays = pd.DataFrame(lista_feriados_expandida)
            df_custom_holidays['ds'] = pd.to_datetime(df_custom_holidays['ds'])
            df_holidays = pd.concat([df_holidays, df_custom_holidays], ignore_index=True)
            print(f"[AXIOM DEBUG] Eventos Customizados injetados com sucesso!")

    # ==========================================
    # CHANGEPOINTS DINÂMICOS (Trava Anti-Crash)
    # ==========================================
    n_pontos = len(df_agrupado)
    n_changepoints_calculado = min(25, max(5, int(n_pontos * 0.8 * 0.3)))
    print(f"[AXIOM DEBUG] Total de dias no histórico: {n_pontos} | Changepoints ajustados para: {n_changepoints_calculado}")

    # ==========================================
    # TREINAMENTO DO FACEBOOK PROPHET
    # ==========================================
    modelo = Prophet(
        scaling='minmax', # Mantemos a sua escolha para proteger contra Black Friday
        holidays=df_holidays,
        growth='linear',
        n_changepoints=n_changepoints_calculado, 
        changepoint_range=0.95,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode='additive', 
        seasonality_prior_scale=10.0,
        holidays_prior_scale=15.0,
        changepoint_prior_scale=0.18, 
        interval_width=0.80, 
        uncertainty_samples=1000 
    )
    
    modelo.fit(df_agrupado)

    # ==========================================
    # PREVISÃO DO FUTURO
    # ==========================================
    futuro = modelo.make_future_dataframe(periods=dias_futuros)
    forecast = modelo.predict(futuro)

    forecast['yhat'] = np.maximum(forecast['yhat'], 0)
    forecast['yhat_lower'] = np.maximum(forecast['yhat_lower'], 0)
    forecast['yhat_upper'] = np.maximum(forecast['yhat_upper'], 0)

    apenas_futuro = forecast.tail(dias_futuros)
    soma_projetada = apenas_futuro['yhat'].sum()

    # ==========================================
    # CROSS-VALIDATION (O Fim do Falso Gabarito)
    # ==========================================
    # Só faz CV se tivermos dados suficientes (ex: mais de 90 dias)
    if n_pontos > 90:
        try:
            print("[AXIOM DEBUG] Iniciando Cross-Validation (Pode levar alguns segundos...)")
            # Usa a sua lógica de dias iniciais, garantindo formato string pro Prophet
            dias_iniciais = max(60, n_pontos - 30)
            
            df_cv = cross_validation(
                modelo,
                initial=f'{dias_iniciais} days',
                period='15 days',
                horizon='30 days',
                parallel=None
            )
            df_perf = performance_metrics(df_cv)
            # Prophet retorna 'mape' em decimal (0.15), multiplicamos por 100
            mape_final = df_perf['mape'].mean() * 100
            print(f"[AXIOM DEBUG] Cross-Validation concluído. MAPE Honesto: {mape_final:.2f}%")
        except Exception as e:
            print(f"[AXIOM AVISO] Falha no CV (histórico talvez esparso): {e}. Usando MAPE Simples.")
            # Fallback para o MAPE simples que tínhamos caso o CV falhe
            df_comparativo = pd.merge(df_agrupado, forecast[['ds', 'yhat']], on='ds', how='inner')
            df_comparativo['y_safe'] = df_comparativo['y'].replace(0, np.nan)
            mape_final = (np.abs((df_comparativo['y_safe'] - df_comparativo['yhat']) / df_comparativo['y_safe']).mean()) * 100
    else:
        # Histórico curto, usa o MAPE Simples
        df_comparativo = pd.merge(df_agrupado, forecast[['ds', 'yhat']], on='ds', how='inner')
        df_comparativo['y_safe'] = df_comparativo['y'].replace(0, np.nan)
        mape_final = (np.abs((df_comparativo['y_safe'] - df_comparativo['yhat']) / df_comparativo['y_safe']).mean()) * 100

    if np.isnan(mape_final) or np.isinf(mape_final):
        mape_final = 0.0

    # Calcula totais para a interface
    df_comparativo_base = pd.merge(df_agrupado, forecast[['ds', 'yhat']], on='ds', how='inner')
    total_real = df_comparativo_base['y'].sum()
    total_previsto = df_comparativo_base['yhat'].sum()
    diferenca = total_real - total_previsto

    # ==========================================
    # PREPARAR OS JSONS
    # ==========================================
    dados_json = {
        'datas': forecast['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'previsao': [round(x, 2) for x in forecast['yhat']],
        'limite_inferior': [round(x, 2) for x in forecast['yhat_lower']],
        'limite_superior': [round(x, 2) for x in forecast['yhat_upper']],
        'datas_reais': df_agrupado['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'valores_reais': [round(x, 2) for x in df_agrupado['y']],
        'mape': round(mape_final, 2),
        'total_real': round(total_real, 2),
        'total_previsto': round(total_previsto, 2),
        'diferenca': round(diferenca, 2),
        'distorcao_picos': round(distorcao, 1)
    }

    componentes = {
        'datas': forecast['ds'].dt.strftime('%Y-%m-%d').tolist(),
        'tendencia': [round(x, 2) for x in forecast['trend']],
        'sazonalidade_semanal': [round(x, 2) for x in forecast['weekly']] if 'weekly' in forecast.columns else [],
        'sazonalidade_anual': [round(x, 2) for x in forecast['yearly']] if 'yearly' in forecast.columns else []
    }

    PrevisaoFaturamentoMacro.objects.create(
        empresa=empresa,
        loja_id=loja_id,
        dados_forecast=dados_json,
        componentes_sazonalidade=componentes,
        faturamento_projetado_total=soma_projetada
    )

    return True, f"Faturamento Macro projetado com sucesso usando Facebook Prophet!"