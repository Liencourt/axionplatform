Documentação Técnica e Científica - Modulo Elasticidade de preço


1. O Algoritmo Base e o Problema da Causalidade Reversa

O Motor: Utilizamos Ordinary Least Squares (Regressão OLS log-log) via biblioteca statsmodels. O coeficiente da variável Preço (log_p) representa a Elasticidade Preço-Demanda.

A Solução da Endogeneidade: Preço e quantidade possuem causalidade reversa (o gerente baixa o preço quando a venda cai, puxando a elasticidade falsamente para perto de zero). Resolvemos isso utilizando Variáveis Instrumentais (Lags de Preço). O modelo treina usando a variável de impacto log_p_lag1 (o preço de ontem) para isolar a pura reação do consumidor.

2. Blindagem de Outliers (Aparando os Extremos)

Técnica de Winsorização: O varejo possui anomalias severas (ex: compras corporativas de 500 unidades num dia atípico). O motor aplica winsorize de 2% nas pontas inferiores e superiores da curva logarítmica de volume (log_y), impedindo que outliers distorçam a inclinação da reta de regressão.

3. Definição de Baseline de Simulação (Ancoragem no Presente)

Preço Base (Mediana Recente): Em vez de usar a média de preço histórica (que cria "preços fantasmas" nunca praticados), o sistema extrai os últimos 30 dias de venda e calcula a Mediana. Isso ignora promoções de 1 dia e crava o verdadeiro "preço de prateleira" atual.

Demanda Base (Corte de 30 dias): A quantidade inicial da simulação (Q_base) não usa a média global, mas sim a média dos últimos 30 dias a partir da última data de venda. Isso ancora a matemática financeira no patamar atual de tração do produto.

O algoritmo exige um volume mínimo de dias com vendas ativas (baseline de 30 dias, escalonável conforme o número de variáveis). Não é necessário que o preço varie dezenas de vezes; basta que exista pelo menos uma alteração de preço histórico (dois patamares de preço) dentro dessa janela de tempo para que o motor consiga traçar a reta da elasticidade.

min_obs = max(30, n_params * 5)
if len(df_sku) < min_obs:
    continue

Esta é a trava do preço. O .nunique() <= 1 significa que se o produto teve apenas 1 preço único a vida inteira (ex: custou R$ 10,00 todos os 30 dias), ele é rejeitado. Para passar por essa catraca, o preço precisa ter mudado pelo menos uma vez (ou seja, ter tido pelo menos 2 preços diferentes, como R$ 10,00 e R$ 9,00) dentro desse histórico de vendas.

if df_sku['log_p'].nunique() <= 1:
    continue

4. A Matemática da "Catraca" e Confiabilidade

Graus de Liberdade: O modelo exige um mínimo de observações min_obs = max(30, n_params * 5). Onde n_params inclui o Preço, 7 dias da semana e N variáveis extras.

Algoritmo da Confiabilidade Composta: Avaliamos duas frentes para atestar o modelo estatístico no Front-end:

R² (R-Squared): Explicação da variância geral.

P-Value (do Preço): Significância estatística pura do preço contra a hipótese nula.

Fórmula no Código: * Alta: R² >= 0.50 AND P-Value < 0.05

Média: R² >= 0.30 AND P-Value < 0.10

Baixa: Qualquer cenário inferior a estes limiares.