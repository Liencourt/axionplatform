#  Axiom Platform - System Prompt & Architecture Rules

##  Visão do Produto
O Axiom é um SaaS B2B Enterprise de Inteligência de Varejo e Pricing. Nós não somos apenas uma calculadora; somos um motor de "Explainable AI" (IA Explicável) que ajuda CEOs e CFOs a maximizarem lucros, estancarem prejuízos e protegerem o Market Share (KVIs) sem usar "caixas pretas" matemáticas.

##  Tech Stack Oficial
- **Backend:** Python 3.11+ / Django
- **Data Science:** Pandas, NumPy, XGBoost, MLxtend (FP-Growth em breve), Prophet.
- **Frontend:** Django Templates, Vanilla JavaScript, Bootstrap 5.3.
- **Visualização de Dados:** Plotly.js (renderizado no frontend via JSON enviado pela API).
- **Banco de Dados:** PostgreSQL (via Django ORM) + Data Warehouse para transações/cupons.

##  Regras Sagradas de Arquitetura (Backend)

1. **Multi-Tenant Estrito (A Regra de Ouro):**
   - NUNCA faça uma query no banco sem filtrar pela empresa.
   - Padrão obrigatório: `Model.objects.filter(..., empresa=request.empresa)` ou `projeto__empresa=request.empresa`.

2. **Matemática e Prevenção de Erros de Ponto Flutuante:**
   - Em loops heurísticos (como o Margin Command), sempre separe o estado original (`preco_orig`, `vol_orig`) do estado simulado (`preco_sim`, `vol_sim`).
   - Calcule elasticidade sempre a partir da base original para evitar drift exponencial (a "Síndrome do Volume Infinito").
   - A elasticidade real deve ser tratada com um teto: `elasticidade = min(valor_db, 0.0)`. Aumentar preço nunca aumenta volume na vida real.

3. **Pandas & Tratamento de Dados:**
   - **NÃO USE** `.fillna()` junto com `np.where()` no Pandas, isso causa o erro `ndarray`.
   - Use o padrão aninhado puro: `df['col'] = np.where(df['col'].isna(), valor_fallback, df['col'])`.

4. **Padrão de API (Endpoints JSON):**
   - Todas as respostas de API devem seguir a estrutura: 
     `{'status': 'sucesso' | 'erro', 'mensagem': '...', 'kpis': {}, 'data': []}`
   - Sempre envolva a lógica da API em blocos `try/except Exception as e:` e retorne `status=400` ou `500` formatado.

##  Regras de UI/UX (Frontend)

1. **Agnóstico ao Tema (Dark/Light Mode):**
   - **PROIBIDO** usar classes de cores absolutas como `bg-white`, `bg-light`, `text-dark` ou `text-black`.
   - **OBRIGATÓRIO** usar variáveis de tema do Bootstrap 5: `bg-body`, `bg-body-tertiary`, `bg-transparent`, `text-body`, `text-muted`.
   - Componentes "Premium" devem usar Glassmorphism suave (backdrop-filter) sem forçar cores de fundo opacas.

2. **Foco na Usabilidade do Executivo:**
   - As telas devem aplicar o conceito de *Graceful Degradation* (Degradação Elegante). Se a IA não consegue atingir uma meta sem quebrar as regras de negócio, ela para, entrega o melhor resultado possível e avisa o usuário.
   - Evitar jogar p-valores ou fórmulas estatísticas puras na interface do C-Level. Traduzir para: "Ouro Oculto", "Estancar Sangria", "Força de Arrasto".

3. **Plotly.js:**
   - Gráficos devem ter fundo transparente: `paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)'`.
   - As cores das fontes dos eixos devem ser neutras (ex: `#888` ou `#aaa`) para funcionarem bem tanto no modo claro quanto no escuro.