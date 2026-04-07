"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          AXIOM PRICING ENGINE — PUBLIC REST API  v1                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Endpoints B2B para integração com o Motor de Pricing Axiom.                ║
║  Todas as rotas estão sob o prefixo:  /api/v1/                              ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  AUTENTICAÇÃO                                                                ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  Header obrigatório em TODAS as requisições:                                 ║
║                                                                              ║
║      X-Axiom-API-Key: <uuid-da-empresa>                                     ║
║                                                                              ║
║  A chave UUID está disponível em: Painel → Configurações → Integração API   ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  FORMATO DE RESPOSTA (padrão em todos os endpoints)                          ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  {                                                                           ║
║      "status":   "sucesso" | "erro",                                         ║
║      "mensagem": "Descrição legível do resultado",                           ║
║      "kpis":     { ...métricas calculadas... },                              ║
║      "data":     [ ...registros detalhados... ]                              ║
║  }                                                                           ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ENDPOINTS DISPONÍVEIS                                                       ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║                                                                              ║
║  [1] GET  /api/v1/elasticidade/<projeto_id>/                                ║
║      Retorna os resultados de elasticidade calculados pelo AutoML para       ║
║      todos os SKUs de um projeto.                                            ║
║                                                                              ║
║  [2] POST /api/v1/simular-preco/                                             ║
║      Simula o impacto financeiro de uma mudança de preço em um SKU.          ║
║                                                                              ║
║  [3] POST /api/v1/otimizar-margem/                                           ║
║      Executa o Axiom Margin Command: recebe uma meta de margem do CFO        ║
║      e devolve o plano de ação cirúrgico por SKU.                            ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import math
import json
import logging
from datetime import timedelta, date
from functools import wraps

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.db import transaction, IntegrityError
from django.db.models import Avg, Max

from accounts.models import Empresa
from projects.models import (
    ProjetoPrecificacao,
    ResultadoPrecificacao,
    VendaHistoricaDW,
    FaturamentoEmpresaDW,
    Loja,
)
from projects.views import extrair_dados_agrupados_do_dw, _montar_kpis

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# AUTENTICAÇÃO
# ──────────────────────────────────────────────────────────────────────────────

def _get_empresa_by_api_key(request):
    """
    Extrai e valida a API Key do header X-Axiom-API-Key.
    Retorna o objeto Empresa ou None se inválida.
    """
    api_key = request.headers.get('X-Axiom-API-Key', '').strip()
    if not api_key:
        return None
    try:
        return Empresa.objects.get(api_key=api_key, ativo=True)
    except (Empresa.DoesNotExist, Exception):
        return None


def require_api_key(view_func):
    """Decorator que protege endpoints públicos via X-Axiom-API-Key."""
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        empresa = _get_empresa_by_api_key(request)
        if empresa is None:
            return JsonResponse({
                'status': 'erro',
                'mensagem': 'API Key inválida ou ausente. Envie o header X-Axiom-API-Key.',
                'kpis': {},
                'data': []
            }, status=401)
        request.empresa = empresa
        return view_func(request, *args, **kwargs)
    return _wrapped


# ──────────────────────────────────────────────────────────────────────────────
# [1] GET /api/v1/elasticidade/<projeto_id>/
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_elasticidade(request, projeto_id):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: GET /api/v1/elasticidade/<projeto_id>/                       │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  DESCRIÇÃO                                                               │
    │  Retorna os parâmetros de elasticidade-preço calculados pelo AutoML     │
    │  para todos os SKUs de um projeto. Use para integrar com seu ERP,        │
    │  planilhas de pricing ou sistemas de BI.                                 │
    │                                                                          │
    │  PARÂMETROS DE URL                                                       │
    │  • projeto_id  (int, obrigatório) — ID do projeto Axiom                  │
    │                                                                          │
    │  PARÂMETROS DE QUERY STRING (opcionais)                                  │
    │  • loja_id     (int) — filtra resultados por loja específica             │
    │  • sku         (str) — filtra por código de produto exato                │
    │  • confianca   (str) — "alta" | "media" | "baixa"                        │
    │                                                                          │
    │  RESPOSTA — campo "data" (lista de objetos):                             │
    │  {                                                                       │
    │    "codigo_produto":    "ABC123",      // Código do SKU                  │
    │    "nome_produto":      "Arroz 5kg",   // Nome do produto                │
    │    "loja":              "Global",      // Nome da loja ou "Global"       │
    │    "elasticidade":      -1.42,         // Coeficiente de elasticidade    │
    │    "ic_lower":          -1.80,         // Intervalo de confiança (inf.)  │
    │    "ic_upper":          -1.05,         // Intervalo de confiança (sup.)  │
    │    "p_value":           0.002,         // P-valor do coeficiente         │
    │    "r_squared":         0.67,          // Poder explicativo do modelo    │
    │    "confianca":         "Alta",        // "Alta" | "Média" | "Baixa"     │
    │    "comportamento":     "Sensível",    // "Sensível" | "Fiel"            │
    │    "preco_atual":       12.90,         // Preço base (R$)                │
    │    "preco_sugerido":    13.50,         // Preço ótimo sugerido (R$)      │
    │    "custo_unitario":    8.20,          // Custo unitário (R$)            │
    │    "margem_projetada":  39.3           // Margem projetada (%)           │
    │  }                                                                       │
    │                                                                          │
    │  RESPOSTA — campo "kpis":                                                │
    │  {                                                                       │
    │    "total_skus":        120,           // Total de SKUs analisados       │
    │    "skus_sensiveis":    45,            // SKUs elásticos (sensíveis)     │
    │    "skus_fieis":        75,            // SKUs inelásticos (fiéis)       │
    │    "pct_confianca_alta": 62.5          // % com confiança alta (%)       │
    │  }                                                                       │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if request.method != 'GET':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)

    try:
        projeto = ProjetoPrecificacao.objects.get(id=projeto_id, empresa=request.empresa)
    except ProjetoPrecificacao.DoesNotExist:
        return JsonResponse({'status': 'erro', 'mensagem': 'Projeto não encontrado.', 'kpis': {}, 'data': []}, status=404)

    try:
        qs = ResultadoPrecificacao.objects.filter(projeto=projeto).select_related('loja')

        # Filtros opcionais por query string
        loja_id = request.GET.get('loja_id')
        sku = request.GET.get('sku', '').strip()
        filtro_confianca = request.GET.get('confianca', '').lower()

        if loja_id:
            qs = qs.filter(loja_id=loja_id)
        if sku:
            qs = qs.filter(codigo_produto=sku)

        mapeamento_nomes = dict(
            VendaHistoricaDW.objects.filter(projeto=projeto)
            .values_list('codigo_produto', 'nome_produto')
            .distinct()
        )

        data = []
        total = sensiveis = fieis = alta_confianca = 0

        for res in qs:
            r2 = res.r_squared or 0.0
            p_val = res.elasticidade_p_value if res.elasticidade_p_value is not None else 1.0

            if r2 >= 0.50 and p_val < 0.05:
                confianca = "Alta"
            elif r2 >= 0.30 and p_val < 0.10:
                confianca = "Média"
            else:
                confianca = "Baixa"

            # Filtro de confiança opcional
            if filtro_confianca == 'alta' and confianca != 'Alta':
                continue
            if filtro_confianca == 'media' and confianca != 'Média':
                continue
            if filtro_confianca == 'baixa' and confianca != 'Baixa':
                continue

            comportamento = "Sensível" if res.elasticidade < -1.0 else "Fiel"

            total += 1
            if comportamento == "Sensível":
                sensiveis += 1
            else:
                fieis += 1
            if confianca == "Alta":
                alta_confianca += 1

            data.append({
                'codigo_produto': res.codigo_produto,
                'nome_produto': mapeamento_nomes.get(res.codigo_produto, res.codigo_produto),
                'loja': res.loja.nome if res.loja else 'Global',
                'elasticidade': round(res.elasticidade, 4),
                'ic_lower': round(res.elasticidade_ic_lower, 4) if res.elasticidade_ic_lower is not None else None,
                'ic_upper': round(res.elasticidade_ic_upper, 4) if res.elasticidade_ic_upper is not None else None,
                'p_value': round(p_val, 4),
                'r_squared': round(r2, 4),
                'confianca': confianca,
                'comportamento': comportamento,
                'preco_atual': round(res.preco_atual, 2) if res.preco_atual else None,
                'preco_sugerido': round(res.preco_sugerido, 2) if res.preco_sugerido else None,
                'custo_unitario': round(res.custo_unitario, 2) if res.custo_unitario else None,
                'margem_projetada': round(res.margem_projetada, 2) if res.margem_projetada else None,
            })

        pct_alta = round((alta_confianca / total * 100), 1) if total > 0 else 0.0

        return JsonResponse({
            'status': 'sucesso',
            'mensagem': f'{total} SKU(s) retornados para o projeto "{projeto.nome}".',
            'kpis': {
                'total_skus': total,
                'skus_sensiveis': sensiveis,
                'skus_fieis': fieis,
                'pct_confianca_alta': pct_alta,
            },
            'data': data,
        })

    except Exception as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=500)


# ──────────────────────────────────────────────────────────────────────────────
# [2] POST /api/v1/simular-preco/
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_simular_preco(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: POST /api/v1/simular-preco/                                  │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  DESCRIÇÃO                                                               │
    │  Simula o impacto financeiro de uma mudança de preço usando o modelo    │
    │  de elasticidade treinado pelo AutoML. Retorna volume projetado,         │
    │  faturamento, lucro e margem — em escala diária.                         │
    │                                                                          │
    │  BODY (JSON) — campos OBRIGATÓRIOS:                                      │
    │  {                                                                       │
    │    "resultado_id":  42,          // ID do ResultadoPrecificacao          │
    │    "novo_preco":    15.90,       // Novo preço a simular (R$)            │
    │    "custo":         8.20         // Custo unitário vigente (R$)          │
    │  }                                                                       │
    │                                                                          │
    │  BODY (JSON) — campos OPCIONAIS:                                         │
    │  {                                                                       │
    │    "elasticidade_customizada": -1.8   // Sobrescreve a elasticidade      │
    │                                       // calculada pelo modelo (float)   │
    │  }                                                                       │
    │                                                                          │
    │  RESPOSTA — campo "kpis":                                                │
    │  {                                                                       │
    │    "quantidade_prevista":    8.5,     // Volume diário projetado (un.)   │
    │    "faturamento_projetado":  134.55,  // Faturamento diário (R$)         │
    │    "lucro_projetado":        62.9,    // Lucro diário (R$)               │
    │    "margem_projetada":       46.8,    // Margem de contribuição (%)      │
    │    "preco_atual_base":       12.90,   // Preço base utilizado (R$)       │
    │    "elasticidade_usada":    -1.42     // Elasticidade efetiva aplicada   │
    │  }                                                                       │
    │                                                                          │
    │  REGRAS DE NEGÓCIO                                                       │
    │  • Elasticidade é limitada a ≤ 0.0 (aumento de preço nunca aumenta      │
    │    volume — regra do motor Axiom).                                        │
    │  • Volume negativo é zerado automaticamente.                              │
    │  • O campo resultado_id deve pertencer a um projeto da sua empresa.      │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)

    try:
        dados = json.loads(request.body)

        # Validação de campos obrigatórios
        campos_obrigatorios = ['resultado_id', 'novo_preco', 'custo']
        faltando = [c for c in campos_obrigatorios if dados.get(c) is None]
        if faltando:
            return JsonResponse({
                'status': 'erro',
                'mensagem': f'Campos obrigatórios ausentes: {", ".join(faltando)}.',
                'kpis': {},
                'data': []
            }, status=400)

        resultado_id = dados['resultado_id']
        novo_preco = float(str(dados['novo_preco']).replace(',', '.'))
        custo = float(str(dados['custo']).replace(',', '.'))
        elasticidade_customizada = dados.get('elasticidade_customizada')

        if novo_preco <= 0:
            return JsonResponse({'status': 'erro', 'mensagem': 'novo_preco deve ser maior que zero.', 'kpis': {}, 'data': []}, status=400)
        if custo < 0:
            return JsonResponse({'status': 'erro', 'mensagem': 'custo não pode ser negativo.', 'kpis': {}, 'data': []}, status=400)

        resultado = ResultadoPrecificacao.objects.get(id=resultado_id, projeto__empresa=request.empresa)

        ultima_data = VendaHistoricaDW.objects.filter(
            projeto_id=resultado.projeto_id,
            codigo_produto=resultado.codigo_produto,
            loja=resultado.loja
        ).aggregate(ultima=Max('data_venda'))['ultima']

        demanda_base_diaria = 0.0
        if ultima_data:
            data_corte = ultima_data - timedelta(days=30)
            media_recente = VendaHistoricaDW.objects.filter(
                projeto_id=resultado.projeto_id,
                codigo_produto=resultado.codigo_produto,
                loja=resultado.loja,
                data_venda__gte=data_corte
            ).aggregate(media=Avg('quantidade'))['media']

            if media_recente:
                demanda_base_diaria = float(media_recente)
            else:
                media_geral = VendaHistoricaDW.objects.filter(
                    projeto_id=resultado.projeto_id,
                    codigo_produto=resultado.codigo_produto,
                    loja=resultado.loja
                ).aggregate(media=Avg('quantidade'))['media']
                demanda_base_diaria = float(media_geral) if media_geral else 0.0

        preco_atual = resultado.preco_atual
        if elasticidade_customizada is not None:
            elasticidade = float(elasticidade_customizada)
        else:
            elasticidade = resultado.elasticidade
        # Teto obrigatório: aumentar preço NUNCA aumenta volume (CLAUDE.md §2)
        elasticidade = min(elasticidade, 0.0)

        razao_preco = novo_preco / preco_atual if preco_atual > 0 else 1
        if razao_preco <= 0:
            razao_preco = 1

        quantidade_diaria_prevista = max(demanda_base_diaria * math.pow(razao_preco, elasticidade), 0.0)

        faturamento_diario = quantidade_diaria_prevista * novo_preco
        lucro_diario = (novo_preco - custo) * quantidade_diaria_prevista
        margem_projetada = ((novo_preco - custo) / novo_preco) * 100 if novo_preco > 0 else 0

        return JsonResponse({
            'status': 'sucesso',
            'mensagem': 'Simulação calculada com sucesso.',
            'kpis': {
                'quantidade_prevista': round(quantidade_diaria_prevista, 2),
                'faturamento_projetado': round(faturamento_diario, 2),
                'lucro_projetado': round(lucro_diario, 2),
                'margem_projetada': round(margem_projetada, 2),
                'preco_atual_base': round(preco_atual, 2),
                'elasticidade_usada': round(elasticidade, 4),
            },
            'data': []
        })

    except ResultadoPrecificacao.DoesNotExist:
        return JsonResponse({'status': 'erro', 'mensagem': 'resultado_id não encontrado nesta empresa.', 'kpis': {}, 'data': []}, status=404)
    except (ValueError, TypeError) as e:
        return JsonResponse({'status': 'erro', 'mensagem': f'Parâmetro inválido: {e}', 'kpis': {}, 'data': []}, status=400)
    except Exception as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=500)


# ──────────────────────────────────────────────────────────────────────────────
# [3] POST /api/v1/otimizar-margem/
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_otimizar_margem(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: POST /api/v1/otimizar-margem/                                │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  DESCRIÇÃO                                                               │
    │  Executa o Axiom Margin Command: dado um projeto e uma meta de margem   │
    │  global, o motor heurístico identifica quais SKUs devem ter o preço      │
    │  ajustado — e em quanto — para atingir a meta com o menor impacto       │
    │  possível no volume de vendas.                                            │
    │                                                                          │
    │  Somente SKUs "Ouro Oculto" (inelásticos) e "Estancar Sangria"          │
    │  (margem negativa em curva C) são ajustados. KVIs elásticos são          │
    │  protegidos automaticamente.                                              │
    │                                                                          │
    │  BODY (JSON) — campos OBRIGATÓRIOS:                                      │
    │  {                                                                       │
    │    "projeto_id":   7,      // ID do ProjetoPrecificacao                  │
    │    "meta_margem":  30.0    // Meta de margem de contribuição global (%)  │
    │  }                                                                       │
    │                                                                          │
    │  BODY (JSON) — campos OPCIONAIS:                                         │
    │  {                                                                       │
    │    "limite_teto":  5.0     // Teto máximo de aumento por SKU (%).        │
    │                            // Padrão: 5.0. Range recomendado: 2–15.      │
    │  }                                                                       │
    │                                                                          │
    │  RESPOSTA — campo "kpis":                                                │
    │  {                                                                       │
    │    "margem_atual_pct":       22.4,   // Margem atual do portfólio (%)    │
    │    "margem_projetada_pct":   30.1,   // Margem após os ajustes (%)       │
    │    "ganho_margem_pp":         7.7,   // Ganho em pontos percentuais      │
    │    "lucro_atual_reais":   84200.0,   // Lucro atual (R$)                 │
    │    "lucro_projetado_reais":95100.0,  // Lucro após os ajustes (R$)       │
    │    "ganho_lucro_reais":    10900.0,  // Ganho incremental de lucro (R$)  │
    │    "receita_atual_reais": 380000.0,  // Receita atual (R$)               │
    │    "receita_projetada_reais":385000, // Receita projetada (R$)           │
    │    "skus_alterados":           18    // Nº de SKUs com preço ajustado    │
    │  }                                                                       │
    │                                                                          │
    │  RESPOSTA — campo "data" (plano de execução, um objeto por SKU):         │
    │  {                                                                       │
    │    "sku":           "XYZ789",        // Código do produto                │
    │    "produto":       "Café 500g",     // Nome do produto                  │
    │    "preco_atual":   8.90,            // Preço atual (R$)                 │
    │    "preco_novo":    9.30,            // Preço sugerido (R$)              │
    │    "aumento_pct":   4.5,             // Variação % do preço              │
    │    "vol_projetado": 120,             // Volume diário projetado (un.)    │
    │    "estrategia":    "Ouro Oculto"    // "Ouro Oculto" | "Estancar Sangria"│
    │  }                                                                       │
    │                                                                          │
    │  CAMPO EXTRA DE RESPOSTA                                                 │
    │  "atingiu_meta" (bool): true se a meta foi atingida com os SKUs          │
    │  disponíveis; false indica Graceful Degradation (melhor resultado         │
    │  possível sem violar regras de negócio).                                  │
    │                                                                          │
    │  NOTA: margem calculada é de contribuição variável.                      │
    │  Custos fixos não estão incluídos no cálculo.                             │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)

    try:
        dados = json.loads(request.body)

        campos_obrigatorios = ['projeto_id', 'meta_margem']
        faltando = [c for c in campos_obrigatorios if dados.get(c) is None]
        if faltando:
            return JsonResponse({
                'status': 'erro',
                'mensagem': f'Campos obrigatórios ausentes: {", ".join(faltando)}.',
                'kpis': {},
                'data': []
            }, status=400)

        projeto_id = dados['projeto_id']
        meta_margem_alvo = float(dados['meta_margem']) / 100.0
        limite_teto_input = float(dados.get('limite_teto', 5.0)) / 100.0

        if not (0 < meta_margem_alvo < 1):
            return JsonResponse({'status': 'erro', 'mensagem': 'meta_margem deve estar entre 1 e 99 (%).', 'kpis': {}, 'data': []}, status=400)

        projeto = ProjetoPrecificacao.objects.get(id=projeto_id, empresa=request.empresa)

        skus_data = extrair_dados_agrupados_do_dw(projeto)

        receita_atual_global = 0.0
        custo_variavel_global = 0.0
        skus_para_otimizar = []

        for p in skus_data:
            receita_sku = p['preco'] * p['volume']
            custo_sku = p['custo_unit'] * p['volume']
            receita_atual_global += receita_sku
            custo_variavel_global += custo_sku

            elasticidade = min(p['elasticidade'], 0.0)
            margem_atual = (p['preco'] - p['custo_unit']) / p['preco'] if p['preco'] > 0 else 0

            is_inelastico = elasticidade >= -1.0
            is_elastico = elasticidade <= -1.5
            is_sangria = margem_atual < 0.10 and p['curva_abc'] == 'C'

            if is_elastico:
                continue

            if is_sangria:
                limite_aumento = 1.15
                motivo = 'Estancar Sangria'
            elif is_inelastico:
                limite_aumento = 1.0 + limite_teto_input
                motivo = 'Ouro Oculto'
            else:
                continue

            skus_para_otimizar.append({
                'sku': p['codigo_produto'],
                'nome': p['nome_produto'],
                'preco_orig': p['preco'],
                'vol_orig': p['volume'],
                'custo_unit': p['custo_unit'],
                'elasticidade': elasticidade,
                'limite_preco': p['preco'] * limite_aumento,
                'motivo': motivo,
                'preco_sim': p['preco'],
                'vol_sim': p['volume'],
            })

        if receita_atual_global == 0:
            return JsonResponse({'status': 'erro', 'mensagem': 'Receita global zerada. Verifique os dados do projeto.', 'kpis': {}, 'data': []}, status=400)

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
                'data': []
            })

        receita_sim = receita_atual_global
        custo_sim = custo_variavel_global
        atingiu_meta = False
        MAX_RODADAS = 20
        PASSO_MAX = 0.02

        for _ in range(MAX_RODADAS):
            margem_sim = (receita_sim - custo_sim) / receita_sim
            if margem_sim >= meta_margem_alvo:
                atingiu_meta = True
                break

            skus_ativos = [s for s in skus_para_otimizar if s['preco_sim'] < s['limite_preco']]
            if not skus_ativos:
                break

            skus_ativos.sort(
                key=lambda s: s['vol_sim'] * (s['preco_sim'] - s['custo_unit']),
                reverse=True
            )

            receita_alvo = custo_sim / (1.0 - meta_margem_alvo)
            delta_necessario = receita_alvo - receita_sim
            impacto_total = sum(s['vol_sim'] * (s['preco_sim'] - s['custo_unit']) for s in skus_ativos)

            for item in skus_ativos:
                if item['preco_sim'] >= item['limite_preco']:
                    continue

                peso = (item['vol_sim'] * (item['preco_sim'] - item['custo_unit'])) / impacto_total if impacto_total > 0 else 1.0 / len(skus_ativos)

                receita_sku_atual = item['preco_sim'] * item['vol_sim']
                receita_sku_alvo = receita_sku_atual + (delta_necessario * peso)
                passo_necessario = (receita_sku_alvo / receita_sku_atual) - 1.0 if receita_sku_atual > 0 else 0.0
                passo_aplicado = max(min(passo_necessario, PASSO_MAX), 0.0)
                novo_preco = min(item['preco_sim'] * (1.0 + passo_aplicado), item['limite_preco'])

                if novo_preco <= item['preco_sim']:
                    continue

                receita_sim -= item['preco_sim'] * item['vol_sim']
                custo_sim -= item['custo_unit'] * item['vol_sim']

                razao = novo_preco / item['preco_orig']
                novo_vol = max(item['vol_orig'] * math.pow(razao, item['elasticidade']), 0.0)

                item['preco_sim'] = novo_preco
                item['vol_sim'] = novo_vol

                receita_sim += novo_preco * novo_vol
                custo_sim += item['custo_unit'] * novo_vol

                if receita_sim > 0 and (receita_sim - custo_sim) / receita_sim >= meta_margem_alvo:
                    atingiu_meta = True
                    break

            if atingiu_meta:
                break

        margem_simulada = (receita_sim - custo_sim) / receita_sim

        plano_de_acao = []
        for item in skus_para_otimizar:
            delta_pct = (item['preco_sim'] / item['preco_orig']) - 1.0
            if delta_pct > 0.0001:
                plano_de_acao.append({
                    'sku': item['sku'],
                    'produto': item['nome'],
                    'preco_atual': round(item['preco_orig'], 2),
                    'preco_novo': round(item['preco_sim'], 2),
                    'aumento_pct': round(delta_pct * 100, 1),
                    'vol_projetado': round(item['vol_sim'], 0),
                    'estrategia': item['motivo'],
                })

        plano_de_acao.sort(key=lambda x: x['aumento_pct'], reverse=True)

        mensagem = (
            f'Meta de {meta_margem_alvo * 100:.1f}% atingida. {len(plano_de_acao)} SKU(s) ajustados.'
            if atingiu_meta
            else f'Meta parcialmente atingida: {margem_simulada * 100:.1f}% (alvo: {meta_margem_alvo * 100:.1f}%). '
                 f'Espaço de manobra esgotado sem violar regras de negócio.'
        )

        return JsonResponse({
            'status': 'sucesso',
            'atingiu_meta': atingiu_meta,
            'mensagem': mensagem,
            'kpis': _montar_kpis(
                margem_atual_global, margem_simulada,
                receita_atual_global, custo_variavel_global,
                receita_sim, custo_sim,
                skus_alterados=len(plano_de_acao)
            ),
            'data': plano_de_acao,
        })

    except ProjetoPrecificacao.DoesNotExist:
        return JsonResponse({'status': 'erro', 'mensagem': 'Projeto não encontrado.', 'kpis': {}, 'data': []}, status=404)
    except (ValueError, KeyError) as e:
        return JsonResponse({'status': 'erro', 'mensagem': f'Parâmetro inválido: {e}', 'kpis': {}, 'data': []}, status=400)
    except Exception as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=500)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO DE INGESTÃO — PUSH API v1
# ══════════════════════════════════════════════════════════════════════════════
#
# Todos os endpoints abaixo são PUSH (o ERP envia dados para o Axiom).
# Autenticação idêntica: header X-Axiom-API-Key
# Regra de ouro: NUNCA processa dados sem validar empresa e loja no mesmo tenant.
# ══════════════════════════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────────────────────────
# HELPER: resolve Loja por id ou nome (com segurança tenant)
# ──────────────────────────────────────────────────────────────────────────────

def _resolver_loja(empresa, loja_raw):
    """
    Recebe empresa e loja_raw (int = loja_id | str = nome | None = sem filial).
    Retorna o objeto Loja ou None.
    Garante que a loja pertence à empresa (anti-IDOR).
    Raises ValueError se o identificador for fornecido mas não encontrado.
    """
    if loja_raw is None:
        return None
    try:
        loja_id = int(loja_raw)
        return Loja.objects.get(id=loja_id, empresa=empresa)
    except (ValueError, TypeError):
        # loja_raw é string — busca por nome
        try:
            return Loja.objects.get(nome=str(loja_raw).strip(), empresa=empresa)
        except Loja.DoesNotExist:
            raise ValueError(f"Loja '{loja_raw}' não encontrada nesta empresa.")
    except Loja.DoesNotExist:
        raise ValueError(f"Loja id={loja_raw} não encontrada nesta empresa.")


def _validar_campos(dados, campos):
    """Retorna lista de campos obrigatórios ausentes ou None."""
    faltando = [c for c in campos if dados.get(c) is None]
    return faltando or None


# ──────────────────────────────────────────────────────────────────────────────
# [4] POST /api/v1/lojas/
#     Cria ou lista filiais da empresa autenticada.
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_lojas(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: GET|POST /api/v1/lojas/                                      │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  GET — Lista todas as filiais cadastradas da empresa.                   │
    │                                                                          │
    │  POST — Cria uma nova filial.                                            │
    │  BODY (JSON):                                                            │
    │  { "nome": "Filial SP-Centro" }                                          │
    │                                                                          │
    │  RESPOSTA POST — campo "data":                                           │
    │  { "id": 3, "nome": "Filial SP-Centro", "ativo": true }                 │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    empresa = request.empresa

    if request.method == 'GET':
        lojas = Loja.objects.filter(empresa=empresa, ativo=True).values('id', 'nome', 'ativo')
        return JsonResponse({'status': 'sucesso', 'mensagem': f'{len(list(lojas))} filial(ais).', 'kpis': {}, 'data': list(lojas)})

    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            nome = str(dados.get('nome', '')).strip()
            if not nome:
                return JsonResponse({'status': 'erro', 'mensagem': 'Campo "nome" é obrigatório.', 'kpis': {}, 'data': []}, status=400)

            loja, criada = Loja.objects.get_or_create(empresa=empresa, nome=nome, defaults={'ativo': True})
            return JsonResponse({
                'status': 'sucesso',
                'mensagem': 'Filial criada.' if criada else 'Filial já existia.',
                'kpis': {},
                'data': [{'id': loja.id, 'nome': loja.nome, 'ativo': loja.ativo}]
            }, status=201 if criada else 200)

        except json.JSONDecodeError:
            return JsonResponse({'status': 'erro', 'mensagem': 'Body inválido — envie JSON válido.', 'kpis': {}, 'data': []}, status=400)
        except Exception as e:
            logger.error(f"[API] api_v1_lojas POST: {e}")
            return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=500)

    return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)


# ──────────────────────────────────────────────────────────────────────────────
# [5] POST /api/v1/projetos/
#     Cria um projeto de precificação ou lista os existentes.
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_projetos(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: GET|POST /api/v1/projetos/                                   │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  GET — Lista projetos da empresa com id, nome, loja e data.             │
    │  Parâmetros de query: ?loja_id=3                                         │
    │                                                                          │
    │  POST — Cria um novo projeto.                                            │
    │  BODY (JSON) — campos OBRIGATÓRIOS:                                      │
    │  { "nome": "Pricing Q4 2025" }                                           │
    │                                                                          │
    │  BODY (JSON) — campos OPCIONAIS:                                         │
    │  {                                                                       │
    │    "loja_id":  3,          // int — ID da filial (ver GET /api/v1/lojas/)│
    │    "loja_nome": "Centro",  // str — alternativa ao loja_id              │
    │    "configuracao_variaveis": {}  // Mapeamento de colunas (avançado)    │
    │  }                                                                       │
    │                                                                          │
    │  RESPOSTA POST — campo "data":                                           │
    │  { "projeto_id": 12, "nome": "Pricing Q4 2025", "loja": "Centro" }      │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    empresa = request.empresa

    if request.method == 'GET':
        qs = ProjetoPrecificacao.objects.filter(empresa=empresa).select_related('loja')
        loja_id = request.GET.get('loja_id')
        if loja_id:
            qs = qs.filter(loja_id=loja_id)
        data = [
            {'projeto_id': p.id, 'nome': p.nome, 'loja': p.loja.nome if p.loja else None, 'criado_em': p.criado_em.strftime('%Y-%m-%d')}
            for p in qs.order_by('-criado_em')
        ]
        return JsonResponse({'status': 'sucesso', 'mensagem': f'{len(data)} projeto(s).', 'kpis': {}, 'data': data})

    if request.method == 'POST':
        try:
            dados = json.loads(request.body)
            nome = str(dados.get('nome', '')).strip()
            if not nome:
                return JsonResponse({'status': 'erro', 'mensagem': 'Campo "nome" é obrigatório.', 'kpis': {}, 'data': []}, status=400)

            loja_raw = dados.get('loja_id') or dados.get('loja_nome')
            try:
                loja = _resolver_loja(empresa, loja_raw)
            except ValueError as e:
                return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=400)

            config = dados.get('configuracao_variaveis', {})
            projeto = ProjetoPrecificacao.objects.create(
                empresa=empresa,
                loja=loja,
                nome=nome,
                configuracao_variaveis=config if isinstance(config, dict) else {}
            )

            return JsonResponse({
                'status': 'sucesso',
                'mensagem': f'Projeto "{nome}" criado com sucesso.',
                'kpis': {},
                'data': [{'projeto_id': projeto.id, 'nome': projeto.nome, 'loja': projeto.loja.nome if projeto.loja else None}]
            }, status=201)

        except json.JSONDecodeError:
            return JsonResponse({'status': 'erro', 'mensagem': 'Body inválido — envie JSON válido.', 'kpis': {}, 'data': []}, status=400)
        except Exception as e:
            logger.error(f"[API] api_v1_projetos POST: {e}")
            return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=500)

    return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)


# ──────────────────────────────────────────────────────────────────────────────
# [6] POST /api/v1/ingestao/vendas/
#     Batch de vendas históricas → DW (elasticidade + micro-forecast)
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_ingestao_vendas(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: POST /api/v1/ingestao/vendas/                                │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  DESCRIÇÃO                                                               │
    │  Recebe um batch de registros de vendas históricas do ERP e os armazena │
    │  no Data Warehouse do Axiom. Esses dados alimentam:                      │
    │    • Motor de Elasticidade-Preço (regressão automática)                  │
    │    • Axiom Forecast Micro (previsão de demanda por SKU)                  │
    │                                                                          │
    │  ⚠ REGRA DE VOLUME: Envie no máximo 5.000 registros por requisição.      │
    │    Para volumes maiores, pagine em múltiplos POSTs.                      │
    │                                                                          │
    │  BODY (JSON):                                                            │
    │  {                                                                       │
    │    "projeto_id":  12,        // int, OBRIGATÓRIO                         │
    │    "loja_id":     3,         // int ou null (filial; null = global)      │
    │    "loja_nome": "Centro",    // str alternativo ao loja_id               │
    │    "registros": [            // array, OBRIGATÓRIO, max 5000 itens       │
    │      {                                                                   │
    │        "codigo_produto":  "SKU001",   // str, OBRIGATÓRIO               │
    │        "nome_produto":    "Arroz 5kg",// str, opcional                  │
    │        "data_venda":      "2025-01-15",// str YYYY-MM-DD, OBRIGATÓRIO   │
    │        "quantidade":      120,         // number > 0, OBRIGATÓRIO        │
    │        "preco_praticado": 12.90,       // number > 0, OBRIGATÓRIO        │
    │        "custo_unitario":  8.20,        // number >= 0, opcional          │
    │        "variaveis_extras": {}          // objeto JSON livre, opcional    │
    │      }                                                                   │
    │    ]                                                                     │
    │  }                                                                       │
    │                                                                          │
    │  COMPORTAMENTO DE DUPLICATAS                                              │
    │  Registros duplicados (mesma empresa + loja + SKU + data) são           │
    │  atualizados (upsert). Nunca gera erro por duplicata.                    │
    │                                                                          │
    │  RESPOSTA — campo "kpis":                                                │
    │  {                                                                       │
    │    "recebidos":   150,   // Total de registros no batch                  │
    │    "inseridos":   140,   // Novos registros criados                      │
    │    "atualizados": 10,    // Registros duplicados atualizados             │
    │    "rejeitados":  0      // Registros com erro de validação              │
    │  }                                                                       │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)

    empresa = request.empresa

    try:
        dados = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'status': 'erro', 'mensagem': 'Body inválido — envie JSON válido.', 'kpis': {}, 'data': []}, status=400)

    # ── Validação dos campos raiz ────────────────────────────────────────────
    projeto_id = dados.get('projeto_id')
    registros = dados.get('registros')

    if not projeto_id:
        return JsonResponse({'status': 'erro', 'mensagem': 'Campo "projeto_id" é obrigatório.', 'kpis': {}, 'data': []}, status=400)
    if not isinstance(registros, list) or len(registros) == 0:
        return JsonResponse({'status': 'erro', 'mensagem': 'Campo "registros" deve ser uma lista não-vazia.', 'kpis': {}, 'data': []}, status=400)
    if len(registros) > 5000:
        return JsonResponse({'status': 'erro', 'mensagem': 'Limite de 5.000 registros por requisição excedido.', 'kpis': {}, 'data': []}, status=400)

    # ── Valida projeto (tenant-safe) ─────────────────────────────────────────
    try:
        projeto = ProjetoPrecificacao.objects.get(id=projeto_id, empresa=empresa)
    except ProjetoPrecificacao.DoesNotExist:
        return JsonResponse({'status': 'erro', 'mensagem': 'projeto_id não encontrado nesta empresa.', 'kpis': {}, 'data': []}, status=404)

    # ── Resolve loja (tenant-safe) ───────────────────────────────────────────
    loja_raw = dados.get('loja_id') or dados.get('loja_nome')
    try:
        loja = _resolver_loja(empresa, loja_raw)
    except ValueError as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=400)

    # ── Processa registros ───────────────────────────────────────────────────
    inseridos = atualizados = rejeitados = 0
    erros = []

    CAMPOS_OBR = ['codigo_produto', 'data_venda', 'quantidade', 'preco_praticado']

    for i, reg in enumerate(registros):
        # Valida campos obrigatórios
        faltando = [c for c in CAMPOS_OBR if reg.get(c) is None]
        if faltando:
            rejeitados += 1
            erros.append({'linha': i, 'erro': f'Campos ausentes: {faltando}'})
            continue

        # Valida e converte tipos
        try:
            data_venda = date.fromisoformat(str(reg['data_venda']))
            quantidade = float(reg['quantidade'])
            preco = float(reg['preco_praticado'])
            custo = float(reg['custo_unitario']) if reg.get('custo_unitario') is not None else None
            if quantidade <= 0 or preco <= 0:
                raise ValueError('quantidade e preco_praticado devem ser > 0')
        except (ValueError, TypeError) as e:
            rejeitados += 1
            erros.append({'linha': i, 'erro': str(e)})
            continue

        extras = reg.get('variaveis_extras', {})
        if not isinstance(extras, dict):
            extras = {}

        try:
            obj, criado = VendaHistoricaDW.objects.update_or_create(
                empresa=empresa,
                loja=loja,
                codigo_produto=str(reg['codigo_produto']).strip(),
                data_venda=data_venda,
                defaults={
                    'projeto': projeto,
                    'nome_produto': str(reg.get('nome_produto', '') or '').strip() or None,
                    'quantidade': quantidade,
                    'preco_praticado': preco,
                    'custo_unitario': custo,
                    'variaveis_extras': extras,
                }
            )
            if criado:
                inseridos += 1
            else:
                atualizados += 1
        except Exception as e:
            rejeitados += 1
            erros.append({'linha': i, 'erro': str(e)})
            logger.error(f"[API] ingestao_vendas linha {i}: {e}")

    status_str = 'sucesso' if rejeitados == 0 else ('aviso' if inseridos + atualizados > 0 else 'erro')
    http_status = 200 if rejeitados == 0 else (207 if inseridos + atualizados > 0 else 400)

    return JsonResponse({
        'status': status_str,
        'mensagem': (
            f'Batch processado: {inseridos} inserido(s), {atualizados} atualizado(s), {rejeitados} rejeitado(s).'
        ),
        'kpis': {
            'recebidos': len(registros),
            'inseridos': inseridos,
            'atualizados': atualizados,
            'rejeitados': rejeitados,
        },
        'data': erros[:50],  # Retorna até 50 erros detalhados
    }, status=http_status)


# ──────────────────────────────────────────────────────────────────────────────
# [7] POST /api/v1/ingestao/faturamento/
#     Batch de faturamento macro → DW (Axiom Macro Forecast / Prophet)
# ──────────────────────────────────────────────────────────────────────────────

@csrf_exempt
@require_api_key
def api_v1_ingestao_faturamento(request):
    """
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ENDPOINT: POST /api/v1/ingestao/faturamento/                           │
    ├─────────────────────────────────────────────────────────────────────────┤
    │  DESCRIÇÃO                                                               │
    │  Recebe um batch de faturamento diário total da empresa/filial e        │
    │  armazena no DW Macro. Alimenta o motor Prophet (Axiom Macro Forecast). │
    │                                                                          │
    │  Use este endpoint para enviar o fechamento do caixa do dia.             │
    │  Diferente de /vendas/ (que registra por SKU), este endpoint recebe      │
    │  apenas a receita total consolidada por dia/loja.                        │
    │                                                                          │
    │  BODY (JSON):                                                            │
    │  {                                                                       │
    │    "loja_id":   3,          // int ou null (null = consolidado empresa)  │
    │    "loja_nome": "Centro",   // str alternativo ao loja_id                │
    │    "registros": [           // array, OBRIGATÓRIO, max 5000 itens        │
    │      {                                                                   │
    │        "data":       "2025-01-15",  // str YYYY-MM-DD, OBRIGATÓRIO      │
    │        "faturamento": 58420.50     // number >= 0, OBRIGATÓRIO          │
    │      }                                                                   │
    │    ]                                                                     │
    │  }                                                                       │
    │                                                                          │
    │  COMPORTAMENTO DE DUPLICATAS                                              │
    │  Registros duplicados (empresa + loja + data) são atualizados (upsert). │
    │                                                                          │
    │  RESPOSTA — campo "kpis":                                                │
    │  {                                                                       │
    │    "recebidos":   30,                                                    │
    │    "inseridos":   25,                                                    │
    │    "atualizados":  5,                                                    │
    │    "rejeitados":   0                                                     │
    │  }                                                                       │
    └─────────────────────────────────────────────────────────────────────────┘
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'erro', 'mensagem': 'Método não permitido.', 'kpis': {}, 'data': []}, status=405)

    empresa = request.empresa

    try:
        dados = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'status': 'erro', 'mensagem': 'Body inválido — envie JSON válido.', 'kpis': {}, 'data': []}, status=400)

    registros = dados.get('registros')
    if not isinstance(registros, list) or len(registros) == 0:
        return JsonResponse({'status': 'erro', 'mensagem': 'Campo "registros" deve ser uma lista não-vazia.', 'kpis': {}, 'data': []}, status=400)
    if len(registros) > 5000:
        return JsonResponse({'status': 'erro', 'mensagem': 'Limite de 5.000 registros por requisição excedido.', 'kpis': {}, 'data': []}, status=400)

    # ── Resolve loja (tenant-safe) ───────────────────────────────────────────
    loja_raw = dados.get('loja_id') or dados.get('loja_nome')
    try:
        loja = _resolver_loja(empresa, loja_raw)
    except ValueError as e:
        return JsonResponse({'status': 'erro', 'mensagem': str(e), 'kpis': {}, 'data': []}, status=400)

    inseridos = atualizados = rejeitados = 0
    erros = []

    for i, reg in enumerate(registros):
        if reg.get('data') is None or reg.get('faturamento') is None:
            rejeitados += 1
            erros.append({'linha': i, 'erro': 'Campos "data" e "faturamento" são obrigatórios.'})
            continue

        try:
            data_fat = date.fromisoformat(str(reg['data']))
            valor = float(reg['faturamento'])
            if valor < 0:
                raise ValueError('faturamento não pode ser negativo.')
        except (ValueError, TypeError) as e:
            rejeitados += 1
            erros.append({'linha': i, 'erro': str(e)})
            continue

        try:
            obj, criado = FaturamentoEmpresaDW.objects.update_or_create(
                empresa=empresa,
                loja=loja,
                data_faturamento=data_fat,
                defaults={'faturamento_total': valor}
            )
            if criado:
                inseridos += 1
            else:
                atualizados += 1
        except Exception as e:
            rejeitados += 1
            erros.append({'linha': i, 'erro': str(e)})
            logger.error(f"[API] ingestao_faturamento linha {i}: {e}")

    status_str = 'sucesso' if rejeitados == 0 else ('aviso' if inseridos + atualizados > 0 else 'erro')
    http_status = 200 if rejeitados == 0 else (207 if inseridos + atualizados > 0 else 400)

    return JsonResponse({
        'status': status_str,
        'mensagem': f'Batch processado: {inseridos} inserido(s), {atualizados} atualizado(s), {rejeitados} rejeitado(s).',
        'kpis': {
            'recebidos': len(registros),
            'inseridos': inseridos,
            'atualizados': atualizados,
            'rejeitados': rejeitados,
        },
        'data': erros[:50],
    }, status=http_status)


# ──────────────────────────────────────────────────────────────────────────────
# DOCUMENTAÇÃO INTERATIVA (Swagger UI + OpenAPI Schema)
# ──────────────────────────────────────────────────────────────────────────────

def api_v1_docs(request):
    """Renderiza a página de documentação interativa (Swagger UI)."""
    return render(request, 'projects/api_docs.html')


def api_v1_schema(request):
    """
    Retorna o schema OpenAPI 3.0 em JSON.
    Usado pelo Swagger UI para renderizar a documentação interativa.
    """
    schema = {
        "openapi": "3.0.3",
        "info": {
            "title": "Axiom Pricing Engine API",
            "version": "1.0.0",
            "description": (
                "Motor de **Explainable AI** para Inteligência de Varejo e Pricing.\n\n"
                "Todos os endpoints exigem o header de autenticação:\n"
                "```\nX-Axiom-API-Key: sua-chave-uuid\n```\n"
                "Obtenha sua chave em **Painel → Configurações → Integração API**.\n\n"
                "**Formato de resposta padrão:**\n"
                "```json\n"
                "{\n"
                '  "status": "sucesso" | "erro",\n'
                '  "mensagem": "Descrição do resultado",\n'
                '  "kpis": {},\n'
                '  "data": []\n'
                "}\n```"
            ),
            "contact": {
                "name": "Suporte Axiom",
                "email": "suporte@axiomlab.com.br"
            }
        },
        "servers": [
            {"url": "/", "description": "Servidor atual"}
        ],
        "security": [{"ApiKeyAuth": []}],
        "components": {
            "securitySchemes": {
                "ApiKeyAuth": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-Axiom-API-Key",
                    "description": "UUID da empresa. Encontre em Painel → Configurações → Integração API."
                }
            },
            "schemas": {
                "RespostaErro": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "example": "erro"},
                        "mensagem": {"type": "string", "example": "Descrição do erro"},
                        "kpis": {"type": "object"},
                        "data": {"type": "array", "items": {}}
                    }
                },
                "SKUElasticidade": {
                    "type": "object",
                    "properties": {
                        "codigo_produto": {"type": "string", "example": "ABC123"},
                        "nome_produto": {"type": "string", "example": "Arroz Tipo 1 5kg"},
                        "loja": {"type": "string", "example": "Filial SP-Centro"},
                        "elasticidade": {"type": "number", "example": -1.42, "description": "Coeficiente de elasticidade-preço"},
                        "ic_lower": {"type": "number", "example": -1.80, "description": "Intervalo de confiança inferior (95%)"},
                        "ic_upper": {"type": "number", "example": -1.05, "description": "Intervalo de confiança superior (95%)"},
                        "p_value": {"type": "number", "example": 0.002, "description": "P-valor do coeficiente (< 0.05 = significativo)"},
                        "r_squared": {"type": "number", "example": 0.67, "description": "Poder explicativo do modelo (0–1)"},
                        "confianca": {"type": "string", "enum": ["Alta", "Média", "Baixa"], "example": "Alta"},
                        "comportamento": {"type": "string", "enum": ["Sensível", "Fiel"], "example": "Sensível", "description": "Sensível = elástico (e < -1). Fiel = inelástico (e >= -1)."},
                        "preco_atual": {"type": "number", "example": 12.90},
                        "preco_sugerido": {"type": "number", "example": 13.50},
                        "custo_unitario": {"type": "number", "example": 8.20},
                        "margem_projetada": {"type": "number", "example": 39.3, "description": "Margem de contribuição projetada (%)"}
                    }
                },
                "SKUPlanoAcao": {
                    "type": "object",
                    "properties": {
                        "sku": {"type": "string", "example": "XYZ789"},
                        "produto": {"type": "string", "example": "Café Torrado 500g"},
                        "preco_atual": {"type": "number", "example": 8.90},
                        "preco_novo": {"type": "number", "example": 9.30},
                        "aumento_pct": {"type": "number", "example": 4.5, "description": "Variação percentual do preço"},
                        "vol_projetado": {"type": "number", "example": 120, "description": "Volume diário projetado após ajuste"},
                        "estrategia": {"type": "string", "enum": ["Ouro Oculto", "Estancar Sangria"], "example": "Ouro Oculto"}
                    }
                }
            }
        },
        "paths": {
            "/api/v1/elasticidade/{projeto_id}/": {
                "get": {
                    "tags": ["Elasticidade"],
                    "summary": "Resultados de Elasticidade por Projeto",
                    "description": (
                        "Retorna os parâmetros de elasticidade-preço calculados pelo AutoML "
                        "para todos os SKUs de um projeto. Use para integrar com ERP, BI ou planilhas de pricing."
                    ),
                    "parameters": [
                        {
                            "name": "projeto_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "integer"},
                            "description": "ID do projeto Axiom",
                            "example": 7
                        },
                        {
                            "name": "loja_id",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "integer"},
                            "description": "Filtra resultados por loja específica"
                        },
                        {
                            "name": "sku",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string"},
                            "description": "Filtra por código de produto exato",
                            "example": "ABC123"
                        },
                        {
                            "name": "confianca",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string", "enum": ["alta", "media", "baixa"]},
                            "description": "Filtra pelo nível de confiança estatística do modelo"
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Lista de SKUs com parâmetros de elasticidade",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string", "example": "sucesso"},
                                            "mensagem": {"type": "string", "example": "120 SKU(s) retornados para o projeto \"Q1 2025\"."},
                                            "kpis": {
                                                "type": "object",
                                                "properties": {
                                                    "total_skus": {"type": "integer", "example": 120},
                                                    "skus_sensiveis": {"type": "integer", "example": 45},
                                                    "skus_fieis": {"type": "integer", "example": 75},
                                                    "pct_confianca_alta": {"type": "number", "example": 62.5}
                                                }
                                            },
                                            "data": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/SKUElasticidade"}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "401": {"description": "API Key inválida ou ausente", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}},
                        "404": {"description": "Projeto não encontrado", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}}
                    }
                }
            },
            "/api/v1/simular-preco/": {
                "post": {
                    "tags": ["Simulação de Preço"],
                    "summary": "Simular Impacto de Mudança de Preço",
                    "description": (
                        "Simula o impacto financeiro de um novo preço usando o modelo de elasticidade "
                        "treinado pelo AutoML. Retorna volume, faturamento, lucro e margem em **escala diária**.\n\n"
                        "A elasticidade é limitada a ≤ 0 internamente: aumentar preço nunca aumenta volume."
                    ),
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["resultado_id", "novo_preco", "custo"],
                                    "properties": {
                                        "resultado_id": {
                                            "type": "integer",
                                            "description": "**Obrigatório.** ID do ResultadoPrecificacao (obtido via /api/v1/elasticidade/)",
                                            "example": 42
                                        },
                                        "novo_preco": {
                                            "type": "number",
                                            "description": "**Obrigatório.** Novo preço a simular (R$). Deve ser > 0.",
                                            "example": 15.90
                                        },
                                        "custo": {
                                            "type": "number",
                                            "description": "**Obrigatório.** Custo unitário vigente (R$). Deve ser >= 0.",
                                            "example": 8.20
                                        },
                                        "elasticidade_customizada": {
                                            "type": "number",
                                            "description": "**Opcional.** Sobrescreve a elasticidade calculada pelo modelo. Útil para análises de sensibilidade. Deve ser <= 0.",
                                            "example": -1.8
                                        }
                                    }
                                },
                                "example": {
                                    "resultado_id": 42,
                                    "novo_preco": 15.90,
                                    "custo": 8.20
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Simulação calculada com sucesso",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string", "example": "sucesso"},
                                            "mensagem": {"type": "string", "example": "Simulação calculada com sucesso."},
                                            "kpis": {
                                                "type": "object",
                                                "properties": {
                                                    "quantidade_prevista": {"type": "number", "example": 8.5, "description": "Volume diário projetado (unidades)"},
                                                    "faturamento_projetado": {"type": "number", "example": 134.55, "description": "Faturamento diário (R$)"},
                                                    "lucro_projetado": {"type": "number", "example": 62.9, "description": "Lucro diário (R$)"},
                                                    "margem_projetada": {"type": "number", "example": 46.8, "description": "Margem de contribuição (%)"},
                                                    "preco_atual_base": {"type": "number", "example": 12.90, "description": "Preço base usado como âncora da simulação (R$)"},
                                                    "elasticidade_usada": {"type": "number", "example": -1.42, "description": "Elasticidade efetivamente aplicada (após teto de 0.0)"}
                                                }
                                            },
                                            "data": {"type": "array", "items": {}}
                                        }
                                    }
                                }
                            }
                        },
                        "400": {"description": "Parâmetro inválido ou ausente", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}},
                        "401": {"description": "API Key inválida ou ausente", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}},
                        "404": {"description": "resultado_id não encontrado", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}}
                    }
                }
            },
            "/api/v1/otimizar-margem/": {
                "post": {
                    "tags": ["Margin Command"],
                    "summary": "Axiom Margin Command — Otimização Global de Margem",
                    "description": (
                        "Dado um projeto e uma **meta de margem global**, o motor heurístico identifica "
                        "quais SKUs devem ter o preço ajustado e em quanto, para atingir a meta com o "
                        "menor impacto possível no volume.\n\n"
                        "**Estratégias aplicadas:**\n"
                        "- **Ouro Oculto:** SKUs inelásticos (demanda fiel) — candidatos naturais a aumento.\n"
                        "- **Estancar Sangria:** SKUs com margem < 10% na curva C — recebem ajuste mais agressivo.\n"
                        "- **KVIs protegidos:** SKUs muito elásticos (e ≤ -1.5) são ignorados para proteger market share.\n\n"
                        "Se a meta não puder ser atingida sem violar regras de negócio, o motor aplica "
                        "**Graceful Degradation**: entrega o melhor resultado possível e sinaliza via `atingiu_meta: false`."
                    ),
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["projeto_id", "meta_margem"],
                                    "properties": {
                                        "projeto_id": {
                                            "type": "integer",
                                            "description": "**Obrigatório.** ID do ProjetoPrecificacao.",
                                            "example": 7
                                        },
                                        "meta_margem": {
                                            "type": "number",
                                            "description": "**Obrigatório.** Meta de margem de contribuição global (%). Range: 1–99.",
                                            "example": 30.0
                                        },
                                        "limite_teto": {
                                            "type": "number",
                                            "description": "**Opcional.** Teto máximo de aumento por SKU (%). Padrão: 5.0. Range recomendado: 2–15.",
                                            "example": 5.0
                                        }
                                    }
                                },
                                "example": {
                                    "projeto_id": 7,
                                    "meta_margem": 30.0,
                                    "limite_teto": 5.0
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Plano de execução gerado",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {"type": "string", "example": "sucesso"},
                                            "atingiu_meta": {"type": "boolean", "example": True, "description": "false = Graceful Degradation (melhor resultado possível sem violar regras)"},
                                            "mensagem": {"type": "string", "example": "Meta de 30.0% atingida. 18 SKU(s) ajustados."},
                                            "kpis": {
                                                "type": "object",
                                                "properties": {
                                                    "margem_atual_pct": {"type": "number", "example": 22.4},
                                                    "margem_projetada_pct": {"type": "number", "example": 30.1},
                                                    "ganho_margem_pp": {"type": "number", "example": 7.7, "description": "Ganho em pontos percentuais"},
                                                    "lucro_atual_reais": {"type": "number", "example": 84200.0},
                                                    "lucro_projetado_reais": {"type": "number", "example": 95100.0},
                                                    "ganho_lucro_reais": {"type": "number", "example": 10900.0},
                                                    "receita_atual_reais": {"type": "number", "example": 380000.0},
                                                    "receita_projetada_reais": {"type": "number", "example": 385000.0},
                                                    "skus_alterados": {"type": "integer", "example": 18}
                                                }
                                            },
                                            "data": {
                                                "type": "array",
                                                "description": "Plano de execução ordenado por maior aumento percentual",
                                                "items": {"$ref": "#/components/schemas/SKUPlanoAcao"}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "400": {"description": "Parâmetro inválido ou ausente", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}},
                        "401": {"description": "API Key inválida ou ausente", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}},
                        "404": {"description": "Projeto não encontrado", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RespostaErro"}}}}
                    }
                }
            }
        }
    }
    return JsonResponse(schema)
