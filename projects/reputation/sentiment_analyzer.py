"""
reputation/sentiment_analyzer.py
-----------------------------------
Análise de sentimento de reviews via Claude Haiku.

Uma única chamada para todos os reviews (eficiente em custo):
  ~1.500 tokens input + ~500 tokens output por análise
  → ~$0.003 por empresa por scan (menos de 1 centavo)

Retorna JSON estruturado com:
  - sentimento_geral: positivo / negativo / neutro
  - score: 0–100 (índice de satisfação)
  - temas_positivos: lista de temas elogiados
  - temas_negativos: lista de temas criticados
  - resumo_executivo: parágrafo para C-Level em PT-BR
  - por_review: sentimento individual de cada review
"""
from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)

# ── Analisador local (fallback sem API) ───────────────────────────────────────

_KW_POS: dict[str, float] = {
    "excelente": 2.0, "ótimo": 2.0, "otimo": 2.0, "maravilhoso": 2.0, "incrível": 2.0,
    "incrivel": 2.0, "perfeito": 1.8, "adorei": 1.8, "amei": 1.8, "fantástico": 1.8,
    "fantastico": 1.8, "top": 1.5, "boa": 1.0, "bom": 1.0, "gostei": 1.5,
    "recomendo": 2.0, "recomendado": 2.0, "satisfeito": 1.5, "satisfeita": 1.5,
    "rápido": 1.0, "rapido": 1.0, "eficiente": 1.2, "organizado": 1.0,
    "limpo": 1.0, "limpeza": 1.2, "atendimento": 1.0, "qualidade": 1.2,
    "preço bom": 1.5, "barato": 1.2, "variedade": 1.0, "completo": 1.0,
    "ótima": 2.0, "otima": 2.0, "parabéns": 1.8, "parabenns": 1.8,
}

_KW_NEG: dict[str, float] = {
    "péssimo": 2.0, "pessimo": 2.0, "horrível": 2.0, "horrivel": 2.0, "terrível": 2.0,
    "terrivel": 2.0, "ruim": 1.8, "decepcionante": 2.0, "decepção": 2.0, "decepcao": 2.0,
    "não recomendo": 2.5, "nao recomendo": 2.5, "nunca mais": 2.5, "problema": 1.2,
    "demorado": 1.5, "demora": 1.5, "fila": 1.2, "fila grande": 2.0,
    "sujo": 1.8, "sujeira": 1.8, "desorganizado": 1.5, "bagunça": 1.5,
    "caro": 1.2, "abusivo": 2.0, "absurdo": 1.8, "vergonha": 2.0,
    "mal atendido": 2.0, "mal atendida": 2.0, "grosseiro": 1.8, "grosso": 1.5,
    "vencido": 2.5, "estragado": 2.5, "podre": 2.5, "mofado": 2.5,
    "reclamação": 1.5, "reclamei": 1.5, "lamentável": 2.0,
    "falta de limpeza": 2.0, "falta de higiene": 2.0, "sem limpeza": 2.0,
    "falta de produtos": 2.0, "falta produto": 1.8, "sem produto": 1.8,
    "prateleira vazia": 2.0, "gondola vazia": 2.0, "gôndola vazia": 2.0,
    "produto em falta": 2.0, "produtos em falta": 2.0,
}


def _analisar_sentimento_local(
    reviews: list[dict],
    rating_geral: float | None,
    total_avaliacoes: int,
) -> dict:
    """Fallback sem API — usa keywords PT-BR + rating para análise."""
    reviews_com_texto = [r for r in reviews if r.get("texto", "").strip()]

    # Score base pelo rating do Google (peso 60%)
    score_rating = int((rating_geral or 3.0) / 5.0 * 100) if rating_geral else 50

    # Score pelas keywords das reviews (peso 40%)
    score_kw = 50
    temas_pos: list[str] = []
    temas_neg: list[str] = []
    por_review = []

    if reviews_com_texto:
        total_pos = 0.0
        total_neg = 0.0
        for i, rv in enumerate(reviews_com_texto):
            texto = rv.get("texto", "").lower()
            pos = sum(p for kw, p in _KW_POS.items() if kw in texto)
            neg = sum(p for kw, p in _KW_NEG.items() if kw in texto)
            total_pos += pos
            total_neg += neg
            for kw in _KW_POS:
                if kw in texto and kw not in temas_pos:
                    temas_pos.append(kw)
            for kw in _KW_NEG:
                if kw in texto and kw not in temas_neg:
                    temas_neg.append(kw)
            rv_rating = rv.get("rating", 3)
            if pos > neg * 1.2 or (pos == 0 and neg == 0 and rv_rating >= 4):
                rv_sent = "positivo"
            elif neg > pos * 1.2 or (pos == 0 and neg == 0 and rv_rating <= 2):
                rv_sent = "negativo"
            else:
                rv_sent = "neutro"
            por_review.append({"indice": i, "sentimento": rv_sent, "temas": []})

        soma = total_pos + total_neg
        if soma > 0:
            score_kw = int((total_pos / soma) * 100)

    score_final = max(0, min(100, int(score_rating * 0.6 + score_kw * 0.4)))

    if score_final >= 65:
        sentimento = "positivo"
    elif score_final < 45:
        sentimento = "negativo"
    else:
        sentimento = "neutro"

    rating_str = f"{rating_geral:.1f}" if rating_geral else "N/A"
    resumo = (
        f"Análise local baseada na nota média do Google ({rating_str}/5 com {total_avaliacoes} avaliações) "
        f"e palavras-chave das reviews. "
        f"Para análise detalhada com IA, adicione créditos à conta Anthropic."
    )

    return {
        "sentimento_geral": sentimento,
        "score": score_final,
        "temas_positivos": temas_pos[:5],
        "temas_negativos": temas_neg[:5],
        "resumo_executivo": resumo,
        "por_review": por_review,
        "tokens_input": 0,
        "tokens_output": 0,
        "fonte_analise": "local",
    }

_SYSTEM_PROMPT = """Você é um especialista em análise de reputação de empresas varejistas brasileiras.
Analise as avaliações do Google de clientes e retorne APENAS um JSON válido, sem texto extra."""

_USER_TEMPLATE = """Analise as seguintes avaliações de clientes de "{nome_empresa}" (nota média Google: {rating}/5 com {total} avaliações).

AVALIAÇÕES:
{reviews_formatadas}

Retorne EXATAMENTE este JSON (sem markdown, sem código block, apenas o JSON bruto):
{{
  "sentimento_geral": "positivo|negativo|neutro",
  "score": <número de 0 a 100 representando satisfação do cliente>,
  "temas_positivos": ["tema1", "tema2", "tema3"],
  "temas_negativos": ["tema1", "tema2", "tema3"],
  "resumo_executivo": "<2-3 frases em português de negócios, direto ao ponto para o CEO>",
  "por_review": [
    {{"indice": 0, "sentimento": "positivo|negativo|neutro", "temas": ["tema"]}}
  ]
}}

Regras para o score:
- 85–100: clientes muito satisfeitos, elogios consistentes
- 70–84: maioria satisfeita, alguns pontos de melhoria
- 50–69: opinião dividida, problemas recorrentes
- 30–49: predominantemente negativo, problemas sérios
- 0–29: crise de imagem, rejeição generalizada

Temas comuns no varejo: atendimento, preço, variedade, fila, limpeza, estacionamento,
prazo de entrega, qualidade dos produtos, localização, horário de funcionamento."""


def _formatar_reviews(reviews: list[dict]) -> str:
    if not reviews:
        return "(Sem avaliações textuais disponíveis)"
    linhas = []
    for i, rv in enumerate(reviews):
        estrelas = "⭐" * int(rv.get("rating", 0))
        data = rv.get("data_relativa") or rv.get("data_iso") or ""
        texto = rv.get("texto", "").strip()
        if not texto:
            continue
        linhas.append(f"[{i}] {estrelas} ({rv.get('rating', '?')}/5) — {data}\n{texto}")
    return "\n\n".join(linhas) if linhas else "(Sem texto nas avaliações disponíveis)"


def _extrair_json(texto: str) -> dict:
    """Extrai JSON do texto da resposta do Claude (remove possíveis delimitadores)."""
    # Remove blocos markdown se existirem
    texto = re.sub(r"```(?:json)?", "", texto).strip()
    # Tenta encontrar o primeiro objeto JSON
    match = re.search(r"\{.*\}", texto, re.DOTALL)
    if match:
        return json.loads(match.group())
    return json.loads(texto)


def analisar_sentimento_reviews(
    reviews: list[dict],
    nome_empresa: str,
    rating_geral: float | None,
    total_avaliacoes: int,
    api_key: str,
) -> dict:
    """
    Chama Claude Haiku para analisar o sentimento das reviews.

    Parâmetros
    ----------
    reviews         : lista de dicts com campos rating, texto, data_relativa
    nome_empresa    : nome oficial do estabelecimento
    rating_geral    : nota média do Google (ex: 4.2)
    total_avaliacoes: total de avaliações (ex: 1847)
    api_key         : Anthropic API key

    Retorna
    -------
    dict com sentimento_geral, score, temas, resumo_executivo, por_review,
    tokens_input, tokens_output
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("Biblioteca 'anthropic' não instalada. Execute: pip install anthropic")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY não configurada em settings.py")

    # Fallback para reviews sem texto
    reviews_com_texto = [r for r in reviews if r.get("texto", "").strip()]
    if not reviews_com_texto:
        logger.warning("[sentiment] Nenhuma review com texto para '%s'.", nome_empresa)
        # Retorna análise básica só pelo rating
        score = int((rating_geral or 3.0) / 5.0 * 100) if rating_geral else 50
        sent = "positivo" if score >= 65 else ("negativo" if score < 45 else "neutro")
        return {
            "sentimento_geral": sent,
            "score": score,
            "temas_positivos": [],
            "temas_negativos": [],
            "resumo_executivo": f"Análise baseada apenas na nota média ({rating_geral}/5) pois as avaliações não possuem texto.",
            "por_review": [],
            "tokens_input": 0,
            "tokens_output": 0,
        }

    rating_str = f"{rating_geral:.1f}" if rating_geral else "N/A"
    reviews_formatadas = _formatar_reviews(reviews_com_texto)

    prompt_usuario = _USER_TEMPLATE.format(
        nome_empresa=nome_empresa,
        rating=rating_str,
        total=total_avaliacoes,
        reviews_formatadas=reviews_formatadas,
    )

    client = anthropic.Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt_usuario}],
        )
    except Exception as exc:
        error_str = str(exc)
        # Fallback para analisador local se for erro de saldo ou autenticação
        if any(k in error_str for k in ("credit balance", "too low", "invalid_api_key", "authentication")):
            logger.warning("[sentiment] API indisponível (%s). Usando análise local.", error_str[:80])
            return _analisar_sentimento_local(reviews, rating_geral, total_avaliacoes)
        logger.error("[sentiment] Erro ao chamar Claude Haiku: %s", exc)
        raise

    texto_resposta = response.content[0].text if response.content else "{}"

    try:
        dados = _extrair_json(texto_resposta)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error("[sentiment] Claude retornou JSON inválido: %s\nResposta: %s", exc, texto_resposta[:500])
        raise ValueError(f"Resposta inválida do modelo: {exc}") from exc

    # Garante campos obrigatórios e tipos corretos
    dados.setdefault("sentimento_geral", "neutro")
    dados.setdefault("score", 50)
    dados.setdefault("temas_positivos", [])
    dados.setdefault("temas_negativos", [])
    dados.setdefault("resumo_executivo", "")
    dados.setdefault("por_review", [])
    dados["score"] = max(0, min(100, int(dados["score"])))

    # Adiciona contagem de tokens para auditoria de custo
    usage = getattr(response, "usage", None)
    dados["tokens_input"]  = getattr(usage, "input_tokens",  0) if usage else 0
    dados["tokens_output"] = getattr(usage, "output_tokens", 0) if usage else 0

    custo_usd = (dados["tokens_input"] * 0.80 + dados["tokens_output"] * 4.00) / 1_000_000
    logger.info(
        "[sentiment] Análise de '%s' concluída — score=%d | sentimento=%s | "
        "tokens=%d+%d | custo estimado=$%.5f",
        nome_empresa, dados["score"], dados["sentimento_geral"],
        dados["tokens_input"], dados["tokens_output"], custo_usd,
    )

    return dados
