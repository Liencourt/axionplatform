"""
Axiom Platform — Motor de Extração de Encarte via Vision LLM
Adaptado do script standalone docs/extract_encarte.py para rodar dentro do Django.

Dependências de sistema: poppler-utils (apt-get install poppler-utils no Cloud Run)
Dependências Python: anthropic, pdf2image, pillow
"""

import anthropic
import base64
import json
import logging
import re
import time
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path

from django.utils import timezone

logger = logging.getLogger('encartes')

# ─── Configurações ────────────────────────────────────────────────────────────

DPI              = 150              # Reduzido de 180 → menor tamanho base das imagens
MAX_WIDTH_PX     = 1200            # Reduzido de 1400 → segunda barreira de tamanho
MAX_IMAGE_BYTES  = 4 * 1024 * 1024 # 4 MB — margem segura abaixo do limite de 5 MB da API
MODEL            = 'claude-opus-4-5'  # Opus: melhor precisão em layouts complexos com preços
MAX_TOKENS       = 8192
RETRY_ATTEMPTS   = 3
RETRY_DELAY      = 5


# ─── Prompt de sistema ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Você é um especialista em extração de dados de encartes de supermercados brasileiros
para um sistema de inteligência de preços (Retail Intelligence).

Sua tarefa é identificar e estruturar TODOS os produtos e preços visíveis na imagem.
Retorne APENAS JSON válido, sem markdown, sem explicação, sem comentários."""


def build_prompt(page_num: int, total_pages: int, concorrente_nome: str, programa_fidelidade: str | None) -> str:
    clube_instrucao = ""
    if programa_fidelidade:
        clube_instrucao = f"""
① PROGRAMA DE FIDELIDADE: "{programa_fidelidade}"
   Quando aparecer o selo/logo do programa ao lado de um preço menor:
   → Cria DOIS objetos em "precos": um tipo "normal" e um tipo "clube" com condicao "{programa_fidelidade}"
"""
    else:
        clube_instrucao = """
① PROGRAMA DE FIDELIDADE: este concorrente não possui programa identificado.
   → Registre apenas o preço visível com o tipo adequado.
"""

    return f"""Página {page_num} de {total_pages} — Encarte {concorrente_nome}.

Extraia TODOS os produtos visíveis nesta página.

═══ PADRÕES DE PREÇO ═══
{clube_instrucao}
② "Por:" antes do preço
   → tipo: "promocional", canal: "loja"

③ "Nesta embalagem Xml saem por: R$X"
   → Registre o volume real em "quantidade" e descreva em "condicao"
   → tipo: "promocional"

④ "Leve X Pague Y"
   → tipo: "condicional", valor = preço POR UNIDADE calculado
   → condicao: "leve X pague Y, preço por unidade"

⑤ "Oferta Especial" / "Preço Especial"
   → tipo: "promocional"

⑥ "50% de desconto na compra da 2ª unidade"
   → tipo: "condicional", condicao: "50% desconto na 2ª unidade"

⑦ Produtos do açougue/peixaria vendidos POR KG
   → quantidade: "kg", valor = preço/kg

⑧ Pack/caixa com múltiplas unidades (ex: cervejas 12 latas)
   → quantidade: "pack c/12 473ml"

⑨ Centavos em fonte menor sobrescrita (ex: visual "9⁹⁸" = R$9,98)
   → converta sempre para float: 9.98

═══ FORMATO DE SAÍDA ═══

Retorne EXATAMENTE este JSON (sem nada antes ou depois):

{{
  "pagina": {page_num},
  "produtos": [
    {{
      "nome": "nome completo exatamente como no encarte",
      "marca": "marca identificada ou null",
      "categoria": "categoria inferida (ex: bebidas, carnes, limpeza, higiene, laticínios, mercearia, hortifruti, vinhos, chocolates, frios)",
      "quantidade": "ex: 5kg / 500ml / 1L / kg / 473ml / pack c/12 473ml",
      "ean": null,
      "validade_oferta": "dd/mm/aaaa a dd/mm/aaaa",
      "condicao_especial": "ex: leve 3 pague 2 | ou null",
      "precos": [
        {{
          "valor": 0.00,
          "tipo": "normal | promocional | clube | condicional",
          "canal": "loja | null",
          "condicao": "nome_do_clube | embalagem 400ml | null"
        }}
      ]
    }}
  ],
  "avisos": []
}}

Regras absolutas:
- "valor" é sempre float com ponto (ex: 26.95, nunca "26,95")
- Todo produto deve ter ao menos um objeto em "precos"
- "avisos" recebe strings descrevendo qualquer ambiguidade encontrada
- Não omita nenhum produto visível, mesmo que incompleto
"""


# ─── Funções auxiliares ───────────────────────────────────────────────────────

def _get_poppler_path() -> str | None:
    """
    Retorna o caminho do poppler configurado em settings.POPPLER_PATH, ou None.

    - Cloud Run (Linux): poppler instalado via apt → None (usa o PATH do sistema)
    - Windows (dev): baixe em https://github.com/oschwartz10612/poppler-windows/releases
      e configure em settings.py:
        POPPLER_PATH = r"C:\\poppler\\Library\\bin"
    """
    from django.conf import settings
    return getattr(settings, 'POPPLER_PATH', None)


def _pdf_to_images(pdf_path: str, dpi: int = DPI):
    """Converte cada página do PDF em imagem PIL."""
    try:
        from pdf2image import convert_from_path
    except ImportError as exc:
        raise RuntimeError(
            "pdf2image não instalado. Execute: pip install pdf2image\n"
            "Também é necessário o poppler:\n"
            "  Linux/Cloud Run: apt-get install poppler-utils\n"
            "  Windows: baixe em https://github.com/oschwartz10612/poppler-windows/releases\n"
            "  e configure POPPLER_PATH no settings.py"
        ) from exc

    poppler_path = _get_poppler_path()
    logger.info("Convertendo PDF para imagens (%s dpi): %s", dpi, pdf_path)

    kwargs = {"dpi": dpi}
    if poppler_path:
        kwargs["poppler_path"] = poppler_path

    images = convert_from_path(str(pdf_path), **kwargs)
    logger.info("%d páginas encontradas", len(images))
    return images


def _resize_if_needed(img, max_width: int = MAX_WIDTH_PX):
    from PIL import Image
    if img.width > max_width:
        ratio = max_width / img.width
        new_size = (max_width, int(img.height * ratio))
        return img.resize(new_size, Image.LANCZOS)
    return img


def _image_to_base64(img, max_bytes: int = MAX_IMAGE_BYTES) -> tuple[str, str]:
    """
    Converte imagem para base64 garantindo que fique abaixo de max_bytes.
    Retorna (base64_data, media_type) — media_type pode ser 'image/png' ou 'image/jpeg'.

    Estratégia:
    1. Tenta PNG em escala decrescente (100% → 50%)
    2. Se ainda estiver grande, usa JPEG qualidade 85
    3. Se ainda assim passar (improvável), força JPEG qualidade 60
    """
    from PIL import Image

    for scale in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]:
        if scale < 1.0:
            new_w = int(img.width * scale)
            new_h = int(img.height * scale)
            img_scaled = img.resize((new_w, new_h), Image.LANCZOS)
        else:
            img_scaled = img

        buffer = BytesIO()
        img_scaled.save(buffer, format="PNG", optimize=True)
        data = buffer.getvalue()

        if len(data) <= max_bytes:
            if scale < 1.0:
                logger.info(
                    "PNG reduzido para %.0f%% (%.2f MB) para caber no limite da API.",
                    scale * 100, len(data) / 1024 / 1024,
                )
            return base64.standard_b64encode(data).decode("utf-8"), "image/png"

    # PNG não coube nem em 50% — tenta JPEG q85
    for quality in [85, 60]:
        buffer = BytesIO()
        img.convert("RGB").save(buffer, format="JPEG", quality=quality, optimize=True)
        data = buffer.getvalue()
        if len(data) <= max_bytes:
            logger.warning(
                "Imagem convertida para JPEG q%d (%.2f MB) para caber no limite da API.",
                quality, len(data) / 1024 / 1024,
            )
            return base64.standard_b64encode(data).decode("utf-8"), "image/jpeg"

    # Não deveria chegar aqui — retorna JPEG q60 de qualquer forma
    return base64.standard_b64encode(data).decode("utf-8"), "image/jpeg"


def _clean_json_response(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


def _extract_page(client, img, page_num: int, total_pages: int, concorrente_nome: str, programa_fidelidade) -> dict:
    img_resized = _resize_if_needed(img)
    img_b64, media_type = _image_to_base64(img_resized)
    prompt = build_prompt(page_num, total_pages, concorrente_nome, programa_fidelidade)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            logger.info("Extraindo página %d/%d (tentativa %d)...", page_num, total_pages, attempt)
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": img_b64,
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    }
                ],
            )
            raw = response.content[0].text
            cleaned = _clean_json_response(raw)
            return json.loads(cleaned)

        except json.JSONDecodeError as exc:
            logger.warning("JSON inválido na tentativa %d: %s", attempt, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
            else:
                return {
                    "pagina": page_num,
                    "produtos": [],
                    "avisos": [f"Falha ao parsear JSON após {RETRY_ATTEMPTS} tentativas: {exc}"],
                }

        except anthropic.APIError as exc:
            logger.warning("Erro de API na tentativa %d: %s", attempt, exc)
            if attempt < RETRY_ATTEMPTS:
                # Erros 529 (overloaded) precisam de backoff maior
                status_code = getattr(exc, 'status_code', 0)
                wait = 30 * attempt if status_code == 529 else RETRY_DELAY * attempt
                logger.info("Aguardando %ds antes de tentar novamente...", wait)
                time.sleep(wait)
            else:
                return {
                    "pagina": page_num,
                    "produtos": [],
                    "avisos": [f"Erro de API: {exc}"],
                }


def _safe_decimal(value) -> Decimal | None:
    """Converte um valor para Decimal de forma segura."""
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal('0.01'))
    except (InvalidOperation, TypeError, ValueError):
        return None


# ─── Entrypoint principal ─────────────────────────────────────────────────────

def _gravar_pagina(extracao, page_data: dict) -> tuple[int, int, int, int, list]:
    """Grava produtos e preços de uma única página. Retorna (produtos, precos, clube, promocional, avisos)."""
    from .models import ProdutoEncarte, PrecoEncarte

    total_produtos = 0
    total_precos = 0
    precos_clube = 0
    precos_promocional = 0
    avisos = []

    page_num = page_data.get("pagina", "?")

    for aviso in page_data.get("avisos", []):
        avisos.append(f"[pág {page_num}] {aviso}")

    for produto_data in page_data.get("produtos", []):
        nome = produto_data.get("nome", "").strip()
        if not nome:
            continue

        produto = ProdutoEncarte.objects.create(
            extracao=extracao,
            pagina=page_num,
            nome=nome,
            marca=produto_data.get("marca") or None,
            categoria=(produto_data.get("categoria") or "").lower() or None,
            quantidade=produto_data.get("quantidade") or None,
            ean=produto_data.get("ean") or None,
            validade_oferta=produto_data.get("validade_oferta") or None,
            condicao_especial=produto_data.get("condicao_especial") or None,
        )
        total_produtos += 1

        for preco_data in produto_data.get("precos", []):
            valor = _safe_decimal(preco_data.get("valor"))
            if valor is None or valor <= 0:
                avisos.append(f"[pág {page_num}] Valor inválido ignorado para '{nome}': {preco_data.get('valor')}")
                continue

            tipo = preco_data.get("tipo", "normal")
            if tipo not in {PrecoEncarte.TIPO_NORMAL, PrecoEncarte.TIPO_PROMOCIONAL,
                            PrecoEncarte.TIPO_CLUBE, PrecoEncarte.TIPO_CONDICIONAL}:
                tipo = PrecoEncarte.TIPO_NORMAL

            PrecoEncarte.objects.create(
                produto=produto,
                valor=valor,
                tipo=tipo,
                canal=preco_data.get("canal") or None,
                condicao=preco_data.get("condicao") or None,
            )
            total_precos += 1
            if tipo == PrecoEncarte.TIPO_CLUBE:
                precos_clube += 1
            elif tipo == PrecoEncarte.TIPO_PROMOCIONAL:
                precos_promocional += 1

    return total_produtos, total_precos, precos_clube, precos_promocional, avisos


def processar_encarte(extracao_id: int) -> None:
    """
    Processa um PDF de encarte e grava os produtos/preços no banco de dados.
    Grava página por página — dados parciais ficam disponíveis mesmo se algo falhar.
    """
    from .models import ExtraçãoEncarte

    try:
        extracao = ExtraçãoEncarte.objects.get(pk=extracao_id)
    except ExtraçãoEncarte.DoesNotExist:
        logger.error("ExtraçãoEncarte id=%d não encontrada", extracao_id)
        return

    extracao.status = ExtraçãoEncarte.STATUS_PROCESSANDO
    extracao.save(update_fields=['status'])

    total_produtos = 0
    total_precos = 0
    precos_clube = 0
    precos_promocional = 0
    all_avisos = []

    try:
        pdf_path = extracao.arquivo_pdf.path
        concorrente_nome = extracao.concorrente.nome
        programa_fidelidade = extracao.concorrente.programa_fidelidade or None

        # 1. Converter PDF em imagens
        images = _pdf_to_images(pdf_path)
        total_pages = len(images)
        extracao.total_paginas = total_pages
        extracao.modelo_extracao = MODEL
        extracao.save(update_fields=['total_paginas', 'modelo_extracao'])

        # 2. Instanciar cliente Anthropic
        client = anthropic.Anthropic()

        # 3. Extrair e gravar página por página
        for i, img in enumerate(images, start=1):
            page_result = _extract_page(client, img, i, total_pages, concorrente_nome, programa_fidelidade)

            # Grava imediatamente no banco
            p, pr, pc, pp, avisos = _gravar_pagina(extracao, page_result)
            total_produtos += p
            total_precos += pr
            precos_clube += pc
            precos_promocional += pp
            all_avisos.extend(avisos)

            # Atualiza contadores visíveis em tempo real
            extracao.total_produtos = total_produtos
            extracao.total_precos = total_precos
            extracao.precos_clube = precos_clube
            extracao.precos_promocional = precos_promocional
            extracao.avisos = all_avisos
            extracao.save(update_fields=[
                'total_produtos', 'total_precos', 'precos_clube',
                'precos_promocional', 'avisos',
            ])

            logger.info("Página %d/%d gravada: %d produtos", i, total_pages, p)

            if i < total_pages:
                time.sleep(1)

        # 4. Marcar como concluído
        extracao.status = ExtraçãoEncarte.STATUS_CONCLUIDO
        extracao.save(update_fields=['status'])

        logger.info("Extração %d concluída: %d produtos, %d preços", extracao_id, total_produtos, total_precos)

    except Exception as exc:
        logger.exception("Erro ao processar encarte id=%d: %s", extracao_id, exc)
        extracao.status = ExtraçãoEncarte.STATUS_ERRO
        extracao.erro_mensagem = str(exc)
        extracao.save(update_fields=['status', 'erro_mensagem'])
        raise
