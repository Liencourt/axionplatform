#!/usr/bin/env python3
"""
Axiom Platform — Extrator de Encarte via Vision LLM
Calibrado para: Supermercados Guanabara

Uso:
    python extract_encarte.py <caminho_do_pdf>

Dependências:
    pip install anthropic pdf2image pillow

Observação: pdf2image requer poppler instalado no sistema.
    Windows: baixe em https://github.com/oschwartz10612/poppler-windows/releases
    macOS:   brew install poppler
    Linux:   apt-get install poppler-utils
"""

import anthropic
import base64
import json
import sys
import re
import time
from datetime import date
from pathlib import Path
from io import BytesIO

try:
    from pdf2image import convert_from_path
    from PIL import Image
except ImportError:
    print("❌ Dependências faltando. Execute:")
    print("   pip install pdf2image pillow")
    sys.exit(1)


# ─── Configurações ────────────────────────────────────────────────────────────

DPI            = 180          # 180dpi: boa leitura sem peso excessivo por página
MAX_WIDTH_PX   = 1400         # redimensionar se necessário (economiza tokens)
MODEL          = "claude-opus-4-5"
MAX_TOKENS     = 4096
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 5            # segundos entre tentativas


# ─── Prompt de sistema ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Você é um especialista em extração de dados de encartes de supermercados brasileiros
para um sistema de inteligência de preços (Retail Intelligence).

Sua tarefa é identificar e estruturar TODOS os produtos e preços visíveis na imagem.
Retorne APENAS JSON válido, sem markdown, sem explicação, sem comentários."""


# ─── Prompt de usuário por página ─────────────────────────────────────────────

def build_prompt(page_num: int, total_pages: int) -> str:
    return f"""Página {page_num} de {total_pages} — Encarte Supermercados Guanabara.

Extraia TODOS os produtos visíveis nesta página.

═══ PADRÕES ESPECÍFICOS DO GUANABARA ═══

① GUANA CLUBE (mais importante)
   Aparece como selo/logo azul "Guana Clube" ao lado de um preço menor.
   → Cria DOIS objetos em "precos": um tipo "normal" e um tipo "clube" com condicao "Guana Clube"
   Exemplo: Arroz R$27,95 + Guana Clube R$26,95 → dois preços

② "Por:" antes do preço
   → tipo: "promocional", canal: "loja"

③ "Nesta embalagem Xml saem por: R$X"
   → O produto tem embalagem diferente. Registre o volume real no campo "quantidade"
      e descreva em "condicao" ex: "embalagem 400ml"
   → tipo: "promocional"

④ "Leve X Pague Y"
   → tipo: "condicional", registre o preço POR UNIDADE calculado em "valor"
   → condicao: "leve 3 pague 2" (ou o que estiver visível)
   Exemplo: Café Leve 3 Pague 2 — R$19,90 cada → valor: 13.27, condicao: "leve 3 pague 2, cada unidade sai R$13,27"

⑤ "Oferta Especial" / "Preço Especial" (badge visual)
   → tipo: "promocional"

⑥ "50% de desconto na compra da 2ª unidade"
   → tipo: "condicional", condicao: "50% desconto na 2ª unidade"

⑦ Produtos do açougue / peixaria vendidos POR KG
   → quantidade: "kg", o valor é o preço/kg

⑧ Pack / caixa com múltiplas unidades (ex: cervejas 12 latas)
   → quantidade: "pack c/12 473ml", se houver preço por unidade, registre também

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
      "validade_oferta": "01/04/2026 a 06/04/2026",
      "condicao_especial": "ex: leve 3 pague 2 | ou null",
      "precos": [
        {{
          "valor": 0.00,
          "tipo": "normal | promocional | clube | condicional",
          "canal": "loja | null",
          "condicao": "Guana Clube | embalagem 400ml | null"
        }}
      ]
    }}
  ],
  "avisos": []
}}

Regras absolutas:
- "valor" é sempre float com ponto (ex: 26.95, nunca "26,95")
- Todo produto deve ter ao menos um objeto em "precos"
- Se houver Guana Clube, o produto TEM DOIS objetos em "precos"
- "avisos" recebe strings descrevendo qualquer ambiguidade encontrada
- Não omita nenhum produto visível, mesmo que incompleto
"""


# ─── Funções auxiliares ───────────────────────────────────────────────────────

def pdf_to_images(pdf_path: Path, dpi: int = DPI) -> list[Image.Image]:
    """Converte cada página do PDF em imagem PIL."""
    print(f"📄 Convertendo PDF para imagens ({dpi} dpi)...")
    images = convert_from_path(str(pdf_path), dpi=dpi)
    print(f"   {len(images)} páginas encontradas")
    return images


def resize_if_needed(img: Image.Image, max_width: int = MAX_WIDTH_PX) -> Image.Image:
    """Redimensiona imagem se largura exceder o limite (economia de tokens)."""
    if img.width > max_width:
        ratio = max_width / img.width
        new_size = (max_width, int(img.height * ratio))
        return img.resize(new_size, Image.LANCZOS)
    return img


def image_to_base64(img: Image.Image) -> str:
    """Converte imagem PIL para base64 PNG."""
    buffer = BytesIO()
    img.save(buffer, format="PNG", optimize=True)
    return base64.standard_b64encode(buffer.getvalue()).decode("utf-8")


def clean_json_response(text: str) -> str:
    """Remove possíveis marcações markdown ao redor do JSON."""
    text = text.strip()
    # Remove ```json ... ``` ou ``` ... ```
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


def extract_page(
    client: anthropic.Anthropic,
    img: Image.Image,
    page_num: int,
    total_pages: int
) -> dict:
    """Envia uma página para o Claude Vision e retorna o JSON parseado."""

    img_resized = resize_if_needed(img)
    img_b64 = image_to_base64(img_resized)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            print(f"   🔍 Extraindo página {page_num}/{total_pages} (tentativa {attempt})...")

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
                                    "media_type": "image/png",
                                    "data": img_b64,
                                },
                            },
                            {
                                "type": "text",
                                "text": build_prompt(page_num, total_pages),
                            },
                        ],
                    }
                ],
            )

            raw = response.content[0].text
            cleaned = clean_json_response(raw)
            data = json.loads(cleaned)
            return data

        except json.JSONDecodeError as e:
            print(f"   ⚠️  JSON inválido na tentativa {attempt}: {e}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
            else:
                # Retorna estrutura vazia com aviso em caso de falha total
                return {
                    "pagina": page_num,
                    "produtos": [],
                    "avisos": [f"Falha ao parsear JSON após {RETRY_ATTEMPTS} tentativas: {str(e)}"],
                }

        except anthropic.APIError as e:
            print(f"   ⚠️  Erro de API na tentativa {attempt}: {e}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return {
                    "pagina": page_num,
                    "produtos": [],
                    "avisos": [f"Erro de API: {str(e)}"],
                }


def validate_product(produto: dict) -> list[str]:
    """Valida um produto e retorna lista de avisos."""
    avisos = []
    nome = produto.get("nome", "")

    if not produto.get("precos"):
        avisos.append(f"Produto sem preços: {nome}")

    for i, preco in enumerate(produto.get("precos", [])):
        valor = preco.get("valor")
        if not isinstance(valor, (int, float)) or valor <= 0:
            avisos.append(f"Valor inválido no produto '{nome}', preço {i+1}: {valor}")

        tipo = preco.get("tipo", "")
        if tipo not in {"normal", "promocional", "clube", "condicional"}:
            avisos.append(f"Tipo inválido no produto '{nome}': '{tipo}'")

        canal = preco.get("canal")
        if canal not in {"loja", "app", "online", None}:
            avisos.append(f"Canal inválido no produto '{nome}': '{canal}'")

    return avisos


def consolidate(pages_data: list[dict], concorrente: str, pdf_name: str) -> dict:
    """Consolida todas as páginas em um único documento de saída."""
    all_products = []
    all_avisos = []
    total_precos = 0
    precos_clube = 0
    precos_promocional = 0

    for page in pages_data:
        page_avisos = page.get("avisos", [])
        if page_avisos:
            all_avisos.extend([f"[pág {page.get('pagina')}] {a}" for a in page_avisos])

        for produto in page.get("produtos", []):
            validation_avisos = validate_product(produto)
            all_avisos.extend(validation_avisos)

            precos = produto.get("precos", [])
            total_precos += len(precos)
            precos_clube += sum(1 for p in precos if p.get("tipo") == "clube")
            precos_promocional += sum(1 for p in precos if p.get("tipo") == "promocional")

            all_products.append(produto)

    return {
        "meta": {
            "concorrente": concorrente,
            "arquivo_origem": pdf_name,
            "vigencia": "01/04/2026 a 06/04/2026",
            "data_extracao": date.today().isoformat(),
            "total_paginas": len(pages_data),
            "total_produtos": len(all_products),
            "total_precos": total_precos,
            "precos_clube": precos_clube,
            "precos_promocional": precos_promocional,
            "tipo_fonte": "pdf_imagem",
            "modelo_extracao": MODEL,
            "avisos": all_avisos,
        },
        "produtos": all_products,
    }


def print_summary(result: dict) -> None:
    """Imprime resumo formatado no terminal."""
    meta = result["meta"]
    print()
    print("─" * 52)
    print("✅ Extração concluída")
    print(f"   Concorrente    : {meta['concorrente']}")
    print(f"   Vigência       : {meta['vigencia']}")
    print(f"   Páginas        : {meta['total_paginas']}")
    print(f"   Produtos       : {meta['total_produtos']}")
    print(f"   Total de preços: {meta['total_precos']}")
    if meta['total_produtos'] > 0:
        media = meta['total_precos'] / meta['total_produtos']
        print(f"   Média por prod : {media:.1f} preços")
    print(f"   Guana Clube    : {meta['precos_clube']} preços com desconto clube")
    print(f"   Promocionais   : {meta['precos_promocional']} preços promocionais")
    if meta['avisos']:
        print(f"   ⚠️  Avisos      : {len(meta['avisos'])} (ver campo meta.avisos no JSON)")
    print("─" * 52)


# ─── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Uso: python extract_encarte.py <caminho_do_pdf>")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    if not pdf_path.exists():
        print(f"❌ Arquivo não encontrado: {pdf_path}")
        sys.exit(1)

    output_path = pdf_path.parent / "encarte_extraido.json"

    print(f"🛒 Axiom — Extrator de Encarte")
    print(f"   Arquivo  : {pdf_path.name}")
    print(f"   Saída    : {output_path.name}")
    print()

    # 1. Converter PDF em imagens
    images = pdf_to_images(pdf_path)

    # 2. Instanciar cliente Anthropic (lê ANTHROPIC_API_KEY do ambiente)
    client = anthropic.Anthropic()

    # 3. Extrair cada página
    pages_data = []
    total = len(images)
    for i, img in enumerate(images, start=1):
        page_result = extract_page(client, img, i, total)
        n_produtos = len(page_result.get("produtos", []))
        print(f"   ✓ Página {i}: {n_produtos} produtos encontrados")
        pages_data.append(page_result)
        # Pequeno delay entre páginas para evitar rate limit
        if i < total:
            time.sleep(1)

    # 4. Consolidar
    print()
    print("📦 Consolidando resultados...")
    result = consolidate(pages_data, "Supermercados Guanabara", pdf_path.name)

    # 5. Salvar
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"💾 Arquivo salvo: {output_path}")

    # 6. Resumo
    print_summary(result)


if __name__ == "__main__":
    main()