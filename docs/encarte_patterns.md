# Padrões Visuais por Rede — Referência de Extração

Carregue este arquivo quando for trabalhar com extração de encartes:
`@docs/encarte_patterns.md`

---

## Supermercados Guanabara

**Vigência típica**: semana corrente (ex: 01/04 a 06/04)
**Programa de fidelidade**: Guana Clube (sem app, sem cartão bancário)
**Páginas típicas**: 10–12 páginas por encarte

### Padrões de preço identificados

| Padrão visual | Tipo | Canal | Ação |
|---|---|---|---|
| Preço sem badge | `normal` | `loja` | valor direto |
| Selo azul "Guana Clube" + valor | `clube` | `loja` | segundo objeto em `precos`, condicao: "Guana Clube" |
| "Por:" antes do valor | `promocional` | `loja` | valor após "Por:" |
| "Oferta Especial" / "Preço Especial" | `promocional` | `loja` | badge decorativo |
| "Nesta embalagem Xml saem por:" | `promocional` | `loja` | registrar volume real em `quantidade`, ex: "400ml" |
| "Leve X Pague Y" | `condicional` | `loja` | valor = preço/unidade calculado |
| "50% desconto na 2ª unidade" | `condicional` | `loja` | condicao: "50% desconto na 2ª unidade" |

### Produtos por kg (açougue e peixaria)
- `quantidade`: "kg"
- `valor`: preço por kg
- `tipo`: "normal" (ou "promocional" se tiver "Por:")

### Packs e caixas (cervejas, refrigerantes)
- `quantidade`: "pack c/12 473ml" (incluir quantidade e volume individual)
- Se houver preço por unidade separado → segundo objeto em `precos` com condicao: "preço por unidade"

### Centavos em fonte sobrescrita menor
O layout do Guanabara renderiza centavos menores e deslocados (ex: visual "9⁹⁸").
O Vision LLM costuma ler corretamente, mas verificar em `meta.avisos` se houver dúvida.

### Categorias frequentes por página
- Pág 1–2: mercearia seca (arroz, feijão, café, açúcar, chocolates)
- Pág 3: azeites e condimentos
- Pág 4: bebidas (refrigerantes, cervejas, destilados)
- Pág 5–6: vinhos (seção exclusiva)
- Pág 7: carnes e pescados
- Pág 8: frios, laticínios e congelados
- Pág 9: higiene pessoal
- Pág 10: cabelos e cosméticos
- Pág 11: limpeza e casa
- Pág 12: chocolates finos (Cachet)

### Exemplo de produto com Guana Clube
```json
{
  "nome": "Arroz Tio João Parboilizado 5kg",
  "marca": "Tio João",
  "categoria": "mercearia",
  "quantidade": "5kg",
  "ean": null,
  "validade_oferta": "01/04/2026 a 06/04/2026",
  "condicao_especial": null,
  "precos": [
    { "valor": 27.95, "tipo": "normal",  "canal": "loja", "condicao": null },
    { "valor": 26.95, "tipo": "clube",   "canal": "loja", "condicao": "Guana Clube" }
  ]
}
```

### Exemplo de produto com Leve 3 Pague 2
```json
{
  "nome": "Café Papagaio Extraforte 500g",
  "marca": "Papagaio",
  "categoria": "mercearia",
  "quantidade": "500g",
  "ean": null,
  "validade_oferta": "01/04/2026 a 06/04/2026",
  "condicao_especial": "leve 3 pague 2",
  "precos": [
    { "valor": 19.90, "tipo": "normal",      "canal": "loja", "condicao": null },
    { "valor": 13.27, "tipo": "condicional", "canal": "loja", "condicao": "leve 3 pague 2, preço por unidade" }
  ]
}
```

### Exemplo de produto com embalagem reduzida
```json
{
  "nome": "Azeite Extravirgem Andorinha Seleção",
  "marca": "Andorinha",
  "categoria": "mercearia",
  "quantidade": "400ml",
  "ean": null,
  "validade_oferta": "01/04/2026 a 06/04/2026",
  "condicao_especial": null,
  "precos": [
    { "valor": 29.98, "tipo": "normal",      "canal": "loja", "condicao": "embalagem 500ml" },
    { "valor": 23.98, "tipo": "promocional", "canal": "loja", "condicao": "nesta embalagem 400ml" }
  ]
}
```

---

## Adicionar nova rede

Copie o bloco acima e adapte. Campos obrigatórios:
- Nome do programa de fidelidade (ou "sem programa")
- Padrões visuais específicos (badge, texto, cor)
- Categorias por página
- Ao menos um exemplo completo de produto com dois preços