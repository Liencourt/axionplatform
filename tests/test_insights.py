"""
tests/test_insights.py
-----------------------
Testes unitários para enrichment/insights.py.

Execute com:
    cd axionplatform
    pytest tests/test_insights.py -v
"""
import pytest

from projects.enrichment.insights import (
    _get_categoria,
    _get_prioridade,
    gerar_insights,
    resumo_executivo,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _corr(variavel, r, p=0.001, n=120):
    return {
        "variavel": variavel,
        "correlacao": r,
        "p_value": p,
        "n_observacoes": n,
        "forca": "forte" if abs(r) >= 0.5 else "moderada",
        "direcao": "positiva" if r > 0 else "negativa",
        "ic_95_lower": round(r - 0.05, 4),
        "ic_95_upper": round(r + 0.05, 4),
        "interpretacao": f"Correlação de {r}",
    }


CORRELACOES_EXEMPLO = [
    _corr("temp_max",        r=0.72),
    _corr("is_feriado",      r=-0.55),
    _corr("quantidade_lag7", r=0.48),
    _corr("precipitacao",    r=-0.31),
    _corr("ibge_pib_per_capita", r=0.28),
    _corr("mes",             r=0.22),
]


# ── Testes: _get_categoria ────────────────────────────────────────────────────

class TestGetCategoria:
    @pytest.mark.parametrize("var,cat", [
        ("temp_max",              "CLIMA"),
        ("temp_lag1",             "CLIMA"),
        ("precipitacao",          "CLIMA"),
        ("chuva_flag",            "CLIMA"),
        ("umidade_media",         "CLIMA"),
        ("is_feriado",            "CALENDARIO"),
        ("is_fim_semana",         "CALENDARIO"),
        ("dias_ate_natal",        "CALENDARIO"),
        ("dias_ate_black_friday", "CALENDARIO"),
        ("mes",                   "CALENDARIO"),
        ("quantidade_lag1",       "TENDENCIA"),
        ("quantidade_mm7",        "TENDENCIA"),
        ("ibge_populacao",        "ECONOMICO"),
        ("ibge_pib_per_capita",   "ECONOMICO"),
        ("preco",                 "ECONOMICO"),
        ("variavel_desconhecida", "GERAL"),
    ])
    def test_categorias(self, var, cat):
        assert _get_categoria(var) == cat


# ── Testes: _get_prioridade ───────────────────────────────────────────────────

class TestGetPrioridade:
    @pytest.mark.parametrize("r,esperado", [
        (0.75,  "alta"),
        (0.50,  "alta"),
        (0.49,  "media"),
        (0.30,  "media"),
        (0.29,  "baixa"),
        (0.10,  "baixa"),
        (-0.60, "alta"),
        (-0.35, "media"),
        (-0.15, "baixa"),
    ])
    def test_prioridades(self, r, esperado):
        assert _get_prioridade(abs(r)) == esperado


# ── Testes: gerar_insights ────────────────────────────────────────────────────

class TestGerarInsights:
    def test_retorna_lista(self):
        resultado = gerar_insights(CORRELACOES_EXEMPLO)
        assert isinstance(resultado, list)

    def test_campos_obrigatorios(self):
        resultado = gerar_insights(CORRELACOES_EXEMPLO)
        campos = {"categoria", "variavel", "correlacao", "prioridade",
                  "titulo", "descricao", "acao"}
        for insight in resultado:
            assert campos.issubset(insight.keys()), f"Campos faltando em: {insight}"

    def test_quantidade_retornada_respeita_max(self):
        resultado = gerar_insights(CORRELACOES_EXEMPLO, max_insights=3)
        assert len(resultado) <= 3

    def test_lista_vazia_retorna_vazio(self):
        assert gerar_insights([]) == []

    def test_ordenado_por_prioridade(self):
        resultado = gerar_insights(CORRELACOES_EXEMPLO)
        prioridades = [r["prioridade"] for r in resultado]
        _ordem = {"alta": 0, "media": 1, "baixa": 2}
        ordens = [_ordem[p] for p in prioridades]
        assert ordens == sorted(ordens), "Insights não estão ordenados por prioridade"

    def test_categorias_corretas(self):
        resultado = gerar_insights(CORRELACOES_EXEMPLO)
        mapa = {i["variavel"]: i["categoria"] for i in resultado}
        assert mapa["temp_max"] == "CLIMA"
        assert mapa["is_feriado"] == "CALENDARIO"
        assert mapa["quantidade_lag7"] == "TENDENCIA"
        assert mapa["ibge_pib_per_capita"] == "ECONOMICO"

    def test_contexto_enriquece_insight(self):
        contexto = {"ibge_classe": "B", "ibge_municipio": "São Paulo", "ibge_uf": "SP"}
        resultado = gerar_insights(
            [_corr("ibge_pib_per_capita", r=0.45)],
            contexto=contexto,
        )
        assert len(resultado) == 1
        # Classe B deve aparecer na ação
        assert "B" in resultado[0]["acao"]

    def test_insight_clima_positivo(self):
        resultado = gerar_insights([_corr("temp_max", r=0.65)])
        assert len(resultado) == 1
        acao = resultado[0]["acao"].lower()
        # Deve sugerir ação para dias quentes
        assert any(w in acao for w in ("calor", "quente", "verão", "campanha", "estoque"))

    def test_insight_clima_negativo(self):
        resultado = gerar_insights([_corr("temp_max", r=-0.60)])
        assert len(resultado) == 1
        acao = resultado[0]["acao"].lower()
        assert any(w in acao for w in ("frio", "promoç", "compens", "produto"))

    def test_insight_chuva_flag(self):
        resultado = gerar_insights([_corr("chuva_flag", r=0.40)])
        assert resultado[0]["categoria"] == "CLIMA"
        assert resultado[0]["titulo"] != ""

    def test_insight_feriado(self):
        resultado = gerar_insights([_corr("is_feriado", r=-0.55)])
        assert resultado[0]["categoria"] == "CALENDARIO"

    def test_insight_lag(self):
        resultado = gerar_insights([_corr("quantidade_lag7", r=0.48)])
        assert resultado[0]["categoria"] == "TENDENCIA"
        assert "7" in resultado[0]["titulo"]

    def test_insight_natal(self):
        resultado = gerar_insights([_corr("dias_ate_natal", r=-0.50)])
        assert "natal" in resultado[0]["titulo"].lower() or "natal" in resultado[0]["acao"].lower()

    def test_insight_black_friday(self):
        resultado = gerar_insights([_corr("dias_ate_black_friday", r=-0.45)])
        acao_titulo = resultado[0]["titulo"].lower() + resultado[0]["acao"].lower()
        assert "black" in acao_titulo or "friday" in acao_titulo

    def test_prioridade_alta_para_r_forte(self):
        resultado = gerar_insights([_corr("temp_max", r=0.72)])
        assert resultado[0]["prioridade"] == "alta"

    def test_prioridade_baixa_para_r_fraco(self):
        resultado = gerar_insights([_corr("mes", r=0.22)])
        assert resultado[0]["prioridade"] == "baixa"

    def test_ic_preservado(self):
        corr = _corr("temp_max", r=0.72)
        resultado = gerar_insights([corr])
        assert resultado[0]["ic_95_lower"] == corr["ic_95_lower"]
        assert resultado[0]["ic_95_upper"] == corr["ic_95_upper"]

    def test_correlacao_desconhecida_gera_insight_geral(self):
        resultado = gerar_insights([_corr("variavel_estranha_xpto", r=0.40)])
        assert resultado[0]["categoria"] == "GERAL"
        assert resultado[0]["titulo"] != ""
        assert resultado[0]["acao"] != ""

    def test_falha_em_uma_nao_interrompe_demais(self):
        """Se um template falhar internamente, os outros insights são gerados."""
        from unittest.mock import patch

        with patch("projects.enrichment.insights._insight_clima_temperatura",
                   side_effect=RuntimeError("mock error")):
            resultado = gerar_insights(CORRELACOES_EXEMPLO)

        # Os insights de outras categorias devem ter sido gerados
        categorias = {i["categoria"] for i in resultado}
        assert "CALENDARIO" in categorias or "TENDENCIA" in categorias


# ── Testes: resumo_executivo ──────────────────────────────────────────────────

class TestResumoExecutivo:
    def test_retorna_string(self):
        insights = gerar_insights(CORRELACOES_EXEMPLO)
        resumo = resumo_executivo(insights)
        assert isinstance(resumo, str)
        assert len(resumo) > 50

    def test_lista_vazia_retorna_mensagem(self):
        resumo = resumo_executivo([])
        assert "não foram encontradas" in resumo.lower()

    def test_menciona_empresa(self):
        insights = gerar_insights(CORRELACOES_EXEMPLO)
        resumo = resumo_executivo(insights, contexto={"nome_empresa": "Loja Teste"})
        assert "Loja Teste" in resumo

    def test_menciona_municipio(self):
        insights = gerar_insights(CORRELACOES_EXEMPLO)
        resumo = resumo_executivo(
            insights,
            contexto={"ibge_municipio": "Campinas", "ibge_uf": "SP"},
        )
        assert "Campinas" in resumo

    def test_menciona_quantidade_de_drivers(self):
        insights = gerar_insights(CORRELACOES_EXEMPLO)
        resumo = resumo_executivo(insights)
        # Número de insights deve aparecer no resumo
        assert str(len(insights)) in resumo

    def test_inclui_acao_top_insight(self):
        insights = gerar_insights([_corr("temp_max", r=0.72)])
        resumo = resumo_executivo(insights)
        # A ação do top insight deve aparecer no resumo
        assert insights[0]["acao"] in resumo
