"""
tests/test_correlation.py
--------------------------
Testes unitários para enrichment/correlation.py.
Usa dados sintéticos com correlações conhecidas para validar resultados.

Execute com:
    cd axionplatform
    pytest tests/test_correlation.py -v
"""
import numpy as np
import pandas as pd
import pytest

from projects.enrichment.correlation import (
    _classificar_forca,
    _ic_pearson,
    calcular_correlacoes,
    matriz_correlacao,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_correlacao_perfeita():
    """DataFrame com correlação perfeita (r=1.0) entre temp e quantidade."""
    n = 60
    np.random.seed(42)
    temp = np.linspace(15, 35, n)
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=n),
        "quantidade": temp * 10 + 100,   # Correlação perfeita positiva
        "temp_max": temp,
    })


@pytest.fixture()
def df_correlacao_negativa():
    """Chuva tem correlação negativa com vendas."""
    n = 60
    np.random.seed(7)
    chuva = np.linspace(0, 100, n)
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=n),
        "quantidade": 1000 - chuva * 5 + np.random.normal(0, 10, n),
        "precipitacao": chuva,
    })


@pytest.fixture()
def df_sem_correlacao():
    """Variável aleatória sem correlação com vendas."""
    n = 60
    np.random.seed(0)
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=n),
        "quantidade": np.random.normal(500, 100, n),
        "ruido": np.random.normal(0, 1, n),
    })


@pytest.fixture()
def df_pequeno():
    """DataFrame com menos de 30 linhas (abaixo do mínimo)."""
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=20),
        "quantidade": range(20),
        "temp_max": range(20),
    })


@pytest.fixture()
def df_com_nulos():
    """DataFrame com valores nulos nas variáveis."""
    n = 60
    np.random.seed(3)
    temp = np.linspace(20, 35, n).tolist()
    qtd = [t * 8 + 50 for t in temp]
    # Insere nulos em 20% das linhas
    for i in range(0, n, 5):
        temp[i] = None
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=n),
        "quantidade": qtd,
        "temp_max": temp,
    })


@pytest.fixture()
def df_enriquecido():
    """DataFrame com múltiplas variáveis — mix de correlacionadas e não."""
    n = 90
    np.random.seed(99)
    temp = np.linspace(18, 38, n)
    base_qtd = temp * 12 + 200 + np.random.normal(0, 30, n)
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=n),
        "quantidade": base_qtd,
        "temp_max": temp,
        "temp_min": temp - 8 + np.random.normal(0, 2, n),
        "precipitacao": np.random.exponential(10, n),
        "is_feriado": np.random.choice([0, 1], n, p=[0.92, 0.08]),
        "dia_semana": np.random.randint(0, 7, n),
        "ruido_puro": np.random.normal(0, 1, n),
    })


# ── Testes: _classificar_forca ────────────────────────────────────────────────

class TestClassificarForca:
    @pytest.mark.parametrize("r,esperado", [
        (0.95, "muito_forte"),
        (-0.85, "muito_forte"),
        (0.65, "forte"),
        (-0.55, "forte"),
        (0.40, "moderada"),
        (-0.35, "moderada"),
        (0.15, "fraca"),
        (-0.12, "fraca"),
        (0.05, "negligivel"),
        (0.0, "negligivel"),
    ])
    def test_classificacoes(self, r, esperado):
        assert _classificar_forca(r) == esperado


# ── Testes: _ic_pearson ───────────────────────────────────────────────────────

class TestIcPearson:
    def test_ic_dentro_limites(self):
        lower, upper = _ic_pearson(0.5, 50)
        assert -1.0 <= lower <= upper <= 1.0

    def test_ic_r_positivo_ambos_positivos(self):
        lower, upper = _ic_pearson(0.8, 100)
        assert lower > 0
        assert upper > lower

    def test_ic_r_negativo(self):
        lower, upper = _ic_pearson(-0.6, 80)
        assert lower < 0
        assert upper < 0

    def test_ic_n_pequeno_retorna_extremos(self):
        lower, upper = _ic_pearson(0.5, 3)
        assert lower == -1.0
        assert upper == 1.0

    def test_ic_quanto_maior_n_menor_intervalo(self):
        lower_100, upper_100 = _ic_pearson(0.5, 100)
        lower_1000, upper_1000 = _ic_pearson(0.5, 1000)
        intervalo_100 = upper_100 - lower_100
        intervalo_1000 = upper_1000 - lower_1000
        assert intervalo_1000 < intervalo_100


# ── Testes: calcular_correlacoes ──────────────────────────────────────────────

class TestCalcularCorrelacoes:
    def test_correlacao_positiva_forte_detectada(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita)
        assert len(resultado) >= 1
        assert resultado[0]["variavel"] == "temp_max"
        assert resultado[0]["correlacao"] > 0.95
        assert resultado[0]["direcao"] == "positiva"

    def test_correlacao_negativa_detectada(self, df_correlacao_negativa):
        resultado = calcular_correlacoes(df_correlacao_negativa)
        # Filtra apenas precipitacao
        prec = next((r for r in resultado if r["variavel"] == "precipitacao"), None)
        assert prec is not None
        assert prec["correlacao"] < 0
        assert prec["direcao"] == "negativa"

    def test_sem_correlacao_nao_retorna(self, df_sem_correlacao):
        resultado = calcular_correlacoes(df_sem_correlacao)
        variaveis = [r["variavel"] for r in resultado]
        assert "ruido" not in variaveis

    def test_df_pequeno_retorna_vazio(self, df_pequeno):
        resultado = calcular_correlacoes(df_pequeno)
        assert resultado == []

    def test_ordenado_por_forca_absoluta(self, df_enriquecido):
        resultado = calcular_correlacoes(df_enriquecido)
        if len(resultado) >= 2:
            for i in range(len(resultado) - 1):
                assert abs(resultado[i]["correlacao"]) >= abs(resultado[i+1]["correlacao"])

    def test_todos_p_value_abaixo_limite(self, df_enriquecido):
        resultado = calcular_correlacoes(df_enriquecido, p_value_max=0.05)
        for item in resultado:
            assert item["p_value"] < 0.05

    def test_estrutura_campos_retornados(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita)
        campos = {"variavel", "correlacao", "p_value", "n_observacoes",
                  "forca", "direcao", "interpretacao", "ic_95_lower", "ic_95_upper"}
        for item in resultado:
            assert campos.issubset(item.keys())

    def test_coluna_alvo_ausente(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita, coluna_alvo="vendas")
        assert resultado == []

    def test_nao_inclui_coluna_alvo_como_variavel(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita, coluna_alvo="quantidade")
        variaveis = [r["variavel"] for r in resultado]
        assert "quantidade" not in variaveis

    def test_com_nulos_funciona(self, df_com_nulos):
        """Correlação deve ser calculada apenas com pares de observações válidas."""
        resultado = calcular_correlacoes(df_com_nulos)
        if resultado:
            for item in resultado:
                assert item["n_observacoes"] >= 30

    def test_variavel_constante_excluida(self):
        """Variável sem variância não deve causar erro."""
        n = 60
        df = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=n),
            "quantidade": np.random.normal(500, 50, n),
            "constante": [10.0] * n,  # sem variância
        })
        resultado = calcular_correlacoes(df)
        variaveis = [r["variavel"] for r in resultado]
        assert "constante" not in variaveis

    def test_p_value_customizado(self, df_enriquecido):
        """Com p_value_max mais restrito, deve retornar menos correlações."""
        resultado_05 = calcular_correlacoes(df_enriquecido, p_value_max=0.05)
        resultado_01 = calcular_correlacoes(df_enriquecido, p_value_max=0.01)
        assert len(resultado_01) <= len(resultado_05)

    def test_min_obs_customizado(self, df_correlacao_perfeita):
        """Com min_obs > len(df), deve retornar vazio."""
        resultado = calcular_correlacoes(df_correlacao_perfeita, min_obs=1000)
        assert resultado == []

    def test_colunas_excluir_customizadas(self, df_enriquecido):
        resultado = calcular_correlacoes(
            df_enriquecido,
            colunas_excluir=["temp_max", "temp_min"],
        )
        variaveis = [r["variavel"] for r in resultado]
        assert "temp_max" not in variaveis
        assert "temp_min" not in variaveis

    def test_type_error_nao_dataframe(self):
        with pytest.raises(TypeError):
            calcular_correlacoes([1, 2, 3])

    def test_ic_lower_menor_correlacao(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita)
        for item in resultado:
            assert item["ic_95_lower"] <= item["correlacao"]
            assert item["correlacao"] <= item["ic_95_upper"]

    def test_interpretacao_e_string_nao_vazia(self, df_correlacao_perfeita):
        resultado = calcular_correlacoes(df_correlacao_perfeita)
        for item in resultado:
            assert isinstance(item["interpretacao"], str)
            assert len(item["interpretacao"]) > 10


# ── Testes: matriz_correlacao ─────────────────────────────────────────────────

class TestMatrizCorrelacao:
    def test_retorna_dataframe(self, df_enriquecido):
        matriz = matriz_correlacao(df_enriquecido)
        assert isinstance(matriz, pd.DataFrame)

    def test_diagonal_e_um(self, df_enriquecido):
        matriz = matriz_correlacao(df_enriquecido)
        for col in matriz.columns:
            assert matriz.loc[col, col] == pytest.approx(1.0)

    def test_simetrica(self, df_enriquecido):
        matriz = matriz_correlacao(df_enriquecido)
        for c1 in matriz.columns:
            for c2 in matriz.columns:
                assert matriz.loc[c1, c2] == pytest.approx(matriz.loc[c2, c1], abs=1e-10)

    def test_colunas_texto_excluidas(self, df_enriquecido):
        matriz = matriz_correlacao(df_enriquecido)
        assert "data" not in matriz.columns
        assert "municipio" not in matriz.columns


# ── Testes: integração processor + calendario + correlation ───────────────────

class TestIntegracaoPipeline:
    def test_pipeline_completo(self):
        """Valida que processor → calendario → correlation não quebra."""
        from projects.enrichment.processor import process_sales_data
        from projects.enrichment.calendario import enrich_calendario

        n = 90
        np.random.seed(55)
        temp = np.linspace(20, 35, n)
        df_raw = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=n).strftime("%d/%m/%Y"),
            "quantidade": (temp * 15 + 200 + np.random.normal(0, 20, n)).astype(int),
            "preco": np.round(np.random.uniform(10, 50, n), 2),
        })

        df_proc, rel = process_sales_data(df_raw, resolver_cep=False)
        assert rel.valido
        assert len(df_proc) == n

        df_cal = enrich_calendario(df_proc)
        assert "is_feriado" in df_cal.columns

        correlacoes = calcular_correlacoes(df_cal, coluna_alvo="quantidade")
        # Não precisa ter correlações — só não pode travar
        assert isinstance(correlacoes, list)
