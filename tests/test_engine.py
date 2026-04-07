"""
tests/test_engine.py
--------------------
Testes unitários para enrichment/engine.py.

Execute com:
    cd axionplatform
    pytest tests/test_engine.py -v
"""
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from projects.enrichment.engine import (
    _classificar_temp,
    criar_features_vendas,
    criar_features_clima,
    criar_features_economicas,
    criar_features,
    montar_dataset_completo,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_base():
    """DataFrame mínimo com 40 dias de vendas."""
    return pd.DataFrame({
        "data":      pd.date_range("2024-01-01", periods=40),
        "quantidade": range(1, 41),
        "preco":     [20.0 + (i % 5) for i in range(40)],
    })


@pytest.fixture()
def df_com_clima(df_base):
    """DataFrame com colunas climáticas."""
    df = df_base.copy()
    df["temp_max"]     = [25.0 + (i % 10) for i in range(40)]
    df["precipitacao"] = [0.0 if i % 3 else 5.0 for i in range(40)]
    return df


# ── Testes: _classificar_temp ─────────────────────────────────────────────────

class TestClassificarTemp:
    @pytest.mark.parametrize("temp,esperado", [
        (35.0, "muito_quente"),
        (32.0, "muito_quente"),
        (30.0, "quente"),
        (26.0, "quente"),
        (22.0, "ameno"),
        (18.0, "ameno"),
        (15.0, "frio"),
        (0.0,  "frio"),
        (-5.0, "frio"),
    ])
    def test_faixas(self, temp, esperado):
        assert _classificar_temp(temp) == esperado


# ── Testes: criar_features_vendas ─────────────────────────────────────────────

class TestCriarFeaturesVendas:
    def test_adiciona_lags(self, df_base):
        df = criar_features_vendas(df_base)
        assert "quantidade_lag1"  in df.columns
        assert "quantidade_lag7"  in df.columns
        assert "quantidade_lag30" in df.columns

    def test_adiciona_medias_moveis(self, df_base):
        df = criar_features_vendas(df_base)
        assert "quantidade_mm7"  in df.columns
        assert "quantidade_mm30" in df.columns

    def test_lag1_deslocado_corretamente(self, df_base):
        df = criar_features_vendas(df_base)
        # índice 1 deve ter lag1 = valor do índice 0
        assert df["quantidade_lag1"].iloc[1] == df_base["quantidade"].iloc[0]

    def test_lag7_deslocado_corretamente(self, df_base):
        df = criar_features_vendas(df_base)
        assert df["quantidade_lag7"].iloc[7] == df_base["quantidade"].iloc[0]

    def test_primeiras_linhas_lag_sao_nan(self, df_base):
        df = criar_features_vendas(df_base)
        assert pd.isna(df["quantidade_lag1"].iloc[0])
        assert pd.isna(df["quantidade_lag7"].iloc[6])

    def test_mm7_nao_nan_apos_3_obs(self, df_base):
        df = criar_features_vendas(df_base)
        # min_periods=3 → a partir do índice 2 não deve ser NaN
        assert not pd.isna(df["quantidade_mm7"].iloc[2])

    def test_nao_modifica_df_original(self, df_base):
        colunas_antes = list(df_base.columns)
        criar_features_vendas(df_base)
        assert list(df_base.columns) == colunas_antes

    def test_coluna_inexistente_retorna_df_original(self, df_base):
        df = criar_features_vendas(df_base, coluna_qtd="coluna_inexistente")
        assert "coluna_inexistente_lag1" not in df.columns
        assert len(df) == len(df_base)

    def test_ordena_por_data(self):
        """DataFrame desordenado deve ser reordenado antes de calcular lags."""
        datas = pd.date_range("2024-01-01", periods=10)
        qtd = list(range(10))
        df_desord = pd.DataFrame({"data": datas[::-1], "quantidade": qtd[::-1]})
        df = criar_features_vendas(df_desord)
        # Após ordenação, lag1[1] == quantidade[0] (valor mais antigo)
        assert df["quantidade_lag1"].iloc[1] == df["quantidade"].iloc[0]


# ── Testes: criar_features_clima ──────────────────────────────────────────────

class TestCriarFeaturesClima:
    def test_adiciona_temp_lags(self, df_com_clima):
        df = criar_features_clima(df_com_clima)
        assert "temp_lag1" in df.columns
        assert "temp_lag7" in df.columns

    def test_adiciona_faixa_termica(self, df_com_clima):
        df = criar_features_clima(df_com_clima)
        assert "faixa_termica" in df.columns
        assert set(df["faixa_termica"].dropna().unique()).issubset(
            {"frio", "ameno", "quente", "muito_quente"}
        )

    def test_adiciona_chuva_flag(self, df_com_clima):
        df = criar_features_clima(df_com_clima)
        assert "chuva_flag" in df.columns
        assert set(df["chuva_flag"].unique()).issubset({0, 1})

    def test_chuva_flag_correto(self, df_com_clima):
        df = criar_features_clima(df_com_clima, limiar_chuva_mm=2.0)
        # precipitacao=5.0 quando i%3==0 → flag=1
        primeiro_com_chuva = df_com_clima[df_com_clima["precipitacao"] > 2.0].index[0]
        assert df["chuva_flag"].iloc[primeiro_com_chuva] == 1

    def test_sem_coluna_temp_nao_adiciona_lags(self, df_base):
        df = criar_features_clima(df_base, coluna_temp_max="temp_inexistente")
        assert "temp_lag1" not in df.columns
        assert "faixa_termica" not in df.columns

    def test_sem_coluna_precipitacao_sem_chuva_flag(self, df_base):
        df = criar_features_clima(df_base, coluna_precipitacao="chuva_inexistente")
        assert "chuva_flag" not in df.columns

    def test_nao_modifica_df_original(self, df_com_clima):
        colunas_antes = list(df_com_clima.columns)
        criar_features_clima(df_com_clima)
        assert list(df_com_clima.columns) == colunas_antes

    def test_temp_lag1_deslocado(self, df_com_clima):
        df = criar_features_clima(df_com_clima)
        assert df["temp_lag1"].iloc[1] == pytest.approx(df_com_clima["temp_max"].iloc[0])


# ── Testes: criar_features_economicas ────────────────────────────────────────

class TestCriarFeaturesEconomicas:
    def test_adiciona_receita(self, df_base):
        df = criar_features_economicas(df_base)
        assert "receita" in df.columns

    def test_receita_calculada_corretamente(self, df_base):
        df = criar_features_economicas(df_base)
        esperado = df_base["quantidade"] * df_base["preco"]
        pd.testing.assert_series_equal(df["receita"].reset_index(drop=True),
                                        esperado.reset_index(drop=True),
                                        check_names=False)

    def test_adiciona_ticket_medio(self, df_base):
        df = criar_features_economicas(df_base)
        assert "ticket_medio" in df.columns

    def test_ticket_medio_e_media_do_dia(self, df_base):
        df = criar_features_economicas(df_base)
        # Cada data tem uma única linha → ticket_medio == preco nesse caso
        pd.testing.assert_series_equal(
            df["ticket_medio"].reset_index(drop=True),
            df_base["preco"].reset_index(drop=True),
            check_names=False,
        )

    def test_sem_preco_nao_adiciona_receita(self, df_base):
        df_sem_preco = df_base.drop(columns=["preco"])
        df = criar_features_economicas(df_sem_preco)
        assert "receita" not in df.columns

    def test_nao_modifica_df_original(self, df_base):
        colunas_antes = list(df_base.columns)
        criar_features_economicas(df_base)
        assert list(df_base.columns) == colunas_antes


# ── Testes: criar_features (pipeline completo) ────────────────────────────────

class TestCriarFeatures:
    def test_aplica_todas_transformacoes(self, df_com_clima):
        df = criar_features(df_com_clima)
        assert "quantidade_lag1" in df.columns
        assert "temp_lag1" in df.columns
        assert "faixa_termica" in df.columns
        assert "chuva_flag" in df.columns
        assert "receita" in df.columns

    def test_nao_modifica_original(self, df_com_clima):
        colunas_antes = list(df_com_clima.columns)
        criar_features(df_com_clima)
        assert list(df_com_clima.columns) == colunas_antes

    def test_linhas_preservadas(self, df_com_clima):
        df = criar_features(df_com_clima)
        assert len(df) == len(df_com_clima)


# ── Testes: montar_dataset_completo ──────────────────────────────────────────

class TestMontarDatasetCompleto:
    def test_sem_apis_externas_retorna_df_com_features(self, df_base):
        """Sem codigo_ibge e codigo_estacao, só calendário + features."""
        df = montar_dataset_completo(df_base)
        # Calendário deve estar presente
        assert "is_feriado" in df.columns
        # Features de vendas também
        assert "quantidade_lag1" in df.columns

    def test_nao_modifica_df_original(self, df_base):
        colunas_antes = list(df_base.columns)
        montar_dataset_completo(df_base)
        assert list(df_base.columns) == colunas_antes

    def test_linhas_preservadas(self, df_base):
        df = montar_dataset_completo(df_base)
        assert len(df) == len(df_base)

    def test_com_ibge_mockado(self, df_base):
        dados_ibge = {
            "codigo_ibge": "3550308", "nome_municipio": "São Paulo",
            "uf": "SP", "populacao": 11_000_000,
            "pib_per_capita": 52_000.0, "classe_economica": "B",
        }
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_ibge):
            df = montar_dataset_completo(df_base, codigo_ibge="3550308")

        assert "ibge_populacao" in df.columns
        assert df["ibge_classe"].iloc[0] == "B"

    def test_clima_falha_graciosamente(self, df_base):
        """Se o clima falhar, pipeline continua sem travar."""
        with patch("projects.enrichment.inmet.enrich_clima", side_effect=Exception("timeout")):
            df = montar_dataset_completo(df_base, codigo_estacao="A701")

        # Sem colunas de clima, mas calendário e features OK
        assert "is_feriado" in df.columns
        assert "temp_max" not in df.columns

    def test_ibge_falha_graciosamente(self, df_base):
        """Se o IBGE falhar, pipeline continua sem travar."""
        with patch("projects.enrichment.ibge.buscar_dados_municipio", side_effect=Exception("timeout")):
            df = montar_dataset_completo(df_base, codigo_ibge="3550308")

        assert "ibge_populacao" not in df.columns
        assert "quantidade_lag1" in df.columns
