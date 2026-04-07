"""
tests/test_inmet.py
-------------------
Testes unitários para enrichment/inmet.py.
Todas as chamadas HTTP são mockadas — sem dependência de rede.

Execute com:
    cd axionplatform
    pytest tests/test_inmet.py -v
"""
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from projects.enrichment.inmet import (
    _campo,
    _haversine,
    _agregar_para_diario,
    buscar_estacao_mais_proxima,
    buscar_dados_climaticos,
    enrich_clima,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

ESTACOES_MOCK = [
    {
        "CD_ESTACAO": "A701",
        "DC_NOME": "SAO PAULO - MIRANTE",
        "SG_ESTADO": "SP",
        "VL_LATITUDE": "-23.4965",
        "VL_LONGITUDE": "-46.6191",
        "CD_SITUACAO": "Operante",
    },
    {
        "CD_ESTACAO": "A711",
        "DC_NOME": "CAMPINAS",
        "SG_ESTADO": "SP",
        "VL_LATITUDE": "-22.8956",
        "VL_LONGITUDE": "-47.0586",
        "CD_SITUACAO": "Operante",
    },
    {
        "CD_ESTACAO": "A820",
        "DC_NOME": "RIO DE JANEIRO",
        "SG_ESTADO": "RJ",
        "VL_LATITUDE": "-22.9128",
        "VL_LONGITUDE": "-43.1758",
        "CD_SITUACAO": "Operante",
    },
]

REGISTROS_MOCK = [
    {
        "DT_MEDICAO": "2024-01-01",
        "HR_MEDICAO": "0000",
        "TEM_MAX": "31.2",
        "TEM_MIN": "18.4",
        "TEM_INS": "22.0",
        "CHUVA": "0.0",
        "UMD_INS": "65.0",
        "VEN_RAJ": "4.5",
    },
    {
        "DT_MEDICAO": "2024-01-01",
        "HR_MEDICAO": "1200",
        "TEM_MAX": "33.8",  # máximo do dia → deve prevalecer
        "TEM_MIN": "17.0",  # mínimo do dia → deve prevalecer
        "TEM_INS": "30.1",
        "CHUVA": "2.4",     # chuva acumulada
        "UMD_INS": "72.0",
        "VEN_RAJ": "8.2",   # rajada máxima
    },
    {
        "DT_MEDICAO": "2024-01-02",
        "HR_MEDICAO": "0000",
        "TEM_MAX": "28.0",
        "TEM_MIN": "16.0",
        "TEM_INS": "20.0",
        "CHUVA": "15.6",
        "UMD_INS": "85.0",
        "VEN_RAJ": "6.0",
    },
]


@pytest.fixture()
def df_vendas_30dias():
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=30),
        "quantidade": range(30),
    })


# ── Testes: _haversine ────────────────────────────────────────────────────────

class TestHaversine:
    def test_mesma_coordenada_e_zero(self):
        assert _haversine(-23.5, -46.6, -23.5, -46.6) == pytest.approx(0.0, abs=0.01)

    def test_sp_campinas_aprox_85km(self):
        """São Paulo → Campinas é ~84 km em linha reta."""
        dist = _haversine(-23.55, -46.63, -22.90, -47.06)
        assert 75 < dist < 100

    def test_sp_rio_aprox_360km(self):
        dist = _haversine(-23.55, -46.63, -22.91, -43.18)
        assert 340 < dist < 380

    def test_resultado_positivo(self):
        assert _haversine(-10, -50, -20, -60) > 0


# ── Testes: _campo ────────────────────────────────────────────────────────────

class TestCampo:
    def test_campo_normal(self):
        assert _campo({"TEM_MAX": "31.5"}, ("TEM_MAX",)) == pytest.approx(31.5)

    def test_fallback_segundo_nome(self):
        assert _campo({"TEMP_MAX": "28.0"}, ("TEM_MAX", "TEMP_MAX")) == pytest.approx(28.0)

    def test_valor_nulo_retorna_none(self):
        assert _campo({"TEM_MAX": None}, ("TEM_MAX",)) is None

    def test_valor_vazio_retorna_none(self):
        assert _campo({"TEM_MAX": ""}, ("TEM_MAX",)) is None

    def test_valor_sentinel_retorna_none(self):
        assert _campo({"TEM_MAX": "-9999"}, ("TEM_MAX",)) is None

    def test_campo_ausente_retorna_none(self):
        assert _campo({}, ("TEM_MAX",)) is None

    def test_valor_invalido_retorna_none(self):
        assert _campo({"TEM_MAX": "N/A"}, ("TEM_MAX",)) is None


# ── Testes: _agregar_para_diario ──────────────────────────────────────────────

class TestAgregarParaDiario:
    def test_retorna_dataframe(self):
        df = _agregar_para_diario(REGISTROS_MOCK)
        assert isinstance(df, pd.DataFrame)

    def test_uma_linha_por_dia(self):
        df = _agregar_para_diario(REGISTROS_MOCK)
        assert len(df) == 2  # 2024-01-01 e 2024-01-02

    def test_temp_max_e_maximo_do_dia(self):
        """Para 2024-01-01, TEM_MAX máximo entre leituras é 33.8."""
        df = _agregar_para_diario(REGISTROS_MOCK)
        linha_01 = df[df["data"] == pd.Timestamp("2024-01-01")]
        assert linha_01["temp_max"].iloc[0] == pytest.approx(33.8)

    def test_temp_min_e_minimo_do_dia(self):
        df = _agregar_para_diario(REGISTROS_MOCK)
        linha_01 = df[df["data"] == pd.Timestamp("2024-01-01")]
        assert linha_01["temp_min"].iloc[0] == pytest.approx(17.0)

    def test_precipitacao_e_soma(self):
        """Chuva do dia 01: 0.0 + 2.4 = 2.4"""
        df = _agregar_para_diario(REGISTROS_MOCK)
        linha_01 = df[df["data"] == pd.Timestamp("2024-01-01")]
        assert linha_01["precipitacao"].iloc[0] == pytest.approx(2.4)

    def test_vento_max_e_rajada_maxima(self):
        df = _agregar_para_diario(REGISTROS_MOCK)
        linha_01 = df[df["data"] == pd.Timestamp("2024-01-01")]
        assert linha_01["vento_max"].iloc[0] == pytest.approx(8.2)

    def test_registros_vazios_retorna_df_vazio(self):
        df = _agregar_para_diario([])
        assert df.empty

    def test_sem_data_retorna_df_vazio(self):
        df = _agregar_para_diario([{"TEM_MAX": "30"}])
        assert df.empty

    def test_todos_campos_presentes(self):
        df = _agregar_para_diario(REGISTROS_MOCK)
        for col in ("temp_max", "temp_min", "temp_media", "precipitacao", "umidade_media", "vento_max"):
            assert col in df.columns


# ── Testes: buscar_estacao_mais_proxima ───────────────────────────────────────

class TestBuscarEstacaoMaisProxima:
    def test_encontra_estacao_proxima(self):
        with patch("projects.enrichment.inmet.listar_estacoes") as mock_list:
            mock_list.return_value = ESTACOES_MOCK
            # Coordenada próxima de São Paulo
            estacao = buscar_estacao_mais_proxima(-23.55, -46.63)
        assert estacao is not None
        assert estacao["CD_ESTACAO"] == "A701"

    def test_retorna_distancia_km(self):
        with patch("projects.enrichment.inmet.listar_estacoes") as mock_list:
            mock_list.return_value = ESTACOES_MOCK
            estacao = buscar_estacao_mais_proxima(-23.55, -46.63)
        assert "distancia_km" in estacao
        assert estacao["distancia_km"] < 20  # SP → A701 < 20 km

    def test_sem_estacoes_retorna_none(self):
        with patch("projects.enrichment.inmet.listar_estacoes") as mock_list:
            mock_list.return_value = []
            assert buscar_estacao_mais_proxima(-23.55, -46.63) is None

    def test_raio_max_excedido_retorna_none(self):
        with patch("projects.enrichment.inmet.listar_estacoes") as mock_list:
            mock_list.return_value = ESTACOES_MOCK
            # Ponto no meio do oceano, longe de tudo
            assert buscar_estacao_mais_proxima(-35.0, -20.0, raio_max_km=10) is None

    def test_ignora_estacoes_desativadas(self):
        estacoes_com_inativa = [
            {**ESTACOES_MOCK[0], "CD_SITUACAO": "Desativada"},   # SP desativada
            ESTACOES_MOCK[1],  # Campinas operante
        ]
        with patch("projects.enrichment.inmet.listar_estacoes") as mock_list:
            mock_list.return_value = estacoes_com_inativa
            # Ponto próximo de SP, mas SP está desativada → deve retornar Campinas
            estacao = buscar_estacao_mais_proxima(-23.55, -46.63)
        # Campinas ainda pode estar dentro do raio padrão de 150 km
        if estacao:
            assert estacao["CD_ESTACAO"] == "A711"


# ── Testes: buscar_dados_climaticos ───────────────────────────────────────────

class TestBuscarDadosClinmaticos:
    def test_retorna_dataframe_com_dados(self):
        resp_mock = MagicMock()
        resp_mock.status_code = 200
        resp_mock.json.return_value = REGISTROS_MOCK

        with (
            patch("projects.enrichment.inmet.requests.get") as mock_get,
            patch("projects.enrichment.inmet.cache_get", return_value=None),
            patch("projects.enrichment.inmet.cache_set"),
        ):
            mock_get.return_value = resp_mock
            df = buscar_dados_climaticos("A701", date(2024, 1, 1), date(2024, 1, 2))

        assert not df.empty
        assert "temp_max" in df.columns

    def test_204_retorna_df_vazio(self):
        resp_mock = MagicMock()
        resp_mock.status_code = 204

        with (
            patch("projects.enrichment.inmet.requests.get") as mock_get,
            patch("projects.enrichment.inmet.cache_get", return_value=None),
        ):
            mock_get.return_value = resp_mock
            df = buscar_dados_climaticos("A701", date(2024, 1, 1), date(2024, 1, 2))

        assert df.empty

    def test_erro_de_rede_retorna_df_vazio(self):
        import requests as req

        with (
            patch("projects.enrichment.inmet.requests.get") as mock_get,
            patch("projects.enrichment.inmet.cache_get", return_value=None),
        ):
            mock_get.side_effect = req.RequestException("timeout")
            df = buscar_dados_climaticos("A701", date(2024, 1, 1), date(2024, 1, 2))

        assert df.empty

    def test_usa_cache_se_disponivel(self):
        dados_cache = [{"data": "2024-01-01", "temp_max": 30.0, "temp_min": 18.0,
                        "temp_media": 24.0, "precipitacao": 0.0, "umidade_media": 70.0, "vento_max": 5.0}]
        with (
            patch("projects.enrichment.inmet.cache_get", return_value=dados_cache),
            patch("projects.enrichment.inmet.requests.get") as mock_get,
        ):
            df = buscar_dados_climaticos("A701", date(2024, 1, 1), date(2024, 1, 1))
            mock_get.assert_not_called()  # não deve chamar a API

        assert not df.empty


# ── Testes: enrich_clima ──────────────────────────────────────────────────────

class TestEnrichClima:
    def test_adiciona_colunas_clima(self, df_vendas_30dias):
        df_clima = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=30),
            "temp_max": [30.0] * 30, "temp_min": [18.0] * 30,
            "temp_media": [24.0] * 30, "precipitacao": [0.0] * 30,
            "umidade_media": [65.0] * 30, "vento_max": [5.0] * 30,
        })
        with (
            patch("projects.enrichment.inmet.buscar_dados_climaticos") as mock_clima,
            patch("projects.enrichment.inmet.cache_get", return_value=None),
        ):
            mock_clima.return_value = df_clima
            df_out = enrich_clima(df_vendas_30dias, "A701", min_dias_cobertura=5)

        assert "temp_max" in df_out.columns
        assert "precipitacao" in df_out.columns
        assert len(df_out) == 30

    def test_sem_cobertura_retorna_df_original(self, df_vendas_30dias):
        """Se dados climáticos cobrem < min_dias, retorna df sem colunas de clima."""
        df_clima_vazio = pd.DataFrame()

        with patch("projects.enrichment.inmet.buscar_dados_climaticos") as mock_clima:
            mock_clima.return_value = df_clima_vazio
            df_out = enrich_clima(df_vendas_30dias, "A701")

        assert "temp_max" not in df_out.columns
        assert len(df_out) == len(df_vendas_30dias)

    def test_coluna_data_inexistente(self, df_vendas_30dias):
        """Retorna df original sem crash se coluna de data não existe."""
        df_out = enrich_clima(df_vendas_30dias, "A701", coluna_data="dt_inexistente")
        assert list(df_out.columns) == list(df_vendas_30dias.columns)

    def test_nao_modifica_df_original(self, df_vendas_30dias):
        colunas_antes = list(df_vendas_30dias.columns)
        with patch("projects.enrichment.inmet.buscar_dados_climaticos") as m:
            m.return_value = pd.DataFrame()
            enrich_clima(df_vendas_30dias, "A701")
        assert list(df_vendas_30dias.columns) == colunas_antes
