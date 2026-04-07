"""
tests/test_ibge.py
------------------
Testes unitários para enrichment/ibge.py.
Todas as chamadas HTTP são mockadas — sem dependência de rede.

Execute com:
    cd axionplatform
    pytest tests/test_ibge.py -v
"""
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from projects.enrichment.ibge import (
    _classificar_classe,
    _extrair_valor_sidra,
    buscar_dados_municipio,
    buscar_dados_multiplos_municipios,
    enrich_ibge,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

SIDRA_POP_MOCK = [
    {
        "resultados": [
            {
                "series": [
                    {
                        "localidade": {"id": "3550308", "nome": "São Paulo"},
                        "serie": {"2023": "11451999"},
                    }
                ]
            }
        ]
    }
]

SIDRA_PIB_MOCK = [
    {
        "resultados": [
            {
                "series": [
                    {
                        "localidade": {"id": "3550308", "nome": "São Paulo"},
                        "serie": {"2021": "52418.34"},
                    }
                ]
            }
        ]
    }
]

LOCALIDADE_MOCK = {
    "id": 3550308,
    "nome": "São Paulo",
    "microrregiao": {
        "mesorregiao": {
            "UF": {
                "sigla": "SP",
                "regiao": {"nome": "Sudeste"},
            }
        }
    },
}


@pytest.fixture()
def df_vendas():
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=10),
        "quantidade": range(10),
        "preco": [10.0] * 10,
    })


def _mock_sidra(url, **kwargs):
    """Retorna mock correto baseado na URL chamada."""
    mock = MagicMock()
    mock.status_code = 200
    if "6579" in url:   # tabela de população
        mock.json.return_value = SIDRA_POP_MOCK
    elif "5938" in url: # tabela de PIB
        mock.json.return_value = SIDRA_PIB_MOCK
    elif "municipios" in url:  # localidades
        mock.json.return_value = LOCALIDADE_MOCK
    else:
        mock.json.return_value = []
    return mock


# ── Testes: _extrair_valor_sidra ──────────────────────────────────────────────

class TestExtrairValorSidra:
    def test_extrai_populacao(self):
        assert _extrair_valor_sidra(SIDRA_POP_MOCK) == pytest.approx(11_451_999.0)

    def test_extrai_pib(self):
        assert _extrair_valor_sidra(SIDRA_PIB_MOCK) == pytest.approx(52_418.34)

    def test_lista_vazia_retorna_none(self):
        assert _extrair_valor_sidra([]) is None

    def test_sem_series_retorna_none(self):
        mock = [{"resultados": [{"series": []}]}]
        assert _extrair_valor_sidra(mock) is None

    def test_valor_traço_retorna_none(self):
        mock = [{"resultados": [{"series": [{"localidade": {}, "serie": {"2023": "-"}}]}]}]
        assert _extrair_valor_sidra(mock) is None

    def test_valor_nao_disponivel_retorna_none(self):
        mock = [{"resultados": [{"series": [{"localidade": {}, "serie": {"2023": "..."}}]}]}]
        assert _extrair_valor_sidra(mock) is None


# ── Testes: _classificar_classe ───────────────────────────────────────────────

class TestClassificarClasse:
    @pytest.mark.parametrize("pib,esperado", [
        (100_000, "A"),
        (80_001,  "A"),
        (80_000,  "A"),
        (79_999,  "B"),
        (45_000,  "B"),
        (40_000,  "B"),
        (39_999,  "C"),
        (25_000,  "C"),
        (20_000,  "C"),
        (19_999,  "D"),
        (12_000,  "D"),
        (10_000,  "D"),
        (9_999,   "E"),
        (0,       "E"),
    ])
    def test_faixas(self, pib, esperado):
        assert _classificar_classe(pib) == esperado


# ── Testes: buscar_dados_municipio ────────────────────────────────────────────

class TestBuscarDadosMunicipio:
    def test_retorna_dict_completo(self):
        with (
            patch("projects.enrichment.ibge.requests.get", side_effect=_mock_sidra),
            patch("projects.enrichment.ibge.cache_get", return_value=None),
            patch("projects.enrichment.ibge.cache_set"),
        ):
            dados = buscar_dados_municipio("3550308")

        assert dados["codigo_ibge"] == "3550308"
        assert dados["populacao"] == 11_451_999
        assert dados["pib_per_capita"] == pytest.approx(52_418.34)
        assert dados["classe_economica"] == "B"  # PIB R$52k → faixa B (40k–80k)
        assert dados["nome_municipio"] == "São Paulo"
        assert dados["uf"] == "SP"

    def test_normaliza_codigo_6_digitos(self):
        """Código de 6 dígitos deve virar 7 (adiciona 0 ao final)."""
        with (
            patch("projects.enrichment.ibge.requests.get", side_effect=_mock_sidra),
            patch("projects.enrichment.ibge.cache_get", return_value=None),
            patch("projects.enrichment.ibge.cache_set"),
        ):
            dados = buscar_dados_municipio("355030")
        assert dados["codigo_ibge"] == "3550300"

    def test_usa_cache_se_disponivel(self):
        dados_cache = {"codigo_ibge": "3550308", "populacao": 11_000_000}
        with (
            patch("projects.enrichment.ibge.cache_get", return_value=dados_cache),
            patch("projects.enrichment.ibge.requests.get") as mock_get,
        ):
            dados = buscar_dados_municipio("3550308")
            mock_get.assert_not_called()
        assert dados["populacao"] == 11_000_000

    def test_api_falha_retorna_dict_com_none(self):
        import requests as req
        with (
            patch("projects.enrichment.ibge.requests.get") as mock_get,
            patch("projects.enrichment.ibge.cache_get", return_value=None),
            patch("projects.enrichment.ibge.cache_set"),
        ):
            mock_get.side_effect = req.RequestException("timeout")
            dados = buscar_dados_municipio("3550308")

        # Não deve lançar exceção
        assert isinstance(dados, dict)
        assert dados["populacao"] is None
        assert dados["pib_per_capita"] is None

    def test_campos_obrigatorios_presentes(self):
        with (
            patch("projects.enrichment.ibge.requests.get", side_effect=_mock_sidra),
            patch("projects.enrichment.ibge.cache_get", return_value=None),
            patch("projects.enrichment.ibge.cache_set"),
        ):
            dados = buscar_dados_municipio("3550308")

        campos = {"codigo_ibge", "nome_municipio", "uf", "regiao",
                  "populacao", "pib_per_capita", "classe_economica",
                  "fonte_populacao", "fonte_pib"}
        assert campos.issubset(dados.keys())


# ── Testes: enrich_ibge ───────────────────────────────────────────────────────

class TestEnrichIbge:
    def test_adiciona_colunas_ibge(self, df_vendas):
        dados_mock = {
            "codigo_ibge": "3550308",
            "nome_municipio": "São Paulo",
            "uf": "SP",
            "populacao": 11_451_999,
            "pib_per_capita": 52_418.34,
            "classe_economica": "A",
        }
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock):
            df_out = enrich_ibge(df_vendas, "3550308")

        assert "ibge_populacao" in df_out.columns
        assert "ibge_pib_per_capita" in df_out.columns
        assert "ibge_classe" in df_out.columns
        assert "ibge_municipio" in df_out.columns
        assert "ibge_uf" in df_out.columns

    def test_valores_iguais_em_todas_linhas(self, df_vendas):
        """Dados IBGE são constantes por empresa/loja — todas as linhas iguais."""
        dados_mock = {
            "codigo_ibge": "3550308", "nome_municipio": "São Paulo",
            "uf": "SP", "populacao": 11_451_999,
            "pib_per_capita": 52_418.34, "classe_economica": "A",
        }
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock):
            df_out = enrich_ibge(df_vendas, "3550308")

        assert df_out["ibge_populacao"].nunique() == 1
        assert df_out["ibge_classe"].iloc[0] == "A"
        assert df_out["ibge_municipio"].iloc[0] == "São Paulo"

    def test_codigo_vazio_retorna_df_original(self, df_vendas):
        df_out = enrich_ibge(df_vendas, "")
        assert "ibge_populacao" not in df_out.columns
        assert len(df_out) == len(df_vendas)

    def test_nao_modifica_df_original(self, df_vendas):
        colunas_antes = list(df_vendas.columns)
        dados_mock = {"codigo_ibge": "3550308", "nome_municipio": "SP",
                      "uf": "SP", "populacao": 11_000_000,
                      "pib_per_capita": 50_000.0, "classe_economica": "A"}
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock):
            enrich_ibge(df_vendas, "3550308")
        assert list(df_vendas.columns) == colunas_antes

    def test_linhas_preservadas(self, df_vendas):
        dados_mock = {"codigo_ibge": "3550308", "nome_municipio": "SP",
                      "uf": "SP", "populacao": None, "pib_per_capita": None, "classe_economica": None}
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock):
            df_out = enrich_ibge(df_vendas, "3550308")
        assert len(df_out) == len(df_vendas)


# ── Testes: buscar_dados_multiplos_municipios ─────────────────────────────────

class TestBuscarDadosMultiplosMunicipios:
    def test_retorna_dict_por_codigo(self):
        dados_mock = {"codigo_ibge": "3550308", "populacao": 11_000_000}
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock):
            resultado = buscar_dados_multiplos_municipios(["3550308", "3304557"])
        assert "3550308" in resultado
        assert "3304557" in resultado

    def test_deduplicacao_de_codigos(self):
        """Código repetido deve resultar em apenas uma chamada de API."""
        dados_mock = {"codigo_ibge": "3550308", "populacao": 11_000_000}
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock) as mock_fn:
            buscar_dados_multiplos_municipios(["3550308", "3550308", "3550308"])
        assert mock_fn.call_count == 1

    def test_lista_vazia_retorna_dict_vazio(self):
        resultado = buscar_dados_multiplos_municipios([])
        assert resultado == {}

    def test_ignora_codigos_nulos(self):
        dados_mock = {"codigo_ibge": "3550308", "populacao": 11_000_000}
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_mock) as mock_fn:
            resultado = buscar_dados_multiplos_municipios([None, "", "3550308"])
        assert None not in resultado
        assert "" not in resultado
        assert mock_fn.call_count == 1
