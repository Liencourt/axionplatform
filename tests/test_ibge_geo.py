"""
tests/test_ibge_geo.py
----------------------
Testes unitários para enrichment/ibge_geo.py.
APIs externas (IBGE Malha, IBGE SIDRA) e shapely são mockadas — sem chamadas de rede.

Execute com:
    cd axionplatform
    pytest tests/test_ibge_geo.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

# ── Importações condicionais ────────────────────────────────────────────────

try:
    from projects.enrichment.ibge_geo import (
        buscar_malha_municipio,
        buscar_dados_setor,
        buscar_setor_por_coordenadas,
        encontrar_setor,
        _SHAPELY_DISPONIVEL,
    )
    _MODULO_DISPONIVEL = True
except ImportError:
    _MODULO_DISPONIVEL = False

pytestmark = pytest.mark.skipif(
    not _MODULO_DISPONIVEL,
    reason="ibge_geo não disponível (shapely ausente?)",
)

# ── GeoJSON de teste ────────────────────────────────────────────────────────

def _geojson_simples(codarea: str = "330455705280001") -> dict:
    """GeoJSON mínimo com um polígono quadrado no Rio de Janeiro."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-43.40, -22.90],
                        [-43.30, -22.90],
                        [-43.30, -22.80],
                        [-43.40, -22.80],
                        [-43.40, -22.90],
                    ]],
                },
                "properties": {"codarea": codarea},
            }
        ],
    }


# ── TestEncontrarSetor ──────────────────────────────────────────────────────

class TestEncontrarSetor:
    def test_ponto_dentro_retorna_codarea(self):
        geojson = _geojson_simples("330455705280001")
        # Ponto no centro do quadrado
        resultado = encontrar_setor(-22.85, -43.35, geojson)
        assert resultado == "330455705280001"

    def test_ponto_fora_retorna_none(self):
        geojson = _geojson_simples()
        # Ponto claramente fora do quadrado (São Paulo)
        resultado = encontrar_setor(-23.55, -46.63, geojson)
        assert resultado is None

    def test_geojson_vazio_retorna_none(self):
        resultado = encontrar_setor(-22.85, -43.35, {"type": "FeatureCollection", "features": []})
        assert resultado is None

    def test_geojson_sem_features_retorna_none(self):
        resultado = encontrar_setor(-22.85, -43.35, {})
        assert resultado is None

    def test_ordem_lon_lat_correta(self):
        """GeoJSON usa [longitude, latitude] — Point(lon, lat) deve funcionar."""
        geojson = _geojson_simples("CORRETO")
        # lat=-22.85, lon=-43.35 está DENTRO; invertido (lat=-43.35, lon=-22.85) está FORA
        assert encontrar_setor(-22.85, -43.35, geojson) == "CORRETO"
        # Invertido (lon, lat trocados) não deve encontrar o setor
        assert encontrar_setor(-43.35, -22.85, geojson) is None

    def test_geometria_invalida_ignorada_graciosamente(self):
        """Feature com geometria corrompida não deve lançar exceção."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "InvalidType", "coordinates": []},
                    "properties": {"codarea": "999"},
                },
                # Feature válida logo depois
                _geojson_simples("VALIDO")["features"][0],
            ],
        }
        # Deve ignorar a inválida e encontrar a válida
        resultado = encontrar_setor(-22.85, -43.35, geojson)
        assert resultado == "VALIDO"

    def test_multiplas_features_retorna_primeira_que_contem(self):
        """Com múltiplos polígonos, retorna o primeiro que contém o ponto."""
        outro_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-44.00, -23.00], [-43.90, -23.00],
                    [-43.90, -22.90], [-44.00, -22.90], [-44.00, -23.00],
                ]],
            },
            "properties": {"codarea": "LONGE"},
        }
        geojson = {
            "type": "FeatureCollection",
            "features": [outro_feature, _geojson_simples("PERTO")["features"][0]],
        }
        # Ponto dentro de "PERTO" mas fora de "LONGE"
        resultado = encontrar_setor(-22.85, -43.35, geojson)
        assert resultado == "PERTO"


# ── TestBuscarMalhaMunicipio ────────────────────────────────────────────────

class TestBuscarMalhaMunicipio:
    def test_retorna_geojson_do_cache(self):
        geojson_mock = _geojson_simples()
        with patch("projects.enrichment.ibge_geo.cache_get", return_value=geojson_mock) as mock_cache, \
             patch("projects.enrichment.ibge_geo.requests.get") as mock_get:
            resultado = buscar_malha_municipio("3304557")
        assert resultado == geojson_mock
        mock_get.assert_not_called()

    def test_faz_get_e_cacheia_quando_cache_miss(self):
        geojson_mock = _geojson_simples()
        mock_resp = MagicMock()
        mock_resp.json.return_value = geojson_mock
        mock_resp.raise_for_status.return_value = None

        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.cache_set") as mock_cache_set, \
             patch("projects.enrichment.ibge_geo.requests.get", return_value=mock_resp):
            resultado = buscar_malha_municipio("3304557")

        assert resultado == geojson_mock
        mock_cache_set.assert_called_once()

    def test_api_falha_retorna_none(self):
        import requests
        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.requests.get",
                   side_effect=requests.RequestException("timeout")):
            resultado = buscar_malha_municipio("3304557")
        assert resultado is None

    def test_url_contem_codigo_ibge_e_resolucao(self):
        """Verifica que a URL montada usa resolucao=5 (polígonos simplificados)."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"type": "FeatureCollection", "features": []}
        mock_resp.raise_for_status.return_value = None

        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.cache_set"), \
             patch("projects.enrichment.ibge_geo.requests.get", return_value=mock_resp) as mock_get:
            buscar_malha_municipio("3304557")

        url_chamada = mock_get.call_args[0][0]
        assert "3304557" in url_chamada
        assert "resolucao=5" in url_chamada


# ── TestBuscarDadosSetor ────────────────────────────────────────────────────

class TestBuscarDadosSetor:
    def _mock_sidra_resp(self, valor: str):
        return [{
            "resultados": [{
                "series": [{
                    "serie": {"2022": valor}
                }]
            }]
        }]

    def test_retorna_dict_com_renda_e_classe(self):
        mock_resp = MagicMock()
        # Renda mensal de R$ 1.500 → anual R$ 18.000 → classe C (≥ R$ 10k, < R$ 20k)
        mock_resp.json.return_value = self._mock_sidra_resp("1500")
        mock_resp.raise_for_status.return_value = None

        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.cache_set"), \
             patch("projects.enrichment.ibge_geo.requests.get", return_value=mock_resp):
            resultado = buscar_dados_setor("330455705280001")

        assert resultado["renda_per_capita"] == 1500.0
        assert resultado["classe_economica"] == "C"
        assert resultado["codigo_setor"] == "330455705280001"

    def test_dados_suprimidos_retorna_nones(self):
        """IBGE suprime dados de setores com poucos domicílios — deve retornar None graciosamente."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = self._mock_sidra_resp("-")
        mock_resp.raise_for_status.return_value = None

        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.cache_set"), \
             patch("projects.enrichment.ibge_geo.requests.get", return_value=mock_resp):
            resultado = buscar_dados_setor("999999999999999")

        assert resultado["renda_per_capita"] is None
        assert resultado["classe_economica"] is None

    def test_usa_cache_se_disponivel(self):
        cached = {
            "codigo_setor": "330455705280001",
            "renda_per_capita": 2000.0,
            "classe_economica": "C",
            "fonte": "IBGE Censo 2022",
        }
        with patch("projects.enrichment.ibge_geo.cache_get", return_value=cached), \
             patch("projects.enrichment.ibge_geo.requests.get") as mock_get:
            resultado = buscar_dados_setor("330455705280001")

        assert resultado == cached
        mock_get.assert_not_called()

    def test_api_falha_retorna_nones(self):
        import requests
        with patch("projects.enrichment.ibge_geo.cache_get", return_value=None), \
             patch("projects.enrichment.ibge_geo.cache_set"), \
             patch("projects.enrichment.ibge_geo.requests.get",
                   side_effect=requests.RequestException("timeout")):
            resultado = buscar_dados_setor("330455705280001")

        assert resultado["renda_per_capita"] is None
        assert resultado["classe_economica"] is None


# ── TestBuscarSetorPorCoordenadas ───────────────────────────────────────────

class TestBuscarSetorPorCoordenadas:
    def test_pipeline_completo_setor_encontrado(self):
        """Malha carregada + ponto dentro + dados de renda → nivel_geo='setor'."""
        geojson = _geojson_simples("330455705280001")
        dados_setor = {
            "codigo_setor": "330455705280001",
            "renda_per_capita": 3000.0,
            "classe_economica": "B",
            "fonte": "IBGE Censo 2022",
        }

        with patch("projects.enrichment.ibge_geo.buscar_malha_municipio", return_value=geojson), \
             patch("projects.enrichment.ibge_geo.buscar_dados_setor", return_value=dados_setor):
            resultado = buscar_setor_por_coordenadas(-22.85, -43.35, "3304557")

        assert resultado["nivel_geo"] == "setor"
        assert resultado["codigo_setor"] == "330455705280001"
        assert resultado["classe_economica"] == "B"
        assert resultado["renda_per_capita"] == 3000.0

    def test_fallback_se_ponto_nao_encontrado_em_nenhum_setor(self):
        """Ponto fora de todos os polígonos → nivel_geo='nenhum'."""
        geojson = _geojson_simples()

        with patch("projects.enrichment.ibge_geo.buscar_malha_municipio", return_value=geojson):
            # Ponto em São Paulo, longe do polígono do Rio
            resultado = buscar_setor_por_coordenadas(-23.55, -46.63, "3304557")

        assert resultado["nivel_geo"] == "nenhum"
        assert resultado["codigo_setor"] is None
        assert resultado["classe_economica"] is None

    def test_fallback_se_malha_falha(self):
        """buscar_malha_municipio retorna None → nivel_geo='nenhum', sem exceção."""
        with patch("projects.enrichment.ibge_geo.buscar_malha_municipio", return_value=None):
            resultado = buscar_setor_por_coordenadas(-22.85, -43.35, "3304557")

        assert resultado["nivel_geo"] == "nenhum"

    def test_fallback_se_setor_sem_dados_renda(self):
        """Setor encontrado mas sem renda (dados suprimidos) → nivel_geo ainda é 'setor'."""
        geojson = _geojson_simples("330455705280001")
        dados_setor = {
            "codigo_setor": "330455705280001",
            "renda_per_capita": None,
            "classe_economica": None,
            "fonte": "IBGE Censo 2022",
        }

        with patch("projects.enrichment.ibge_geo.buscar_malha_municipio", return_value=geojson), \
             patch("projects.enrichment.ibge_geo.buscar_dados_setor", return_value=dados_setor):
            resultado = buscar_setor_por_coordenadas(-22.85, -43.35, "3304557")

        # Setor foi encontrado geograficamente, mas sem dados de renda
        assert resultado["nivel_geo"] == "setor"
        assert resultado["codigo_setor"] == "330455705280001"
        assert resultado["classe_economica"] is None

    def test_retorna_fallback_se_coordenadas_none(self):
        resultado = buscar_setor_por_coordenadas(None, None, "3304557")
        assert resultado["nivel_geo"] == "nenhum"

    def test_retorna_fallback_se_codigo_ibge_vazio(self):
        resultado = buscar_setor_por_coordenadas(-22.85, -43.35, "")
        assert resultado["nivel_geo"] == "nenhum"

    def test_sem_shapely_retorna_fallback(self):
        """Se shapely não estiver instalado, retorna nenhum sem exceção."""
        with patch("projects.enrichment.ibge_geo._SHAPELY_DISPONIVEL", False):
            resultado = buscar_setor_por_coordenadas(-22.85, -43.35, "3304557")
        assert resultado["nivel_geo"] == "nenhum"


# ── TestEnrichIbgeComSetor (integração com ibge.py) ────────────────────────

class TestEnrichIbgeComSetor:
    """Testa que enrich_ibge() usa dados de setor quando lat/lon fornecidos."""

    @pytest.fixture()
    def df_vendas(self):
        import pandas as pd
        return pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=5),
            "quantidade": [100.0, 120.0, 90.0, 150.0, 110.0],
        })

    def test_adiciona_colunas_setor_quando_lat_lon_fornecidos(self, df_vendas):
        from projects.enrichment.ibge import enrich_ibge

        dados_municipio = {
            "codigo_ibge": "3304557",
            "nome_municipio": "Rio de Janeiro",
            "uf": "RJ",
            "regiao": "Sudeste",
            "populacao": 6_748_000,
            "pib_per_capita": 45_000.0,
            "classe_economica": "B",
        }
        dados_setor = {
            "nivel_geo": "setor",
            "codigo_setor": "330455705280001",
            "renda_per_capita": 800.0,
            "classe_economica": "D",  # Zona Oeste → classe menor
        }

        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_municipio), \
             patch("projects.enrichment.ibge_geo.buscar_setor_por_coordenadas", return_value=dados_setor), \
             patch("projects.enrichment.ibge_geo.buscar_malha_municipio"), \
             patch("projects.enrichment.ibge_geo.buscar_dados_setor"):
            resultado = enrich_ibge(df_vendas, "3304557", lat=-22.85, lon=-43.35)

        assert "ibge_setor_codigo" in resultado.columns
        assert resultado["ibge_setor_codigo"].iloc[0] == "330455705280001"
        assert resultado["ibge_nivel_geo"].iloc[0] == "setor"
        # Classificação deve usar o setor (D), não o município (B)
        assert resultado["ibge_classe"].iloc[0] == "D"

    def test_retrocompatibilidade_sem_lat_lon(self, df_vendas):
        """Chamada sem lat/lon deve funcionar exatamente como antes (sem colunas de setor)."""
        from projects.enrichment.ibge import enrich_ibge

        dados_municipio = {
            "codigo_ibge": "3550308",
            "nome_municipio": "São Paulo",
            "uf": "SP",
            "populacao": 12_325_000,
            "pib_per_capita": 85_000.0,
            "classe_economica": "A",
        }

        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_municipio):
            resultado = enrich_ibge(df_vendas, "3550308")

        assert "ibge_classe" in resultado.columns
        assert resultado["ibge_classe"].iloc[0] == "A"
        assert resultado["ibge_nivel_geo"].iloc[0] == "municipio"
        # Colunas de setor devem existir mas com None
        assert resultado["ibge_setor_codigo"].iloc[0] is None

    def test_adiciona_coluna_bairro_quando_fornecido(self, df_vendas):
        from projects.enrichment.ibge import enrich_ibge

        dados_municipio = {
            "nome_municipio": "Rio de Janeiro", "uf": "RJ",
            "populacao": None, "pib_per_capita": None, "classe_economica": None,
        }
        with patch("projects.enrichment.ibge.buscar_dados_municipio", return_value=dados_municipio):
            resultado = enrich_ibge(df_vendas, "3304557", bairro="Curicica")

        assert "ibge_bairro" in resultado.columns
        assert resultado["ibge_bairro"].iloc[0] == "Curicica"

    def test_sem_codigo_ibge_retorna_df_inalterado(self, df_vendas):
        from projects.enrichment.ibge import enrich_ibge
        resultado = enrich_ibge(df_vendas, "")
        assert "ibge_classe" not in resultado.columns
        assert len(resultado) == len(df_vendas)
