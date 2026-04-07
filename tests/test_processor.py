"""
tests/test_processor.py
-----------------------
Testes unitários para enrichment/processor.py.
APIs externas (ViaCEP, Nominatim) são mockadas — sem chamadas de rede.

Execute com:
    cd axionplatform
    pytest tests/test_processor.py -v
"""
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from projects.enrichment.processor import (
    ValidationReport,
    _limpar_cep,
    _lookup_viacep,
    process_sales_data,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_basico():
    """DataFrame mínimo válido: data + quantidade."""
    return pd.DataFrame({
        "data": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "quantidade": [100.0, 150.0, 120.0],
    })


@pytest.fixture()
def df_completo():
    """DataFrame com todas as colunas opcionais."""
    return pd.DataFrame({
        "data": pd.date_range("2024-01-01", periods=5),
        "quantidade": [100, 200, 150, 300, 250],
        "produto": ["A", "B", "A", "C", "B"],
        "preco": [10.50, 20.00, 10.50, 35.00, 20.00],
        "cep": ["01310-100", "04547-130", "01310-100", "20040-020", "04547-130"],
    })


@pytest.fixture()
def df_aliases():
    """DataFrame com nomes de colunas aliases (não canônicos)."""
    return pd.DataFrame({
        "date": ["2024-03-01", "2024-03-02"],
        "qty": [50, 75],
        "price": [9.99, 19.99],
    })


@pytest.fixture()
def df_formato_br():
    """DataFrame com formato brasileiro de data e número."""
    return pd.DataFrame({
        "data": ["01/01/2024", "15/06/2024", "25/12/2024"],
        "quantidade": ["1.000", "2.500", "3.750"],  # ponto como milhar
        "preco": ["10,50", "25,00", "99,90"],       # vírgula como decimal
    })


@pytest.fixture()
def mock_viacep_response():
    """Resposta simulada do ViaCEP para CEP 01310-100."""
    return {
        "cep": "01310-100",
        "logradouro": "Avenida Paulista",
        "bairro": "Bela Vista",
        "localidade": "São Paulo",
        "uf": "SP",
        "ibge": "3550308",
    }


# ── Testes: _limpar_cep ───────────────────────────────────────────────────────

class TestLimparCep:
    def test_cep_formatado(self):
        assert _limpar_cep("01310-100") == "01310100"

    def test_cep_apenas_numeros(self):
        assert _limpar_cep("01310100") == "01310100"

    def test_cep_com_espacos(self):
        assert _limpar_cep(" 04547 130 ") == "04547130"

    def test_cep_invalido_curto(self):
        assert _limpar_cep("1234") is None

    def test_cep_invalido_longo(self):
        assert _limpar_cep("123456789") is None

    def test_cep_vazio(self):
        assert _limpar_cep("") is None

    def test_cep_letras(self):
        assert _limpar_cep("ABC-DEF") is None


# ── Testes: process_sales_data (DataFrame) ────────────────────────────────────

class TestProcessSalesDataDataFrame:
    def test_df_basico_valido(self, df_basico):
        df_out, rel = process_sales_data(df_basico, resolver_cep=False)
        assert rel.valido is True
        assert len(rel.erros) == 0
        assert rel.linhas_validas == 3
        assert pd.api.types.is_datetime64_any_dtype(df_out["data"])
        assert pd.api.types.is_float_dtype(df_out["quantidade"])

    def test_aliases_mapeados(self, df_aliases):
        df_out, rel = process_sales_data(df_aliases, resolver_cep=False)
        assert rel.valido is True
        assert "data" in df_out.columns
        assert "quantidade" in df_out.columns
        assert "preco" in df_out.columns

    def test_formato_data_br(self, df_formato_br):
        df_out, rel = process_sales_data(df_formato_br, resolver_cep=False)
        assert rel.valido is True
        assert pd.api.types.is_datetime64_any_dtype(df_out["data"])

    def test_numero_formato_br(self, df_formato_br):
        df_out, rel = process_sales_data(df_formato_br, resolver_cep=False)
        assert rel.valido is True
        # "1.000" deve virar 1000.0 e "10,50" deve virar 10.5
        assert df_out["quantidade"].iloc[0] == pytest.approx(1000.0)
        assert df_out["preco"].iloc[0] == pytest.approx(10.50)

    def test_coluna_obrigatoria_faltando_data(self):
        df = pd.DataFrame({"quantidade": [100, 200]})
        df_out, rel = process_sales_data(df, resolver_cep=False)
        assert rel.valido is False
        assert "data" in rel.colunas_faltando

    def test_coluna_obrigatoria_faltando_quantidade(self):
        df = pd.DataFrame({"data": ["2024-01-01", "2024-01-02"]})
        df_out, rel = process_sales_data(df, resolver_cep=False)
        assert rel.valido is False
        assert "quantidade" in rel.colunas_faltando

    def test_linhas_com_quantidade_nula_removidas(self):
        df = pd.DataFrame({
            "data": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "quantidade": [100.0, None, 200.0],
        })
        df_out, rel = process_sales_data(df, resolver_cep=False)
        assert rel.linhas_validas == 2
        assert rel.linhas_rejeitadas == 1
        assert len(df_out) == 2

    def test_colunas_completamente_vazias_removidas(self):
        df = pd.DataFrame({
            "data": ["2024-01-01", "2024-01-02"],
            "quantidade": [10, 20],
            "coluna_vazia": [None, None],
        })
        df_out, rel = process_sales_data(df, resolver_cep=False)
        assert "coluna_vazia" not in df_out.columns

    def test_aviso_amostra_pequena(self):
        df = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=10),
            "quantidade": range(10),
        })
        _, rel = process_sales_data(df, resolver_cep=False)
        assert any("30" in aviso for aviso in rel.avisos)

    def test_df_vazio_sem_crash(self):
        df = pd.DataFrame({"data": [], "quantidade": []})
        df_out, rel = process_sales_data(df, resolver_cep=False)
        # Sem erros de colunas obrigatórias, mas linhas = 0
        assert rel.linhas_validas == 0

    def test_to_dict_retorna_dicionario(self, df_basico):
        _, rel = process_sales_data(df_basico, resolver_cep=False)
        d = rel.to_dict()
        assert isinstance(d, dict)
        assert "valido" in d
        assert "total_linhas" in d
        assert "erros" in d


# ── Testes: process_sales_data (arquivo CSV) ─────────────────────────────────

class TestProcessSalesDataArquivo:
    def test_arquivo_inexistente(self, tmp_path):
        caminho = tmp_path / "nao_existe.csv"
        df_out, rel = process_sales_data(caminho, resolver_cep=False)
        assert rel.valido is False
        assert any("não encontrado" in e for e in rel.erros)

    def test_csv_valido(self, tmp_path):
        csv = "data,quantidade,produto\n2024-01-01,100,A\n2024-01-02,200,B\n"
        caminho = tmp_path / "vendas.csv"
        caminho.write_text(csv, encoding="utf-8")
        df_out, rel = process_sales_data(caminho, resolver_cep=False)
        assert rel.valido is True
        assert len(df_out) == 2

    def test_csv_encoding_latin1(self, tmp_path):
        csv = "data,quantidade,produto\n2024-01-01,100,Ação\n"
        caminho = tmp_path / "vendas_latin.csv"
        caminho.write_bytes(csv.encode("latin-1"))
        df_out, rel = process_sales_data(caminho, resolver_cep=False)
        assert rel.valido is True


# ── Testes: Enriquecimento de CEP (com mock) ─────────────────────────────────

class TestEnriquecimentoCep:
    def test_resolver_cep_false_nao_chama_api(self, df_completo):
        with patch("projects.enrichment.processor._lookup_viacep") as mock_viacep:
            df_out, rel = process_sales_data(df_completo, resolver_cep=False)
            mock_viacep.assert_not_called()

    def test_cep_resolvido_adiciona_colunas(self, df_completo, mock_viacep_response):
        with (
            patch("projects.enrichment.processor._lookup_viacep") as mock_viacep,
            patch("projects.enrichment.processor._lookup_coordenadas") as mock_coords,
        ):
            mock_viacep.return_value = mock_viacep_response
            mock_coords.return_value = (-23.5614, -46.6558)

            df_out, rel = process_sales_data(df_completo, resolver_cep=True)

        assert "municipio" in df_out.columns
        assert "uf" in df_out.columns
        assert "lat" in df_out.columns
        assert "lon" in df_out.columns
        assert rel.ceps_resolvidos > 0

    def test_cep_invalido_conta_erro(self, mock_viacep_response):
        df = pd.DataFrame({
            "data": ["2024-01-01", "2024-01-02"],
            "quantidade": [100, 200],
            "cep": ["INVALIDO", "01310-100"],
        })
        with (
            patch("projects.enrichment.processor._lookup_viacep") as mock_viacep,
            patch("projects.enrichment.processor._lookup_coordenadas") as mock_coords,
        ):
            mock_viacep.return_value = mock_viacep_response
            mock_coords.return_value = (-23.56, -46.65)

            _, rel = process_sales_data(df, resolver_cep=True)

        assert rel.ceps_com_erro >= 1

    def test_viacep_falhou_continua_sem_geo(self):
        df = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=3),
            "quantidade": [100, 200, 300],
            "cep": ["01310-100", "01310-100", "01310-100"],
        })
        with (
            patch("projects.enrichment.processor._lookup_viacep") as mock_viacep,
            patch("projects.enrichment.processor._lookup_coordenadas"),
        ):
            mock_viacep.return_value = None  # Simula falha da API

            df_out, rel = process_sales_data(df, resolver_cep=True)

        # Processamento deve continuar sem crashar
        assert rel.valido is True
        assert rel.ceps_com_erro == 1  # 1 CEP único com erro
        assert df_out["municipio"].isna().all()

    def test_deduplicacao_cep_minimiza_chamadas_api(self, mock_viacep_response):
        """CEPs repetidos devem resultar em apenas 1 chamada de API (cache)."""
        df = pd.DataFrame({
            "data": pd.date_range("2024-01-01", periods=5),
            "quantidade": [100] * 5,
            "cep": ["01310100"] * 5,  # mesmo CEP 5 vezes
        })
        with (
            patch("projects.enrichment.processor._lookup_viacep") as mock_viacep,
            patch("projects.enrichment.processor._lookup_coordenadas") as mock_coords,
        ):
            mock_viacep.return_value = mock_viacep_response
            mock_coords.return_value = (-23.56, -46.65)

            # Limpa cache do lru_cache antes do teste
            _lookup_viacep.cache_clear()

            _, rel = process_sales_data(df, resolver_cep=True)

        # lru_cache garante 1 chamada para o mesmo CEP
        assert mock_viacep.call_count == 1


# ── Testes: ValidationReport ──────────────────────────────────────────────────

class TestValidationReport:
    def test_report_valido_por_padrao(self):
        rel = ValidationReport()
        assert rel.valido is True
        assert rel.erros == []
        assert rel.avisos == []

    def test_to_dict_campos_obrigatorios(self):
        rel = ValidationReport(
            total_linhas=100,
            linhas_validas=98,
            linhas_rejeitadas=2,
            valido=True,
        )
        d = rel.to_dict()
        campos_esperados = {
            "colunas_encontradas", "colunas_faltando", "total_linhas",
            "linhas_validas", "linhas_rejeitadas", "erros", "avisos",
            "ceps_resolvidos", "ceps_com_erro", "valido",
        }
        assert campos_esperados.issubset(d.keys())
