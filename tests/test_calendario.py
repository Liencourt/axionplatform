"""
tests/test_calendario.py
------------------------
Testes unitários para enrichment/calendario.py.
Sem dependências externas — todos os cálculos são determinísticos.

Execute com:
    cd axionplatform
    pytest tests/test_calendario.py -v
"""
import pytest
import pandas as pd

from projects.enrichment.calendario import (
    enrich_calendario,
    _feriados_ano,
    _black_friday,
    _segundo_domingo,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def df_ano_2024():
    """DataFrame cobrindo datas relevantes de 2024."""
    datas = [
        "2024-01-01",   # Ano Novo (feriado)
        "2024-03-29",   # Sexta-Feira Santa (feriado móvel)
        "2024-04-21",   # Tiradentes (feriado)
        "2024-05-12",   # Dia das Mães (2º domingo de maio)
        "2024-06-12",   # Dia dos Namorados
        "2024-08-11",   # Dia dos Pais (2º domingo de agosto)
        "2024-11-29",   # Black Friday 2024
        "2024-12-25",   # Natal (feriado)
        "2024-12-31",   # Réveillon
        "2024-01-06",   # Sábado comum
        "2024-01-07",   # Domingo comum
        "2024-01-08",   # Segunda-feira comum
    ]
    return pd.DataFrame({
        "data": pd.to_datetime(datas),
        "quantidade": [100.0] * len(datas),
    })


@pytest.fixture()
def df_multiplos_anos():
    """DataFrame cobrindo 2023-2025 para testar feriados multi-ano."""
    return pd.DataFrame({
        "data": pd.date_range("2023-01-01", "2025-12-31", freq="ME"),
        "quantidade": range(36),
    })


# ── Testes: Estrutura das colunas ────────────────────────────────────────────

class TestEstruturaColunas:
    def test_colunas_adicionadas(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        colunas_esperadas = {
            "dia_semana", "is_fim_semana", "is_feriado",
            "is_vespera_feriado", "mes", "dia_mes", "trimestre",
            "semana_ano", "dias_ate_natal", "dias_ate_black_friday",
            "evento_especial",
        }
        assert colunas_esperadas.issubset(df_out.columns)

    def test_nao_modifica_colunas_originais(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        assert "data" in df_out.columns
        assert "quantidade" in df_out.columns

    def test_nao_modifica_df_original(self, df_ano_2024):
        colunas_antes = set(df_ano_2024.columns)
        _ = enrich_calendario(df_ano_2024)
        assert set(df_ano_2024.columns) == colunas_antes


# ── Testes: dia_semana e fim de semana ───────────────────────────────────────

class TestDiaSemana:
    def test_segunda_feira(self):
        df = pd.DataFrame({"data": ["2024-01-08"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dia_semana"].iloc[0] == 0  # 0 = Segunda

    def test_sabado_e_fim_de_semana(self):
        df = pd.DataFrame({"data": ["2024-01-06"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dia_semana"].iloc[0] == 5  # 5 = Sábado
        assert df_out["is_fim_semana"].iloc[0] == True

    def test_domingo_e_fim_de_semana(self):
        df = pd.DataFrame({"data": ["2024-01-07"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dia_semana"].iloc[0] == 6  # 6 = Domingo
        assert df_out["is_fim_semana"].iloc[0] == True

    def test_terca_feira_nao_fim_semana(self):
        df = pd.DataFrame({"data": ["2024-01-09"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["is_fim_semana"].iloc[0] == False


# ── Testes: Feriados ─────────────────────────────────────────────────────────

class TestFeriados:
    def test_ano_novo_e_feriado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx_ano_novo = df_ano_2024["data"].dt.strftime("%m-%d") == "01-01"
        assert df_out.loc[idx_ano_novo, "is_feriado"].iloc[0] == True

    def test_natal_e_feriado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx_natal = df_ano_2024["data"].dt.strftime("%m-%d") == "12-25"
        assert df_out.loc[idx_natal, "is_feriado"].iloc[0] == True

    def test_tiradentes_e_feriado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%m-%d") == "04-21"
        assert df_out.loc[idx, "is_feriado"].iloc[0] == True

    def test_sexta_santa_2024(self, df_ano_2024):
        """Sexta-Feira Santa 2024 = 29 de março."""
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-03-29"
        assert df_out.loc[idx, "is_feriado"].iloc[0] == True

    def test_segunda_feira_comum_nao_e_feriado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-01-08"
        assert df_out.loc[idx, "is_feriado"].iloc[0] == False

    def test_vespera_de_feriado(self):
        """24/12 é véspera de Natal."""
        df = pd.DataFrame({"data": ["2024-12-24"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["is_vespera_feriado"].iloc[0] == True

    def test_dia_normal_nao_vespera(self):
        df = pd.DataFrame({"data": ["2024-01-08"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["is_vespera_feriado"].iloc[0] == False


# ── Testes: Feriados por ano (_feriados_ano) ──────────────────────────────────

class TestFeriadosAno:
    def test_2024_tem_feriados(self):
        feriados = _feriados_ano(2024)
        assert len(feriados) > 10

    def test_ano_novo_em_2024(self):
        feriados = _feriados_ano(2024)
        assert pd.Timestamp("2024-01-01") in feriados

    def test_consciencia_negra_2024(self):
        """20/11 incluído desde Lei Federal 14.759/2023."""
        feriados = _feriados_ano(2024)
        assert pd.Timestamp("2024-11-20") in feriados

    def test_corpus_christi_2024(self):
        """Corpus Christi 2024 = 30 de maio (Páscoa 31/03 + 60 dias)."""
        feriados = _feriados_ano(2024)
        assert pd.Timestamp("2024-05-30") in feriados

    def test_cache_retorna_mesmo_objeto(self):
        """lru_cache deve retornar o mesmo frozenset."""
        f1 = _feriados_ano(2024)
        f2 = _feriados_ano(2024)
        assert f1 is f2  # mesma referência (cached)


# ── Testes: Eventos Especiais ─────────────────────────────────────────────────

class TestEventosEspeciais:
    def test_natal_detectado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%m-%d") == "12-25"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "natal"

    def test_ano_novo_detectado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%m-%d") == "01-01"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "ano_novo"

    def test_dia_namorados_detectado(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%m-%d") == "06-12"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "dia_namorados"

    def test_dia_maes_2024(self, df_ano_2024):
        """Dia das Mães 2024 = 12 de maio (2º domingo de maio)."""
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-05-12"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "dia_maes"

    def test_dia_pais_2024(self, df_ano_2024):
        """Dia dos Pais 2024 = 11 de agosto."""
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-08-11"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "dia_pais"

    def test_black_friday_2024(self, df_ano_2024):
        """Black Friday 2024 = 29 de novembro."""
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-11-29"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == "black_friday"

    def test_dia_comum_sem_evento(self, df_ano_2024):
        df_out = enrich_calendario(df_ano_2024)
        idx = df_ano_2024["data"].dt.strftime("%Y-%m-%d") == "2024-01-08"
        assert df_out.loc[idx, "evento_especial"].iloc[0] == ""


# ── Testes: Dias até Eventos Sazonais ─────────────────────────────────────────

class TestDiasAteEventos:
    def test_natal_no_proprio_dia_e_zero(self):
        df = pd.DataFrame({"data": ["2024-12-25"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dias_ate_natal"].iloc[0] == 0

    def test_dias_ate_natal_positivo_antes(self):
        df = pd.DataFrame({"data": ["2024-12-20"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dias_ate_natal"].iloc[0] == 5

    def test_apos_natal_aponta_proximo_ano(self):
        df = pd.DataFrame({"data": ["2024-12-26"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dias_ate_natal"].iloc[0] == 364  # 2024 é bissexto: dez/2024 → dez/2025 = 364 dias

    def test_black_friday_no_proprio_dia(self):
        """Black Friday 2024 = 29/11."""
        df = pd.DataFrame({"data": ["2024-11-29"], "quantidade": [100]})
        df["data"] = pd.to_datetime(df["data"])
        df_out = enrich_calendario(df)
        assert df_out["dias_ate_black_friday"].iloc[0] == 0


# ── Testes: _black_friday ─────────────────────────────────────────────────────

class TestBlackFriday:
    def test_bf_2024(self):
        assert _black_friday(2024) == pd.Timestamp("2024-11-29")

    def test_bf_2025(self):
        assert _black_friday(2025) == pd.Timestamp("2025-11-28")

    def test_bf_2023(self):
        assert _black_friday(2023) == pd.Timestamp("2023-11-24")

    def test_bf_sempre_sexta(self):
        for ano in range(2020, 2030):
            assert _black_friday(ano).dayofweek == 4  # 4 = Sexta

    def test_bf_sempre_novembro(self):
        for ano in range(2020, 2030):
            assert _black_friday(ano).month == 11


# ── Testes: _segundo_domingo ─────────────────────────────────────────────────

class TestSegundoDomingo:
    def test_dia_maes_2024(self):
        assert _segundo_domingo(2024, 5) == pd.Timestamp("2024-05-12")

    def test_dia_pais_2024(self):
        assert _segundo_domingo(2024, 8) == pd.Timestamp("2024-08-11")

    def test_sempre_domingo(self):
        for mes in [5, 8]:
            for ano in range(2020, 2030):
                assert _segundo_domingo(ano, mes).dayofweek == 6  # 6 = Domingo


# ── Testes: Casos de Erro ─────────────────────────────────────────────────────

class TestCasosErro:
    def test_coluna_data_faltando(self):
        df = pd.DataFrame({"quantidade": [100, 200]})
        with pytest.raises(KeyError):
            enrich_calendario(df)

    def test_coluna_customizada(self):
        df = pd.DataFrame({
            "data_venda": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "quantidade": [100, 200],
        })
        df_out = enrich_calendario(df, coluna_data="data_venda")
        assert "dia_semana" in df_out.columns

    def test_multiplos_anos_sem_erro(self, df_multiplos_anos):
        df_multiplos_anos["data"] = pd.to_datetime(df_multiplos_anos["data"])
        df_out = enrich_calendario(df_multiplos_anos)
        assert len(df_out) == len(df_multiplos_anos)
        assert df_out["is_feriado"].dtype == bool
