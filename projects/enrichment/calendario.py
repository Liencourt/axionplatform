"""
enrichment/calendario.py
------------------------
Gera variáveis de calendário para qualquer data brasileira.

Variáveis adicionadas ao DataFrame:
  - dia_semana        int  0=Segunda … 6=Domingo
  - is_fim_semana     bool
  - is_feriado        bool feriados nacionais BR
  - is_vespera_feriado bool
  - mes               int  1–12
  - dia_mes           int  1–31
  - trimestre         int  1–4
  - semana_ano        int  número ISO da semana
  - dias_ate_natal    int  dias até o próximo 25/12 (0 no próprio dia)
  - dias_ate_black_friday int
  - evento_especial   str  ex: "dia_maes", "black_friday", "" (vazio se nenhum)

Feriados nacionais BR calculados por ano:
  Fixos:  01/01, 21/04, 01/05, 07/09, 12/10, 02/11, 15/11, 20/11, 25/12
  Móveis (relativo à Páscoa):
    Carnaval          D - 48 (segunda), D - 47 (terça)
    Sexta-Feira Santa D - 2
    Corpus Christi    D + 60

Nota: feriados municipais e estaduais NÃO estão incluídos por design
(variam por município — adicionar via EventoCalendario do banco se necessário).
"""
from __future__ import annotations

import logging
from functools import lru_cache

import pandas as pd
from dateutil.easter import easter

logger = logging.getLogger(__name__)


# ── Feriados Nacionais ────────────────────────────────────────────────────────

# Feriados fixos: (mês, dia)
_FERIADOS_FIXOS = {
    (1, 1),   # Confraternização Universal
    (4, 21),  # Tiradentes
    (5, 1),   # Dia do Trabalho
    (9, 7),   # Independência do Brasil
    (10, 12), # Nossa Senhora Aparecida / Dia das Crianças
    (11, 2),  # Finados
    (11, 15), # Proclamação da República
    (11, 20), # Consciência Negra (Lei Federal 14.759/2023)
    (12, 25), # Natal
}


@lru_cache(maxsize=50)
def _feriados_ano(ano: int) -> frozenset[pd.Timestamp]:
    """
    Retorna conjunto de timestamps dos feriados nacionais para o ano dado.
    Cacheado por ano — chamado uma vez por ano único no DataFrame.
    """
    feriados: set[pd.Timestamp] = set()

    # Fixos
    for mes, dia in _FERIADOS_FIXOS:
        try:
            feriados.add(pd.Timestamp(ano, mes, dia))
        except ValueError:
            pass  # data inválida para o ano

    # Móveis baseados na Páscoa
    pascoa = pd.Timestamp(easter(ano))
    feriados.add(pascoa - pd.Timedelta(days=48))  # Segunda de Carnaval
    feriados.add(pascoa - pd.Timedelta(days=47))  # Terça de Carnaval
    feriados.add(pascoa - pd.Timedelta(days=2))   # Sexta-Feira Santa
    feriados.add(pascoa + pd.Timedelta(days=60))  # Corpus Christi

    return frozenset(feriados)


def _gerar_feriados(anos: "pd.Series | list[int]") -> frozenset[pd.Timestamp]:
    """Gera feriados para múltiplos anos (união)."""
    resultado: set[pd.Timestamp] = set()
    for ano in anos:
        resultado.update(_feriados_ano(int(ano)))
    return frozenset(resultado)


# ── Eventos Especiais ─────────────────────────────────────────────────────────

# Eventos de data fixa: (mês, dia) → nome
_EVENTOS_FIXOS: dict[tuple[int, int], str] = {
    (1, 1):   "ano_novo",
    (2, 14):  "dia_namorados_intl",  # Valentine's (relevante para e-commerce)
    (4, 21):  "tiradentes",
    (5, 1):   "dia_trabalho",
    (6, 12):  "dia_namorados",
    (9, 7):   "independencia",
    (10, 12): "dia_criancas",
    (10, 31): "halloween",
    (11, 2):  "finados",
    (11, 15): "proclamacao_republica",
    (12, 24): "vespera_natal",
    (12, 25): "natal",
    (12, 26): "pos_natal",
    (12, 31): "reveillon",
}


@lru_cache(maxsize=50)
def _eventos_moveis_ano(ano: int) -> dict[pd.Timestamp, str]:
    """Eventos de data variável para o ano dado."""
    eventos: dict[pd.Timestamp, str] = {}

    # Páscoa
    pascoa = pd.Timestamp(easter(ano))
    eventos[pascoa] = "pascoa"
    eventos[pascoa - pd.Timedelta(days=48)] = "carnaval"
    eventos[pascoa - pd.Timedelta(days=47)] = "carnaval"

    # Dia das Mães — 2ª domingo de maio
    eventos[_segundo_domingo(ano, 5)] = "dia_maes"

    # Dia dos Namorados (fixo, mas listado aqui para completude)
    # Dia dos Pais — 2º domingo de agosto
    eventos[_segundo_domingo(ano, 8)] = "dia_pais"

    # Black Friday — 4ª sexta-feira de novembro
    eventos[_black_friday(ano)] = "black_friday"

    # Cyber Monday — Black Friday + 3 dias
    eventos[_black_friday(ano) + pd.Timedelta(days=3)] = "cyber_monday"

    return eventos


def _segundo_domingo(ano: int, mes: int) -> pd.Timestamp:
    """Retorna o 2º domingo de um dado mês/ano."""
    primeiro_dia = pd.Timestamp(ano, mes, 1)
    dias_ate_domingo = (6 - primeiro_dia.dayofweek) % 7
    primeiro_domingo = primeiro_dia + pd.Timedelta(days=dias_ate_domingo)
    return primeiro_domingo + pd.Timedelta(weeks=1)


def _black_friday(ano: int) -> pd.Timestamp:
    """
    Sexta-feira após o 4º quinta-feira de novembro (dia após o Thanksgiving).
    = 4ª quinta-feira de novembro + 1 dia.
    """
    novembro_1 = pd.Timestamp(ano, 11, 1)
    # Encontra a primeira quinta-feira (dayofweek=3)
    dias_ate_quinta = (3 - novembro_1.dayofweek) % 7
    primeira_quinta = novembro_1 + pd.Timedelta(days=dias_ate_quinta)
    quarta_quinta = primeira_quinta + pd.Timedelta(weeks=3)  # 4ª quinta
    return quarta_quinta + pd.Timedelta(days=1)              # sexta seguinte


def _proximo_natal(data: pd.Timestamp) -> pd.Timestamp:
    """Retorna o próximo 25/12 a partir de data (inclusive)."""
    natal_ano = pd.Timestamp(data.year, 12, 25)
    return natal_ano if data <= natal_ano else pd.Timestamp(data.year + 1, 12, 25)


def _proximo_black_friday(data: pd.Timestamp) -> pd.Timestamp:
    bf = _black_friday(data.year)
    return bf if data <= bf else _black_friday(data.year + 1)


def _detectar_evento(data: pd.Timestamp, eventos_moveis: dict[pd.Timestamp, str]) -> str:
    """Retorna nome do evento especial na data ou '' se nenhum."""
    chave_fixa = (data.month, data.day)
    if chave_fixa in _EVENTOS_FIXOS:
        return _EVENTOS_FIXOS[chave_fixa]
    return eventos_moveis.get(data.normalize(), "")


# ── Função Principal ──────────────────────────────────────────────────────────

def enrich_calendario(
    df: pd.DataFrame,
    coluna_data: str = "data",
) -> pd.DataFrame:
    """
    Adiciona variáveis de calendário ao DataFrame de vendas.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame que já passou por process_sales_data() — coluna 'data'
        deve ser datetime64[ns].
    coluna_data : str
        Nome da coluna de datas (padrão: 'data').

    Retorna
    -------
    pd.DataFrame com colunas de calendário adicionadas.

    Levanta
    -------
    KeyError  se coluna_data não existir no DataFrame.
    ValueError se coluna_data não for datetime.
    """
    if coluna_data not in df.columns:
        raise KeyError(f"Coluna '{coluna_data}' não encontrada no DataFrame.")

    df = df.copy()
    datas = pd.to_datetime(df[coluna_data])

    if datas.isna().all():
        raise ValueError(f"Coluna '{coluna_data}' contém apenas valores nulos.")

    # ── Variáveis básicas ────────────────────────────────────────────────────
    df["dia_semana"]  = datas.dt.dayofweek        # 0=Segunda, 6=Domingo
    df["is_fim_semana"] = datas.dt.dayofweek >= 5
    df["mes"]         = datas.dt.month
    df["dia_mes"]     = datas.dt.day
    df["trimestre"]   = datas.dt.quarter
    df["semana_ano"]  = datas.dt.isocalendar().week.astype(int)

    # ── Feriados ─────────────────────────────────────────────────────────────
    anos_unicos = datas.dt.year.dropna().unique().tolist()
    # Inclui ano anterior/posterior para vésperas que cruzam virada de ano
    anos_extendidos = sorted(set(anos_unicos + [a - 1 for a in anos_unicos] + [a + 1 for a in anos_unicos]))
    feriados = _gerar_feriados(anos_extendidos)

    datas_normalizadas = datas.dt.normalize()
    df["is_feriado"]         = datas_normalizadas.isin(feriados)
    df["is_vespera_feriado"] = (datas_normalizadas + pd.Timedelta(days=1)).isin(feriados)

    # ── Dias até eventos sazonais ─────────────────────────────────────────────
    df["dias_ate_natal"] = datas.apply(
        lambda d: ((_proximo_natal(d) - d).days if pd.notna(d) else None)
    )
    df["dias_ate_black_friday"] = datas.apply(
        lambda d: ((_proximo_black_friday(d) - d).days if pd.notna(d) else None)
    )

    # ── Evento especial ──────────────────────────────────────────────────────
    # Pré-computa eventos móveis por ano para evitar recalcular por linha
    cache_eventos: dict[int, dict] = {}
    for ano in anos_unicos:
        cache_eventos[int(ano)] = _eventos_moveis_ano(int(ano))

    def _evento_linha(data):
        if pd.isna(data):
            return ""
        eventos_moveis = cache_eventos.get(data.year, {})
        return _detectar_evento(data, eventos_moveis)

    df["evento_especial"] = datas.apply(_evento_linha)

    logger.debug(
        "enrich_calendario: %d linhas processadas, %d feriados no período, anos=%s",
        len(df),
        df["is_feriado"].sum(),
        anos_unicos,
    )

    return df
