"""
reputation/google_places.py
-----------------------------
Wrapper para Google Places API.

Funções públicas:
  buscar_empresas(nome, api_key)        → list[dict] de até 10 candidatos
  buscar_detalhes_lugar(place_id, api_key) → dict com reviews, rating, etc.

Requisitos:
  pip install googlemaps
  settings.GOOGLE_PLACES_API_KEY

Limitações do Google Places:
  - Máximo de 5 reviews por chamada (limitação da API, sem paginação)
  - Dados retornados em PT-BR quando language='pt-BR'
"""
from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    import googlemaps
    _GOOGLEMAPS_OK = True
except ImportError:
    _GOOGLEMAPS_OK = False
    logger.warning("googlemaps não instalado. Execute: pip install googlemaps")


def _client(api_key: str):
    if not _GOOGLEMAPS_OK:
        raise RuntimeError("Biblioteca 'googlemaps' não instalada. Execute: pip install googlemaps")
    if not api_key:
        raise ValueError("GOOGLE_PLACES_API_KEY não configurada em settings.py")
    return googlemaps.Client(key=api_key)


def buscar_empresas(nome: str, api_key: str) -> list[dict]:
    """
    Busca empresas pelo nome e retorna até 10 candidatos.

    Retorna lista de dicts com:
      place_id, nome, endereco, rating, total_avaliacoes, foto_url
    """
    if not nome or not nome.strip():
        return []

    gmaps = _client(api_key)

    try:
        resultado = gmaps.places(query=nome.strip(), language="pt-BR")
    except Exception as exc:
        logger.error("[google_places] Erro na busca por '%s': %s", nome, exc)
        raise

    candidatos: list[dict] = []
    for place in (resultado.get("results") or [])[:10]:
        foto_url = None
        fotos = place.get("photos")
        if fotos:
            ref = fotos[0].get("photo_reference")
            if ref:
                foto_url = (
                    f"https://maps.googleapis.com/maps/api/place/photo"
                    f"?maxwidth=120&photo_reference={ref}&key={api_key}"
                )

        candidatos.append({
            "place_id":        place.get("place_id", ""),
            "nome":            place.get("name", ""),
            "endereco":        place.get("formatted_address", ""),
            "rating":          place.get("rating"),
            "total_avaliacoes": place.get("user_ratings_total", 0),
            "foto_url":        foto_url,
        })

    return candidatos


def buscar_detalhes_lugar(place_id: str, api_key: str) -> dict:
    """
    Busca detalhes de um lugar pelo Place ID, incluindo até 5 reviews.

    Retorna dict com:
      place_id, nome, endereco, url_maps, rating, total_avaliacoes,
      foto_url, reviews (list)

    Cada review:
      autor, foto_autor, rating, texto, data_unix, data_relativa
    """
    gmaps = _client(api_key)

    campos = [
        "place_id", "name", "formatted_address", "url",
        "rating", "user_ratings_total",
        "reviews", "photo",
    ]

    try:
        resultado = gmaps.place(place_id=place_id, fields=campos, language="pt-BR")
    except Exception as exc:
        logger.error("[google_places] Erro ao buscar detalhes de '%s': %s", place_id, exc)
        raise

    lugar = resultado.get("result", {})

    # Foto principal
    foto_url = None
    fotos = lugar.get("photo")
    if fotos:
        ref = fotos[0].get("photo_reference")
        if ref:
            foto_url = (
                f"https://maps.googleapis.com/maps/api/place/photo"
                f"?maxwidth=400&photo_reference={ref}&key={api_key}"
            )

    # Reviews
    reviews_raw: list[dict] = []
    for rv in (lugar.get("reviews") or []):
        ts = rv.get("time", 0)
        reviews_raw.append({
            "autor":        rv.get("author_name", "Anônimo"),
            "foto_autor":   rv.get("profile_photo_url"),
            "rating":       rv.get("rating", 0),
            "texto":        rv.get("text", "").strip(),
            "data_unix":    ts,
            "data_iso":     datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else "",
            "data_relativa": rv.get("relative_time_description", ""),
        })

    return {
        "place_id":        lugar.get("place_id", place_id),
        "nome":            lugar.get("name", ""),
        "endereco":        lugar.get("formatted_address", ""),
        "url_maps":        lugar.get("url", ""),
        "rating":          lugar.get("rating"),
        "total_avaliacoes": lugar.get("user_ratings_total", 0),
        "foto_url":        foto_url,
        "reviews":         reviews_raw,
    }
