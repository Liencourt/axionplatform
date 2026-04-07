"""
enrichment/_cache.py
--------------------
Cache TTL simples para Cloud Run e desenvolvimento local.

Estratégia de dois níveis:
  L1 — dict em memória por processo (lru_cache / _STORE)
       Custo zero, zerado a cada cold start (aceitável para dados de clima/IBGE)
  L2 — GCS JSON com metadado expires_at
       Persistente entre instâncias; usado apenas quando GCS estiver disponível.

Em desenvolvimento local, GCS não está disponível e apenas L1 é usado.
Em Cloud Run, L2 evita refetch em cold starts dentro da janela de TTL.

TTLs recomendados (em segundos):
  INMET lista de estações : 7 dias  = 604_800
  INMET dados climáticos  : 24 h    =  86_400
  IBGE dados município    : 30 dias = 2_592_000
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

# ── L1: Memória por processo ────────────────────────────────────────────────
_STORE: dict[str, tuple[float, Any]] = {}   # key → (expires_monotonic, data)


def cache_get(key: str) -> Any | None:
    """Retorna dado do cache L1 (memória) ou L2 (GCS), ou None se expirado."""
    # L1
    item = _STORE.get(key)
    if item and time.monotonic() < item[0]:
        return item[1]

    # L2 (GCS) — só tenta se USE_GCS estiver ativo ou não estivermos em DEBUG
    if _gcs_disponivel():
        data = _gcs_get(key)
        if data is not None:
            # Promove para L1 com TTL restante
            restante = data["_expires_at"] - time.time()
            if restante > 0:
                _STORE[key] = (time.monotonic() + restante, data["payload"])
                return data["payload"]

    return None


def cache_set(key: str, value: Any, ttl: int) -> None:
    """Salva dado em L1 (memória) e L2 (GCS se disponível)."""
    _STORE[key] = (time.monotonic() + ttl, value)

    if _gcs_disponivel():
        _gcs_set(key, value, ttl)


# ── L2: GCS ──────────────────────────────────────────────────────────────────

def _gcs_disponivel() -> bool:
    from django.conf import settings
    return os.getenv("USE_GCS", "false").lower() == "true" or not settings.DEBUG


def _gcs_get(key: str) -> dict | None:
    try:
        from google.cloud import storage as gcs
        client = gcs.Client()
        bucket = client.bucket(os.getenv("BUCKET_NAME", "axiom-platform-datasets"))
        blob = bucket.blob(f"enrichment_cache/{key}.json")
        raw = blob.download_as_text(timeout=5)
        data = json.loads(raw)
        if time.time() < data.get("_expires_at", 0):
            return data
        logger.debug("Cache GCS expirado: %s", key)
    except Exception as exc:
        logger.debug("Cache GCS get falhou (%s): %s", key, exc)
    return None


def _gcs_set(key: str, value: Any, ttl: int) -> None:
    try:
        from google.cloud import storage as gcs
        client = gcs.Client()
        bucket = client.bucket(os.getenv("BUCKET_NAME", "axiom-platform-datasets"))
        blob = bucket.blob(f"enrichment_cache/{key}.json")
        payload = json.dumps({"_expires_at": time.time() + ttl, "payload": value})
        blob.upload_from_string(payload, content_type="application/json", timeout=5)
        logger.debug("Cache GCS set: %s (TTL=%ds)", key, ttl)
    except Exception as exc:
        logger.debug("Cache GCS set falhou (%s): %s", key, exc)
