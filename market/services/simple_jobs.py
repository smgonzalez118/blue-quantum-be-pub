from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Any, Union

from django.conf import settings

from market.models import Ticker, PriceDaily
from market.services.trading_days import last_us_trading_day
from market.services.etl_grouped import update_last_candle_grouped_then_fallback
from market.services.indicators import compute_indicators_for_ticker
from market.services.signals import compute_signal_for_ticker
from utils.universe import get_dashboard_universe


# ------------------------ Utils comunes ------------------------

def _dedupe_upper(seq: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in (seq or []):
        u = (s or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _active_symbols() -> List[str]:
    return list(
        Ticker.objects.filter(is_active=True).values_list("symbol", flat=True)
    )


def _symbols_from_mode_or_list(mode_or_symbols: Union[str, Iterable[str], None]) -> List[str]:
    """
    Soporta:
      - str: 'sp100' | 'adrs' | 'commodities' | 'etfs' | 'custom' → usa get_dashboard_universe(mode)
      - Iterable[str]: lista explícita de símbolos
      - None: todos los activos
    """
    if isinstance(mode_or_symbols, str):
        mode = mode_or_symbols.strip().lower()
        try:
            syms = get_dashboard_universe(mode=mode)
            return _dedupe_upper(syms)
        except Exception:
            # fallback: activos
            return _dedupe_upper(_active_symbols())
    elif mode_or_symbols is None:
        return _dedupe_upper(_active_symbols())
    else:
        return _dedupe_upper(mode_or_symbols)


# ------------------------ EOD helper opcional ------------------------

def fetch_eod_all_active(target_date: Optional[dt.date]) -> Dict[str, Any]:
    """
    Trae EOD para todos los tickers activos usando el pipeline:
      1) intento /grouped (una sola request)
      2) fallback per-símbolo en tandas
    Si target_date es None, usa último hábil de AYER.
    """
    if target_date is None:
        target_date = last_us_trading_day(dt.date.today() - dt.timedelta(days=1))

    symbols = _active_symbols()
    stats = update_last_candle_grouped_then_fallback(symbols, target_date)
    return {
        "date": target_date.isoformat(),
        "count_symbols": len(symbols),
        "stats": stats,
    }


# ------------------------ PRECALC “pro”: time-box + progreso ------------------------

def compute_indicators_and_signals_all(
    mode_or_symbols: Union[str, Iterable[str], None] = None,
) -> Dict[str, Any]:
    """
    Recalcula indicadores (daily/weekly) y la señal (daily) para un universo,
    en modo time-box, con progreso persistente y sin lanzar excepciones.

    Entradas:
      - mode_or_symbols:
          * 'sp100' | 'adrs' | 'commodities' | 'etfs' | 'custom' → universo por helper
          * lista de símbolos
          * None → todos los activos

    Control por env:
      - PRECALC_MAX_SECONDS (float, default 25.0)
      - PRECALC_BURST       (int,   default 10)    # cuántos tickers por mini-ronda
      - PRECALC_SLEEP       (float, default 0.10)  # pausa corta entre mini-rondas

    Persistencia de progreso:
      - .cache/precalc_progress_<key>.json
        donde <key> depende del universo (mode, 'all_active' o hash de lista)

    Devuelve SIEMPRE un JSON con:
      {
        ok: True|False,
        partial: True|False,
        count_symbols, processed, errors, remaining, elapsed,
        config: {...},
        progress_file: "..."
      }
    """
    # ---------- Config ----------
    try:
        max_seconds = float(getattr(settings, "PRECALC_MAX_SECONDS", None) or
                            settings.__dict__.get("PRECALC_MAX_SECONDS", None) or
                            (os.getenv("PRECALC_MAX_SECONDS", "30")))
    except Exception:
        max_seconds = 25.0

    try:
        burst = int(getattr(settings, "PRECALC_BURST", None) or
                    settings.__dict__.get("PRECALC_BURST", None) or
                    (os.getenv("PRECALC_BURST", "8")))
    except Exception:
        burst = 10

    try:
        tiny_sleep = float(getattr(settings, "PRECALC_SLEEP", None) or
                           settings.__dict__.get("PRECALC_SLEEP", None) or
                           (os.getenv("PRECALC_SLEEP", "0.12")))
    except Exception:
        tiny_sleep = 0.10

    started_at = time.perf_counter()

    # ---------- Universo ----------
    symbols = _symbols_from_mode_or_list(mode_or_symbols)
    if not symbols:
        return {
            "ok": False,
            "partial": True,   # permitimos reintentos arriba
            "error": "universe_empty",
            "count_symbols": 0,
            "processed": 0,
            "errors": 0,
            "remaining": 0,
            "elapsed": 0.0,
        }

    # Clave de progreso
    if isinstance(mode_or_symbols, str):
        key = mode_or_symbols.strip().lower() or "custom"
    elif mode_or_symbols is None:
        key = "all_active"
    else:
        raw = ",".join(sorted(symbols))
        key = "list_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:8]

    cache_dir = Path(".cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    progress_path = cache_dir / f"precalc_progress_{key}.json"

    # ---------- Cargar/crear progreso ----------
    try:
        if progress_path.exists():
            saved = json.loads(progress_path.read_text(encoding="utf-8"))
            order = saved.get("order") or symbols
            if set(order) != set(symbols):
                order = symbols
                saved = {"idx": 0, "order": order}
        else:
            order = symbols
            saved = {"idx": 0, "order": order}
    except Exception:
        order = symbols
        saved = {"idx": 0, "order": order}

    idx = int(saved.get("idx", 0))
    n = len(order)

    processed = 0
    errors = 0

    # ---------- Loop time-box ----------
    try:
        while (time.perf_counter() - started_at) < max_seconds and idx < n:
            end = min(n, idx + burst)
            batch = order[idx:end]
            for sym in batch:
                if (time.perf_counter() - started_at) >= max_seconds:
                    break
                try:
                    compute_indicators_for_ticker(sym, "daily")
                except Exception:
                    errors += 1
                try:
                    compute_indicators_for_ticker(sym, "weekly")
                except Exception:
                    errors += 1
                try:
                    compute_signal_for_ticker(sym, "daily")
                except Exception:
                    errors += 1
                processed += 1
            idx = end
            if idx < n:
                time.sleep(tiny_sleep)
    except Exception as e:
        # No explotamos, devolvemos un payload “retry-friendly”
        elapsed = time.perf_counter() - started_at
        partial = True
        # Intento guardar progreso hasta idx actual
        try:
            progress_path.write_text(json.dumps({"idx": idx, "order": order}), encoding="utf-8")
        except Exception:
            pass
        return {
            "ok": False,
            "partial": partial,
            "error": str(e),
            "count_symbols": len(symbols),
            "processed": processed,
            "errors": errors,
            "remaining": max(0, n - idx),
            "elapsed": elapsed,
            "config": {
                "PRECALC_MAX_SECONDS": max_seconds,
                "PRECALC_BURST": burst,
                "PRECALC_SLEEP": tiny_sleep,
            },
            "progress_file": str(progress_path),
        }

    elapsed = time.perf_counter() - started_at
    partial = idx < n

    # Guardar/limpiar progreso
    try:
        if partial:
            progress_path.write_text(json.dumps({"idx": idx, "order": order}), encoding="utf-8")
        else:
            progress_path.unlink(missing_ok=True)
    except Exception:
        pass

    return {
        "ok": True,
        "partial": partial,
        "count_symbols": len(symbols),
        "processed": processed,
        "errors": errors,
        "remaining": max(0, n - idx),
        "elapsed": elapsed,
        "config": {
            "PRECALC_MAX_SECONDS": max_seconds,
            "PRECALC_BURST": burst,
            "PRECALC_SLEEP": tiny_sleep,
            "progress_key": key,
        },
        "progress_file": str(progress_path),
    }

