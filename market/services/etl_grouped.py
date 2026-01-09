# market/services/etl_grouped.py
from __future__ import annotations
import os
import time
import random
import datetime as dt
from typing import Iterable

import requests
from django.db import transaction

from market.models import Ticker, PriceDaily
from market.services.polygon_client import PolygonClient
from market.services.trading_days import last_us_trading_day

# --------------------------------------------------------------------------------------
# Config por variables de entorno (default seguros para Render/Gunicorn)
# --------------------------------------------------------------------------------------
POLYGON_RPM           = int(os.getenv("POLYGON_RPM", "5"))             # límite "lógico"
INTERNAL_SLEEP        = float(os.getenv("INTERNAL_SLEEP", "0.20"))     # sleep corto con jitter
INTERNAL_MAX_SECONDS  = float(os.getenv("INTERNAL_MAX_SECONDS", "25")) # time-box de la request
INTERNAL_FALLBACK_BR  = int(os.getenv("INTERNAL_FALLBACK_BURST", "10"))# máx fallback por tanda

# --------------------------------------------------------------------------------------
# Normalización mínima de símbolos "de clase" para Polygon
# --------------------------------------------------------------------------------------
CLASS_MAP = {
    # Berkshire / Brown-Forman
    "BRK": "BRK.B",
    "BRK.B": "BRK.B",
    "BRK.A": "BRK.A",
    "BF": "BF.B",
    "BF.B": "BF.B",
    "BF.A": "BF.A",
}

def _normalize_for_polygon(sym: str) -> str:
    s = (sym or "").upper().strip()
    return CLASS_MAP.get(s, s)

def _alt_class_symbol(sym_norm: str) -> str | None:
    """Si llega BRK.B probamos BRK.A (y viceversa). Igual para BF."""
    s = sym_norm.upper()
    if s.endswith(".B"):
        return s[:-2] + ".A"
    if s.endswith(".A"):
        return s[:-2] + ".B"
    return None

def _ensure_tickers(symbols: list[str]) -> dict[str, Ticker]:
    """
    Asegura existencia de Ticker para cada símbolo. Devuelve map symbol->Ticker obj.
    (Se usa el símbolo "original" de tu universo, no el normalizado)
    """
    syms = [s.upper().strip() for s in symbols if s]
    exists = {t.symbol: t for t in Ticker.objects.filter(symbol__in=syms)}
    need = [s for s in syms if s not in exists]
    if need:
        Ticker.objects.bulk_create(
            [Ticker(symbol=s, is_active=True) for s in need],
            ignore_conflicts=True,
        )
        exists = {t.symbol: t for t in Ticker.objects.filter(symbol__in=syms)}
    return exists

def _coerce_row_for_upsert(date: dt.date, symbol_orig: str, r: dict) -> dict:
    """
    Convierte una fila cruda (grouped/eod) en dict listo para upsert.
    Asegura adj_close si viene None. Mantiene symbol ORIGINAL.
    """
    open_ = r.get("open")
    high_ = r.get("high")
    low_ = r.get("low")
    close_ = r.get("close")
    adj_close_ = r.get("adj_close")
    volume_ = r.get("volume")

    if adj_close_ is None:
        adj_close_ = close_

    return {
        "symbol": symbol_orig,
        "open": open_,
        "high": high_,
        "low": low_,
        "close": close_,
        "adj_close": adj_close_,
        "volume": volume_,
    }

def _upsert_prices(date: dt.date, rows: list[dict]) -> int:
    """
    rows: [{"symbol","open","high","low","close","adj_close","volume"}]
    """
    if not rows:
        return 0
    tickers_map = _ensure_tickers([r["symbol"] for r in rows])
    objs: list[PriceDaily] = []
    for r in rows:
        t = tickers_map.get(r["symbol"])
        if not t:
            continue
        objs.append(
            PriceDaily(
                ticker=t,
                date=date,
                open=r["open"],
                high=r["high"],
                low=r["low"],
                close=r["close"],
                adj_close=r["adj_close"],
                volume=r["volume"],
            )
        )
    if not objs:
        return 0
    with transaction.atomic():
        PriceDaily.objects.bulk_create(
            objs,
            batch_size=1000,
            update_conflicts=True,
            update_fields=["open", "high", "low", "close", "adj_close", "volume"],
            unique_fields=["ticker", "date"],
        )
    return len(objs)

def update_last_candle_grouped_then_fallback(
    universe_symbols: Iterable[str],
    target_date: dt.date | None = None,
) -> dict:
    """
    1) Intenta traer TODO con grouped (una request).
    2) Upsert masivo para los símbolos del universo (usando símbolo ORIGINAL).
    3) Para símbolos del universo que no vinieron en grouped o vinieron sin OHLC,
       hace fallback per-símbolo con:
         - time-box por request (INTERNAL_MAX_SECONDS)
         - burst máx por tanda (INTERNAL_FALLBACK_BR)
         - sleeps cortos (INTERNAL_SLEEP) con jitter
       y prueba alternativa de clase (.B ⇄ .A) si 404.
    Devuelve métricas y si el trabajo quedó parcial (partial=True).
    """
    t0 = time.monotonic()
    cli = PolygonClient()
    date = target_date or last_us_trading_day()
    uni_orig = sorted({(s or "").upper().strip() for s in universe_symbols if s})

    # Mapeo original -> normalizado
    orig_to_norm = {orig: _normalize_for_polygon(orig) for orig in uni_orig}
    # Invertido (si dos orig mapean al mismo norm, nos quedamos con el primero)
    norm_to_orig: dict[str, str] = {}
    for o, n in orig_to_norm.items():
        norm_to_orig.setdefault(n, o)

    # ----------------------------------------------------------------------------------
    # 1) GROUPED
    # ----------------------------------------------------------------------------------
    grouped_rows_raw: list[dict] = []
    try:
        all_rows = cli.grouped_daily_stocks(date) or []
        if all_rows:
            wanted_norm = set(orig_to_norm.values())
            for r in all_rows:
                sym_norm = (r.get("symbol") or "").upper()
                if sym_norm in wanted_norm and (r.get("close") is not None):
                    sym_orig = norm_to_orig.get(sym_norm, sym_norm)
                    grouped_rows_raw.append(_coerce_row_for_upsert(date, sym_orig, r))
    except PermissionError:
        # sin permiso (plan), seguimos con fallback
        grouped_rows_raw = []

    upserted_grouped = _upsert_prices(date, grouped_rows_raw)
    got_syms_orig = {r["symbol"] for r in grouped_rows_raw}
    missing_orig = [s for s in uni_orig if s not in got_syms_orig]

    # chequeo de tiempo tras grouped
    elapsed = time.monotonic() - t0
    if elapsed >= INTERNAL_MAX_SECONDS or not missing_orig:
        # Si no hay faltantes o se nos fue el tiempo, devolvemos ya
        return {
            "date": date.isoformat(),
            "universe": len(uni_orig),
            "grouped_upserted": upserted_grouped,
            "fallback_attempted": 0,
            "fallback_ok": 0,
            "http_404": 0,
            "http_other": 0,
            "missing_after": max(0, len(uni_orig) - len(got_syms_orig)),
            "total_effective": upserted_grouped,
            "elapsed": elapsed,
            "partial": len(missing_orig) > 0,  # parcial si quedaron faltantes
        }

    # ----------------------------------------------------------------------------------
    # 2) FALLBACK por símbolo (acotado por tiempo y burst)
    # ----------------------------------------------------------------------------------
    fallback_attempted = 0
    fallback_ok = 0
    http_404 = 0
    http_other = 0
    processed_in_burst = 0
    partial = False

    # Presupuesto por RPM y por tiempo (aprox)
    time_left = max(0.0, INTERNAL_MAX_SECONDS - elapsed)
    by_rpm   = int(POLYGON_RPM * INTERNAL_MAX_SECONDS)
    by_sleep = int(time_left / max(INTERNAL_SLEEP, 0.05))
    fallback_budget = max(0, min(INTERNAL_FALLBACK_BR, by_rpm, by_sleep))

    for sym_orig in missing_orig[:fallback_budget]:
        # cortar por tiempo
        if (time.monotonic() - t0) >= INTERNAL_MAX_SECONDS:
            partial = True
            break

        sym_norm = orig_to_norm.get(sym_orig, sym_orig)
        try:
            fallback_attempted += 1
            one = cli.eod_bar(sym_norm, date)
            if one:
                _upsert_prices(date, [_coerce_row_for_upsert(date, sym_orig, one)])
                fallback_ok += 1
        except PermissionError:
            # no podemos usar eod en el plan; seguimos (quedará partial)
            pass
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 404:
                http_404 += 1
                alt = _alt_class_symbol(sym_norm)
                if alt:
                    try:
                        fallback_attempted += 1
                        one = cli.eod_bar(alt, date)
                        if one:
                            _upsert_prices(date, [_coerce_row_for_upsert(date, sym_orig, one)])
                            fallback_ok += 1
                    except Exception:
                        # si falla el alternativo, seguimos
                        pass
            else:
                http_other += 1
        finally:
            # sleeps cortitos con jitter para no clavar el worker
            time.sleep(max(0.05, INTERNAL_SLEEP + random.uniform(-0.05, 0.05)))
            processed_in_burst += 1

        # cortar si agotamos la tanda
        if processed_in_burst >= fallback_budget:
            # si aún quedan faltantes, marcamos partial
            partial = True
            break

    elapsed = time.monotonic() - t0
    total_effective = upserted_grouped + fallback_ok
    missing_after = max(0, len(uni_orig) - len(got_syms_orig) - fallback_ok)

    return {
        "date": date.isoformat(),
        "universe": len(uni_orig),
        "grouped_upserted": upserted_grouped,
        "fallback_attempted": fallback_attempted,
        "fallback_ok": fallback_ok,
        "http_404": http_404,
        "http_other": http_other,
        "missing_after": missing_after,
        "total_effective": total_effective,
        "elapsed": elapsed,
        "partial": partial or (missing_after > 0),
    }
