# market/views_internal.py
from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional
from pathlib import Path
from io import StringIO
from tempfile import NamedTemporaryFile

from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.core.management import call_command
from django.db import connection
from django.core.cache import cache

from utils.universe import get_dashboard_universe
from market.services.trading_days import last_us_trading_day
from market.services.etl_grouped import update_last_candle_grouped_then_fallback
from market.services.simple_jobs import compute_indicators_and_signals_all

# ---------------------------------------------------------------------
# Helpers comunes
# ---------------------------------------------------------------------

def json_ok(payload: Dict[str, Any], status: int = 200) -> JsonResponse:
    payload.setdefault("ok", True)
    return JsonResponse(payload, status=status)

def json_error(message: str, status: int = 400, **extra) -> JsonResponse:
    data = {"ok": False, "error": message}
    if extra:
        data.update(extra)
    return JsonResponse(data, status=status)

def _get_token(request) -> str:
    # Header estándar o alterno + fallback a query param
    return (
        request.headers.get("X-Internal-Token", "")
        or request.META.get("HTTP_X_INTERNAL_TOKEN", "")
        or request.GET.get("token", "")
        or ""
    )

def _auth_ok(request) -> bool:
    token = _get_token(request)
    expected = (
        getattr(settings, "INTERNAL_API_TOKEN", "")
        or getattr(settings, "JOB_RUN_TOKEN", "")
        or ""
    )
    return bool(expected) and (token == expected)

def _json_body(request) -> Dict[str, Any]:
    try:
        if request.body:
            return json.loads(request.body.decode("utf-8"))
    except Exception:
        pass
    return {}

def _call_command_captured(cmd_name: str, **kwargs) -> str:
    """
    Ejecuta un management command capturando stdout para devolverlo en la API.
    """
    buf = StringIO()
    call_command(cmd_name, stdout=buf, **kwargs)
    return buf.getvalue()

def _dedupe_upper(seq: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in (seq or []):
        u = (s or "").strip().upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _symbols_from_request(body: Dict[str, Any]) -> List[str]:
    """
    Prioridad:
      1) body["symbols"]           → lista explícita
      2) body["all_active"] = true → todos los Ticker.is_active
      3) body["universe_mode"]     → get_dashboard_universe(mode)
      4) fallback                  → get_dashboard_universe("custom")
    """
    # 1) explícitos
    raw_syms = body.get("symbols")
    if isinstance(raw_syms, (list, tuple)) and raw_syms:
        return _dedupe_upper(raw_syms)

    # 2) all_active
    if bool(body.get("all_active", False)):
        try:
            from market.models import Ticker
            qs = Ticker.objects.filter(is_active=True).values_list("symbol", flat=True)
            return _dedupe_upper(list(qs))
        except Exception:
            pass  # cae a universe_mode

    # 3) universe_mode
    mode = str(body.get("universe_mode", "custom")).lower().strip()
    try:
        syms = get_dashboard_universe(mode=mode)
        if syms:
            return _dedupe_upper(syms)
    except Exception:
        pass

    # 4) fallback
    return _dedupe_upper(get_dashboard_universe(mode="custom"))

def _call_command_with_symbols(cmd_name: str, symbols: List[str], **kwargs) -> str:
    """
    Para pasar muchos símbolos de forma segura al management command:
      - Escribe un archivo temporal con un símbolo por línea.
      - Llama al command con --tickers-file=<tmp>.
      - Borra el archivo temporal al final.
    (Los commands `sync_company_profile` y `sync_fundamentals` soportan `--tickers-file`.)
    """
    tmp = NamedTemporaryFile("w+", encoding="utf-8", delete=False)
    try:
        for s in symbols:
            tmp.write(s + "\n")
        tmp.flush()
        tmp.close()
        return _call_command_captured(cmd_name, tickers_file=tmp.name, **kwargs)
    finally:
        try:
            Path(tmp.name).unlink(missing_ok=True)
        except Exception:
            pass

# ---------------------------------------------------------------------
# Healthz (warm-up/readiness para jobs) — único y con auth
# ---------------------------------------------------------------------

@csrf_exempt
@require_http_methods(["GET"])
def healthz(request):
    """
    Health-check simple para jobs (warm-up).
    - Requiere X-Internal-Token (igual que el resto).
    - Verifica DB (SELECT 1) y cache (set/get pequeño).
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    db_ok, cache_ok = True, True
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except Exception:
        db_ok = False

    try:
        cache_key = "healthz_ping"
        cache.set(cache_key, "pong", 30)
        cache_ok = cache.get(cache_key) == "pong"
    except Exception:
        cache_ok = False

    http_status = 200 if (db_ok and cache_ok) else 503
    return JsonResponse(
        {
            "ok": db_ok and cache_ok,
            "db": db_ok,
            "cache": cache_ok,
            "ts": timezone.now().isoformat(),
        },
        status=http_status,
    )

# ---------------------------------------------------------------------
# Endpoints internos (precios/indicadores)
# ---------------------------------------------------------------------

@csrf_exempt
@require_http_methods(["POST"])
def update_last_candle(request):
    """
    Actualiza la(s) última(s) vela(s) EOD para el universo:
      - Prioriza /v2/aggs/grouped/... (una sola request)
      - Fallback per-símbolo (en tandas cortas)

    Novedades:
      - backfill_days: reponer N últimos días hábiles (loop de más viejo → más nuevo)
      - auto_backfill: deduce cuántos días faltan hasta el último hábil actual (con tope)
        * auto_mode = "global" (default): estima con un ancla global rápida
        * auto_mode = "per_symbol": mira una muestra de símbolos para anclar en el peor caso

    Siempre responde JSON (200 completo, 206 si partial=true, 200 con partial=true en errores).
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    body = _json_body(request)

    # ---------- Fecha objetivo ----------
    forced_prev_day = False
    date_str = body.get("date")
    if date_str:
        try:
            target_date = dt.date.fromisoformat(date_str)
        except Exception:
            return json_error("date inválida (YYYY-MM-DD)")
        if target_date >= timezone.localdate():
            target_date = last_us_trading_day(timezone.localdate() - dt.timedelta(days=1))
            forced_prev_day = True
    else:
        target_date = last_us_trading_day(timezone.localdate() - dt.timedelta(days=1))
        forced_prev_day = True

    # ---------- Universo ----------
    symbols = _symbols_from_request(body)
    if not symbols:
        return json_error("No hay símbolos para procesar (universe vacío).")

    # ---------- Config / echo ----------
    cfg = {
        "INTERNAL_MAX_SECONDS": float(os.getenv("INTERNAL_MAX_SECONDS", "25")),
        "INTERNAL_SLEEP": float(os.getenv("INTERNAL_SLEEP", "0.20")),
        "INTERNAL_FALLBACK_BURST": int(os.getenv("INTERNAL_FALLBACK_BURST", "10")),
        "POLYGON_RPM": int(os.getenv("POLYGON_RPM", "5")),
    }
    chosen_mode = (
        str(body.get("universe_mode", "custom")).lower().strip()
        if not body.get("symbols") and not body.get("all_active")
        else None
    )

    # ---------- Parámetros de backfill ----------
    # Tope de seguridad
    try:
        max_backfill_days = int(os.getenv("MAX_BACKFILL_DAYS", "5"))
    except Exception:
        max_backfill_days = 5
    if max_backfill_days < 1:
        max_backfill_days = 1

    # Manual (fallback por defecto)
    try:
        backfill_days = int(body.get("backfill_days", 1))
    except Exception:
        backfill_days = 1
    if backfill_days < 1:
        backfill_days = 1

    # Auto-backfill
    auto_backfill = bool(body.get("auto_backfill", False))
    auto_mode = str(body.get("auto_mode", "global")).strip().lower()  # "global" | "per_symbol"

    # Helpers para auto
    def _trading_days_between_exclusive(start_d: dt.date, end_d: dt.date, cap: int) -> int:
        """
        Cuenta cuántos días hábiles hay en (start_d, end_d], limitado por cap.
        Si start_d >= end_d → 0.
        """
        if not (isinstance(start_d, dt.date) and isinstance(end_d, dt.date)):
            return 0
        if start_d >= end_d:
            return 0
        count = 0
        d = end_d
        while d > start_d and count < cap:
            count += 1
            d = last_us_trading_day(d - dt.timedelta(days=1))
        return count

    def _auto_anchor_date(symbols_list: list[str]) -> dt.date:
        """
        Devuelve la fecha "ancla" desde la que faltaría backfillear:
          - global: intenta una estimación rápida
          - per_symbol: mira una muestra de símbolos (mínima última fecha)
        Fallback robusto: si algo falla, devuelve target_date - 1 hábil (o sea, backfill 1).
        """
        # Primero intentamos un ancla global vía DB (si está PriceDaily)
        if auto_mode == "global":
            try:
                from django.db.models import Max
                # Import lazy para no romper si cambia el modelo
                from market.models import PriceDaily  # tu modelo correcto
                qs = PriceDaily.objects.all()
                # Si el modelo tiene campo 'symbol' y no FK, este filtro puede cambiar.
                # Si tiene FK 'ticker', no filtramos por símbolos para que sea más barato.
                last = qs.aggregate(m=Max("date"))["m"]
                if isinstance(last, dt.date):
                    return last
            except Exception:
                pass  # fallback abajo

        # per_symbol (o fallback del global)
        sample_n = 0
        try:
            sample_n = int(os.getenv("AUTO_BACKFILL_SAMPLE", "40"))
        except Exception:
            sample_n = 40
        sample = symbols_list[:max(1, sample_n)]
        last_dates: list[dt.date] = []

        try:
            # Evitamos heavy imports arriba
            from utils.data_access import prices_df
            for s in sample:
                try:
                    df = prices_df(symbol=s, tf="daily")
                    if df is None or df.empty:
                        continue
                    # asumimos columna 'date' o índice datelike
                    # normalizamos a date
                    if "date" in df.columns:
                        d = df["date"].iloc[-1]
                    else:
                        d = df.index[-1]
                    if hasattr(d, "date"):
                        d = d.date()
                    if isinstance(d, dt.date):
                        last_dates.append(d)
                except Exception:
                    continue
            if last_dates:
                # ancla = peor caso de la muestra (mínima fecha más reciente)
                return min(last_dates)
        except Exception:
            pass

        # Fallback final: target_date - 1 hábil (equivale a suponer falta 1)
        return last_us_trading_day(target_date - dt.timedelta(days=1))

    # Si auto_backfill está activo, prevalece sobre backfill_days manual
    if auto_backfill:
        anchor = _auto_anchor_date(symbols)
        gap = _trading_days_between_exclusive(anchor, target_date, cap=max_backfill_days)
        backfill_days = max(1, min(gap if gap > 0 else 1, max_backfill_days))

    # ---------- Construcción de fechas ----------
    dates: list[dt.date] = []
    d = target_date
    for _ in range(backfill_days):
        dates.append(d)
        d = last_us_trading_day(d - dt.timedelta(days=1))
    dates = sorted(set(dates))  # defensa y orden ascendente

    # ---------- Ejecución ----------
    runs = []
    partial_any = False
    last_stats = None

    for d in dates:
        try:
            stats = update_last_candle_grouped_then_fallback(symbols, d)
        except Exception as e:
            stats = {"error": str(e), "partial": True}
        partial_any = partial_any or bool(stats.get("partial"))
        last_stats = stats
        runs.append({"date": d.isoformat(), "stats": stats})

    http_status = 200 if not partial_any else 206

    return json_ok(
        {
            "date": target_date.isoformat(),
            "forced_prev_day": forced_prev_day,
            "count_symbols": len(symbols),
            "mode": chosen_mode,
            "config": cfg,
            "stats": last_stats,   # del target_date (última corrida)
            "runs": runs,          # detalle de todas las fechas
            "partial": partial_any,
            "auto_backfill": auto_backfill,
            "auto_mode": auto_mode,
            "backfill_days_effective": backfill_days,
            "max_backfill_days": max_backfill_days,
        },
        status=http_status,
    )


@csrf_exempt
@require_http_methods(["POST", "GET"])
def precalc(request):
    """
    Precalcula indicadores y señales (diario y semanal) en modo time-box usando el servicio
    compute_indicators_and_signals_all() de simple_jobs.py.
    Devuelve 200 (todo completo) o 206 (partial=true) y NUNCA 500 HTML.
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    body = _json_body(request) if request.method == "POST" else {}
    mode = None if (body.get("symbols") or body.get("all_active")) else str(body.get("universe_mode", "custom")).lower().strip()

    # El servicio resuelve universo, aplica time-boxing, persiste progreso y nunca lanza excepciones.
    res = compute_indicators_and_signals_all(mode)
    status_code = 200 if (res.get("ok") and not res.get("partial")) else (206 if res.get("ok") else 200)
    return JsonResponse(res, status=status_code)

# ---------------------------------------------------------------------
# NUEVO: Company profile & Fundamentals (Finnhub)
# ---------------------------------------------------------------------

@csrf_exempt
@require_http_methods(["POST"])
def sync_company_profile(request):
    """
    Sincroniza CompanyProfile desde Finnhub.
    Auth: X-Internal-Token
    Body JSON (ejemplos):
      {}                                  → usa universo 'custom' por defecto
      {"universe_mode":"sp100"}           → usa SP100
      {"all_active": true}                → todos los Ticker.is_active
      {"symbols": ["AAPL","MSFT"]}        → subset explícito
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    body = _json_body(request)
    symbols = _symbols_from_request(body)
    if not symbols:
        return json_error("No hay símbolos para procesar.")

    try:
        stdout = _call_command_with_symbols("sync_company_profile", symbols)
        return json_ok({"count": len(symbols), "stdout": stdout})
    except Exception as e:
        # Mantener JSON; si falla, informar sin romper workflow
        return json_error("internal_error", status=500, detail=str(e))

@csrf_exempt
@require_http_methods(["POST"])
def sync_fundamentals(request):
    """
    Sincroniza Fundamentals (Finnhub basic_financials).
    Auth: X-Internal-Token
    Body JSON (ejemplos):
      {}                                               → universo 'custom'
      {"universe_mode":"sp100"}                        → usa SP100
      {"all_active": true}                             → todos los Ticker.is_active
      {"symbols": ["AAPL","MSFT"]}                     → subset explícito
      {"metrics": "eps,roeTTM,fcfMargin"}              → métricas específicas (opcional)
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    body = _json_body(request)
    symbols = _symbols_from_request(body)
    metrics = str(body.get("metrics", "")).strip()
    if not symbols:
        return json_error("No hay símbolos para procesar.")

    kwargs: Dict[str, Any] = {}
    if metrics:
        kwargs["metrics"] = metrics

    try:
        stdout = _call_command_with_symbols("sync_fundamentals", symbols, **kwargs)
        return json_ok({"count": len(symbols), "metrics": metrics or None, "stdout": stdout})
    except Exception as e:
        return json_error("internal_error", status=500, detail=str(e))

@csrf_exempt
@require_http_methods(["POST"])
def backfill_range(request):
    """
    Backfill de velas EOD para un rango de fechas [start, end] (YYYY-MM-DD).
    Reintenta localmente si una fecha queda parcial.
    """
    if not _auth_ok(request):
        return json_error("forbidden", status=403)

    body = _json_body(request)
    try:
        start = dt.date.fromisoformat(body["start"])
        end   = dt.date.fromisoformat(body["end"])
    except Exception:
        return json_error("Parámetros 'start'/'end' inválidos (YYYY-MM-DD).")

    symbols = _symbols_from_request(body)
    if not symbols:
        return json_error("No hay símbolos para procesar.")

    out = []
    d = start
    while d <= end:
        try:
            stats = update_last_candle_grouped_then_fallback(symbols, d)
        except Exception as e:
            out.append({"date": d.isoformat(), "error": str(e), "partial": True})
            d += dt.timedelta(days=1)
            continue

        entry: Dict[str, Any] = {"date": d.isoformat(), "stats": stats}
        # reintentos suaves si quedó parcial
        if stats.get("partial"):
            try:
                stats2 = update_last_candle_grouped_then_fallback(symbols, d)
                entry["retry1"] = stats2
                if stats2.get("partial"):
                    stats3 = update_last_candle_grouped_then_fallback(symbols, d)
                    entry["retry2"] = stats3
            except Exception as e2:
                entry["retry_error"] = str(e2)
                entry["partial"] = True

        out.append(entry)
        d += dt.timedelta(days=1)

    return json_ok({"result": out})
