# activo/management/commands/sync_fundamentals.py
from __future__ import annotations

import json
import os
import time
from typing import Iterable, List, Dict, Any, Set

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction

import finnhub
from finnhub.exceptions import FinnhubAPIException

from market.models import Ticker
from activo.models import FundamentalMetric

# -------------------- Config por defecto --------------------

RPM = 25
BASE_SLEEP = max(0.0, 60.0 / RPM)  # pausa base entre requests

DEFAULT_METRICS = [
    "eps", "grossMargin", "netMargin", "operatingMargin",
    "peTTM", "roaTTM", "roeTTM", "roicTTM", "fcfMargin", "totalDebtToTotalAsset",
]

PROGRESS_DIR = ".cache"
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "sync_fundamentals_progress.json")


# -------------------- Helpers --------------------

def _mk_client() -> finnhub.Client:
    api_key = getattr(settings, "FINNHUB_APIKEY", None)
    if not api_key:
        raise RuntimeError("FINNHUB_APIKEY no configurada en settings/env.")
    return finnhub.Client(api_key=api_key)


def _symbols_base(symbols: Iterable[str] | None) -> List[str]:
    if symbols:
        return sorted({s.strip().upper() for s in symbols if s and s.strip()})
    # Por defecto: todos los activos marcados como activos en Ticker
    return list(Ticker.objects.filter(is_active=True).values_list("symbol", flat=True))


def _ensure_progress_dir() -> None:
    try:
        os.makedirs(PROGRESS_DIR, exist_ok=True)
    except Exception:
        pass


def _load_progress() -> Set[str]:
    if not os.path.exists(PROGRESS_FILE):
        return set()
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        done = set(data.get("done", []))
        return {str(s).upper() for s in done}
    except Exception:
        return set()


def _save_progress(done: Set[str]) -> None:
    _ensure_progress_dir()
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"done": sorted(list(done))}, f, ensure_ascii=False, indent=2)
    except Exception:
        # no romper si no se puede guardar progreso
        pass


def _is_rate_limit(e: Exception) -> bool:
    # FinnhubAPIException con status 429 o mensaje indicativo
    if isinstance(e, FinnhubAPIException):
        try:
            # FinnhubAPIException suele tener .status_code
            if getattr(e, "status_code", None) == 429:
                return True
        except Exception:
            pass
        if "429" in str(e) or "API limit reached" in str(e):
            return True
    # fallback genérico por si el cliente cambia
    return "429" in str(e) or "rate limit" in str(e).lower()


def _iter_series(series: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    """
    Normaliza la estructura Finnhub de series:
      series = {
        "annual":    { "<metric>": [ {period:"YYYY-MM-DD", v: ...}, ... ], ... },
        "quarterly": { "<metric>": [ ... ], ... }
      }
    Devuelve filas con: metric, period_end (YYYY-MM-DD), value.
    """
    out: List[Dict[str, Any]] = []
    if not series or not isinstance(series, dict):
        return out

    def _coerce_rows(arr: list[dict], metric_name: str) -> None:
        for row in arr or []:
            dt = row.get("period") or row.get("date")
            val = row.get("v") if "v" in row else row.get("value")
            if dt is None or val is None:
                continue
            out.append({
                "metric": metric_name,
                "period_end": str(dt)[:10],
                "value": float(val),
            })

    # NOTA: En este helper NO discriminamos por métrica; eso se filtra en el caller.
    # Este helper arma una lista plana si le pasás {"annual": [..], "quarterly":[..]} para UNA métrica.
    if "annual" in series and isinstance(series["annual"], list):
        # forma alternativa (no usada en nuestra llamada actual)
        pass  # cubierto por la rama "dict por período" más abajo

    # Se espera dict por período -> list
    for per in ("annual", "quarterly"):
        val = series.get(per)
        # Si val es {metric -> list}, eso lo maneja el caller. Este helper lo usamos por-métrica.
        if isinstance(val, list):
            # formato no esperado aquí, pero lo soportamos por robustez
            _coerce_rows(val, metric_name="(unknown)")
    return out  # Sin uso en esta versión (dejado por compatibilidad)


# -------------------- Comando --------------------

class Command(BaseCommand):
    help = "Sincroniza Fundamentals desde Finnhub (company_basic_financials). Resume por archivo de progreso."

    def add_arguments(self, parser):
        parser.add_argument("--symbols", nargs="*", help="Símbolos explícitos. Si se omite → Ticker.is_active=True.")
        parser.add_argument("--metrics", type=str,
                            help="Lista separada por coma (p.ej: eps,roaTTM,roeTTM). Default: set razonable.")
        parser.add_argument("--sleep", type=float, default=BASE_SLEEP,
                            help=f"Sleep base entre requests para rate limit. Default ~{BASE_SLEEP:.2f}s")
        parser.add_argument("--retries", type=int, default=6, help="Reintentos en 429 con backoff exponencial.")
        parser.add_argument("--resume", action="store_true", default=True,
                            help="(default) Reanuda desde el último progreso.")
        parser.add_argument("--fresh", action="store_true", default=False,
                            help="Ignora el progreso previo y procesa todos los símbolos.")

    def handle(self, *args, **opts):
        symbols = _symbols_base(opts.get("symbols"))
        metrics = [m.strip() for m in (opts.get("metrics") or "").split(",") if m.strip()] or DEFAULT_METRICS
        sleep_s = float(opts.get("sleep") or BASE_SLEEP)
        retries = int(opts.get("retries") or 6)
        do_fresh = bool(opts.get("fresh"))
        do_resume = bool(opts.get("resume")) and not do_fresh

        # Progress
        done: Set[str] = set()
        if do_resume:
            done = _load_progress()

        # Filtro: si resumimos, evitamos los ya hechos
        if done:
            symbols = [s for s in symbols if s.upper() not in done]

        cli = _mk_client()
        ok = 0
        skipped = 0
        errors = 0
        upserts = 0

        total_activos = Ticker.objects.filter(is_active=True).count()
        self.stdout.write(self.style.NOTICE(
            f"Fundamentals: procesando {len(symbols)} símbolos (total activos={total_activos}; "
            f"metrics={metrics})…"
        ))

        for sym in symbols:
            sym = sym.upper().strip()
            # Validar que exista el Ticker (FK requerida por el modelo)
            try:
                ticker_obj = Ticker.objects.get(symbol=sym)
            except Ticker.DoesNotExist:
                skipped += 1
                done.add(sym)
                _save_progress(done)
                continue

            # Reintento con backoff exponencial ante 429
            attempt = 0
            while True:
                try:
                    data = cli.company_basic_financials(symbol=sym, metric="all")

                    if not data or (not data.get("metric") and not data.get("series")):
                        # ETFs/commodities suelen devolver vacío: lo anotamos como “skip”
                        skipped += 1
                        ok += 1  # se procesó sin error
                        done.add(sym)
                        _save_progress(done)
                        time.sleep(sleep_s)
                        break

                    # ----- Series con fecha (guardamos en FundamentalMetric) -----
                    ser_all = data.get("series") or {}
                    annual_map = ser_all.get("annual") or {}
                    quarterly_map = ser_all.get("quarterly") or {}

                    # Guardar por cada métrica solicitada
                    with transaction.atomic():
                        for m in metrics:
                            ann_list = annual_map.get(m) or []
                            qtr_list = quarterly_map.get(m) or []

                            # Transformar a filas planas -> period_end + value
                            rows: List[Dict[str, Any]] = []
                            for row in ann_list:
                                dt = row.get("period") or row.get("date")
                                val = row.get("v") if "v" in row else row.get("value")
                                if dt is None or val is None:
                                    continue
                                rows.append({"period_end": str(dt)[:10], "value": float(val)})

                            for row in qtr_list:
                                dt = row.get("period") or row.get("date")
                                val = row.get("v") if "v" in row else row.get("value")
                                if dt is None or val is None:
                                    continue
                                rows.append({"period_end": str(dt)[:10], "value": float(val)})

                            # Upsert por (ticker, metric, period_end)
                            for r in rows:
                                FundamentalMetric.objects.update_or_create(
                                    ticker=ticker_obj,
                                    metric=m,
                                    period_end=r["period_end"],
                                    defaults={"value": r["value"]},
                                )
                                upserts += 1

                    ok += 1
                    done.add(sym)
                    _save_progress(done)
                    time.sleep(sleep_s)
                    break  # listo este símbolo

                except Exception as e:
                    if _is_rate_limit(e) and attempt < retries:
                        attempt += 1
                        wait = sleep_s * (2 ** (attempt - 1))
                        self.stdout.write(self.style.WARNING(
                            f"[{sym}] 429 rate limit — retry {attempt}/{retries} en {wait:.1f}s"
                        ))
                        time.sleep(wait)
                        continue  # reintenta

                    # Cualquier otro error (o superó los reintentos)
                    errors += 1
                    self.stdout.write(self.style.ERROR(f"[{sym}] {type(e).__name__}: {e}"))
                    # igual marcamos como done para no bloquear el progreso general;
                    # si querés reintentar luego, corré con --fresh o borrá del progress el símbolo problemático
                    done.add(sym)
                    _save_progress(done)
                    time.sleep(sleep_s)
                    break

        self.stdout.write(self.style.SUCCESS(
            f"Fundamentals OK={ok} skipped={skipped} errors={errors} upserts={upserts} "
            f"— progreso en {PROGRESS_FILE}"
        ))

