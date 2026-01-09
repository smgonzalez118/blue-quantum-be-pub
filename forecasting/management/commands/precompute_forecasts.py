# forecasting/management/commands/precompute_forecasts.py
from __future__ import annotations

import gc
from typing import Iterable, List, Optional

from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings

from market.models import Ticker
from forecasting.models import ForecastResult
from forecasting.services_global import (
    predict_rows_for,
    list_available_horizons,
    _last_train_end_from_prices,
)
from forecasting.io import invalidate_model_cache
from utils.data_access import prices_df


# ------------------------- Helpers -------------------------

def _parse_horizons(cli_value: Optional[str]) -> List[int]:
    """
    Prioridad:
      1) --horizons="21,63"  (también acepta 'h021,h063')
      2) FORECAST_HORIZONS env (mismo formato)
      3) detección automática por archivos model_hXXX.pkl en artifacts
    """
    def _parse(s: str) -> List[int]:
        out: List[int] = []
        for p in (s or "").split(","):
            p = p.strip().lower()
            if not p:
                continue
            if p.startswith("h"):
                p = p[1:]
            if p.endswith("d"):
                p = p[:-1]
            try:
                out.append(int(p))
            except Exception:
                pass
        return sorted(set(out))

    if cli_value:
        return _parse(cli_value)

    env = getattr(settings, "FORECAST_HORIZONS", None)
    if isinstance(env, (list, tuple)):
        return sorted(set(int(x) for x in env))
    if isinstance(env, str):
        hs = _parse(env)
        if hs:
            return hs

    return list_available_horizons()


def _symbols(syms: Optional[Iterable[str]]) -> List[str]:
    if syms:
        return sorted({s.strip().upper() for s in syms if s and s.strip()})
    return list(Ticker.objects.filter(is_active=True).values_list("symbol", flat=True))


def _last_close(sym: str) -> float | None:
    """Último close/adj_close del símbolo desde tu store."""
    try:
        df = prices_df(symbol=sym, tf="daily")
        if df is None or df.empty:
            return None
        col = "adj_close" if "adj_close" in df.columns else "close"
        s = df[col].astype(float).dropna()
        return float(s.iloc[-1]) if len(s) else None
    except Exception:
        return None


def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _extract_last_pred(rows: list) -> float | None:
    """
    Extrae un valor de predicción de la última fila de 'rows',
    tolerando distintos formatos: {"price_pred"} | {"yhat"} | {"y"} | valor suelto.
    """
    if not rows:
        return None
    last = rows[-1]
    if isinstance(last, dict):
        return _to_float(last.get("price_pred") or last.get("yhat") or last.get("y") or last.get("value"))
    return _to_float(last)


# ------------------------- Comando -------------------------

class Command(BaseCommand):
    help = (
        "Genera y guarda predicciones DIARIAS para los tickers activos (o los pasados por --symbols) "
        "usando los artefactos en forecasting/artifacts (model_hXXX.pkl). "
        "Guarda un yhat compacto por (ticker, horizon, train_end). "
        "Optimizado para bajo uso de memoria."
    )

    def add_arguments(self, parser):
        parser.add_argument("--symbols", nargs="*", help="Símbolos a procesar. Si se omite → todos activos.")
        parser.add_argument("--horizons", type=str, default=None,
                            help='Override de horizontes, ej: "21,63" o "h021,h063".')
        parser.add_argument("--model-name", type=str,
                            default=getattr(settings, "FORECAST_MODEL_NAME", "rf"),
                            help="Etiqueta del modelo (p.ej. 'rf' | 'lgbm').")
        parser.add_argument("--keep-history", action="store_true",
                            help="No borrar versiones anteriores (mantener historial).")
        parser.add_argument("--skip-if-unchanged", action="store_true",
                            help="Si no cambió train_end, saltar ese ticker+horizon.")
        parser.add_argument("--sleep", type=float, default=0.0,
                            help="Pausa (segundos) entre símbolos para bajar picos.")

    def _latest_saved_train_end(self, t: Ticker, model_name: str, horizon: int):
        return (ForecastResult.objects
                .filter(ticker=t, timeframe="daily", model_name=model_name, horizon=horizon)
                .order_by("-train_end")
                .values_list("train_end", flat=True)
                .first())

    def handle(self, *args, **opts):
        import time

        symbols     = _symbols(opts.get("symbols"))
        horizons    = _parse_horizons(opts.get("horizons"))
        model_name  = opts.get("model_name")
        keep_hist   = bool(opts.get("keep_history"))
        skip_same   = bool(opts.get("skip_if_unchanged"))
        sleep_s     = float(opts.get("sleep") or 0.0)

        if not horizons:
            self.stderr.write(self.style.ERROR(
                "No hay horizontes disponibles (ni env FORECAST_HORIZONS ni archivos model_hXXX.pkl)."
            ))
            return

        self.stdout.write(self.style.NOTICE(
            f"Forecasts: {len(symbols)} símbolos — horizons={horizons} — model={model_name} "
            f"— keep_history={'yes' if keep_hist else 'no'}"
        ))

        ok = errors = upserts = skipped = 0

        for i, sym in enumerate(symbols, start=1):
            try:
                t = Ticker.objects.get(symbol=sym)
            except Ticker.DoesNotExist:
                self.stderr.write(self.style.WARNING(f"[{sym}] ticker no existe — skip"))
                skipped += 1
                continue

            # Vela más reciente disponible
            try:
                train_end = _last_train_end_from_prices(sym)
            except Exception as e:
                self.stderr.write(self.style.WARNING(f"[{sym}] sin precios ({type(e).__name__}) — skip"))
                skipped += 1
                continue

            price_now_hint = _last_close(sym)

            for h in horizons:
                try:
                    if skip_same:
                        last_saved = self._latest_saved_train_end(t, model_name, h)
                        if last_saved == train_end:
                            skipped += 1
                            self.stdout.write(self.style.WARNING(f"[{sym}] h={h} sin vela nueva — skip"))
                            continue

                    payload = predict_rows_for(sym, h)  # {'train_end': date, 'rows': [...]}
                    price_pred = _extract_last_pred(payload.get("rows") or [])
                    if price_pred is None:
                        raise ValueError("no se pudo inferir price_pred del payload")

                    yhat_doc = {
                        "price_now": price_now_hint,
                        "price_pred": price_pred,
                        "ret_pct_pred": (
                            None if (price_now_hint in (None, 0))
                            else round((price_pred / price_now_hint - 1.0) * 100.0, 6)
                        ),
                    }

                    with transaction.atomic():
                        obj, _ = ForecastResult.objects.update_or_create(
                            ticker=t,
                            timeframe="daily",
                            model_name=model_name,
                            train_end=train_end,    # usamos la del dataset
                            horizon=h,
                            defaults={
                                "yhat": yhat_doc,
                                "metrics": {"price_now": price_now_hint} if price_now_hint is not None else None,
                                "model_version": f"h{h:03d}",
                            },
                        )
                        upserts += 1

                        # si no mantenemos historial, dejamos solo la versión actual
                        if not keep_hist:
                            (ForecastResult.objects
                             .filter(ticker=t, timeframe="daily", model_name=model_name, horizon=h)
                             .exclude(pk=obj.pk)
                             .delete())

                    ok += 1
                    self.stdout.write(self.style.SUCCESS(f"[{sym}] h={h} ✔  pred={price_pred:.6f}"))

                except Exception as e:
                    errors += 1
                    self.stderr.write(self.style.ERROR(f"[{sym}] h={h} {type(e).__name__}: {e}"))

                finally:
                    # liberar memoria entre horizontes
                    invalidate_model_cache()
                    gc.collect()

            # pausa opcional y GC adicional cada 10 símbolos
            if sleep_s > 0:
                time.sleep(sleep_s)
            if i % 10 == 0:
                gc.collect()

        self.stdout.write(self.style.SUCCESS(
            f"Done — OK={ok} errors={errors} upserts={upserts} skipped={skipped}"
        ))
