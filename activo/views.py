# analisis_detallado.py  (drop-in adaptado)
from __future__ import annotations
from typing import List, Tuple, Optional

import logging
import traceback
import time
import os

import numpy as np
import pandas as pd

from django.db import transaction, connections
from django.db.models import Q
from django.db.utils import OperationalError
from rest_framework import viewsets
from rest_framework.decorators import api_view, action, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from decouple import config

# === MODELOS / SERIALIZERS ===
from market.models import Ticker
from .models import ReporteTecnico
from .serializers import ReporteTecnicoSerializer
from dashboard.models import AtributoDashboard
from activo.models import CompanyProfile

# === DATA ACCESS ===
from utils.data_access import prices_df
from utils.csv_loader import cargar_csv_local  # fallback opcional p/ benchmark

# === CONFIG ===
BENCH_TICKER = config("BENCH_TICKER", default="SPY")

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Helpers de fechas/series
# --------------------------------------------------------------------------------------

def _normalize_date_col(df: pd.DataFrame, date_col: str = "date", normalize: bool = True) -> pd.DataFrame:
    x = df.copy()
    x[date_col] = pd.to_datetime(x[date_col], errors="coerce").dt.tz_localize(None)
    if normalize:
        x[date_col] = x[date_col].dt.normalize()
    return x.dropna(subset=[date_col])


def _infer_freq(dates: pd.Series) -> str:
    d = pd.to_datetime(dates, errors="coerce").dropna().sort_values()
    if len(d) < 3:
        return "B"
    med = d.diff().dropna().dt.days.median()
    return "B" if (med is not None and med <= 3) else "W-FRI"


def _to_week_wfri_last(df: pd.DataFrame, value_cols: Tuple[str, ...] = ("close",)) -> pd.DataFrame:
    x = df.copy().sort_values("date").set_index("date")
    agg = {c: "last" for c in value_cols if c in x.columns}
    out = x.resample("W-FRI").agg(agg).dropna().reset_index()
    return out


def _to_business_day(df: pd.DataFrame, value_cols: Tuple[str, ...] = ("close",)) -> pd.DataFrame:
    x = df.copy().sort_values("date").set_index("date")
    x = x[~x.index.duplicated(keep="last")]
    out = x.asfreq("B")
    for c in value_cols:
        if c in out.columns:
            out[c] = out[c].ffill()
    return out.dropna(subset=list(value_cols)).reset_index()


def _pick_price_col(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    if "adj_close" in x.columns:
        x = x.rename(columns={"adj_close": "close"})
    if "close" not in x.columns:
        raise ValueError("dataset sin columna close/adj_close")
    return x


# --------------------------------------------------------------------------------------
# Helpers de reporte técnico / upsert robusto
# --------------------------------------------------------------------------------------

def _safe_upsert_reporte(*, ticker: str, tf_code: str, defaults: dict,
                         retries: int = 5, base_sleep: float = 0.08):
    """
    Upsert sin update_or_create (evita select_for_update interno).
    Reintenta en caso de 'database is locked' con backoff exponencial.
    """
    last_err = None
    update_fields = list(defaults.keys())
    for i in range(retries):
        try:
            with transaction.atomic():
                try:
                    obj = ReporteTecnico.objects.get(ticker=ticker, timeframe=tf_code)
                    for k, v in defaults.items():
                        setattr(obj, k, v)
                    obj.save(update_fields=update_fields)
                    return obj, False
                except ReporteTecnico.DoesNotExist:
                    obj = ReporteTecnico(ticker=ticker, timeframe=tf_code, **defaults)
                    obj.save()
                    return obj, True
        except OperationalError as e:
            last_err = e
            connections.close_all()
            time.sleep(base_sleep * (2 ** i))
        except Exception as e:
            last_err = e
            break
    raise last_err


def _tf_norm_in(s: str | None) -> str:
    """Normaliza input del front a 'D'/'W'."""
    s = (s or "D").upper()
    if s in ("DIARIO", "DAILY"): return "D"
    if s in ("SEMANAL", "WEEKLY"): return "W"
    if s in ("D", "W"): return s
    return "D"


# --------------------------------------------------------------------------------------
# Empresa / comparables
# --------------------------------------------------------------------------------------

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def info_empresa(request, ticker: str):
    sym = (ticker or "").upper().strip()
    try:
        t = Ticker.objects.get(symbol=sym)
    except Ticker.DoesNotExist:
        return Response({"detail": f"Ticker no encontrado: {sym}"}, status=404)

    row = (
        CompanyProfile.objects
        .filter(ticker=t)
        .values(
            "name", "country", "exchange", "currency", "sector", "industry",
            "market_cap", "shares_outstanding", "logo", "weburl"
        )
        .first()
    ) or {}

    out = {
        "name": row.get("name") or sym,
        "logo": row.get("logo") or "",
        "weburl": row.get("weburl") or "",
        "country": row.get("country") or "",
        "currency": row.get("currency") or "",
        "exchange": row.get("exchange") or "",
        "ipo": "",
        "marketCapitalization": float(row["market_cap"]) if row.get("market_cap") is not None else None,
        "shareOutstanding": float(row["shares_outstanding"]) if row.get("shares_outstanding") is not None else None,
        "finnhubIndustry": row.get("industry") or row.get("sector") or "",
        "phone": "",
        "ticker": sym,
    }
    return Response(out)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def comparables(request, ticker: str):
    sym = (ticker or "").upper().strip()
    try:
        t = Ticker.objects.get(symbol=sym)
    except Ticker.DoesNotExist:
        return Response([])

    me = CompanyProfile.objects.filter(ticker=t).values("sector", "industry").first()
    if not me:
        return Response([])

    sector = (me.get("sector") or "").strip()
    industry = (me.get("industry") or "").strip()

    from django.db.models import Q
    q = Q()
    if industry:
        q &= Q(industry=industry)
    if sector:
        q &= Q(sector=sector)

    peers = (
        CompanyProfile.objects
        .filter(q)
        .exclude(ticker=t)
        .values_list("ticker__symbol", flat=True)[:12]
    )
    return Response(sorted([p.upper() for p in peers]))


# --------------------------------------------------------------------------------------
# Cambios recientes
# --------------------------------------------------------------------------------------

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def cambios_recientes(request, ticker: str):
    try:
        t = str(ticker).upper().strip()
        df = prices_df(symbol=t, tf="daily")
        if df is None or df.empty:
            return Response({"error": "No se pudo obtener historial de precios"}, status=500)

        if df.index.name == "date":
            df = df.reset_index()

        df = _pick_price_col(df[["date", "close"]])
        df = _normalize_date_col(df).sort_values("date")
        df = df[~df["date"].duplicated(keep="last")]

        if _infer_freq(df["date"]) != "B":
            df = _to_business_day(df, ("close",))

        s = pd.to_numeric(df["close"], errors="coerce").dropna()
        s.index = pd.to_datetime(df["date"])
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if s.empty:
            return Response({"error": "Serie de cierres vacía"}, status=500)

        def last_n(n: int) -> Optional[float]:
            return float(s.iloc[-n]) if len(s) >= n else None

        precio_actual   = last_n(1)
        cierre_anterior = last_n(2)
        cierre_semana   = last_n(6)
        cierre_mes      = last_n(22)
        cierre_ano      = last_n(253)

        ultimo_anio = s.index[-1].year
        s_ytd = s[s.index.year == ultimo_anio]
        if len(s_ytd) >= 1:
            cierre_ytd_base = float(s_ytd.iloc[0])
        else:
            prev = s[s.index.year == (ultimo_anio - 1)]
            cierre_ytd_base = float(prev.iloc[-1]) if len(prev) else None

        def pct_change(base):
            if base is None or precio_actual is None or base == 0:
                return None
            return round(100.0 * (precio_actual / base - 1.0), 2)

        data = {
            "ticker": t,
            "precio_actual": round(precio_actual, 2) if precio_actual is not None else None,
            "cierre_anterior": round(cierre_anterior, 2) if cierre_anterior is not None else None,
            "cambio_1d": pct_change(cierre_anterior),
            "cambio_1w": pct_change(cierre_semana),
            "cambio_1m": pct_change(cierre_mes),
            "cambio_1y": pct_change(cierre_ano),
            "cambio_ytd": pct_change(cierre_ytd_base),
        }
        return Response(data, status=200)

    except Exception as e:
        log.error("cambios_recientes error: %s\n%s", e, traceback.format_exc())
        return Response({"error": "Error en cambios_recientes", "detalle": str(e)}, status=500)


# --------------------------------------------------------------------------------------
# Precio + EMAs
# --------------------------------------------------------------------------------------

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def precio_evo(request, ticker, timeframe):
    try:
        t = str(ticker).upper().strip()
        if timeframe not in ["semanal", "diario"]:
            return Response({'error': 'Timeframe inválido. Use "diario" o "semanal".'}, status=400)

        tf = 'weekly' if timeframe == 'semanal' else 'daily'
        df = prices_df(symbol=t, tf=tf)
        if df is None or df.empty:
            return Response({'error': f'Sin datos para {t}.'}, status=500)

        if df.index.name == "date":
            df = df.reset_index()

        df = _pick_price_col(df[["date", "close"]])
        df = _normalize_date_col(df).sort_values("date")
        df = df[~df["date"].duplicated(keep="last")]

        real = _infer_freq(df["date"]) 
        if timeframe == "diario" and real != "B":
            df = _to_business_day(df, ("close",))
        elif timeframe == "semanal" and real == "B":
            df = _to_week_wfri_last(df, ("close",))

        spans = [5, 10, 20, 30, 100] if timeframe == "diario" else [5, 10, 20, 30]
        for span in spans:
            df[f'EMA{span}'] = df['close'].ewm(span=span, adjust=False).mean()

        df = df.tail(252 if timeframe == "diario" else 52)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        cols = ["date", "close"] + [f"EMA{span}" for span in spans]
        return Response(df[cols].round(2).to_dict(orient="records"), status=200)

    except Exception as e:
        log.error("precio_evo error: %s\n%s", e, traceback.format_exc())
        return Response({'error': f'Error en precio_evo: {e}'}, status=500)


# --------------------------------------------------------------------------------------
# Comparativo normalizado
# --------------------------------------------------------------------------------------

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def comparativo_normalizado(request, ticker, timeframe):
    try:
        if timeframe not in ["semanal", "diario"]:
            return Response({'error': 'Timeframe inválido. Use "diario" o "semanal".'}, status=400)
        tf = 'weekly' if timeframe == 'semanal' else 'daily'

        t = str(ticker).upper().strip()
        df_a = prices_df(symbol=t, tf=tf)
        if df_a is None or df_a.empty:
            return Response({'error': f'Sin datos para {t}.'}, status=500)
        if df_a.index.name == "date":
            df_a = df_a.reset_index()
        df_a = _pick_price_col(df_a[["date", "close"]])
        df_a = _normalize_date_col(df_a).sort_values("date")
        df_a = df_a[~df_a["date"].duplicated(keep="last")]

        df_b = prices_df(symbol=BENCH_TICKER, tf=tf)
        if (df_b is None or df_b.empty) and BENCH_TICKER and BENCH_TICKER != t:
            df_b = cargar_csv_local(BENCH_TICKER, tf=tf)
        if df_b is None or df_b.empty:
            return Response({'error': f'Sin datos para benchmark {BENCH_TICKER}.'}, status=500)
        if df_b.index.name == "date":
            df_b = df_b.reset_index()
        df_b = _pick_price_col(df_b[["date", "close"]])
        df_b = _normalize_date_col(df_b).sort_values("date")
        df_b = df_b[~df_b["date"].duplicated(keep="last")]

        if timeframe == "semanal":
            df_a = _to_week_wfri_last(df_a, ("close",))
            df_b = _to_week_wfri_last(df_b, ("close",))
            window = 104; ann = 52.0
        else:
            if _infer_freq(df_a["date"]) != "B":
                df_a = _to_business_day(df_a, ("close",))
            if _infer_freq(df_b["date"]) != "B":
                df_b = _to_business_day(df_b, ("close",))
            window = 252; ann = 252.0

        df = pd.merge(
            df_a.rename(columns={"close": "Activo"}),
            df_b.rename(columns={"close": "Benchmark"}),
            on="date", how="inner"
        ).dropna().sort_values("date")

        if df.empty:
            return Response({'error': 'No hay intersección de fechas.'}, status=500)

        df = df.tail(window)
        a0, a1 = float(df["Activo"].iloc[0]), float(df["Activo"].iloc[-1])
        b0, b1 = float(df["Benchmark"].iloc[0]), float(df["Benchmark"].iloc[-1])
        if a0 == 0 or b0 == 0:
            return Response({'error': 'Valor base 0 para normalización.'}, status=500)

        activo_ret = (a1 / a0 - 1.0) * 100.0
        bench_ret  = (b1 / b0 - 1.0) * 100.0
        spread     = activo_ret - bench_ret

        rets = df[["Activo", "Benchmark"]].pct_change().dropna()
        corr = float(rets["Activo"].corr(rets["Benchmark"])) if len(rets) > 1 else None

        vol_a = float(rets["Activo"].std(ddof=0) * (ann ** 0.5)) * 100.0 if len(rets) > 1 else None
        vol_b = float(rets["Benchmark"].std(ddof=0) * (ann ** 0.5)) * 100.0 if len(rets) > 1 else None

        beta = None
        if len(rets) > 1 and rets["Benchmark"].var(ddof=0) != 0:
            beta = float(rets.cov().loc["Activo", "Benchmark"] / rets["Benchmark"].var(ddof=0))

        metrics = {
            "window": f"{window}{'w' if timeframe=='semanal' else 'd'}",
            "bench_ticker": BENCH_TICKER,
            "activo_ret_pct": round(activo_ret, 2),
            "bench_ret_pct": round(bench_ret, 2),
            "spread_pct": round(spread, 2),
            "corr": None if corr is None else round(corr, 3),
            "beta": None if beta is None else round(beta, 2),
            "vol_ann_activo": None if vol_a is None else round(vol_a, 2),
            "vol_ann_bench": None if vol_b is None else round(vol_b, 2),
        }

        norm = df.copy()
        norm["Activo"]    = (norm["Activo"] / a0) * 100.0
        norm["Benchmark"] = (norm["Benchmark"] / b0) * 100.0
        norm["date"] = norm["date"].dt.strftime("%Y-%m-%d")

        return Response({"series": norm.round(2).to_dict("records"), "metrics": metrics}, status=200)

    except Exception as e:
        log.error("comparativo_normalizado error: %s\n%s", e, traceback.format_exc())
        return Response({'error': f'Error en comparativo_normalizado: {e}'}, status=500)


# --------------------------------------------------------------------------------------
# Volatilidad
# --------------------------------------------------------------------------------------

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def volatilidad(request, ticker, timeframe):
    try:
        t = str(ticker).upper().strip()
        if timeframe not in ["diario", "semanal"]:
            return Response({'error': 'Timeframe inválido'}, status=400)

        tf = 'weekly' if timeframe == 'semanal' else 'daily'
        df = prices_df(symbol=t, tf=tf)
        if df is None or df.empty:
            return Response({'error': f'Sin datos para {t}.'}, status=500)

        if df.index.name == "date":
            df = df.reset_index()
        df = _pick_price_col(df[["date", "close"]])
        df = _normalize_date_col(df).sort_values("date")
        df = df[~df["date"].duplicated(keep="last")]

        real = _infer_freq(df["date"])
        if timeframe == "diario" and real != "B":
            df = _to_business_day(df, ("close",))
            real = "B"
        elif timeframe == "semanal" and real == "B":
            df = _to_week_wfri_last(df, ("close",))
            real = "W-FRI"

        r = np.log1p(df["close"].pct_change())
        if real == "B":
            window = 20
            factor = np.sqrt(252)
        else:
            window = 8
            factor = np.sqrt(52)

        df["Volatilidad"] = r.rolling(window, min_periods=window).std() * factor * 100.0
        df["EMA20Volat"] = df["Volatilidad"].ewm(span=20, adjust=False).mean()
        df["EMA50Volat"] = df["Volatilidad"].ewm(span=50, adjust=False).mean()

        df = df[["date", "Volatilidad", "EMA20Volat", "EMA50Volat"]].dropna()
        df = df.tail(252 if timeframe == "diario" else 52)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        return Response(df.round(2).to_dict(orient="records"), status=200)

    except Exception as e:
        log.error("volatilidad error: %s\n%s", e, traceback.format_exc())
        return Response({'error': f'Error en volatilidad: {e}'}, status=500)


# --------------------------------------------------------------------------------------
# Reporte técnico (usa snapshot de dashboard + upsert robusto)
# --------------------------------------------------------------------------------------

class ReporteTecnicoViewSet(viewsets.ModelViewSet):
    queryset = ReporteTecnico.objects.all().order_by('activo')
    serializer_class = ReporteTecnicoSerializer
    permission_classes = [IsAuthenticated]

    def _build_activo(self, ticker: str) -> str:
        from utils.nombres import get_nombre_ticker as _get_nombre_ticker
        nombre = _get_nombre_ticker(ticker)
        return f"{ticker.upper()} ({nombre})"

    def _copy_from_dashboard(self, ticker: str, tf_code: str) -> tuple[Optional[ReporteTecnico], Optional[dict], Optional[Response]]:
        t = ticker.upper().strip()

        # Sólo lectura (sin atomic aquí)
        try:
            row = (
                AtributoDashboard.objects
                .filter(ticker=t, timeframe=tf_code)
                .values(
                    "ticker", "activo", "precio", "macd",
                    "pmm5", "pmm10", "pmm20", "pmm30",
                    "mm5_10", "mm10_20", "tripleCruce",
                    "pmm100", "rsi", "dmi", "adx",
                )
                .first()
            )
        except Exception as e:
            return None, None, Response({"error": f"DB error leyendo AtributoDashboard: {type(e).__name__}: {e}"}, status=500)

        if not row:
            return None, None, Response(
                {
                    "error": "No hay snapshot precalculado para este ticker/timeframe.",
                    "detalle": f"Falta AtributoDashboard(ticker='{t}', timeframe='{tf_code}'). Corre el precálculo primero."
                },
                status=409,
            )

        # ⚠️ No incluir ticker/timeframe en defaults para evitar kwargs duplicados
        defaults = {
            "activo": row.get("activo") or self._build_activo(t),
            "precio": row.get("precio"),
            "macd": row.get("macd"),
            "pmm5": row.get("pmm5"),
            "pmm10": row.get("pmm10"),
            "pmm20": row.get("pmm20"),
            "pmm30": row.get("pmm30"),
            "mm5_10": row.get("mm5_10"),
            "mm10_20": row.get("mm10_20"),
            "tripleCruce": row.get("tripleCruce"),
            "pmm100": row.get("pmm100", None),
            "rsi": row.get("rsi"),
            "dmi": row.get("dmi"),
            "adx": row.get("adx"),
        }

        try:
            obj, _created = _safe_upsert_reporte(
                ticker=t, tf_code=tf_code, defaults=defaults
            )
        except Exception as e:
            log.error("_safe_upsert_reporte error: %s\n%s", e, traceback.format_exc())
            return None, None, Response({"error": f"No se pudo guardar ReporteTecnico: {e}"}, status=400)

        return obj, defaults, None

    @action(detail=False, methods=['post'], url_path='generar_diario')
    def generar_diario(self, request):
        try:
            symbol = (request.data.get("symbol") or request.data.get("ticker") or "").upper().strip()
            if not symbol:
                return Response({"error": "ticker/symbol requerido"}, status=400)
            obj, payload, err = self._copy_from_dashboard(symbol, "D")
            if err: return err
            data = ReporteTecnicoSerializer(obj).data
            return Response(data, status=200)
        except Exception as e:
            log.error("generar_diario error: %s\n%s", e, traceback.format_exc())
            return Response({"error": str(e)}, status=400)

    @action(detail=False, methods=['post'], url_path='generar_semanal')
    def generar_semanal(self, request):
        try:
            symbol = (request.data.get("symbol") or request.data.get("ticker") or "").upper().strip()
            if not symbol:
                return Response({"error": "ticker/symbol requerido"}, status=400)
            obj, payload, err = self._copy_from_dashboard(symbol, "W")
            if err: return err
            data = ReporteTecnicoSerializer(obj).data
            return Response(data, status=200)
        except Exception as e:
            log.error("generar_semanal error: %s\n%s", e, traceback.format_exc())
            return Response({"error": str(e)}, status=400)


# --------------------------------------------------------------------------------------
# Fundamentals (con fallback directo a DB si el helper devuelve vacío)
# --------------------------------------------------------------------------------------

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def fundamentals(request, ticker):
    """
    Devuelve:
      {
        snapshot: { <metric>: {ultimo, prom_4, prom_8, prom_12}, ... },
        history:  { <metric>: [{period:'YYYY-MM-DD', value: float}, ...], ... }
      }
    - Si utils.fundamentals_db.fundamentals_from_db() ya devuelve snapshot/history, lo usamos.
    - Si no, calculamos directo desde FundamentalMetric.
    """
    sym = str(ticker).upper().strip()
    debug = request.query_params.get("debug") in ("1", "true", "yes")

    # 1) Intento con helper existente
    try:
        from utils.fundamentals_db import fundamentals_from_db
        data = fundamentals_from_db(sym)
        if isinstance(data, dict) and ("snapshot" in data or "history" in data):
            return Response(data, status=200)
        # algunas versiones devuelven {"items":[...]}; las ignoramos y pasamos a fallback
    except Exception as e:
        log.warning("fundamentals_from_db falló: %s", e)

    # 2) Fallback directo a DB
    from activo.models import FundamentalMetric as F
    try:
        t = Ticker.objects.get(symbol=sym)
    except Ticker.DoesNotExist:
        return Response({"snapshot": {}, "history": {}}, status=200)

    # Traemos toda la serie por métrica, ordenada
    rows = (
        F.objects
        .filter(ticker=t)
        .order_by("metric", "period_end")
        .values_list("metric", "period_end", "value")
    )

    from collections import defaultdict

    series = defaultdict(list)   # metric -> [(period, value), ...]
    for m, dt, v in rows:
        if v is None or dt is None:
            continue
        try:
            series[str(m)].append((str(dt), float(v)))
        except Exception:
            # ignora no numéricos
            continue

    # Orden “preferida” primero; el resto alfabético
    PREFERRED = [
        "eps", "fcfMargin", "grossMargin", "netMargin", "operatingMargin",
        "totalDebtToTotalAsset", "peTTM", "roaTTM", "roeTTM", "roicTTM",
    ]
    ordered = [m for m in PREFERRED if m in series] + [
        m for m in sorted(series.keys()) if m not in PREFERRED
    ]

    snapshot = {}
    history  = {}

    for m in ordered:
        seq = series[m]               # [(period, value), ...] ascendente
        if not seq:
            continue

        # --- history: últimos 16 puntos (o todos si hay menos)
        last_n = 16
        history[m] = [{"period": p, "value": round(v, 4)} for p, v in seq[-last_n:]]

        # --- snapshot: agregados sobre la serie completa (usualmente trimestral)
        values = [v for _, v in seq]
        def avg_tail(n):
            return round(sum(values[-n:]) / n, 4) if len(values) >= n else None

        snapshot[m] = {
            "ultimo": round(values[-1], 4),
            "prom_4":  avg_tail(4),
            "prom_8":  avg_tail(8),
            "prom_12": avg_tail(12),
        }

    payload = {"snapshot": snapshot, "history": history}
    if debug:
        payload["meta"] = {"metrics": list(ordered), "rows": sum(len(v) for v in series.values())}
    return Response(payload, status=200)
