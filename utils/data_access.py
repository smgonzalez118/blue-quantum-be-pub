# utils/data_access.py
from __future__ import annotations

import pandas as pd
from django.conf import settings
from django.db.models import Count

from market.models import PriceDaily
from utils.csv_loader import cargar_csv_local


# ------------------------- Normalización común -------------------------

_DATE_CANDS = ["date", "timestamp", "Timestamp", "time", "Date", "Datetime", "INDEX", "Index"]
_COLMAP = {
    # nombres posibles -> nombre estándar
    "open": "open", "Open": "open", "OPEN": "open",
    "high": "high", "High": "high", "HIGH": "high",
    "low": "low",   "Low": "low",   "LOW": "low",
    "close": "close", "Close": "close", "CLOSE": "close", "c": "close", "C": "close",
    "adj_close": "adj_close", "Adj Close": "adj_close", "AdjClose": "adj_close",
    "adjclose": "adj_close", "ADJ_CLOSE": "adj_close", "ADJ CLOSE": "adj_close",
    "PRICE": "close", "Price": "close",
    "volume": "volume", "Volume": "volume", "VOLUME": "volume",
}

def _parse_dates_series(raw: pd.Series) -> pd.Series:
    """
    Parse robusto de fechas:
      - dayfirst=True (dd-mm-aaaa / dd/mm/aaaa)
      - si es numérico, intenta epoch en s/ms/us/ns
      - devuelve tz-naive
    """
    s0 = raw.copy()

    # intento normal (dayfirst)
    s = pd.to_datetime(s0, errors="coerce", dayfirst=True)

    # si casi todo quedó NaT y el origen es numérico, probar epoch
    if s.notna().mean() < 0.6 and pd.api.types.is_numeric_dtype(s0):
        for unit in ("s", "ms", "us", "ns"):
            tmp = pd.to_datetime(s0, errors="coerce", unit=unit)
            if tmp.notna().mean() >= 0.9:
                s = tmp
                break

    try:
        s = s.dt.tz_localize(None)
    except Exception:
        pass
    return s

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un DataFrame con:
      - índice: date (tz-naive, ordenado)
      - columnas: open, high, low, close, adj_close, volume (las que existan)
    Si no puede mapear fecha/cierre, devuelve DF vacío.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])

    df = df.copy()

    # 1) SOLO si el índice ya es DatetimeIndex, pásalo a columna 'date'
    if isinstance(df.index, pd.DatetimeIndex):
        idx_name = df.index.name or "date"
        df = df.reset_index().rename(columns={idx_name: "date"})
    # Importante: NO tratar RangeIndex/Int64Index como fecha (evita 1970 por epoch)

    # 2) Si aún no existe 'date', intenta mapear por candidatos
    if "date" not in df.columns:
        for c in _DATE_CANDS:
            if c in df.columns:
                df = df.rename(columns={c: "date"})
                break

    # 3) Mapear nombres OHLCV a estándar (case-insensitive / variantes)
    rename_dict = {}
    for col in list(df.columns):
        if col in _COLMAP:
            rename_dict[col] = _COLMAP[col]
        else:
            low = str(col).lower()
            if low in _COLMAP:
                rename_dict[col] = _COLMAP[low]
    if rename_dict:
        df = df.rename(columns=rename_dict)

    # 4) Validación mínima
    if "date" not in df.columns or "close" not in df.columns:
        return pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])

    # 5) Tipos y orden
    df["date"] = _parse_dates_series(df["date"])

    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["date", "close"]).sort_values("date")

    # 🔧 si hay fechas repetidas, quedate con la última (o la primera si preferís)
    df = df[~df["date"].duplicated(keep="last")]

    # 6) Setear índice
    df = df.set_index("date")

    # 7) Orden de columnas (las que existan)
    cols = [c for c in ["open", "high", "low", "close", "adj_close", "volume"] if c in df.columns]
    return df[cols]

def _parse_single_date(x):
    # por si algún día se usa start/end en dd-mm-aaaa
    return pd.to_datetime(x, errors="coerce", dayfirst=True)

def _apply_range(df: pd.DataFrame, start, end) -> pd.DataFrame:
    if df.empty:
        return df
    if start is not None:
        df = df[df.index >= _parse_single_date(start)]
    if end is not None:
        df = df[df.index <= _parse_single_date(end)]
    return df

def _resample_weekly(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Diario -> semanal (W-FRI) con OHLCV correcto.
    """
    if df_daily.empty:
        return df_daily

    df = df_daily.sort_index()

    agg = {}
    if "open" in df:      agg["open"] = "first"
    if "high" in df:      agg["high"] = "max"
    if "low"  in df:      agg["low"]  = "min"
    if "close" in df:     agg["close"] = "last"
    if "adj_close" in df: agg["adj_close"] = "last"
    if "volume" in df:    agg["volume"] = "sum"

    out = df.resample("W-FRI").agg(agg)

    # 🔧 Dropeamos semanas solo si falta el close (lo esencial)
    need = [c for c in ["open","high","low","close"] if c in out.columns]
    out = out.dropna(subset=need)

    return out


# ------------------------- API pública -------------------------

def prices_df(
    symbol: str,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    tf: str = "daily",
) -> pd.DataFrame:
    """
    Lee precios desde la BD como DataFrame indexado por fecha.
    Si no hay filas y USE_CSV_FALLBACK=True, cae al CSV usando cargar_csv_local(symbol, tf).
    Soporta tf='daily' (nativo) y tf='weekly' (se arma desde daily).

    Devuelve columnas estandarizadas: open, high, low, close, adj_close, volume
    con índice datetime 'date' (tz-naive), ordenado asc.
    """
    tf = (tf or "daily").lower()

    # ---- 1) BD (siempre daily en BD)
    try:
        qs = PriceDaily.objects.filter(ticker__symbol=symbol.upper())
        if start:
            qs = qs.filter(date__gte=start)
        if end:
            qs = qs.filter(date__lte=end)
        qs = qs.order_by("date").values("date", "open", "high", "low", "close", "adj_close", "volume")
        rows = list(qs)

        if rows:
            df = pd.DataFrame(rows)
            df = _normalize_ohlcv(df)          # normaliza y setea índice 'date'
            df = _apply_range(df, start, end)
            if tf == "weekly":
                return _resample_weekly(df)
            return df

        # ---- 2) Sin filas en BD → fallback opcional a CSV
        if getattr(settings, "USE_CSV_FALLBACK", False):
            df_csv = cargar_csv_local(symbol, tf)   # puede venir diario
            df_csv = _normalize_ohlcv(df_csv)
            df_csv = _apply_range(df_csv, start, end)
            if tf == "weekly" and not df_csv.empty:
                return _resample_weekly(df_csv)
            return df_csv

        # ---- 3) Ni BD ni fallback
        return pd.DataFrame(columns=["open", "high", "low", "close", "adj_close", "volume"])

    except Exception:
        # error de BD → fallback si está habilitado
        if getattr(settings, "USE_CSV_FALLBACK", False):
            df_csv = cargar_csv_local(symbol, tf)
            df_csv = _normalize_ohlcv(df_csv)
            return _apply_range(df_csv, start, end)
        raise

def latest_price(symbol: str) -> dict | None:
    """Último registro desde la BD (no usa CSV por definición)."""
    return (
        PriceDaily.objects.filter(ticker__symbol=symbol.upper())
        .order_by("-date")
        .values("date", "open", "high", "low", "close", "adj_close", "volume")
        .first()
    )

def prices_bulk_df(
    symbols: list[str],
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    tf: str = "daily",
) -> pd.DataFrame:
    """
    Varios símbolos. Intenta BD por símbolo; si alguno no tiene filas y hay fallback,
    se completa con CSV. Devuelve columnas: symbol, date, open, high, low, close, adj_close, volume.
    """
    parts: list[pd.DataFrame] = []
    for sym in (s.upper() for s in symbols):
        df = prices_df(sym, start=start, end=end, tf=tf)
        if df.empty:
            continue
        tmp = df.reset_index().copy()
        tmp.insert(0, "symbol", sym)
        parts.append(tmp)

    if not parts:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"])
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["symbol", "date"]).reset_index(drop=True)

def duplicates_report(symbol: str) -> pd.DataFrame:
    """Reporta fechas duplicadas en BD para un símbolo (sanidad)."""
    qs = (
        PriceDaily.objects.filter(ticker__symbol=symbol.upper())
        .values("date")
        .annotate(n=Count("date"))
        .filter(n__gt=1)
        .order_by("date")
    )
    return pd.DataFrame(list(qs))


