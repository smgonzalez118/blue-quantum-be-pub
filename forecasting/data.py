# forecasting/data.py
from __future__ import annotations
from pathlib import Path
from typing import List
import pandas as pd
from .config import CFG

# NUEVO: intentamos usar tu acceso a BD si está disponible
try:
    from utils.data_access import prices_df as _prices_df
except Exception:
    _prices_df = None

REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]

__all__ = ["load_price_csv", "load_ticker", "load_benchmark", "load_universe"]

def load_price_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed", na=False)]
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns:
        for cand in ("timestamp", "datetime", "time", "fechahora"):
            if cand in df.columns:
                df = df.rename(columns={cand: "date"})
                break
        else:
            if df.index.name and str(df.index.name).lower() in ("date", "datetime", "timestamp"):
                df = df.reset_index().rename(columns={df.index.name: "date"})
            elif "index" in df.columns:
                df = df.rename(columns={"index": "date"})
            else:
                raise ValueError(f"No se encontró columna de fecha en {path}. Columnas: {list(df.columns)}")

    if "close" not in df.columns and "adj close" in df.columns:
        df = df.rename(columns={"adj close": "close"})

    rename_cap = {c.capitalize(): c for c in ["open", "high", "low", "close", "volume"]}
    df = df.rename(columns={k: v for k, v in rename_cap.items() if k in df.columns})

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas {missing} en {path}. Columnas: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df

def _load_ticker_from_db(ticker: str) -> pd.DataFrame | None:
    if _prices_df is None:
        return None
    try:
        df = _prices_df(symbol=ticker, tf="daily")  # tu firma actual
        if df is None or df.empty:
            return None
        if df.index.name == "date":
            df = df.reset_index()
        # normalizamos nombres
        cols = [c.lower() for c in df.columns]
        df.columns = cols
        if "adj_close" in df.columns and "close" not in df.columns:
            df = df.rename(columns={"adj_close": "close"})
        needed = {"date", "open", "high", "low", "close", "volume"}
        missing = needed - set(df.columns)
        if missing:
            # si faltan algunas (ej. volume), las completamos con 0/NaN
            for m in missing:
                df[m] = 0.0 if m == "volume" else None
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df[["date", "open", "high", "low", "close", "volume"]]
    except Exception:
        return None

def load_ticker(ticker: str) -> pd.DataFrame:
    ticker = ticker.upper()
    # 1) BD si se puede
    df = _load_ticker_from_db(ticker)
    if df is None:
        # 2) CSV fallback
        path = CFG.DAILY_DIR / f"{ticker}.csv"
        df = load_price_csv(path)
    df["ticker"] = ticker
    return df

def load_benchmark() -> pd.DataFrame:
    """
    Devuelve df con columnas: date, bench_close.
    1) Intenta BD con SPY (o el que definas más adelante en CFG si querés).
    2) Fallback CSV en datasets/benchmark/SPY.csv.
    """
    # 1) BD primero (si tenés utils.data_access.prices_df disponible)
    if _prices_df is not None:
        try:
            dfb = _prices_df(symbol="SPY", tf="daily")
            if dfb is not None and not dfb.empty:
                if dfb.index.name == "date":
                    dfb = dfb.reset_index()
                dfb.columns = [c.lower() for c in dfb.columns]
                if "adj_close" in dfb.columns and "close" not in dfb.columns:
                    dfb = dfb.rename(columns={"adj_close": "close"})
                dfb["date"] = pd.to_datetime(dfb["date"], errors="coerce").dt.tz_localize(None)
                dfb = dfb.dropna(subset=["date"]).sort_values("date")
                return dfb[["date", "close"]].rename(columns={"close": "bench_close"})
        except Exception:
            pass  # seguimos al CSV

    # 2) CSV fallback
    path = CFG.BENCH_DIR / CFG.BENCH_FILE  # ← ahora es SPY.csv
    df = load_price_csv(path).rename(columns={"close": "bench_close"})
    return df[["date", "bench_close"]]

def load_universe(tickers: List[str]) -> pd.DataFrame:
    dfs = [load_ticker(t) for t in tickers]
    return pd.concat(dfs, ignore_index=True)


