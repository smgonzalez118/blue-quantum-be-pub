from __future__ import annotations
import pandas as pd
import numpy as np

# Compat con NumPy 2.x
if not hasattr(np, "NaN"):
    np.NaN = np.nan

# pandas-ta opcional (si no está, caemos a cálculo manual)
try:
    import pandas_ta as ta
except Exception:  # pragma: no cover
    ta = None

# ---------------------------------------------------------------------
# Helpers numéricos
# ---------------------------------------------------------------------

def _pct_change(s: pd.Series, n: int) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").pct_change(n)

def _rolling_vol(logret: pd.Series, n: int) -> pd.Series:
    return pd.to_numeric(logret, errors="coerce").rolling(n).std()

def _ema(s: pd.Series, span: int) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").ewm(span=span, adjust=False).mean()

# ---------------------------------------------------------------------
# Bollinger Bands helpers
# Queremos SIEMPRE estos nombres EXACTOS:
#   BBL_20_2.0 / BBM_20_2.0 / BBU_20_2.0  (+ alias BBB_20_2.0 = basis)
# ---------------------------------------------------------------------

_FMT = "20_2.0"
_REQ_BB = (f"BBL_{_FMT}", f"BBM_{_FMT}", f"BBU_{_FMT}")
_ALIAS_BBB = f"BBB_{_FMT}"

def _canonicalize_bb_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra variantes posibles a los nombres canónicos que espera el modelo.
    Variantes cubiertas: ..._20_2, ..._20_2.00, minúsculas.
    """
    ren = {}
    for base in ("BBL", "BBM", "BBU"):
        can = f"{base}_{_FMT}"
        variants = [
            f"{base}_20_2", f"{base}_20_2.00",
            f"{base}_20_2,0", f"{base}_20_2,00",
            f"{base.lower()}_20_2", f"{base.lower()}_20_2.0", f"{base.lower()}_20_2.00",
        ]
        for v in variants:
            if v in df.columns and can not in df.columns:
                ren[v] = can
    if ren:
        df = df.rename(columns=ren)
    # alias BBB (basis)
    if _ALIAS_BBB not in df.columns and f"BBM_{_FMT}" in df.columns:
        df[_ALIAS_BBB] = df[f"BBM_{_FMT}"]
    return df

def _ensure_bbands(df: pd.DataFrame, close_col: str = "close",
                   length: int = 20, std: float = 2.0) -> pd.DataFrame:
    """
    Asegura columnas canónicas en df:
      BBL_20_2.0, BBM_20_2.0, BBU_20_2.0, BBB_20_2.0
    Si faltan, intenta con pandas-ta; si no, cálculo manual.
    """
    df = _canonicalize_bb_names(df)
    if all(c in df.columns for c in _REQ_BB):
        if _ALIAS_BBB not in df.columns:
            df[_ALIAS_BBB] = df[f"BBM_{_FMT}"]
        return df

    s = pd.to_numeric(df[close_col], errors="coerce")
    out = df.copy()

    # 1) Intento con pandas-ta (genera típicamente los nombres canónicos)
    if ta is not None:
        try:
            bb = ta.bbands(s, length=length, std=std)
            # Copiamos lo que venga y canonizamos
            for c in bb.columns:
                out[c] = bb[c]
            out = _canonicalize_bb_names(out)
            if all(c in out.columns for c in _REQ_BB):
                if _ALIAS_BBB not in out.columns:
                    out[_ALIAS_BBB] = out[f"BBM_{_FMT}"]
                return out
        except Exception:
            pass  # cae a cálculo manual

    # 2) Cálculo manual (SMA +/- k*stddev)
    m = s.rolling(length).mean()
    sd = s.rolling(length).std(ddof=0)
    out[f"BBM_{_FMT}"] = m
    out[f"BBL_{_FMT}"] = m - std * sd
    out[f"BBU_{_FMT}"] = m + std * sd
    out[_ALIAS_BBB]   = out[f"BBM_{_FMT}"]
    return out

# ---------------------------------------------------------------------
# Feature builder principal
# ---------------------------------------------------------------------

def compute_technical_features(df: pd.DataFrame, bench: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula features técnicos por ticker y los enriquece con benchmark.
    Espera columnas en df:    date, open, high, low, close, volume, ticker
    Espera columnas en bench: date, close (se renombra a bench_close)
    """
    # Copias defensivas
    df = df.copy()
    bench = bench.copy()

    # --- Merge con benchmark (evita close_x/close_y)
    if "close" in bench.columns and "bench_close" not in bench.columns:
        bench = bench.rename(columns={"close": "bench_close"})
    bench = bench[["date", "bench_close"]]
    df = df.merge(bench, on="date", how="left")

    out_parts: list[pd.DataFrame] = []

    # --- Por ticker
    for tkr, g in df.groupby("ticker", sort=False):
        g = g.sort_values("date").reset_index(drop=True)

        # --- Retornos simples
        g["ret_1"]  = _pct_change(g["close"], 1)
        g["ret_5"]  = _pct_change(g["close"], 5)
        g["ret_21"] = _pct_change(g["close"], 21)

        # --- Log-returns y vol rolling
        g["logret_1"] = np.log(pd.to_numeric(g["close"], errors="coerce")).diff()
        g["vol_21"]   = _rolling_vol(g["logret_1"], 21)
        g["vol_63"]   = _rolling_vol(g["logret_1"], 63)

        # --- RSI / MACD / EMAs (robustos si falta pandas-ta)
        if ta is not None:
            try:
                g["rsi_14"] = ta.rsi(g["close"], length=14)
            except Exception:
                g["rsi_14"] = pd.Series(index=g.index, dtype="float64")
            try:
                macd = ta.macd(g["close"], fast=12, slow=26, signal=9)
            except Exception:
                macd = None
        else:
            g["rsi_14"] = pd.Series(index=g.index, dtype="float64")
            macd = None

        if macd is not None:
            # nombres estándar de pandas-ta
            for src, dst in [("MACD_12_26_9", "macd"),
                             ("MACDs_12_26_9", "macd_signal"),
                             ("MACDh_12_26_9", "macd_hist")]:
                if src in macd.columns:
                    g[dst] = macd[src]
        else:
            # MACD manual simple
            ema12 = _ema(g["close"], 12)
            ema26 = _ema(g["close"], 26)
            macd_line = ema12 - ema26
            signal = macd_line.ewm(span=9, adjust=False).mean()
            g["macd"] = macd_line
            g["macd_signal"] = signal
            g["macd_hist"] = macd_line - signal

        # EMAs y ratios precio/EMA
        for e in (10, 20, 50):
            g[f"ema_{e}"] = _ema(g["close"], e)
            g[f"px_over_ema{e}"] = g["close"] / g[f"ema_{e}"] - 1.0

        # --- Bollinger (garantiza nombres EXACTOS + alias)
        g = _ensure_bbands(g, close_col="close", length=20, std=2.0)

        # %B y bandwidth (con protecciones)
        rng = (g[f"BBU_{_FMT}"] - g[f"BBL_{_FMT}"]).replace([0, np.inf, -np.inf], np.nan)
        g["bb_perc_b"] = (g["close"] - g[f"BBL_{_FMT}"]) / rng
        basis = g[_ALIAS_BBB].replace(0, np.nan)
        g["bb_bw"] = (g[f"BBU_{_FMT}"] - g[f"BBL_{_FMT}"]) / basis

        # --- Benchmark returns y correlación rolling
        g["bench_ret_1"]  = _pct_change(g["bench_close"], 1)
        g["bench_ret_21"] = _pct_change(g["bench_close"], 21)
        g["corr_21"] = (
            pd.to_numeric(g["ret_1"], errors="coerce")
              .rolling(21)
              .corr(pd.to_numeric(g["bench_ret_1"], errors="coerce"))
        )

        out_parts.append(g)

    feat = pd.concat(out_parts, ignore_index=True)

    # Limpieza final (quita filas con NaN en cualquier feature)
    feat = feat.dropna().reset_index(drop=True)
    # Canonicalización final por si alguna transformación a posteriori tocó nombres
    feat = _canonicalize_bb_names(feat)
    return feat
