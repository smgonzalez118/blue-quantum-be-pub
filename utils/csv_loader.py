from pathlib import Path
from typing import Optional, List
import pandas as pd

# ------------------ Rutas ------------------

def _roots() -> List[Path]:
    roots: List[Path] = []
    try:
        from django.conf import settings as dj  # type: ignore
        if getattr(dj, "CSV_ROOT", ""):
            roots.append(Path(dj.CSV_ROOT))
        if getattr(dj, "BASE_DIR", ""):
            roots.append(Path(dj.BASE_DIR))
    except Exception:
        pass

    here = Path(__file__).resolve()
    roots += [here.parent.parent, here.parents[2], Path.cwd()]

    out, seen = [], set()
    for r in roots:
        p = Path(r).resolve()
        if p.exists() and str(p) not in seen:
            out.append(p)
            seen.add(str(p))
    return out

def _dirs_for_tf(tf: str) -> List[Path]:
    tf = (tf or "daily").lower()
    out: List[Path] = []
    for r in _roots():
        d = r / "datasets" / tf
        if d.exists():
            out.append(d)
    return out

def _try_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return None

# ------------------ Normalización ------------------

# candidatos “de verdad” (sacamos 'index' y 'Unnamed: 0' para evitar falsos positivos)
_DATE_CANDS = [
    "date","Date","DATE",
    "datetime","Datetime","DATETIME",
    "timestamp","Timestamp","TIMESTAMP",
    "time","Time","TIME",
]

_COLMAP = {
    "Open":"open","open":"open","OPEN":"open",
    "High":"high","high":"high","HIGH":"high",
    "Low":"low","low":"low","LOW":"low",
    "Close":"close","close":"close","CLOSE":"close","c":"close","C":"close",
    "Adj Close":"adj_close","AdjClose":"adj_close","adjclose":"adj_close","adj_close":"adj_close",
    "Volume":"volume","volume":"volume","VOLUME":"volume",
    "PRICE":"close","Price":"close",
}

def _best_date_col(df: pd.DataFrame) -> Optional[str]:
    """
    Elige la mejor columna de fecha por:
    - nombre candidato
    - % parsable >= 0.8
    - y rango de días > 2 (para evitar 1970+ns/índices)
    Ignora columnas tipo 'index', 'Unnamed: 0', etc.
    """
    for c in list(df.columns):
        name = str(c)
        if name.startswith("Unnamed"):
            continue
        if name in _DATE_CANDS:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().mean() >= 0.8:
                span = (s.max() - s.min()) if s.notna().any() else pd.Timedelta(0)
                if isinstance(span, pd.Timedelta) and span >= pd.Timedelta(days=3):
                    return c
    return None

def _infer_freq(index: pd.Index) -> str:
    s = pd.to_datetime(index, errors="coerce")
    if len(s) < 3:
        return "B"
    med = s.to_series().diff().dt.days.dropna().median()
    return "B" if med <= 3 else "W-FRI"

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    x = df.copy()

    # map OHLCV
    ren = {}
    for col in list(x.columns):
        if col in _COLMAP:
            ren[col] = _COLMAP[col]
        else:
            low = str(col).lower()
            if low in _COLMAP:
                ren[col] = _COLMAP[low]
    if ren:
        x = x.rename(columns=ren)

    # elegir columna fecha
    date_col = _best_date_col(x)

    # si nada válido, como ÚLTIMO recurso: usar índice si YA es datetime
    if date_col is None and isinstance(x.index, pd.DatetimeIndex):
        idx_name = x.index.name or "date"
        x = x.reset_index().rename(columns={idx_name: "date"})
        date_col = "date"

    if date_col is None or "close" not in x.columns:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    if date_col != "date":
        x = x.rename(columns={date_col: "date"})

    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.tz_localize(None)

    for c in ["open","high","low","close","adj_close","volume"]:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")

    x = x.dropna(subset=["date","close"]).sort_values("date").set_index("date")
    x = x[~x.index.duplicated(keep="last")]

    cols = [c for c in ["open","high","low","close","adj_close","volume"] if c in x.columns]
    return x[cols]

# ------------------ API ------------------

def cargar_csv_local(symbol: str, tf: str = "daily") -> pd.DataFrame:
    """
    Ignora el tf para la ruta física y SIEMPRE lee desde datasets/daily/.
    (El semanal se deriva por resample en memoria.)
    Prioriza <SYM>_daily.csv y luego <SYM>.csv.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    # Forzamos a buscar en 'daily' para mantener una sola fuente en disco
    dirs = _dirs_for_tf("daily")
    if not dirs:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    preferred = f"{sym}_daily.csv"
    generic   = f"{sym}.csv"

    for d in dirs:
        # 1) sufijo _daily primero
        p = d / preferred
        df = _try_read_csv(p)
        if df is not None and not df.empty:
            out = _normalize(df)
            out.attrs["__source_path__"] = str(p)
            out.attrs["__freq__"] = _infer_freq(out.index)
            return out

        # 2) genérico después
        p = d / generic
        df = _try_read_csv(p)
        if df is not None and not df.empty:
            out = _normalize(df)
            out.attrs["__source_path__"] = str(p)
            out.attrs["__freq__"] = _infer_freq(out.index)
            return out

    return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])




