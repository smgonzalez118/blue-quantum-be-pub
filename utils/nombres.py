# utils/nombres.py
from __future__ import annotations
from pathlib import Path
from functools import lru_cache
from typing import Dict, Iterable, Tuple
import pandas as pd

# Archivos posibles (en orden de prioridad)
_CANDIDATE_FILENAMES = [
    "tickers_nombres.csv",
    "tickers_names.csv",
    "ticker_names.csv",
    "symbols_names.csv",
]

def _roots() -> list[Path]:
    roots: list[Path] = []
    # 1) settings (si existen)
    try:
        from django.conf import settings  # type: ignore
        if getattr(settings, "CSV_ROOT", ""):
            roots.append(Path(settings.CSV_ROOT))
        if getattr(settings, "BASE_DIR", ""):
            roots.append(Path(settings.BASE_DIR))
    except Exception:
        pass
    # 2) proyecto
    here = Path(__file__).resolve()
    roots += [
        here.parent.parent,              # /utils -> repo root
        here.parents[2],                 # por si cambia estructura
        Path.cwd(),                      # cwd
        Path.cwd() / "datasets",         # datasets/
        Path.cwd() / "market",           # market/
    ]
    # únicos y existentes
    out, seen = [], set()
    for r in roots:
        p = Path(r).resolve()
        if p.exists() and str(p) not in seen:
            out.append(p); seen.add(str(p))
    return out

def _find_csv() -> Path | None:
    for base in _roots():
        for name in _CANDIDATE_FILENAMES:
            p = base / name
            if p.exists():
                return p
        # También probamos en subcarpetas típicas
        for sub in ["datasets", "market", "data"]:
            for name in _CANDIDATE_FILENAMES:
                p = base / sub / name
                if p.exists():
                    return p
    return None

def _norm_sym(x: str) -> str:
    return (x or "").strip().upper()

def _norm_name(x: str) -> str:
    # Respetamos mayúsculas/minúsculas del nombre tal cual viene
    return (x or "").strip()

def _read_df(path: Path) -> pd.DataFrame:
    # tolerante a BOM y separadores comunes
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path, encoding="latin-1")
    # normalizamos headers
    df.columns = [str(c).strip().lower() for c in df.columns]
    # detectar columnas
    sym_col = next((c for c in ("symbol","ticker","tkr","sym") if c in df.columns), None)
    name_col = next((c for c in ("name","nombre","company","companyname","desc","description") if c in df.columns), None)
    if not sym_col or not name_col:
        raise ValueError(f"{path.name}: columnas esperadas no encontradas (symbol/ticker, name/nombre/...)")
    out = df[[sym_col, name_col]].dropna()
    out = out.rename(columns={sym_col: "symbol", name_col: "name"})
    out["symbol"] = out["symbol"].map(_norm_sym)
    out["name"] = out["name"].map(_norm_name)
    out = out[out["symbol"] != ""]
    return out

@lru_cache(maxsize=1)
def _mapping() -> Tuple[Dict[str, str], str]:
    """
    Devuelve (mapa_symbol->name, fuente_path_str).
    Si no hay CSV, devuelve {} y fuente="".
    """
    p = _find_csv()
    if not p:
        return {}, ""
    try:
        df = _read_df(p)
        m = {row.symbol: row.name for row in df.itertuples(index=False)}
        return m, str(p)
    except Exception:
        return {}, ""

def get_nombre_ticker(symbol: str) -> str:
    """
    'MSFT' -> 'Microsoft' (si está en CSV o en DB Ticker.name), si no, 'MSFT'.
    """
    sym = _norm_sym(symbol)
    if not sym:
        return ""
    m, _ = _mapping()
    if sym in m and m[sym]:
        return m[sym]
    # fallback a DB si existe
    try:
        from market.models import Ticker  # type: ignore
        name = Ticker.objects.filter(symbol=sym).values_list("name", flat=True).first()
        if name:
            return _norm_name(name)
    except Exception:
        pass
    return sym  # último recurso

def get_activo_label(symbol: str) -> str:
    """
    'MSFT' -> 'MSFT (Microsoft)'
    Si no hay nombre, devuelve 'MSFT (MSFT)' para mantener formato.
    """
    sym = _norm_sym(symbol)
    name = get_nombre_ticker(sym) or sym
    return f"{sym} ({name})"

def nombres_source_path() -> str:
    """
    Para debug/logs: dónde se cargó el CSV (si se encontró).
    """
    _, src = _mapping()
    return src

def invalidate_cache() -> None:
    """Si reemplazás el CSV en caliente, podés limpiar el cache."""
    try:
        _mapping.cache_clear()
    except Exception:
        pass

# --- Retro-compatibilidad con código viejo ---
def format_activo(symbol: str) -> str:
    """Alias legacy: devuelve 'TICKER (Nombre)'. Mantiene compat. con imports antiguos."""
    return get_activo_label(symbol)