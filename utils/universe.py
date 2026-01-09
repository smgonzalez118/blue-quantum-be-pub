# utils/universe.py
from typing import Iterable, List
from pathlib import Path
import os, glob, re, logging

logger = logging.getLogger(__name__)

# Archivos posibles para S&P 500 (uno por línea; permite comentarios con #)
SP500_PATHS = [
    Path(__file__).resolve().parent.parent / "market" / "sp500_symbols.txt",
    Path.cwd() / "market" / "sp500_symbols.txt",
    Path.cwd() / "sp500_symbols.txt",
]

# Directorios donde buscamos universos custom
UNIVERSE_DIRS = [
    Path(__file__).resolve().parent.parent / "market" / "universes",
    Path.cwd() / "market" / "universes",
]

# Nombres de archivos dentro de UNIVERSE_DIRS
SP100_FILE       = "sp100.txt"
ADRS_AR_FILE     = "adrs_ar.txt"
SECTOR_ETFS_FILE = "sector_etfs.txt"
COMMODITIES_FILE = "commodities.txt"

# ----------------- utilidades -----------------

def _dedupe(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        s = (s or "").strip().upper()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def _sanitize_symbol(x: str) -> str:
    s = (x or "").replace("\u200b","").replace("\ufeff","").strip().upper()
    # si viene "SIMBOLO,NOMBRE" me quedo con SIMBOLO
    s = s.split(",", 1)[0].strip()
    # validar: letras, números, punto o guion
    if not s or not all(ch.isalnum() or ch in ".-" for ch in s):
        return ""
    return s

def _read_lines(path: Path) -> List[str]:
    if not path.exists(): 
        return []
    out: List[str] = []
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.lstrip().startswith("#"):
                continue
            sym = _sanitize_symbol(line)
            if sym:
                out.append(sym)
    return _dedupe(out)

# ----------------- fuentes “amplias” -----------------

def _from_sp500_file() -> List[str]:
    for p in SP500_PATHS:
        if p.exists():
            syms = _read_lines(p)
            logger.info("Universo S&P500 desde: %s (n=%d)", p, len(syms))
            return syms
    return []

def _from_env() -> List[str]:
    raw = os.getenv("DASHBOARD_TICKERS", "")
    if not raw: 
        return []
    out = _dedupe([_sanitize_symbol(t) for t in raw.split(",")])
    if out:
        logger.info("Universo desde env DASHBOARD_TICKERS (n=%d)", len(out))
    return out

def _from_settings() -> List[str]:
    try:
        from django.conf import settings
        lst = getattr(settings, "DASHBOARD_TICKERS", None)
        if isinstance(lst, (list, tuple)) and lst:
            out = _dedupe([_sanitize_symbol(str(x)) for x in lst])
            if out:
                logger.info("Universo desde settings.DASHBOARD_TICKERS (n=%d)", len(out))
            return out
    except Exception:
        pass
    return []

def _from_filesystem_csv() -> List[str]:
    """
    Lee símbolos directamente de los CSV presentes:
    datasets/daily/*.csv  ⇒  nombre antes de '_' o '.'
    """
    roots = [Path.cwd(), Path(__file__).resolve().parent.parent]
    daily_dirs = [r / "datasets" / "daily" for r in roots]
    files: list[str] = []
    for d in daily_dirs:
        files += glob.glob(str(d / "*.csv"))

    def _file_symbol(name: str) -> str:
        base = Path(name).stem
        return _sanitize_symbol(re.split(r"[_\.]", base)[0])

    syms = [_file_symbol(p) for p in files]
    out = [s for s in syms if s]
    out = _dedupe(out)
    if out:
        logger.info("Universo desde filesystem (CSV daily): %d símbolos", len(out))
    return out

# ----------------- universos custom (demo) -----------------

def _read_from_universe_dirs(filename: str) -> List[str]:
    agg: List[str] = []
    for d in UNIVERSE_DIRS:
        agg += _read_lines(d / filename)
    return _dedupe(agg)

def _sp100() -> List[str]:
    syms = _read_from_universe_dirs(SP100_FILE)
    if syms: logger.info("SP100: %d símbolos", len(syms))
    return syms

def _adrs_ar() -> List[str]:
    syms = _read_from_universe_dirs(ADRS_AR_FILE)
    if syms: logger.info("ADRs AR: %d símbolos", len(syms))
    return syms

def _sector_etfs() -> List[str]:
    syms = _read_from_universe_dirs(SECTOR_ETFS_FILE)
    if syms: logger.info("ETFs sectoriales: %d símbolos", len(syms))
    return syms

def _commodities() -> List[str]:
    syms = _read_from_universe_dirs(COMMODITIES_FILE)
    if syms: logger.info("Commodities: %d símbolos", len(syms))
    return syms

def _custom_universe() -> List[str]:
    # SP100 + ADRs AR + ETFs sectoriales + commodities
    agg: List[str] = []
    agg += _sp100()
    agg += _adrs_ar()
    agg += _sector_etfs()
    agg += _commodities()
    agg = _dedupe([x for x in agg if x])
    if agg:
        logger.info("Universo CUSTOM/DEMO total: %d símbolos", len(agg))
    return agg

# ----------------- API principal -----------------

def get_dashboard_universe(mode: str = "union") -> List[str]:
    """
    mode:
      - "demo" / "custom": SP100 + ADRs AR + ETFs sectoriales + commodities
      - "sp100": solo SP100 (market/universes/sp100.txt)
      - "adrs":  solo ADRs AR (market/universes/adrs_ar.txt)
      - "etfs":  solo ETFs sectoriales (market/universes/sector_etfs.txt)
      - "commodities": solo commodities (market/universes/commodities.txt)
      - "sp500": usa archivo market/sp500_symbols.txt (fallback a files/env/settings)
      - "files": usa símbolos presentes en datasets/daily/*.csv
      - "union": settings + env + sp500 + files (en ese orden)
    """
    m = (mode or "union").lower().strip()

    # fuentes “amplias”
    s_settings = _from_settings()
    s_env      = _from_env()
    s_sp500    = _from_sp500_file()
    s_files    = _from_filesystem_csv()

    if m in ("demo", "custom"):
        tickers = _custom_universe()
    elif m == "sp100":
        tickers = _sp100()
    elif m == "adrs":
        tickers = _adrs_ar()
    elif m in ("etf", "etfs"):
        tickers = _sector_etfs()
    elif m in ("cmdty", "commodities"):
        tickers = _commodities()
    elif m == "sp500":
        tickers = s_sp500 or s_files or s_settings or s_env
    elif m == "files":
        tickers = s_files or s_sp500 or s_settings or s_env
    else:  # union
        tickers = _dedupe([*s_settings, *s_env, *s_sp500, *s_files])

    # red de seguridad
    if not tickers:
        tickers = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AMD","ABNB"]

    return tickers
