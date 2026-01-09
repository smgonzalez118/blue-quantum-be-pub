# market/management/commands/seed_ohlc_from_csv.py
from __future__ import annotations

import pandas as pd
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from market.models import Ticker
from market.services.etl import upsert_prices_bulk
from utils.csv_loader import cargar_csv_local
from utils.universe import get_dashboard_universe

DATE_CANDS = ["date", "timestamp", "Timestamp", "time", "Date", "Datetime", "INDEX", "Index"]
COLMAP = {
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
    s0 = raw.copy()
    s = pd.to_datetime(s0, errors="coerce", dayfirst=True)
    # si casi todo NaT y es numérico, probamos epoch
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

def _normalize_csv_df(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve DF con índice date y columnas open,high,low,close,adj_close,volume (las que existan)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    x = df.copy()

    # si viene con DatetimeIndex, lo pasamos a columna date
    if isinstance(x.index, pd.DatetimeIndex):
        idx_name = x.index.name or "date"
        x = x.reset_index().rename(columns={idx_name: "date"})

    # buscar columna de fecha
    if "date" not in x.columns:
        for c in DATE_CANDS:
            if c in x.columns:
                x = x.rename(columns={c: "date"})
                break

    # mapear columnas OHLCV
    rename = {}
    for col in list(x.columns):
        if col in COLMAP:
            rename[col] = COLMAP[col]
        else:
            low = str(col).lower()
            if low in COLMAP:
                rename[col] = COLMAP[low]
    if rename:
        x = x.rename(columns=rename)

    # validación mínima
    if "date" not in x.columns or "close" not in x.columns:
        return pd.DataFrame(columns=["open","high","low","close","adj_close","volume"])

    # tipos
    x["date"] = _parse_dates_series(x["date"])
    for c in ["open","high","low","close","adj_close","volume"]:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")

    # limpieza y orden
    x = x.dropna(subset=["date","close"]).sort_values("date")
    # deduplicar por fecha (quedarse con la última fila para esa fecha)
    x = x[~x["date"].duplicated(keep="last")]

    # índice por fecha
    x = x.set_index("date")

    cols = [c for c in ["open","high","low","close","adj_close","volume"] if c in x.columns]
    return x[cols]

class Command(BaseCommand):
    help = (
        "Carga OHLC desde datasets/<tf>/ para un conjunto de tickers y guarda en la BD "
        "(UPSERT por (ticker,date)).\n"
        "Ej: python manage.py seed_ohlc_from_csv --tf daily --mode files --chunksize 5000\n"
        "     python manage.py seed_ohlc_from_csv --tf weekly --symbols AAPL MSFT (se ignora weekly y se carga daily)"
    )

    def add_arguments(self, parser):
        parser.add_argument("--tf", choices=["daily", "weekly"], default="daily",
                            help="Timeframe del CSV (solo se usa 'daily'; 'weekly' será ignorado y se usará daily).")
        parser.add_argument(
            "--mode",
            choices=["files", "sp500", "union", "sp100", "adrs", "etfs", "commodities", "mi_lista_tickers"],
            default="files",
            help="Fuente de universo (utils.universe.get_dashboard_universe). Ignorado si usás --symbols o --tickers-file.",
        )
        parser.add_argument(
            "--symbols",
            nargs="*",
            help="Lista explícita de símbolos (opcional). Si se omite, se usa --mode o --tickers-file.",
        )
        parser.add_argument(
            "--tickers-file",
            type=str,
            help="Archivo con tickers (uno por línea; permite 'SIMBOLO,Nombre' y comentarios con #).",
        )
        parser.add_argument("--chunksize", type=int, default=10000)

    def handle(self, *args, **opts):
        tf = (opts["tf"] or "daily").lower()
        chunksize = int(opts["chunksize"])

        # avisamos si piden weekly (lo ignoramos)
        if tf == "weekly":
            self.stdout.write(self.style.WARNING(
                "Se recibió --tf weekly, pero la BD guarda SOLO daily. "
                "Se cargará desde datasets/daily y el semanal se derivará por resample (W-FRI)."
            ))
        tf_csv = "daily"  # siempre leemos CSV diarios

        # resolver universo
        symbols = [s.upper() for s in (opts.get("symbols") or [])]
        if not symbols and opts.get("tickers_file"):
            p = Path(opts["tickers_file"])
            if not p.exists():
                raise CommandError(f"No existe el archivo: {p}")
            lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()]
            for ln in lines:
                if not ln or ln.startswith("#"): 
                    continue
                sym = ln.split(",", 1)[0].strip().upper()
                if sym:
                    symbols.append(sym)
        if not symbols:
            symbols = get_dashboard_universe(mode=opts["mode"])

        if not symbols:
            raise CommandError("No hay símbolos para procesar (revisá --symbols, --tickers-file o --mode).")

        self.stdout.write(f"Sembrando OHLC desde CSV tf={tf_csv}, symbols={len(symbols)}…")
        total_rows = 0

        for sym in symbols:
            df_raw = cargar_csv_local(sym, tf=tf_csv)
            if df_raw is None or df_raw.empty:
                self.stdout.write(self.style.WARNING(f"[{sym}] sin CSV válido en datasets/{tf_csv}/"))
                continue

            x = _normalize_csv_df(df_raw)
            if x.empty or "close" not in x.columns:
                self.stdout.write(self.style.WARNING(f"[{sym}] CSV sin columnas date/close utilizables"))
                continue

            # construir rows para upsert
            rows = []
            for dt_idx, r in x.iterrows():
                rows.append({
                    "date": dt_idx.date(),
                    "open":      float(r["open"])      if "open" in x.columns and pd.notna(r["open"]) else 0.0,
                    "high":      float(r["high"])      if "high" in x.columns and pd.notna(r["high"]) else 0.0,
                    "low":       float(r["low"])       if "low"  in x.columns and pd.notna(r["low"])  else 0.0,
                    "close":     float(r["close"]),
                    "adj_close": float(r["adj_close"]) if "adj_close" in x.columns and pd.notna(r["adj_close"]) else float(r["close"]),
                    "volume":    int(r["volume"])      if "volume" in x.columns and pd.notna(r["volume"]) else 0,
                })

            if not rows:
                self.stdout.write(self.style.WARNING(f"[{sym}] 0 filas para upsert"))
                continue

            t, _ = Ticker.objects.get_or_create(symbol=sym, defaults={"name": "", "is_active": True})
            if not t.is_active:
                t.is_active = True
                t.save(update_fields=["is_active"])

            inserted = 0
            for i in range(0, len(rows), chunksize):
                batch = rows[i:i+chunksize]
                inserted += upsert_prices_bulk(t, batch)
            total_rows += inserted
            self.stdout.write(self.style.SUCCESS(f"[{sym}] upsert: {inserted}"))

        self.stdout.write(self.style.SUCCESS(f"SEMILLA CSV OK — filas upsert: {total_rows}"))
