# market/services/indicators.py
from __future__ import annotations

import pandas as pd
from django.db import transaction
from django.utils import timezone

from utils.data_access import prices_df
from utils.functions import calcular_rsi, calcular_macd_crossover, calcular_dmi_adx
from utils.nombres import get_nombre_ticker
from dashboard.models import AtributoDashboard


def _tf_code(tf: str) -> str:
    tf = (tf or "daily").lower()
    return "W" if tf == "weekly" else "D"


def _bull(condition: bool) -> str:
    return "BULL" if bool(condition) else "BEAR"


def compute_indicators_for_ticker(symbol: str, tf: str = "daily") -> int:
    """
    Lee precios (daily/weekly), calcula EMAs/RSI/DMI/ADX/MACD y actualiza
    AtributoDashboard con flags BULL/BEAR (no valores numéricos).
    Retorna 1 si upsert ok, 0 si se salteó.
    """
    df = prices_df(symbol, tf=tf)
    if df is None or df.empty or "close" not in df.columns:
        return 0

    x = df.copy().reset_index().rename(columns={"date": "date"})
    x["close"] = pd.to_numeric(x["close"], errors="coerce")
    # high/low/open opcionales
    for c in ("high", "low", "open"):
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["close"]).sort_values("date")
    if x.empty:
        return 0

    s = x["close"]
    # EMAs numéricas
    ema5   = s.ewm(span=5,  adjust=False).mean()
    ema10  = s.ewm(span=10, adjust=False).mean()
    ema20  = s.ewm(span=20, adjust=False).mean()
    ema30  = s.ewm(span=30, adjust=False).mean()
    ema100 = s.ewm(span=100, adjust=False).mean()

    ultima_close = float(s.iloc[-1])
    last_row = x.iloc[-1]

    # Flags BULL/BEAR
    pmm5   = _bull(ultima_close > float(ema5.iloc[-1]))
    pmm10  = _bull(ultima_close > float(ema10.iloc[-1]))
    pmm20  = _bull(ultima_close > float(ema20.iloc[-1]))
    pmm30  = _bull(ultima_close > float(ema30.iloc[-1]))
    pmm100 = _bull(ultima_close > float(ema100.iloc[-1]))
    mm5_10   = _bull(float(ema5.iloc[-1])  > float(ema10.iloc[-1]))
    mm10_20  = _bull(float(ema10.iloc[-1]) > float(ema20.iloc[-1]))
    triple   = _bull(
        float(ema5.iloc[-1]) > float(ema10.iloc[-1]) and
        float(ema5.iloc[-1]) > float(ema20.iloc[-1]) and
        float(ema10.iloc[-1]) > float(ema20.iloc[-1])
    )

    # MACD (flag bull/bear a partir de cruce macd vs signal)
    macd_flag = calcular_macd_crossover(
        pd.DataFrame({"close": s})  # función espera DF
    )

    # RSI: BULL si > 50
    rsi_series = calcular_rsi(s)
    rsi_flag = _bull(float(rsi_series.iloc[-1]) > 50) if hasattr(rsi_series, "iloc") else "-"

    # DMI/ADX (si hay high/low)
    if {"high", "low"}.issubset(x.columns) and x[["high", "low"]].notna().any().any():
        dmi_dir, adx_strength = calcular_dmi_adx(x.rename(columns={"date": "index"}).set_index("index"))
        dmi_flag = dmi_dir or "-"
        adx_flag = adx_strength or "--"
    else:
        dmi_flag, adx_flag = "-", "--"

    # nombre descriptivo
    nombre = get_nombre_ticker(symbol)
    activo_display = f"{symbol.upper()} ({nombre})"

    tfc = _tf_code(tf)

    with transaction.atomic():
        AtributoDashboard.objects.update_or_create(
            ticker=symbol.upper(),
            timeframe=tfc,  # CharField('D'/'W') con unique (ticker,timeframe)
            defaults={
                "activo": activo_display,
                "precio": round(ultima_close, 2),

                # Flags (BULL/BEAR) — NO guardamos los valores numéricos
                "macd": macd_flag,
                "pmm5": pmm5,
                "pmm10": pmm10,
                "pmm20": pmm20,
                "pmm30": pmm30,
                "mm5_10": mm5_10,
                "mm10_20": mm10_20,
                "tripleCruce": triple,
                "pmm100": pmm100,
                "rsi": rsi_flag,
                "dmi": dmi_flag,
                "adx": adx_flag,
            },
        )
    return 1
