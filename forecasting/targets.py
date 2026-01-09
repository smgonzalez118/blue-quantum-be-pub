# forecasting/targets.py
from __future__ import annotations
import pandas as pd
import numpy as np
from .config import CFG

def add_forward_log_returns(feat: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas target_{key} = log(close[t+h]/close[t]) por ticker."""
    feat = feat.sort_values(["ticker", "date"]).reset_index(drop=True)
    out = []
    for tkr, g in feat.groupby("ticker", sort=False):
        g = g.copy()
        for k, h in CFG.horizons().items():
            g[f"target_{k}"] = np.log(g["close"].shift(-h) / g["close"])
        out.append(g)  # ← append UNA sola vez por ticker
    df = pd.concat(out, ignore_index=True)
    return df
