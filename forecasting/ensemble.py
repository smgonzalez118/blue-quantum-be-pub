# forecasting/ensemble.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Dict
import numpy as np
import pandas as pd

# Importamos de forma lazy para evitar circulares y fallas si faltan deps
try:
    from .models import TrainedModel, _HAVE_PROPHET
except Exception:
    TrainedModel = object  # type: ignore
    _HAVE_PROPHET = False

__all__ = ["EnsemblePred", "ensemble_predict"]  # ← export explícito


@dataclass
class EnsemblePred:
    yhat_logret: float
    yhat_price: float
    yhat_components: Dict[str, float]  # {"rf":..., "lgbm":..., "prophet":...}


def _prophet_logret(df_tkr: pd.DataFrame, horizon_days: int) -> float | None:
    if not _HAVE_PROPHET:
        return None
    try:
        from prophet import Prophet
        tmp = df_tkr[["date", "close"]].rename(columns={"date": "ds", "close": "y"}).copy()
        m = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=True)
        m.fit(tmp)
        future = m.make_future_dataframe(periods=horizon_days, freq="D")
        fcst = m.predict(future)
        y0 = float(tmp["y"].iloc[-1])
        yH = float(fcst["yhat"].iloc[-1])
        return float(np.log(yH / y0))
    except Exception:
        # si Prophet falla, degradamos silenciosamente
        return None


def ensemble_predict(
    models: TrainedModel,
    X_row: pd.DataFrame,
    last_close: float,
    df_ticker_hist: pd.DataFrame,
    horizon_days: int,
    weights: Tuple[float, float, float] = (0.4, 0.4, 0.2),
) -> EnsemblePred:
    w_rf, w_lgb, w_pr = weights
    parts: Dict[str, float] = {}

    # RF
    rf_pred = float(models.rf.predict(X_row[models.feature_cols])[0]) if getattr(models, "rf", None) else 0.0
    parts["rf"] = rf_pred

    # LGBM
    lgb_model = getattr(models, "lgbm", None)
    lgb_pred = float(lgb_model.predict(X_row[models.feature_cols])[0]) if lgb_model is not None else 0.0
    parts["lgbm"] = lgb_pred

    # Prophet (por ticker)
    pr_pred = _prophet_logret(df_ticker_hist, horizon_days)
    if pr_pred is None:
        total_w = w_rf + w_lgb
        yhat_log = (w_rf / total_w) * rf_pred + (w_lgb / total_w) * lgb_pred
    else:
        parts["prophet"] = pr_pred
        yhat_log = w_rf * rf_pred + w_lgb * lgb_pred + w_pr * pr_pred

    yhat_price = float(last_close * np.exp(yhat_log))
    return EnsemblePred(yhat_logret=yhat_log, yhat_price=yhat_price, yhat_components=parts)
