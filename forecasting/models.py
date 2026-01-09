# forecasting/models.py
from __future__ import annotations
import warnings
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import root_mean_squared_error
import joblib

try:
    # prophet opcional
    from prophet import Prophet
    _HAVE_PROPHET = True
except Exception:
    _HAVE_PROPHET = False

try:
    import lightgbm as lgb
    _HAVE_LGBM = True
except Exception:
    _HAVE_LGBM = False

from .config import CFG


# --------------------------
# Serialización simple (1 archivo, compresión moderada)
# --------------------------
@dataclass
class TrainedModel:
    feature_cols: List[str]
    rf: Optional[RandomForestRegressor]
    # Puede ser lgb.Booster o LGBMRegressor
    lgbm: object | None

    def save(self, path: str):
        """
        Guardado simple en un único archivo .pkl/.joblib con compresión moderada.
        (Equilibrio entre tamaño y velocidad de carga)
        """
        joblib.dump(
            {
                "feature_cols": self.feature_cols,
                "rf": self.rf,
                "lgbm": self.lgbm,
            },
            path,
            compress=3,  # si querés aún más rápido al cargar, bajá a 1 o quitalo
        )

    @staticmethod
    def load(path: str) -> "TrainedModel":
        obj = joblib.load(path)
        return TrainedModel(**obj)


# --------------------------
# Selección de features
# --------------------------
def _select_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    drop_cols = {"open", "high", "low", "ema_10", "ema_20", "ema_50", "bench_close"}
    non_feat = {"date", "ticker"}
    cand = [c for c in df.columns if c not in drop_cols and c not in non_feat and not c.startswith("target_")]
    X = df[cand].copy()

    # Ticker como categoría codificada (estable y reproducible)
    if "ticker" in df.columns:
        X["ticker_code"] = pd.Categorical(df["ticker"]).codes
        cand.append("ticker_code")

    # Reemplazos de seguridad
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return X, cand


# --------------------------
# Entrenamiento (RF + LGBM)
# --------------------------
def train_global(df: pd.DataFrame, target_col: str, n_splits: int = 5, random_state: int = 42) -> TrainedModel:
    """
    Entrena RF + LGBM (si está disponible) sobre todo el universo con CV temporal.
    Devuelve un contenedor con los dos modelos y los feature_cols.
    """
    X, feat_cols = _select_features(df)
    y = df[target_col]

    tss = TimeSeriesSplit(n_splits=n_splits)
    oof_pred = np.zeros(len(df))

    # RandomForest
    rf = RandomForestRegressor(
        n_estimators=300,      # antes 500
        max_depth=None,
        min_samples_leaf=3,
        n_jobs=-1,
        random_state=random_state,
    )

    for i, (tr_idx, va_idx) in enumerate(tss.split(X), start=1):
        rf.fit(X.iloc[tr_idx], y.iloc[tr_idx])
        oof_pred[va_idx] = rf.predict(X.iloc[va_idx])
        print(f"[{target_col}] RF fold {i}/{n_splits} listo")

    rf_rmse = root_mean_squared_error(y, oof_pred)

    # LightGBM
    lgbm_model = None
    if _HAVE_LGBM:
        lgbm_model = lgb.LGBMRegressor(
            n_estimators=800,
            learning_rate=0.03,
            num_leaves=63,
            min_data_in_leaf=15,      # 10–20 según veas
            feature_fraction=0.9,
            bagging_fraction=0.9,
            bagging_freq=1,
            force_col_wise=True,
            verbosity=-1,
            random_state=42,
            n_jobs=-1,
        )
        oof_pred_lgb = np.zeros(len(df))
        for tr_idx, va_idx in tss.split(X):
            lgbm_model.fit(
                X.iloc[tr_idx], y.iloc[tr_idx],
                eval_set=[(X.iloc[va_idx], y.iloc[va_idx])],
                eval_metric="rmse",
                callbacks=[lgb.early_stopping(stopping_rounds=100, verbose=False)]
            )
            # pred con best_iteration_
            oof_pred_lgb[va_idx] = lgbm_model.predict(
                X.iloc[va_idx],
                num_iteration=getattr(lgbm_model, "best_iteration_", None)
            )
        lgb_rmse = root_mean_squared_error(y, oof_pred_lgb)
    else:
        lgb_rmse = np.nan

    print(f"CV RMSE — {target_col}: RF={rf_rmse:.6f}, LGBM={lgb_rmse:.6f}")

    return TrainedModel(feature_cols=feat_cols, rf=rf, lgbm=lgbm_model)



# forecasting/models.py
from django.db import models
# en SQLite podés usar models.JSONField desde Django 3.1+

class ForecastResult(models.Model):
    ticker = models.ForeignKey('market.Ticker', on_delete=models.CASCADE)
    timeframe = models.CharField(max_length=8)        # 'daily'|'weekly'
    model_name = models.CharField(max_length=32)      # 'prophet'|'sarima'|...
    train_end = models.DateField()                    # última fecha usada
    horizon = models.PositiveIntegerField()           # pasos adelante
    yhat = models.JSONField(default=list)             # [{date,yhat,...}]
    metrics = models.JSONField(null=True, blank=True, default=dict)
    model_version = models.CharField(max_length=16, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (('ticker','timeframe','model_name','train_end','horizon'),)
        indexes = [models.Index(fields=['ticker','timeframe','model_name','-train_end'])]
