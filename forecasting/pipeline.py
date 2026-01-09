# forecasting/pipeline.py
from __future__ import annotations

import logging
from typing import List, Dict, Any, Tuple

import numpy as np
import pandas as pd

from .config import CFG
from .data import load_universe, load_benchmark, load_ticker
from .features import compute_technical_features
from .targets import add_forward_log_returns
from .models import train_global
from .io import save_model, save_meta, get_model, invalidate_model_cache

# tu ensemble actual
from forecasting.ensemble import ensemble_predict

log = logging.getLogger(__name__)


# ----------------------------- helpers -----------------------------

def _safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _coerce_pred_output(pred: Any, last_close: float) -> Tuple[float | None, float | None, Dict[str, float]]:
    """
    Normaliza el output del ensemble a (yhat_price, yhat_logret, components)
    Soporta:
      - objeto con attrs: .yhat_price, .yhat_logret, .yhat_components
      - dict con esas claves u otras variantes razonables
      - número solo (lo interpretamos como yhat_price)
    """
    yhat_price: float | None = None
    yhat_logret: float | None = None
    components: Dict[str, float] = {}

    # Caso objeto con atributos
    if hasattr(pred, "yhat_price") or hasattr(pred, "yhat_logret"):
        yhat_price = _safe_float(getattr(pred, "yhat_price", None))
        yhat_logret = _safe_float(getattr(pred, "yhat_logret", None))
        comps = getattr(pred, "yhat_components", {}) or {}
        if isinstance(comps, dict):
            try:
                components = {str(k): float(v) for k, v in comps.items()}
            except Exception:
                components = {}
        return yhat_price, yhat_logret, components

    # Caso dict
    if isinstance(pred, dict):
        # precio
        for k in ("yhat_price", "price_pred", "price", "close_pred", "yhat"):
            if k in pred and yhat_price is None:
                yhat_price = _safe_float(pred[k])
        # logret
        for k in ("yhat_logret", "ret_log_pred", "logret", "log_return"):
            if k in pred and yhat_logret is None:
                yhat_logret = _safe_float(pred[k])
        # componentes
        for k in ("yhat_components", "components", "weights"):
            if k in pred and isinstance(pred[k], dict):
                try:
                    components = {str(kk): float(vv) for kk, vv in pred[k].items()}
                except Exception:
                    components = {}
                break
        return yhat_price, yhat_logret, components

    # Caso número suelto -> lo tomamos como precio objetivo
    num = _safe_float(pred)
    if num is not None:
        yhat_price = num
        # si no hay logret pero sí precio actual, podemos inferir abajo
    return yhat_price, yhat_logret, components


# ----------------------------- training -----------------------------

def train_all(tickers: List[str]) -> Dict[str, float]:
    """
    Entrena un modelo por horizonte usando el universo dado y guarda artefactos.
    Al final invalida el cache en memoria para que próximas predicciones carguen
    los modelos recién entrenados.
    """
    bench = load_benchmark()
    uni = load_universe(tickers)
    feat = compute_technical_features(uni, bench)
    feat_tgt = add_forward_log_returns(feat)

    metrics: Dict[str, float] = {}
    for key, _h in CFG.horizons_dict().items():
        target_col = f"target_{key}"
        df_train = feat_tgt.dropna(subset=[target_col]).copy()

        tm = train_global(df_train, target_col=target_col)
        save_model(key, tm)

        # placeholder simple (podés cambiar por una métrica real de CV/validación)
        metrics[key] = float(df_train[target_col].abs().mean())

    save_meta({"tickers": tickers})

    # 🔄 importante: limpiamos el cache de modelos en memoria
    invalidate_model_cache()

    return metrics


# ----------------------------- inference -----------------------------

def _last_row_features(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve (Xrow, df_t):
      - Xrow: última fila con features calculadas para predicción
      - df_t: df crudo del ticker (para obtener close actual, etc.)
    """
    bench = load_benchmark()
    df_t = load_ticker(ticker)
    feat = compute_technical_features(df_t, bench)

    if feat is None or feat.empty:
        raise ValueError(f"Sin features para {ticker} (dataset vacío o insuficiente).")

    # usar última fila como input de features
    Xrow = feat.tail(1).copy()

    # En entrenamiento se agregó 'ticker_code'. Para una única serie en predicción
    # dejamos un placeholder estable (0). Si luego guardás un mapping real, podés reemplazar.
    Xrow["ticker_code"] = 0
    return Xrow, df_t


def predict_ticker(ticker: str) -> Dict[str, Dict]:
    """
    Genera predicciones por horizonte para un ticker.
    Usa get_model(key) -> cache en memoria (no re-lee del disco en cada request).
    Si un modelo/horizonte no existe o falla, se salta y continúa con los demás.
    """
    ticker = ticker.upper().strip()
    Xrow, df_t = _last_row_features(ticker)

    if df_t.empty:
        raise FileNotFoundError(f"Sin datos para {ticker}")

    last_close = float(df_t["close"].iloc[-1])
    out: Dict[str, Dict] = {}

    for key, horizon in CFG.horizons_dict().items():
        try:
            tm = get_model(key)  # 🧠 cacheado en memoria
        except Exception as e:
            # modelo no disponible para este horizonte → continuar
            log.warning("Modelo no disponible para %s (h=%s): %s", ticker, horizon, e)
            continue

        try:
            raw_pred = ensemble_predict(tm, Xrow, last_close, df_t, horizon_days=horizon)
            yhat_price, yhat_logret, components = _coerce_pred_output(raw_pred, last_close)

            # si falta logret pero tengo precio, infiero; o viceversa
            if yhat_logret is None and yhat_price is not None and last_close:
                try:
                    yhat_logret = float(np.log(yhat_price / last_close))
                except Exception:
                    yhat_logret = None
            if yhat_price is None and yhat_logret is not None:
                try:
                    yhat_price = float(last_close * np.exp(yhat_logret))
                except Exception:
                    yhat_price = None

            # construir salida si tengo al menos algo
            out[key] = {
                "days": int(horizon),
                "price_now": float(last_close),
                "price_pred": None if yhat_price is None else float(yhat_price),
                "ret_log_pred": None if yhat_logret is None else float(yhat_logret),
                "ret_pct_pred": None if yhat_logret is None else float((np.exp(yhat_logret) - 1.0) * 100.0),
                "components": components,
            }

        except Exception as e:
            # no frenamos todo si un horizonte falla
            log.error("Fallo de predicción %s (h=%s): %s", ticker, horizon, e)
            continue

    return out


if __name__ == "__main__":
    # Ejemplo rápido:
    universe = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA"]
    print("Entrenando modelos globales...")
    train_all(universe)
    print("Predicciones:")
    print(predict_ticker("AAPL"))
