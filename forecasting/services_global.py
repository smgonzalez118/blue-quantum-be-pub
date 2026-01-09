# forecasting/services_global.py
from __future__ import annotations

import os
import re
import logging
from datetime import date
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from django.conf import settings

from utils.data_access import prices_df  # última vela desde tu BD/CSV
from forecasting.pipeline import predict_ticker  # usa tus artefactos/modelos

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Directorio de artefactos y detección de horizontes disponibles
# ----------------------------------------------------------------------
_DEFAULT_MODELS_DIR = os.path.join(
    getattr(settings, "BASE_DIR", os.getcwd()),
    "forecasting",
    "artifacts",
)
PKL_PATTERN = re.compile(r"h(\d{1,3})", re.IGNORECASE)

# Solo trabajamos con estos horizontes
_ALLOWED_H = (21, 63)


def _models_dir() -> str:
    return getattr(settings, "FORECAST_MODELS_DIR", _DEFAULT_MODELS_DIR)


def list_available_horizons() -> List[int]:
    """
    Revisa los artefactos y devuelve solo los horizontes permitidos (21, 63)
    que estén efectivamente presentes en disco.
    """
    d = _models_dir()
    try:
        files = os.listdir(d)
    except Exception:
        return []

    hs: set[int] = set()
    for f in files:
        m = PKL_PATTERN.search(f)  # permite ...h021..., h63, etc.
        if not m:
            continue
        try:
            n = int(m.group(1))
        except Exception:
            continue
        if n in _ALLOWED_H:
            hs.add(n)
    return sorted(hs)


# ----------------------------------------------------------------------
# Última fecha (train_end) y último close desde BD
# ----------------------------------------------------------------------
def _last_train_end_from_prices(symbol: str) -> date:
    """
    Devuelve la última fecha disponible de OHLC para el símbolo en DAILY,
    usada como 'train_end' para versionar el forecast.
    """
    df = prices_df(symbol=symbol, tf="daily")
    if df is None or df.empty:
        raise ValueError("No hay precios")
    if df.index.name == "date":
        df = df.reset_index()
    if "date" not in df.columns:
        raise ValueError("DataFrame sin columna 'date'")
    last = df["date"].max()
    return last.date() if hasattr(last, "date") else last


def _latest_close(symbol: str) -> float | None:
    try:
        df = prices_df(symbol=symbol, tf="daily")
        if df is None or df.empty:
            return None
        if df.index.name == "date":
            df = df.reset_index()
        c = pd.to_numeric(df["close"], errors="coerce").dropna()
        return float(c.iloc[-1]) if len(c) else None
    except Exception:
        return None


# ----------------------------------------------------------------------
# Sanitización de retornos (fracción) por horizonte
# ----------------------------------------------------------------------
_RET_LIMITS: Dict[int, float] = {
    # dejamos límites razonables (aunque solo usamos 21 y 63)
    5:   0.15,   # ±15% ~ 1 semana
    21:  0.30,   # ±30% ~ 1 mes
    63:  0.60,   # ±60% ~ 3 meses
    126: 1.00,   # ±100% ~ 6 meses
    252: 1.50,   # ±150% ~ 1 año
}


def _clip_ret(ret_frac: float | None, horizon: int) -> float | None:
    if ret_frac is None:
        return None
    try:
        ret = float(ret_frac)
    except Exception:
        return None
    lim = _RET_LIMITS.get(int(horizon), 1.50)
    if abs(ret) > lim:
        return max(-lim, min(lim, ret))
    return ret


def _safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


# ----------------------------------------------------------------------
# Extractores robustos de numbers desde payloads “creativos”
# ----------------------------------------------------------------------
PRED_KEYS = [
    # orden de preferencia para precio objetivo
    "price_pred", "yhat", "y_hat", "prediction", "pred", "y_pred",
    "price", "close_pred", "forecast", "value", "y",
]
RET_KEYS = [
    "ret_pct_pred", "ret_pct", "ret", "return_pct", "yhat_pct",
]
PRICE_NOW_KEYS = [
    "price_now", "close_now", "last_close", "y0", "y_t", "price",
]


def _extract_number(obj: Any) -> float | None:
    """Saca un float de casi cualquier contenedor."""
    if obj is None:
        return None
    # escalares
    if isinstance(obj, (float, int, np.floating, np.integer)):
        return float(obj)
    # numpy arrays
    if isinstance(obj, np.ndarray):
        if obj.size == 0:
            return None
        try:
            return float(np.ravel(obj)[0])
        except Exception:
            return None
    # lista/tupla
    if isinstance(obj, (list, tuple)):
        if not obj:
            return None
        return _extract_number(obj[0])
    # pandas
    if isinstance(obj, pd.Series):
        if obj.shape[0] == 0:
            return None
        try:
            return float(obj.iloc[0])
        except Exception:
            return None
    if isinstance(obj, pd.DataFrame):
        if obj.shape[0] == 0:
            return None
        # buscar columnas “conocidas”
        for k in PRED_KEYS + RET_KEYS + PRICE_NOW_KEYS:
            if k in obj.columns:
                try:
                    return float(obj[k].iloc[0])
                except Exception:
                    pass
        # si no, primera celda
        try:
            return float(obj.iloc[0, 0])
        except Exception:
            return None
    # dict
    if isinstance(obj, dict):
        # 1) claves conocidas
        for k in PRED_KEYS + RET_KEYS + PRICE_NOW_KEYS:
            if k in obj:
                val = _extract_number(obj[k])
                if val is not None:
                    return val
        # 2) inspección recursiva
        for v in obj.values():
            val = _extract_number(v)
            if val is not None:
                return val
    # objetos con .item()
    if hasattr(obj, "item"):
        try:
            return float(obj.item())
        except Exception:
            pass
    return None


def _extract_price_pred(obj: Any) -> float | None:
    """Intenta extraer price_pred según claves comunes o estructuras típicas."""
    if isinstance(obj, dict):
        # Primero claves “de precio”
        for k in PRED_KEYS:
            if k in obj:
                n = _extract_number(obj[k])
                if n is not None:
                    return n
        # A veces vienen anidados: {'forecast': {'yhat': ...}}
        for key in ("forecast", "fcst", "pred"):
            if key in obj:
                n = _extract_number(obj[key])
                if n is not None:
                    return n
        # Recursivo general
        for v in obj.values():
            n = _extract_number(v)
            if n is not None:
                return n
    # Fuera de dict: intento genérico
    return _extract_number(obj)


def _extract_ret_pct(obj: Any) -> float | None:
    """Extrae un % retorno desde estructuras típicas."""
    if isinstance(obj, dict):
        for k in RET_KEYS:
            if k in obj:
                n = _extract_number(obj[k])
                if n is not None:
                    return float(n)
        for v in obj.values():
            n = _extract_ret_pct(v)
            if n is not None:
                return float(n)
    # Si viniera como array/serie/df suelto
    n = _extract_number(obj)
    return float(n) if n is not None else None


def _extract_price_now(obj: Any) -> float | None:
    if isinstance(obj, dict):
        for k in PRICE_NOW_KEYS:
            if k in obj:
                n = _extract_number(obj[k])
                if n is not None:
                    return float(n)
        for v in obj.values():
            n = _extract_price_now(v)
            if n is not None:
                return float(n)
    n = _extract_number(obj)
    return float(n) if n is not None else None


# ----------------------------------------------------------------------
# Localizar payload del horizonte dentro de lo que devuelve predict_ticker
# ----------------------------------------------------------------------
def _find_horizon_payload(pred_map: Any, horizon: int) -> Any:
    """
    Intenta ubicar el payload correspondiente a hNNN dentro de estructuras varias:
      - dict con clave 'hNNN'
      - dict cuyos valores son dicts con campo 'h'/'horizon'
      - lista de dicts con 'h'/'horizon'
      - si no se distingue por horizonte, devuelve el objeto original
    """
    key = f"h{int(horizon):03d}"
    # caso estándar
    if isinstance(pred_map, dict) and key in pred_map:
        return pred_map[key]
    # dict con sub-dicts etiquetados
    if isinstance(pred_map, dict):
        for v in pred_map.values():
            if isinstance(v, dict):
                h = v.get("h") or v.get("horizon")
                if h is not None and int(h) == int(horizon):
                    return v
    # lista de dicts
    if isinstance(pred_map, (list, tuple)):
        for v in pred_map:
            if isinstance(v, dict):
                h = v.get("h") or v.get("horizon")
                if h is not None and int(h) == int(horizon):
                    return v
    # fallback: devolver tal cual (quizás no separa por horizonte)
    return pred_map


# ----------------------------------------------------------------------
# Predicción para un símbolo + horizonte (wrapper robusto)
# ----------------------------------------------------------------------
def predict_rows_for(symbol: str, horizon: int) -> Dict:
    """
    Wrapper robusto:
      - Llama a `predict_ticker(symbol)` (tus artefactos)
      - Ubica el payload del horizonte
      - Normaliza price_now / price_pred / ret_pct_pred
      - Sanea retornos y arma formato:
        {"train_end": <date>, "rows": [{"date": "...", "price_now":..., "price_pred":..., "ret_pct_pred": ...}]}
    """
    symbol = str(symbol).upper().strip()
    if not symbol:
        raise ValueError("Símbolo vacío")

    # train_end de la última vela
    train_end = _last_train_end_from_prices(symbol)

    # Ejecutamos pipeline (puede devolver distintas formas)
    pred_map = predict_ticker(symbol)  # forma libre
    payload = _find_horizon_payload(pred_map, horizon)

    # Primero intentamos sacar números desde el propio payload
    price_now = _extract_price_now(payload)
    price_pred = _extract_price_pred(payload)
    ret_pct = _extract_ret_pct(payload)

    # Si no vino price_now, usamos último close de BD
    if price_now is None:
        price_now = _latest_close(symbol)

    # Normalización de retorno y precio destino
    ret_frac = None
    if ret_pct is not None:
        ret_frac = ret_pct / 100.0
    elif (price_now is not None) and (price_pred is not None) and price_now != 0:
        ret_frac = (price_pred / price_now - 1.0)

    ret_frac = _clip_ret(ret_frac, horizon)

    # Reconstrucción coherente
    if (price_now is not None) and (ret_frac is not None):
        price_pred = round(price_now * (1.0 + ret_frac), 2)
        ret_pct = round(ret_frac * 100.0, 2)
    elif ret_frac is not None:
        ret_pct = round(ret_frac * 100.0, 2)
    elif price_pred is not None and price_now is not None and price_now != 0:
        # último intento: inferir ret_pct desde los dos precios
        ret_frac = (price_pred / price_now - 1.0)
        ret_frac = _clip_ret(ret_frac, horizon)
        if ret_frac is not None:
            ret_pct = round(ret_frac * 100.0, 2)

    if price_pred is None:
        # Log informativo para depurar si vuelve a ocurrir
        try:
            shape = None
            if isinstance(payload, np.ndarray):
                shape = payload.shape
            elif isinstance(payload, (pd.Series, pd.DataFrame)):
                shape = payload.shape
            elif isinstance(payload, dict):
                shape = f"dict keys={list(payload.keys())[:10]}"
            else:
                shape = type(payload).__name__
            log.error(
                "No se pudo extraer price_pred (h=%s). Tipo payload=%s Detalle=%s",
                horizon, type(payload).__name__, shape
            )
        except Exception:
            pass
        # No cortamos el proceso: devolvemos con price_pred=None
        pass

    row = {
        "date": train_end.isoformat(),
        "price_now": price_now,
        "price_pred": price_pred,
        "ret_pct_pred": ret_pct,
    }
    return {"train_end": train_end, "rows": [row]}


