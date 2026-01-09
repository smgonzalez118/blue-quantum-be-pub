# market/services/signals.py
from __future__ import annotations

from typing import Optional, Dict, List, Tuple
import pandas as pd
from django.db import transaction
from django.utils import timezone

from utils.data_access import prices_df
from utils.nombres import format_activo
from senales.models import SenalTecnica


# ------------------------- helpers de tiempo/fecha -------------------------

def _to_aware(ts_obj) -> pd.Timestamp:
    ts = pd.to_datetime(ts_obj, errors="coerce")
    if pd.isna(ts):
        return pd.Timestamp.now(tz=timezone.get_current_timezone())
    if ts.tzinfo is None:
        return ts.tz_localize(timezone.get_current_timezone())
    return ts.tz_convert(timezone.get_current_timezone())

def _is_weekend(ts: pd.Timestamp) -> bool:
    try:
        return int(ts.weekday()) >= 5  # 5=Sat, 6=Sun
    except Exception:
        return False

def _snap_weekend_to_prev_bar_index(i: int, idx: pd.Index) -> int:
    """
    Si el índice i cae en un bar con fecha de fin de semana, usar i-1 (si existe).
    """
    if i <= 0:
        return i
    ts = pd.to_datetime(idx[i], errors="coerce")
    if pd.isna(ts):
        return i
    return i - 1 if _is_weekend(ts) else i


# ------------------------- cruce genérico y rendimiento -------------------------

def _last_cross_info(a: pd.Series, b: pd.Series, close: pd.Series) -> Optional[Dict]:
    """
    Último cruce entre series a y b.
    Si el cruce cae sábado/domingo, la señal (fecha y precio_senal) se asigna al bar hábil anterior.
    """
    if a is None or b is None or close is None or min(len(a), len(b), len(close)) < 2:
        return None

    diff = (a - b).astype(float)
    regime = (diff > 0).astype(int)
    changes = regime.diff().fillna(0)

    idx_changes = changes.to_numpy().nonzero()[0]
    if len(idx_changes) == 0:
        return None

    i = idx_changes[-1]
    tipo = "BUY" if float(changes.iloc[i]) > 0 else "SELL"

    # Si el bar del cruce cae en finde, usar bar anterior
    use_i = _snap_weekend_to_prev_bar_index(i, close.index)

    ts_signal = pd.to_datetime(close.index[use_i], errors="coerce")
    if pd.isna(ts_signal):
        ts_signal = pd.to_datetime(close.index[i], errors="coerce")
    ts_aware = _to_aware(ts_signal)

    precio_senal = float(close.iloc[use_i])
    precio_actual = float(close.iloc[-1])

    if tipo == "BUY":
        rendimiento = (precio_actual / precio_senal - 1.0) * 100.0
    else:
        rendimiento = (precio_senal / precio_actual - 1.0) * 100.0

    return {
        "tipo": tipo,
        "fecha": ts_aware.to_pydatetime(),
        "precio_senal": round(precio_senal, 4),
        "precio_actual": round(precio_actual, 4),
        "rendimiento": round(float(rendimiento), 2),
    }


# ------------------------- upsert con 1 fila por indicador/timeframe -------------------------

def _save_current_signal(entry: dict) -> None:
    """
    Mantiene UNA sola fila vigente por (activo, timeframe, indicador).
    Si existe, actualiza; si hay varias, deduplica dejando la más reciente.
    """
    with transaction.atomic():
        qs = (SenalTecnica.objects
              .filter(activo=entry["activo"],
                      timeframe=entry["timeframe"],
                      indicador=entry["indicador"])
              .order_by("-fecha"))
        obj = qs.first()
        # dedupe si hubiera más de una
        if qs.count() > 1:
            qs.exclude(pk=obj.pk).delete()

        if obj:
            obj.tipo = entry["tipo"]
            obj.fecha = entry["fecha"]
            obj.precio_senal = entry["precio_senal"]
            obj.precio_actual = entry["precio_actual"]
            obj.rendimiento = entry["rendimiento"]
            obj.detalle = entry.get("detalle", "")
            obj.save()
        else:
            SenalTecnica.objects.create(**entry)


# ------------------------- API principal -------------------------

def compute_signal_for_ticker(symbol: str, tf: str = "daily") -> int:
    """
    Calcula y guarda señales del último cruce para `symbol`:

      tf: "daily" | "weekly"  → timeframe guardado: "D" | "W"

    Indicadores:
      - EMA5/EMA10
      - EMA10/EMA20
      - PRICE/EMA5
      - PRICE/EMA10   (opcional; comentar si no se usa)

    Retorna la cantidad de señales upserteadas (máx 4).
    """
    tf = (tf or "daily").lower()
    tf_code = "W" if tf == "weekly" else "D"

    # 1) precios
    df = prices_df(symbol, tf=tf)
    if df is None or df.empty or "close" not in df.columns:
        return 0

    x = df.copy()
    if x.index.name == "date":
        x = x.reset_index()
    x = x.rename(columns={"date": "date"})
    x["close"] = pd.to_numeric(x["close"], errors="coerce")
    x = x.dropna(subset=["close"]).sort_values("date")
    if x.empty:
        return 0

    x = x.set_index("date")
    s = x["close"]

    # 2) EMAs
    ema5  = s.ewm(span=5,  adjust=False).mean()
    ema10 = s.ewm(span=10, adjust=False).mean()
    ema20 = s.ewm(span=20, adjust=False).mean()

    # 3) Pares de cruce a evaluar
    pairs: List[Tuple[str, pd.Series, pd.Series]] = [
        ("EMA5/EMA10",  ema5,  ema10),
        ("EMA10/EMA20", ema10, ema20),
        ("PRICE/EMA5",  s,     ema5),
        ("PRICE/EMA10", s,     ema10),
    ]
    label = lambda name: f"{name} (W)" if tf_code == "W" else name

    out: List[dict] = []
    for base_name, a, b in pairs:
        info = _last_cross_info(a, b, s)
        if not info:
            continue
        out.append({
            "activo": format_activo(symbol),
            "timeframe": tf_code,
            "indicador": label(base_name),
            "detalle": "",
            **info,
        })

    if not out:
        return 0

    # 4) Guardado (1 fila vigente por indicador/timeframe)
    for r in out:
        _save_current_signal(r)
    return len(out)
