# utils/optimizer.py
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Any

from utils.data_access import prices_df


# ===================== Helpers de timeframe =====================

def _tf_and_factor(timeframe: str) -> Tuple[str, int]:
    """
    'diario' -> ('daily', 252)
    'semanal' -> ('weekly', 52)
    """
    t = (timeframe or "diario").lower()
    if t.startswith("seman"):
        return "weekly", 52
    return "daily", 252


# ===================== Limpieza / Alineación =====================

def _daily_clean(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Serie diaria:
      - índice = fecha (sin hora, tz naive)
      - último valor de cada día (si hay duplicados por día)
    """
    x = df.copy()
    if x.index.name != "date":
        x = x.set_index("date")
    x.index = pd.to_datetime(x.index, errors="coerce").tz_localize(None).normalize()
    s = pd.to_numeric(x[col], errors="coerce").dropna()
    s = s.groupby(s.index).last().sort_index()  # último del día
    return s


def _weekly_clean(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Serie semanal alineada a W-FRI (viernes):
      - toma el último valor disponible de cada semana
    """
    x = df.copy()
    if x.index.name != "date":
        x = x.set_index("date")
    x.index = pd.to_datetime(x.index, errors="coerce").tz_localize(None)
    s = pd.to_numeric(x[col], errors="coerce").dropna()

    w = s.to_frame("v")
    w["week"] = w.index.to_period("W-FRI")
    s = w.groupby("week")["v"].last()
    s.index = s.index.to_timestamp(how="end").normalize()  # viernes
    s = s.sort_index()
    return s


def _load_returns(activos: List[str], tf: str) -> pd.DataFrame:
    """
    Devuelve matriz de retornos (filas=fechas, columnas=tickers) alineada y sin NaN.
    - Usa 'adj_close' si existe; si no, 'close'.
    - Diario: fecha pura (sin hora) y último valor por día.
    - Semanal: W-FRI y último valor semanal.
    - Retornos simples (pct_change).
    """
    if not activos:
        raise ValueError("Lista de activos vacía.")

    cleaners = {"daily": _daily_clean, "weekly": _weekly_clean}
    if tf not in cleaners:
        tf = "daily"
    clean_fn = cleaners[tf]

    series: List[pd.Series] = []
    tickers: List[str] = []

    for sym in (s.upper().strip() for s in activos):
        df = prices_df(symbol=sym, tf=tf)
        if df is None or df.empty:
            continue

        col = "adj_close" if "adj_close" in df.columns else "close"

        try:
            s = clean_fn(df, col)
        except Exception:
            continue

        r = s.pct_change().dropna()
        if r.empty:
            continue

        series.append(r.rename(sym).astype("float64"))
        tickers.append(sym)

    if len(series) < 2:
        raise ValueError("No se pudieron construir retornos para 2 o más activos.")

    # Intersección estricta
    R = pd.concat(series, axis=1, join="inner").dropna(how="any")
    if R.empty or R.shape[1] < 2:
        # Reintento: calendario común + ffill limitado
        start = max(s.index.min() for s in series)
        end = min(s.index.max() for s in series)
        if start >= end:
            raise ValueError("Serie de retornos vacía/insuficiente luego de alinear fechas.")

        freq = "B" if tf == "daily" else "W-FRI"
        idx = pd.date_range(start, end, freq=freq)

        series2: List[pd.Series] = []
        for r in series:
            rr = r.reindex(idx).ffill(limit=1)
            series2.append(rr)

        R2 = pd.concat(series2, axis=1, join="inner").dropna(how="any")
        if R2.empty or R2.shape[1] < 2:
            raise ValueError("Serie de retornos vacía/insuficiente luego de alinear fechas.")
        return R2

    return R


# ===================== Generación de pesos =====================

def _bounded_dirichlet_weights(
    n: int,
    min_w: float = 0.0,
    max_w: float = 1.0,
    tries: int = 2000,
    tol: float = 1e-9,
) -> np.ndarray:
    """
    Devuelve pesos w>=0 que suman 1 y cumplen min_w <= w_i <= max_w.
    - Rechazo de muestras Dirichlet.
    - Fallback por clipping + redistribución si no encuentra en 'tries'.
    """
    # factibilidad
    if n * min_w - 1.0 > tol or 1.0 - n * max_w > tol:
        raise ValueError(
            f"Límites de peso inviables: n*min={n*min_w:.3f}, n*max={n*max_w:.3f} (debe cumplirse n*min ≤ 1 ≤ n*max)."
        )

    # 1) Rechazo
    for _ in range(tries):
        w = np.random.dirichlet(np.ones(n))
        if (w >= min_w - tol).all() and (w <= max_w + tol).all():
            return w

    # 2) Fallback: clipping + redistribución iterativa
    w = np.random.dirichlet(np.ones(n))
    for _ in range(200):
        w = np.clip(w, min_w, max_w)
        s = w.sum()
        if s <= 0:
            w = np.full(n, 1.0 / n)
            continue
        if abs(s - 1.0) < 1e-12:
            break
        # redistribuir el sobrante/faltante en los "activos libres"
        free = (w > min_w + 1e-12) & (w < max_w - 1e-12)
        k = int(free.sum())
        if k == 0:
            # repartir equitativamente respetando límites
            w = np.full(n, min_w)
            rem = 1.0 - n * min_w
            if rem > tol:
                caps = max_w - w
                # distribuir rem de forma proporcional a la capacidad disponible
                cap_sum = np.sum(caps)
                if cap_sum > tol:
                    w += caps * (rem / cap_sum)
            w = np.clip(w, min_w, max_w)
            w /= w.sum()
            return w
        w[free] += (1.0 - s) / k

    w = np.clip(w, min_w, max_w)
    w /= w.sum()
    return w


# ===================== Montecarlo =====================

def optimizar_montecarlo(
    activos: List[str],
    timeframe: str = "diario",
    q: int = 1000,
    metrica: str = "sharpe",
    rf: float = 0.0,                # tasa libre de riesgo ANUAL
    max_weight: float | None = None,
    min_weight: float | None = None,
    **_: Any                         # ignora kwargs inesperados sin romper
) -> Tuple[pd.DataFrame, Dict[str, Any], pd.DataFrame, List[Dict[str, Any]]]:
    """
    Monte Carlo clásico:
      - Maximiza: 'sharpe' | 'sortino' | 'rentabilidad'
      - Minimiza: 'riesgo'
      - rf: tasa anual libre de riesgo (se resta al retorno para Sharpe/Sortino)
      - max_weight/min_weight: límites por activo (0..1 o 0..100)
    Retorna:
      - carteras: DataFrame con simulaciones ordenadas por la métrica elegida
      - rta: dict con métricas de la cartera óptima + pesos %
      - rta_df: DataFrame (ticker, weight_pct)
      - allocations: [{ticker, weight, weight_pct}]
    """
    # normalizar límites si vienen en %
    def _norm_bound(x: float | None, default: float) -> float:
        if x is None:
            return default
        x = float(x)
        return x / 100.0 if x > 1.001 else x

    min_w = _norm_bound(min_weight, 0.0)
    max_w = _norm_bound(max_weight, 1.0)

    tf, ann = _tf_and_factor(timeframe)
    R = _load_returns(activos, tf=tf)      # retornos por período (filas=fechas)
    tickers = list(R.columns)
    n = len(tickers)

    # parámetros anualizados
    mu = R.mean().values * ann            # retorno esperado anual (vector n)
    cov = R.cov().values * ann            # matriz covarianza anual (n x n)

    # cache para sortino
    R_mat = R.values                      # T x n
    results = []
    weights_list = []

    q = int(q) if q and int(q) > 0 else 1000

    for _ in range(q):
        # pesos con límites
        w = _bounded_dirichlet_weights(n, min_w=min_w, max_w=max_w)

        ret_ann = float(np.dot(w, mu))
        vol_ann = float(np.sqrt(np.dot(w, np.dot(cov, w))))
        sharpe = (ret_ann - rf) / vol_ann if vol_ann > 0 else np.nan

        # sortino (downside deviation) anualizado
        port_period = R_mat.dot(w)        # retornos del portafolio por período
        downside = np.std(np.minimum(0.0, port_period), ddof=1) * np.sqrt(ann)
        sortino = (ret_ann - rf) / downside if downside > 0 else np.nan

        results.append([ret_ann, vol_ann, sharpe, sortino])
        weights_list.append(w)

    carteras = pd.DataFrame(results, columns=["ret_anual", "vol_anual", "sharpe", "sortino"])
    carteras["weights"] = weights_list

    # elegir la "mejor" según métrica
    m = (metrica or "sharpe").lower()
    if m == "riesgo":
        idx_best = carteras["vol_anual"].idxmin()
        carteras = carteras.sort_values("vol_anual", ascending=True)
    elif m == "rentabilidad":
        idx_best = carteras["ret_anual"].idxmax()
        carteras = carteras.sort_values("ret_anual", ascending=False)
    elif m == "sortino":
        idx_best = carteras["sortino"].idxmax()
        carteras = carteras.sort_values("sortino", ascending=False)
    else:
        m = "sharpe"
        idx_best = carteras["sharpe"].idxmax()
        carteras = carteras.sort_values("sharpe", ascending=False)

    w_opt = np.array(carteras.loc[idx_best, "weights"], dtype=float)
    ret_opt = float(carteras.loc[idx_best, "ret_anual"])
    vol_opt = float(carteras.loc[idx_best, "vol_anual"])
    sh_opt  = float(carteras.loc[idx_best, "sharpe"])
    so_opt  = float(carteras.loc[idx_best, "sortino"])

    # salida en %
    weights_pct = [round(float(x) * 100.0, 2) for x in w_opt]  # <- fuerza float nativo + redondeo
    def _normalize_to_100(weights_pct):
        total = sum(weights_pct)
        if not weights_pct or total == 100.0:
            return weights_pct
        diff = round(100.0 - total, 2)
        # asigna el ajuste al mayor peso (minimiza impacto visual)
        i = int(max(range(len(weights_pct)), key=lambda k: weights_pct[k]))
        weights_pct[i] = round(weights_pct[i] + diff, 2)
        return weights_pct

    weights_pct = _normalize_to_100(weights_pct)

    rta_weights_dict = {tickers[i]: weights_pct[i] for i in range(n)}

    rta = {
        "metrica": m,
        "ret_anual": round(ret_opt, 4),
        "vol_anual": round(vol_opt, 4),
        "sharpe": round(sh_opt, 4),
        "sortino": round(so_opt, 4),
        "weights_pct": rta_weights_dict,
    }

    rta_df = pd.DataFrame({"ticker": tickers, "weight_pct": weights_pct})

    allocations = [
        {"ticker": tickers[i], "weight": weights_pct[i], "weight_pct": weights_pct[i]}
        for i in range(n)
    ]

    return carteras, rta, rta_df, allocations

