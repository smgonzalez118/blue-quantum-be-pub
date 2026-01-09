# forecasting/api/views.py
from __future__ import annotations

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from market.models import Ticker
from forecasting.models import ForecastResult

# ----------------------------------------------------------------------
# LÍMITES DE SANITIZACIÓN (retorno fraccional: 0.30 = +30%)
# Ajustalos si querés ser más/menos conservador.
# ----------------------------------------------------------------------
_RET_LIMITS = {
    5:   0.15,   # ±15% ~ 1 semana
    21:  0.30,   # ±30% ~ 1 mes
    63:  0.60,   # ±60% ~ 3 meses
    126: 1.00,   # ±100% ~ 6 meses
    252: 1.50,   # ±150% ~ 1 año
}


def _coerce_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _clip_ret(ret_frac, horizon: int):
    """Acota el retorno fraccional según el horizonte."""
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


def _parse_row_like(yhat_item: dict, horizon: int):
    """
    Espera un dict con (price_now, price_pred, ret_pct_pred) como guardamos en BD.
    Recalcula/aclipa por seguridad y devuelve tupla:
    (price_now, price_pred, ret_pct_pred)
    """
    price_now  = _coerce_float(yhat_item.get("price_now"))
    price_pred = _coerce_float(yhat_item.get("price_pred"))
    ret_pct    = _coerce_float(yhat_item.get("ret_pct_pred"))

    # Derivar retorno si hace falta
    if ret_pct is not None:
        ret_frac = ret_pct / 100.0
    elif (price_now is not None) and (price_pred is not None) and price_now != 0:
        ret_frac = (price_pred / price_now - 1.0)
    else:
        ret_frac = None

    # Clip y rehacer precio/porcentaje coherentes
    ret_frac = _clip_ret(ret_frac, horizon)

    if (price_now is not None) and (ret_frac is not None):
        price_pred = round(price_now * (1.0 + ret_frac), 2)
        ret_pct = round(ret_frac * 100.0, 2)
    elif ret_frac is not None:
        ret_pct = round(ret_frac * 100.0, 2)

    return price_now, price_pred, ret_pct


class ForecastLatestView(APIView):
    """
    Devuelve los forecasts precalculados más recientes para un ticker.
    Respuesta:
      {
        "ticker": "AAPL",
        "horizons": [
          {"key":"h005","days":5,"price_now":..., "price_pred":..., "ret_pct_pred":...},
          ...
        ]
      }
    """

    def post(self, request):
        ticker = str(request.data.get("ticker", "")).upper().strip()
        if not ticker:
            return Response({"detail": "ticker requerido"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            t = Ticker.objects.get(symbol=ticker)
        except Ticker.DoesNotExist:
            return Response({"detail": f"Ticker no encontrado: {ticker}"}, status=status.HTTP_404_NOT_FOUND)

        qs = ForecastResult.objects.filter(ticker=t, timeframe="daily").order_by("-train_end", "-created_at")
        if not qs.exists():
            return Response({"detail": "No hay forecasts precalculados para este ticker."},
                            status=status.HTTP_404_NOT_FOUND)

        latest_train_end = qs.first().train_end
        rows = (ForecastResult.objects
                .filter(ticker=t, timeframe="daily", train_end=latest_train_end)
                .order_by("horizon"))

        out = []
        for r in rows:
            # En BD guardamos yhat como lista con 1 dict; tomamos el último por robustez
            yhat_list = r.yhat if isinstance(r.yhat, list) else [r.yhat]
            yitem = (yhat_list or [{}])[-1]

            price_now, price_pred, ret_pct = _parse_row_like(yitem, int(r.horizon))

            out.append({
                "key": f"h{int(r.horizon):03d}",
                "days": int(r.horizon),
                "price_now": price_now,
                "price_pred": price_pred,
                "ret_pct_pred": ret_pct,
            })

        out.sort(key=lambda x: x["days"])
        return Response({"ticker": ticker, "horizons": out}, status=status.HTTP_200_OK)
