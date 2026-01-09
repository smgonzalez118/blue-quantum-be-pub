# forecasting/views.py
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from market.models import Ticker
from .models import ForecastResult

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def forecast_latest(request, ticker: str, horizon: int):
    try:
        t = Ticker.objects.get(symbol=ticker.upper())
    except Ticker.DoesNotExist:
        return Response({"detail": "Ticker no encontrado"}, status=404)
    row = (
        ForecastResult.objects
        .filter(ticker=t, timeframe="daily", horizon=int(horizon))
        .order_by("-train_end", "-created_at")
        .values("train_end","yhat","model_name","model_version")
        .first()
    )
    return Response(row or {}, status=200)
