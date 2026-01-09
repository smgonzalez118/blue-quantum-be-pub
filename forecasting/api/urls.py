# forecasting/api/urls.py
from django.urls import path
from .views import ForecastLatestView
from .internal import InternalPrecomputeForecasts

urlpatterns = [
    # mantiene el endpoint que usa tu front
    path("forecast/generate/", ForecastLatestView.as_view(), name="forecast-generate"),
    path("internal/forecast/precompute", InternalPrecomputeForecasts.as_view()),
]
