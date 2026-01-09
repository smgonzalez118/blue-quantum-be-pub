from django.urls import path
from market import views_internal as iv

urlpatterns = [
    path("internal/market/update-last-candle", iv.update_last_candle),
    path("internal/market/precalc", iv.precalc),

    # POST
    path("internal/company/sync-profile", iv.sync_company_profile, name="internal-sync-profile"),
    # POST
    path("internal/company/sync-fundamentals", iv.sync_fundamentals, name="internal-sync-fundamentals"),

    path("internal/market/healthz", iv.healthz, name="market_healthz"),
]