# activo/admin.py
from django.contrib import admin
from .models import CompanyProfile, FundamentalMetric, ComparablePeer

@admin.register(CompanyProfile)
class CompanyProfileAdmin(admin.ModelAdmin):
    list_display = ("ticker", "name", "sector", "industry", "exchange", "currency", "market_cap", "updated_at")
    list_filter = ("sector", "industry", "exchange", "currency")
    search_fields = ("ticker__symbol", "name", "sector", "industry")

@admin.register(FundamentalMetric)
class FundamentalMetricAdmin(admin.ModelAdmin):
    list_display = ("ticker", "metric", "period_end", "value")
    list_filter = ("metric",)
    search_fields = ("ticker__symbol",)
    date_hierarchy = "period_end"

@admin.register(ComparablePeer)
class ComparablePeerAdmin(admin.ModelAdmin):
    list_display = ("base", "peer", "rank")
    search_fields = ("base__symbol", "peer__symbol")