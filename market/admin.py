# market/admin.py
from django.contrib import admin
from .models import Ticker, PriceDaily

@admin.register(Ticker)
class TickerAdmin(admin.ModelAdmin):
    list_display = ("symbol","name","is_active")
    search_fields = ("symbol","name")

@admin.register(PriceDaily)
class PriceDailyAdmin(admin.ModelAdmin):
    list_display = ("ticker","date","close","volume")
    list_filter = ("ticker",)
    date_hierarchy = "date"
