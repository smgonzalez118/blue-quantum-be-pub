#dashboard/serializers.py
from rest_framework import serializers
from .models import AtributoDashboard, Favorite

class AtributoDashboardSerializer(serializers.ModelSerializer):
    # devolvemos 'activo' ya formateado: "MSFT (Microsoft)"
    activo = serializers.SerializerMethodField()

    class Meta:
        model = AtributoDashboard
        fields = [
            "id", "ticker", "activo", "timeframe", "precio",
            "macd", "pmm5", "pmm10", "pmm20", "pmm30",
            "mm5_10", "mm10_20", "tripleCruce", "pmm100",
            "rsi", "dmi", "adx", "updated_at",
        ]

    def get_activo(self, obj):
        sym = (obj.ticker or "").strip().upper()
        try:
            from utils.nombres import get_nombre_ticker
            name = (get_nombre_ticker(sym) or "").strip()
        except Exception:
            name = ""
        return f"{sym} ({name if name else sym})" if sym else ""

class FavoriteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Favorite
        fields = ["id", "ticker", "created_at"]  # <-- era "symbol", corregido
        read_only_fields = ["id", "created_at"]
