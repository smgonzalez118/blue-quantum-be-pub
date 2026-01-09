# senales/serializers.py
import re
from rest_framework import serializers
from .models import SenalTecnica

class SenalTecnicaSerializer(serializers.ModelSerializer):
    activo_label = serializers.SerializerMethodField()

    class Meta:
        model = SenalTecnica
        fields = [
            "activo",
            "activo_label",
            "timeframe",
            "tipo",
            "fecha",
            "indicador",
            "precio_senal",
            "precio_actual",
            "detalle",
            "rendimiento",
        ]

    def get_activo_label(self, obj):
        raw = (obj.activo or "").strip().upper()
        # ⬇️ ticker limpio: corta en espacio o '(' y se queda con el primer token
        sym = re.split(r"[ \t(]", raw, 1)[0] if raw else ""

        try:
            from utils.nombres import get_nombre_ticker
            name = (get_nombre_ticker(sym) or "").strip()
        except Exception:
            name = ""

        # Fallback si no hay nombre
        return f"{sym} ({name if name else sym})" if sym else ""
