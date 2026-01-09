from rest_framework import viewsets
from rest_framework.decorators import action, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.utils.dateparse import parse_datetime
from django.db.models import Q

import logging

from .serializers import SenalTecnicaSerializer
from .models import SenalTecnica

logger = logging.getLogger(__name__)


class SenalTecnicaViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Solo lectura de señales precalculadas.

    Filtros por query params:
      - ticker=MSFT o ticker=MSFT,AAPL (filtra por 'activo' exacto, que ahora es el TICKER)
      - indicador=EMA5/EMA10 (contains)
      - tipo=BUY|SELL
      - timeframe=daily|weekly|D|W (default: daily)
      - date_from=2024-01-01T00:00:00
      - date_to=2025-01-01T00:00:00
      - search=texto (ticker/indicador, contains)
      - limit=100 (limita sin paginar)
    Orden default: -fecha (recientes primero)
    """
    permission_classes = [IsAuthenticated]
    serializer_class = SenalTecnicaSerializer

    def get_queryset(self):
        qs = SenalTecnica.objects.all()

        # timeframe (acepta daily/weekly o D/W)
        tf_raw = (self.request.query_params.get("timeframe") or "daily").strip().lower()
        if tf_raw in ("w", "week", "weekly", "semanal"):
            tf_code = "W"
        else:
            tf_code = "D"
        qs = qs.filter(timeframe=tf_code)

        # ticker(s): ahora activo guarda solo el símbolo (exacto, en MAYÚSCULAS)
        tickers = self.request.query_params.get("ticker")
        if tickers:
            symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
            if symbols:
                qs = qs.filter(activo__in=symbols)

        indicador = self.request.query_params.get("indicador")
        if indicador:
            qs = qs.filter(indicador__icontains=indicador.strip())

        tipo = self.request.query_params.get("tipo")
        if tipo:
            tip = tipo.strip().upper()
            if tip in ("BUY", "SELL"):
                qs = qs.filter(tipo=tip)

        date_from = self.request.query_params.get("date_from")
        if date_from:
            dtf = parse_datetime(date_from)
            if dtf:
                qs = qs.filter(fecha__gte=dtf)

        date_to = self.request.query_params.get("date_to")
        if date_to:
            dtt = parse_datetime(date_to)
            if dtt:
                qs = qs.filter(fecha__lte=dtt)

        search = self.request.query_params.get("search")
        if search:
            s = search.strip()
            if s:
                qs = qs.filter(Q(activo__icontains=s) | Q(indicador__icontains=s))

        qs = qs.order_by("-fecha")

        limit = self.request.query_params.get("limit")
        if limit:
            try:
                n = int(limit)
                if n > 0:
                    return qs[:n]
            except Exception:
                pass

        return qs

    # Deprecated: el cálculo corre por jobs nocturnos / endpoints internos
    @action(detail=False, methods=['post'], permission_classes=[IsAuthenticated])
    def generar_diario(self, request):
        return Response(
            {"detail": "Endpoint deprecado. Las señales se precalculan por jobs nocturnos."},
            status=410
        )

    @action(detail=False, methods=['post'], permission_classes=[IsAuthenticated])
    def generar_semanal(self, request):
        return Response(
            {"detail": "Endpoint deprecado. Las señales se precalculan por jobs nocturnos."},
            status=410
        )
