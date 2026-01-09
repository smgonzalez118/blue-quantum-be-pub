from rest_framework import viewsets, mixins, status
from rest_framework.decorators import action, permission_classes, api_view
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.db.models import Q
from django.db import IntegrityError
from django.conf import settings
from .models import AtributoDashboard, Favorite
from .serializers import AtributoDashboardSerializer, FavoriteSerializer

import pandas as pd
import os
import logging

from utils.data_access import prices_df  # usado en favoritos_detalle

UNIVERSE_MODE = getattr(settings, "UNIVERSE_MODE", "sp500")  # no se usa acá, pero lo dejamos si tu frontend lo espera
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger(__name__)


class DashboardViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Solo lectura de snapshots ya precalculados en AtributoDashboard.

    Filtros por query params:
      - ticker=MSFT (uno o varios separados por coma)
      - search=texto (busca en ticker/activo)
      - ordering=precio|-precio|activo|-activo|ticker|-ticker (default: activo)
    """
    permission_classes = [IsAuthenticated]
    serializer_class = AtributoDashboardSerializer

    def get_queryset(self):
        qs = AtributoDashboard.objects.all()

        # timeframe (default: daily)
        tf = (self.request.query_params.get("timeframe") or "daily").lower()
        tf_code = "D" if tf == "daily" else "W"
        qs = qs.filter(timeframe=tf_code)

        tickers = self.request.query_params.get("ticker")
        if tickers:
            symbols = [t.strip().upper() for t in tickers.split(",") if t.strip()]
            qs = qs.filter(ticker__in=symbols)

        search = self.request.query_params.get("search")
        if search:
            s = search.strip()
            qs = qs.filter(Q(ticker__icontains=s) | Q(activo__icontains=s))

        ordering = self.request.query_params.get("ordering", "activo")
        allowed = {"precio", "-precio", "activo", "-activo", "ticker", "-ticker"}
        if ordering not in allowed:
            ordering = "activo"
        return qs.order_by(ordering)


    # Deprecated: el cálculo corre por jobs nocturnos / endpoints internos
    @action(detail=False, methods=['post'], permission_classes=[IsAuthenticated])
    def generar_diario(self, request):
        return Response(
            {"detail": "Endpoint deprecado. Los datos se precalculan por jobs nocturnos."},
            status=410
        )

    @action(detail=False, methods=['post'], permission_classes=[IsAuthenticated])
    def generar_semanal(self, request):
        return Response(
            {"detail": "Endpoint deprecado. Los datos se precalculan por jobs nocturnos."},
            status=410
        )


class FavoriteViewSet(mixins.ListModelMixin,
                      mixins.CreateModelMixin,
                      mixins.DestroyModelMixin,
                      viewsets.GenericViewSet):
    """
    /favorites/               GET  -> lista del usuario
                              POST -> crea (idempotente por (user, ticker))
    /favorites/{id}/          DELETE -> elimina por id del recurso
    /favorites/toggle/        POST -> {ticker} agrega/borra (idempotente)
    /favorites/tickers/       GET  -> [ "AAPL", "MSFT", ... ]
    /favorites/by-ticker/{t}/ DELETE -> elimina por ticker (de este usuario)
    """
    permission_classes = [IsAuthenticated]
    serializer_class = FavoriteSerializer

    def get_queryset(self):
        return Favorite.objects.filter(user=self.request.user).order_by("-created_at")

    def perform_create(self, serializer):
        t = str(serializer.validated_data.get("ticker", "")).upper().strip()
        if not t:
            raise ValueError("ticker requerido")
        try:
            serializer.save(user=self.request.user, ticker=t)
        except IntegrityError:
            # ya existe -> idempotente
            pass

    @action(detail=False, methods=["post"], url_path="toggle")
    def toggle(self, request):
        t = str(request.data.get("ticker", "")).upper().strip()
        if not t:
            return Response({"detail": "ticker requerido"}, status=400)
        fav, created = Favorite.objects.get_or_create(user=request.user, ticker=t)
        if created:
            return Response({"status": "added", "ticker": t}, status=201)
        fav.delete()
        return Response({"status": "removed", "ticker": t}, status=200)

    @action(detail=False, methods=["get"], url_path="tickers")
    def tickers(self, request):
        syms = self.get_queryset().values_list("ticker", flat=True)
        return Response(sorted(list(syms)))

    @action(detail=False, methods=["delete"], url_path=r"by-ticker/(?P<ticker>[^/]+)")
    def delete_by_ticker(self, request, ticker: str):
        t = str(ticker).upper().strip()
        Favorite.objects.filter(user=self.request.user, ticker=t).delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def favoritos_detalle(request):
    """
    Devuelve para los favoritos del usuario:
      - nombre y sector (si existen en tickers_nombres.csv)
      - precio_actual y variaciones 1d, 1w, 1m, 1y, YTD
    Lee precios con prices_df(symbol=ticker, tf="daily").
    Robusto a encodings del CSV (utf-8, utf-8-sig, latin-1) y a favoritos sin datos.
    """
    import os
    import pandas as pd

    # --- localizar CSV ---
    TICKERS_CSV = getattr(settings, "TICKERS_CSV", None)
    if not TICKERS_CSV:
        csv_root = getattr(settings, "CSV_ROOT", BASE_DIR)
        cand = os.path.join(csv_root, "datasets", "tickers_nombres.csv")
        TICKERS_CSV = cand if os.path.exists(cand) else os.path.join(
            BASE_DIR, "datasets", "tickers_nombres.csv"
        )

    # --- leer favoritos ---
    favoritos = list(
        Favorite.objects.filter(user=request.user).values_list("ticker", flat=True)
    )
    if not favoritos:
        return Response([])

    # --- lector robusto del CSV ---
    def _read_info_csv(path: str) -> pd.DataFrame | None:
        if not os.path.exists(path):
            return None
        for enc in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError:
                continue
            except Exception:
                break
        try:
            return pd.read_csv(path, encoding="utf-8", errors="ignore")
        except Exception:
            return None

    df_info = _read_info_csv(TICKERS_CSV)

    info_ok = False
    if df_info is not None and not df_info.empty:
        cols = {str(c).strip() for c in df_info.columns}
        info_ok = {"Ticker", "Nombre"}.issubset(cols)
        if info_ok:
            df_info["Ticker"] = df_info["Ticker"].astype(str).str.upper().str.strip()
            df_info["Nombre"] = df_info["Nombre"].astype(str).str.strip()
            if "Sector" in df_info.columns:
                df_info["Sector"] = df_info["Sector"].astype(str).str.strip()

    def lookup_nombre_sector(tkr: str) -> tuple[str, str]:
        if not info_ok:
            return (tkr, "")
        row = df_info[df_info["Ticker"] == tkr.upper().strip()]
        if row.empty:
            return (tkr, "")
        nombre = row["Nombre"].iloc[0] if "Nombre" in row else tkr
        sector = row["Sector"].iloc[0] if "Sector" in row else ""
        return (str(nombre), str(sector))

    out = []

    for tkr in favoritos:
        t = str(tkr).upper().strip()
        nombre, sector = lookup_nombre_sector(t)

        cambios = {
            "precio_actual": None,
            "cambio_1d": None,
            "cambio_1w": None,
            "cambio_1m": None,
            "cambio_1y": None,
            "cambio_ytd": None,
        }

        try:
            df = prices_df(symbol=t, tf="daily")
            if df is None or df.empty:
                raise ValueError("sin datos de precios")

            if df.index.name == "date":
                df = df.reset_index()

            if "date" not in df.columns or "close" not in df.columns:
                raise ValueError("dataset sin columnas requeridas (date/close)")

            df = df[["date", "close"]].copy()
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.normalize()
            df["close"] = pd.to_numeric(df["close"], errors="coerce").astype("float64")
            df = df.dropna(subset=["date", "close"]).sort_values("date")
            if df.empty:
                raise ValueError("serie vacía tras normalización")

            s = df["close"].reset_index(drop=True)

            def last_n(n_back: int):
                if len(s) >= n_back:
                    return float(s.iloc[-n_back])
                return None

            precio_actual   = last_n(1)
            cierre_anterior = last_n(2)
            cierre_semana   = last_n(6)    # ~5 ruedas
            cierre_mes      = last_n(22)   # ~21 ruedas
            cierre_ano      = last_n(253)  # ~252 ruedas

            ultimo_anio = int(df["date"].max().year)
            s_ytd = df[df["date"].dt.year == ultimo_anio]["close"]
            if len(s_ytd) >= 1:
                base_ytd = float(s_ytd.iloc[0])
            else:
                s_prev = df[df["date"].dt.year == (ultimo_anio - 1)]["close"]
                base_ytd = float(s_prev.iloc[-1]) if len(s_prev) >= 1 else None

            def pct(base):
                if base is None or precio_actual is None or base == 0:
                    return None
                return round(100.0 * (precio_actual / base - 1.0), 2)

            cambios.update({
                "precio_actual": round(precio_actual, 2) if precio_actual is not None else None,
                "cambio_1d": pct(cierre_anterior),
                "cambio_1w": pct(cierre_semana),
                "cambio_1m": pct(cierre_mes),
                "cambio_1y": pct(cierre_ano),
                "cambio_ytd": pct(base_ytd),
            })

        except Exception as e:
            # No rompe el endpoint: deja constancia por item.
            cambios["error"] = f"{t}: {e}"

        out.append({
            "ticker": t,
            "nombre": nombre,
            "sector": sector,
            **cambios
        })

    return Response(out)

