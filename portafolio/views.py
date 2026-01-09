# views.py (o donde tengas la view del optimizador)
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from utils import optimizer as opt

#from utils.optimizer import optimizar_montecarlo


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def generate_portfolio(request):
    """
    Body esperado:
    {
      "activos": ["AAPL","MSFT","KO"],
      "timeframe": "diario" | "semanal",
      "q": 1000,
      "metrica": "sharpe" | "sortino" | "rentabilidad" | "riesgo",
      "rf": 0.0,
      "min_weight": 0,    # % por activo
      "max_weight": 100   # % por activo
    }
    """
    try:
        p = request.data or {}
        activos   = list(p.get("activos", []))
        timeframe = str(p.get("timeframe", "diario")).lower()
        q         = int(p.get("q", 500))
        metrica   = str(p.get("metrica", "sharpe")).lower()
        rf        = float(p.get("rf", 0.0))
        min_w     = float(p.get("min_weight", 0.0))     # % por activo
        max_w     = float(p.get("max_weight", 100.0))   # % por activo

        # Validaciones básicas
        if len(activos) < 2:
            return Response({"error": "Se requieren al menos 2 activos."}, status=400)
        if min_w < 0 or max_w < 0:
            return Response({"error": "Los pesos no pueden ser negativos."}, status=400)
        if min_w > max_w:
            return Response({"error": "El peso mínimo no puede ser mayor que el máximo."}, status=400)
        if max_w > 100:
            return Response({"error": "El peso máximo no puede superar 100%."}, status=400)

        # Factibilidad: n·min ≤ 100 ≤ n·max
        n = len(activos)
        if n * min_w > 100 + 1e-9:
            return Response(
                {"error": f"Inviable: {n} × {min_w}% > 100%. Bajá el mínimo o agregá activos."},
                status=400,
            )
        if n * max_w < 100 - 1e-9:
            return Response(
                {"error": f"Inviable: {n} × {max_w}% < 100%. Subí el máximo o agregá activos."},
                status=400,
            )

        # ⬇️ OJO: el optimizador devuelve 4 valores (no 5)
        carteras, rta, rta_df, allocations = opt.optimizar_montecarlo(
            activos=activos,
            timeframe=timeframe,
            q=q,
            metrica=metrica,
            rf=rf,
            min_weight=min_w,   # en %
            max_weight=max_w,   # en %
        )

        top = carteras.head(10).copy()
        # Asegurar que los pesos sean serializables
        top["weights"] = top["weights"].apply(lambda w: [float(x) for x in w])

        return Response({
            "inputs": {
                "activos": activos,
                "timeframe": timeframe,
                "q": q,
                "metrica": metrica,
                "rf": rf,
                "min_weight": min_w,
                "max_weight": max_w,
            },
            "cartera_optima": rta,          # contiene ret_anual, vol_anual, sharpe, sortino y weights_pct
            "allocations": allocations,     # [{ticker, weight_pct}]
            "top10_simulaciones": top.to_dict(orient="records"),
        }, status=200)

    except ValueError as e:
        # Errores de datos/validación
        return Response({"error": str(e)}, status=400)
    except Exception as e:
        # Cualquier otra cosa
        return Response({"error": str(e)}, status=500)








