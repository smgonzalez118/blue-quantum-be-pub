# forecasting/api/internal.py
from __future__ import annotations

import os
import sys
import gc
import json
import time
import hashlib
import subprocess
from pathlib import Path
from typing import Iterable, List, Optional

from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from market.models import Ticker
from utils.universe import get_dashboard_universe
from forecasting.io import model_exists, artifact_path


# -------------------- Auth & parsing helpers --------------------

def _auth_ok(request) -> bool:
    token = (
        request.headers.get("X-Internal-Token")
        or request.META.get("HTTP_X_INTERNAL_TOKEN")
        or getattr(request, "query_params", {}).get("token")
        or ""
    )
    expected = getattr(settings, "INTERNAL_API_TOKEN", "")
    return bool(expected) and token == expected


def _parse_symbols(val: Optional[Iterable[str] | str]) -> Optional[List[str]]:
    if not val:
        return None
    if isinstance(val, str):
        return [s.strip().upper() for s in val.split(",") if s and s.strip()]
    return [str(s).strip().upper() for s in val if s and str(s).strip()]


# -------------------- KICKOFF asíncrono de forecasts --------------------

@method_decorator(csrf_exempt, name="dispatch")
class InternalPrecomputeForecasts(APIView):
    """
    POST /api/forecasting/internal/forecast/precompute

    Body JSON opcional:
      - symbols: "AAPL,MSFT" | ["AAPL","MSFT"]
      - all_active: true
      - universe_mode: "sp100" | "adrs" | "commodities" | "etfs" | ...
      - model_name: "rf" (default: settings.FORECAST_MODEL_NAME o 'rf')
      - horizons: "21,63" | "h021,h063" (se pasa al command)
      - skip_if_unchanged: bool (default true)
      - sleep: float (pausa entre símbolos en el command)

    Respuesta: 202 Accepted con metadata del kickoff.
    """
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        if not _auth_ok(request):
            return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)

        body = request.data or {}
        model_name = (body.get("model_name") or getattr(settings, "FORECAST_MODEL_NAME", "rf")).strip()
        horizons_arg = body.get("horizons")
        if isinstance(horizons_arg, list):
            # admitir lista → "21,63"
            horizons_arg = ",".join(str(h) for h in horizons_arg)
        symbols = _parse_symbols(body.get("symbols"))
        sleep_s = float(body.get("sleep", 0.0))
        skip_if_unchanged = bool(body.get("skip_if_unchanged", True))

        # Resolver universo → lista de símbolos si no vino 'symbols'
        if not symbols:
            if bool(body.get("all_active", False)):
                qs = Ticker.objects.filter(is_active=True).values_list("symbol", flat=True)
                symbols = [s.upper() for s in qs]
            else:
                mode = str(body.get("universe_mode", "custom")).lower().strip()
                symbols = [s.upper() for s in get_dashboard_universe(mode=mode)]

        if not symbols:
            return Response({"detail": "Universo vacío"}, status=status.HTTP_400_BAD_REQUEST)

        # Validar que haya al menos un artefacto (defensivo, para feedback temprano)
        # No aborta si falta alguno; lo filtra el command.
        artifacts_missing = []
        for key in ("h021", "h063"):
            if not model_exists(key):
                artifacts_missing.append(str(artifact_path(key)))

        # Construir comando manage.py
        env = os.environ.copy()
        cmd = [sys.executable, "manage.py", "precompute_forecasts", "--model-name", model_name]
        if horizons_arg:
            cmd += ["--horizons", str(horizons_arg)]
        if skip_if_unchanged:
            cmd += ["--skip-if-unchanged"]
        if sleep_s > 0:
            cmd += ["--sleep", str(sleep_s)]
        # pasar símbolos explícitos (evita depender de all_active en el command)
        cmd += ["--symbols", *symbols]

        # Lanzar en background (no bloquea la request)
        try:
            subprocess.Popen(cmd, env=env)
        except Exception as e:
            return Response({"ok": False, "error": f"no se pudo lanzar el proceso: {e}"}, status=500)

        key_raw = ",".join(symbols[:8]) + ("..." if len(symbols) > 8 else "")
        job_key = hashlib.md5((",".join(symbols)).encode("utf-8")).hexdigest()[:10]

        return Response(
            {
                "ok": True,
                "status": "accepted",
                "job_key": job_key,
                "model": model_name,
                "symbols": len(symbols),
                "hint_first_symbols": key_raw,
                "skip_if_unchanged": skip_if_unchanged,
                "artifacts_missing": artifacts_missing or None,
                "cmd": " ".join(cmd),
            },
            status=202,
        )

    def get(self, request):
        return Response({"detail": "Method not allowed"}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

