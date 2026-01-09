# market/services/polygon_client.py
from __future__ import annotations

import datetime as dt
import time
from typing import Any, Dict, List, Optional

import requests
from decouple import config

POLYGON_API_KEY = config("POLYGON_API_KEY", default="")
BASE = "https://api.polygon.io"
DEFAULT_TIMEOUT = 30


class PolygonClient:
    """
    Cliente mínimo/robusto para Polygon:
      - _get(): agrega apiKey, maneja 403 como PermissionError y deja pasar otras HTTPError
      - grouped_daily_stocks(): /v2/aggs/grouped/...
      - eod_bar(): intenta /v2/aggs ... y cae a /v1/open-close
      - range_aggs(): rango diario ajustado con /v2/aggs
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT):
        self.api_key = api_key or POLYGON_API_KEY
        self.timeout = int(timeout)

        if not self.api_key:
            raise RuntimeError("Falta POLYGON_API_KEY en el entorno")

    # -------- utils

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = dict(params or {})
        params["apiKey"] = self.api_key
        url = f"{BASE}{path}"
        r = requests.get(url, params=params, timeout=self.timeout)

        if r.status_code == 403:
            # Dejá que el caller decida qué hacer (agrupado, fallback, etc.)
            raise PermissionError(
                f"Polygon 403 Forbidden en {path}. "
                "Tu plan/clave puede no tener acceso a este endpoint."
            )
        # Para 404/429/etc. dejamos que raise_for_status() levante HTTPError
        r.raise_for_status()
        # Polygon siempre responde JSON en estos endpoints
        return r.json() if r.content else {}

    @staticmethod
    def resolve_symbol(symbol: str) -> str:
        """
        Corrige alias “cortos” que a veces aparecen en listas (S&P, etc.)
        y que Polygon NO reconoce en endpoints por-símbolo.
        """
        s = (symbol or "").upper().strip()
        # Mapeos específicos y seguros:
        if s == "BF":
            return "BF.B"   # Brown-Forman
        if s == "BRK":
            return "BRK.B"  # Berkshire
        return s

    # -------- EOD por símbolo

    def eod_bar_aggs(self, symbol: str, date: dt.date) -> Optional[Dict[str, Any]]:
        """
        Intenta EOD con /v2/aggs/ticker/{sym}/range/1/day/{date}/{date}?adjusted=true
        Devuelve dict normalizado o None si no hay resultados.
        """
        sym = self.resolve_symbol(symbol)
        path = f"/v2/aggs/ticker/{sym}/range/1/day/{date}/{date}"
        data = self._get(path, params={"adjusted": "true", "sort": "asc", "limit": 2})
        if not isinstance(data, dict) or data.get("resultsCount", 0) == 0:
            return None
        res = data["results"][0]
        return {
            "open": res["o"],
            "high": res["h"],
            "low":  res["l"],
            "close": res["c"],
            "adj_close": res["c"],  # adjusted=true
            "volume": int(res.get("v", 0)),
        }

    def eod_bar_openclose(self, symbol: str, date: dt.date) -> Optional[Dict[str, Any]]:
        """
        Fallback: /v1/open-close/{symbol}/{date}
        Suele estar habilitado en el plan free.
        Puede lanzar HTTPError (404 p.ej. cuando el símbolo no existe).
        """
        sym = self.resolve_symbol(symbol)
        path = f"/v1/open-close/{sym}/{date.isoformat()}"
        data = self._get(path)
        if not isinstance(data, dict) or data.get("status") != "OK":
            return None
        return {
            "open":  data["open"],
            "high":  data["high"],
            "low":   data["low"],
            "close": data["close"],
            # No hay “adjusted” explícito acá. Usamos close (o afterHours si viene).
            "adj_close": data.get("afterHours", data["close"]),
            "volume": int(data.get("volume", 0)),
        }

    def eod_bar(self, symbol: str, date: dt.date) -> Optional[Dict[str, Any]]:
        """
        Primero intenta v2/aggs; si falla por permisos u otra razón,
        cae a v1/open-close. Si v1 devuelve 404, deja pasar HTTPError y
        que el caller lo cuente (etl_grouped ya lo maneja).
        """
        # 1) aggs
        try:
            row = self.eod_bar_aggs(symbol, date)
            if row:
                return row
        except PermissionError:
            # sin permisos para aggs → probamos open-close
            pass
        # 2) open-close (puede lanzar HTTPError 404/429/etc.)
        return self.eod_bar_openclose(symbol, date)

    # -------- Rangos

    def range_aggs(self, symbol: str, start: dt.date, end: dt.date) -> Optional[List[Dict[str, Any]]]:
        """
        Rango diario ajustado con /v2/aggs (máx 50k results por llamado).
        """
        sym = self.resolve_symbol(symbol)
        path = f"/v2/aggs/ticker/{sym}/range/1/day/{start}/{end}"
        data = self._get(path, params={"adjusted": "true", "sort": "asc", "limit": 50000})
        if not isinstance(data, dict) or data.get("resultsCount", 0) == 0:
            return None
        out: List[Dict[str, Any]] = []
        for res in data["results"]:
            d = dt.datetime.utcfromtimestamp(res["t"] / 1000).date()
            out.append({
                "date": d,
                "open":  res["o"],
                "high":  res["h"],
                "low":   res["l"],
                "close": res["c"],
                "adj_close": res["c"],  # adjusted=true
                "volume": int(res.get("v", 0)),
            })
        return out

    # -------- Grouped

    def grouped_daily_stocks(self, date: dt.date, include_otc: bool = False) -> List[Dict[str, Any]]:
        """
        GET /v2/aggs/grouped/locale/us/market/stocks/{date}
        Devuelve lista normalizada:
          {"symbol","open","high","low","close","adj_close","volume","vwap"}
        """
        path = f"/v2/aggs/grouped/locale/us/market/stocks/{date.isoformat()}"
        data = self._get(path, params={"adjusted": "true", "include_otc": str(include_otc).lower()})
        results = data.get("results", []) if isinstance(data, dict) else []
        out: List[Dict[str, Any]] = []
        for r in results:
            sym = r.get("T")
            if not sym:
                continue
            out.append({
                "symbol": sym,
                "open":   r.get("o"),
                "high":   r.get("h"),
                "low":    r.get("l"),
                "close":  r.get("c"),
                "adj_close": r.get("c"),  # adjusted=true
                "volume": int(r.get("v") or 0),
                "vwap":   r.get("vw"),
            })
        return out
