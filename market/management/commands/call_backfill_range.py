# market/management/commands/call_backfill_range.py
from __future__ import annotations

import os
import time
import json
import sys
import typing as t
from pathlib import Path
from datetime import date, timedelta

import requests
from django.core.management.base import BaseCommand, CommandError


def _read_symbols_file(p: str) -> list[str]:
    path = Path(p)
    if not path.exists():
        raise CommandError(f"--symbols-file no existe: {p}")
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = (line or "").strip().upper()
        if s:
            out.append(s)
    # dedupe preservando orden
    seen, dedup = set(), []
    for s in out:
        if s not in seen:
            seen.add(s)
            dedup.append(s)
    return dedup


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


class Command(BaseCommand):
    help = "Cliente para el endpoint interno /internal/market/backfill-range (por fechas)."

    def add_arguments(self, parser):
        parser.add_argument("--base", type=str, default=os.getenv("BACKEND_BASE_URL") or os.getenv("BACKEND_BASE"),
                            help="Base URL del backend (ej: https://tuapp.onrender.com)")
        parser.add_argument("--token", type=str, default=os.getenv("INTERNAL_API_TOKEN") or os.getenv("JOB_RUN_TOKEN"),
                            help="Token interno (X-Internal-Token).")
        parser.add_argument("--start", type=str, required=True, help="Fecha inicio YYYY-MM-DD")
        parser.add_argument("--end", type=str, required=True, help="Fecha fin YYYY-MM-DD")

        # Selección de universo (una de estas opciones)
        parser.add_argument("--universe-mode", type=str, default=None,
                            help="Modo de universo: custom | sp100 | files | adrs | etfs | commodities")
        parser.add_argument("--all-active", action="store_true", help="Usar todos los Ticker.is_active")
        parser.add_argument("--symbols", type=str, default=None,
                            help="Lista de símbolos separada por coma (ej: AAPL,MSFT)")
        parser.add_argument("--symbols-file", type=str, default=None,
                            help="Archivo con símbolos (uno por línea)")

        # Reintentos si la API devuelve partial en alguna fecha
        parser.add_argument("--retries", type=int, default=int(os.getenv("BACKFILL_RETRIES", "3")),
                            help="Reintentos por fecha si partial=true (default 3)")
        parser.add_argument("--sleep", type=float, default=float(os.getenv("BACKFILL_SLEEP", "3")),
                            help="Sleep (segundos) entre reintentos por fecha (default 3)")
        parser.add_argument("--max-time", type=int, default=int(os.getenv("BACKFILL_MAX_TIME", "120")),
                            help="Timeout por request en segundos (default 120)")

    def handle(self, *args, **opt):
        base: str | None = opt["base"]
        token: str | None = opt["token"]
        if not base:
            raise CommandError("--base no especificado (o env BACKEND_BASE/BACKEND_BASE_URL)")
        if not token:
            raise CommandError("--token no especificado (o env INTERNAL_API_TOKEN/JOB_RUN_TOKEN)")

        try:
            start = date.fromisoformat(opt["start"])
            end = date.fromisoformat(opt["end"])
        except Exception as e:
            raise CommandError(f"Fechas inválidas: {e}")

        if end < start:
            raise CommandError("--end debe ser >= --start")

        # Construcción del payload base (selección de universo)
        payload: dict[str, t.Any] = {}

        if opt["symbols_file"]:
            payload["symbols"] = _read_symbols_file(opt["symbols_file"])
        elif opt["symbols"]:
            syms = [s.strip().upper() for s in opt["symbols"].split(",") if s.strip()]
            # dedupe
            seen, dedup = set(), []
            for s in syms:
                if s not in seen:
                    seen.add(s)
                    dedup.append(s)
            payload["symbols"] = dedup
        elif opt["all_active"]:
            payload["all_active"] = True
        else:
            mode = (opt["universe_mode"] or "custom").strip().lower()
            payload["universe_mode"] = mode

        url = f"{base.rstrip('/')}/internal/market/backfill-range"
        headers = {
            "Content-Type": "application/json",
            "X-Internal-Token": token,
        }

        # Hacemos llamado por rango completo, con reintentos por fecha si partial
        # (el endpoint del server ya puede reintentar internamente; esto es una red extra de seguridad)
        partial_retries: int = int(opt["retries"])
        sleep_secs: float = float(opt["sleep"])
        max_time: int = int(opt["max_time"])

        # Enviamos un request por cada fecha para poder reintentar por fecha de forma simple:
        total_ok = 0
        total_partial = 0
        total_errors = 0
        results: list[dict] = []

        for d in _daterange(start, end):
            local_payload = dict(payload)
            local_payload["start"] = d.isoformat()
            local_payload["end"] = d.isoformat()

            attempt = 1
            last_resp = None
            while True:
                try:
                    resp = requests.post(url, headers=headers, data=json.dumps(local_payload), timeout=max_time)
                    last_resp = resp
                    data = resp.json()
                except Exception as e:
                    self.stderr.write(self.style.ERROR(f"[{d}] Error request: {e}"))
                    total_errors += 1
                    break

                if resp.status_code not in (200, 206):  # 206 Partial Content también es válido
                    self.stderr.write(self.style.ERROR(f"[{d}] HTTP {resp.status_code}: {data}"))
                    total_errors += 1
                    break

                # detectar si alguna entrada del 'result' vino con partial=true
                is_partial = False
                try:
                    result = data.get("result", [])
                    for entry in result:
                        stats = entry.get("stats", {}) or entry.get("retry1", {}) or {}
                        if (entry.get("stats", {}) or {}).get("partial") is True:
                            is_partial = True
                            break
                        if (entry.get("retry1", {}) or {}).get("partial") is True:
                            is_partial = True
                            break
                        if (entry.get("retry2", {}) or {}).get("partial") is True:
                            is_partial = True
                            break
                except Exception:
                    pass

                if is_partial and attempt < partial_retries:
                    self.stdout.write(f"[{d}] Parcial (intento {attempt}/{partial_retries-1}) → reintento en {sleep_secs}s...")
                    time.sleep(sleep_secs)
                    attempt += 1
                    continue

                # resumimos estado
                if is_partial:
                    total_partial += 1
                    self.stdout.write(self.style.WARNING(f"[{d}] OK pero PARCIAL"))
                else:
                    total_ok += 1
                    self.stdout.write(self.style.SUCCESS(f"[{d}] OK"))

                results.append({"date": d.isoformat(), "response": data})
                break

        # Resumen final
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Fechas OK: {total_ok}"))
        if total_partial:
            self.stdout.write(self.style.WARNING(f"Fechas parciales: {total_partial}"))
        if total_errors:
            self.stdout.write(self.style.ERROR(f"Fechas con error: {total_errors}"))

        # Exit code no-cero si hubo errores
        if total_errors > 0:
            raise CommandError("Backfill range finalizado con errores.")



# EJEMPLO DE USO
#python manage.py call_backfill_range \
#  --base "https://TUAPP.onrender.com" \
#  --token "$INTERNAL_API_TOKEN" \
#  --start 2025-09-22 --end 2025-09-23 \
#  --universe-mode sp100