# market/management/commands/precalc_indicators.py
from __future__ import annotations

from django.core.management.base import BaseCommand
from market.services.simple_jobs import compute_indicators_and_signals_all


class Command(BaseCommand):
    help = "Precalcula indicadores y señales para tickers activos (D y W). "\
           "Uso: --only AAPL,MSFT  --timeframes D W"

    def add_arguments(self, parser):
        parser.add_argument(
            "--only",
            type=str,
            help="Lista de tickers separada por coma para acotar el cálculo (ej: AAPL,MSFT).",
        )
        parser.add_argument(
            "--timeframes",
            nargs="+",
            choices=["D", "W", "d", "w"],
            default=["D", "W"],
            help="Timeframes a recalcular (por defecto D y W).",
        )

    def handle(self, *args, **opts):
        only_raw = (opts.get("only") or "").strip()
        only = [s.strip().upper() for s in only_raw.split(",") if s.strip()] if only_raw else None

        tf_in = opts.get("timeframes", ["D", "W"])
        timeframes = tuple(sorted({t.upper() for t in tf_in}))  # ("D",), ("W",) o ("D","W")

        self.stdout.write(self.style.NOTICE(
            f"Precalc → only={only if only else 'ALL ACTIVE'}, timeframes={','.join(timeframes)}"
        ))

        res = compute_indicators_and_signals_all(only=only, timeframes=timeframes)

        self.stdout.write(self.style.SUCCESS(f"PRECALC OK: {res}"))
