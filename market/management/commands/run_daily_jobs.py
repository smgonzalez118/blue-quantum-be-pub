import datetime as dt
from django.core.management.base import BaseCommand
from market.services.simple_jobs import fetch_eod_all_active, compute_indicators_and_signals_all

class Command(BaseCommand):
    help = "Corre el pipeline diario: EOD (y si están disponibles, indicadores y señales)."

    def add_arguments(self, parser):
        parser.add_argument("--date", required=False, help="YYYY-MM-DD (default: último día hábil US)")

    def handle(self, *args, **opts):
        date_iso = opts.get("date")
        target_date = dt.date.fromisoformat(date_iso) if date_iso else None

        eod = fetch_eod_all_active(target_date)
        self.stdout.write(self.style.SUCCESS(f"EOD OK: {eod}"))

        # Si ya tenés ETAPA 2, esto los corre; si no, devuelve 0 sin fallar
        post = compute_indicators_and_signals_all()
        self.stdout.write(self.style.SUCCESS(f"POST OK: {post}"))
