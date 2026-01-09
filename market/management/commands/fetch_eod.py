import datetime as dt
from django.core.management.base import BaseCommand
from market.models import Ticker
from market.services.etl import fetch_and_store_eod
from market.services.trading_days import last_us_trading_day

class Command(BaseCommand):
    help = "Trae EOD para un símbolo y fecha (YYYY-MM-DD) y lo guarda en la BD."

    def add_arguments(self, parser):
        parser.add_argument("--symbol", required=True)
        parser.add_argument("--date", required=False, help="YYYY-MM-DD (default: último día hábil US)")

    def handle(self, *args, **opts):
        symbol = opts["symbol"].upper()
        date_str = opts.get("date")
        date = dt.date.fromisoformat(date_str) if date_str else last_us_trading_day()

        Ticker.objects.get_or_create(symbol=symbol)
        inserted = fetch_and_store_eod(symbol, date)
        self.stdout.write(self.style.SUCCESS(f"{symbol} {date}: {'OK' if inserted else 'SIN DATOS'}"))
