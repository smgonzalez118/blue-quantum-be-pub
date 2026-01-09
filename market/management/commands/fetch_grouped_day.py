# market/management/commands/fetch_grouped_day.py
import datetime as dt
from django.core.management.base import BaseCommand
#from market.models import Ticker
from market.services.trading_days import last_us_trading_day
from utils.universe import get_dashboard_universe
from market.services.polygon_client import PolygonClient
from market.services.etl import upsert_grouped_day

class Command(BaseCommand):
    help = "Trae grouped daily para una fecha y hace upsert de tu universo."

    def add_arguments(self, p):
        p.add_argument("--date", help="YYYY-MM-DD (default: último día hábil US)")
        p.add_argument("--include-otc", action="store_true")

    def handle(self, *a, **o):
        d = dt.date.fromisoformat(o["date"]) if o.get("date") else last_us_trading_day()
        uni = set(get_dashboard_universe(mode="union"))  # o tu 'demo pro'
        cli = PolygonClient()
        rows = cli.grouped_daily(d, include_otc=o["include_otc"])
        n = upsert_grouped_day(rows, uni)
        self.stdout.write(self.style.SUCCESS(f"{d}: upsert grouped OK → {n} filas"))
