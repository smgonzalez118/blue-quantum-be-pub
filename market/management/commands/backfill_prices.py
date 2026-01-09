import time
import datetime as dt
from django.core.management.base import BaseCommand, CommandError
from market.models import Ticker
from market.services.polygon_client import PolygonClient
from market.services.etl import upsert_prices_bulk
from market.services.trading_days import last_us_trading_day

RPM = 5     # requests por minuto (fallback día a día)
SLEEP = 60 / RPM  # ~12s entre requests

class Command(BaseCommand):
    help = ("Backfill diario para un símbolo entre --from y --to (YYYY-MM-DD). "
            "Intenta rango con aggs; si el plan no lo permite, cae a día a día "
            "respetando ~5 rpm y salteando fines de semana.")

    def add_arguments(self, parser):
        parser.add_argument("--symbol", required=True)
        parser.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
        parser.add_argument("--to", dest="date_to", required=False, help="YYYY-MM-DD (default: último día hábil US)")

    def handle(self, *args, **opts):
        symbol = opts["symbol"].upper()
        d_from = dt.date.fromisoformat(opts["date_from"])
        d_to = dt.date.fromisoformat(opts["date_to"]) if opts.get("date_to") else last_us_trading_day()

        if d_from > d_to:
            raise CommandError("--from debe ser <= --to")

        t, _ = Ticker.objects.get_or_create(symbol=symbol)
        cli = PolygonClient()

        self.stdout.write(f"[{symbol}] Backfill {d_from} → {d_to} (intentando aggs)…")

        rows = None
        try:
            rows = cli.range_aggs(symbol, d_from, d_to)
        except PermissionError:
            rows = None

        if rows:
            inserted = upsert_prices_bulk(t, rows)
            self.stdout.write(self.style.SUCCESS(f"[{symbol}] Aggs OK: {inserted} velas"))
            return

        # Fallback: día a día (más lento; respeta 5 rpm)
        self.stdout.write(f"[{symbol}] Aggs no disponible → fallback día a día (~5 rpm)")
        total = 0
        cur = d_from
        while cur <= d_to:
            if cur.weekday() < 5:
                try:
                    one = cli.eod_bar(symbol, cur)
                    if one:
                        one["date"] = cur
                        total += upsert_prices_bulk(t, [one])
                except PermissionError as e:
                    raise CommandError(
                        f"403 Forbidden también en daily para {symbol}. Verificá plan/dataset."
                    ) from e
                time.sleep(SLEEP)
            cur += dt.timedelta(days=1)

        self.stdout.write(self.style.SUCCESS(f"[{symbol}] Fallback OK: {total} velas"))
