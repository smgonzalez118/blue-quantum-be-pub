# market/management/commands/update_last_candle.py
from django.core.management.base import BaseCommand
from market.services.simple_jobs import fetch_eod_all_active

class Command(BaseCommand):
    help = "Trae la última vela EOD para el universo (ENV/arg) o todos los activos."

    def add_arguments(self, parser):
        parser.add_argument("--universe", type=str, default=None,
                            help="sp100 | adrs | etfs | commodities | ... (opcional)")

    def handle(self, *args, **opts):
        universe = opts.get("universe")
        res = fetch_eod_all_active(universe)
        ok = res.get("ok", False)
        msg = f"EOD stats: {res}"
        if ok:
            self.stdout.write(self.style.SUCCESS(msg))
        else:
            self.stderr.write(self.style.ERROR(msg))

