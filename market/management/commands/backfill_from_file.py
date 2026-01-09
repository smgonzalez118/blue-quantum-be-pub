import time
import datetime as dt
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = (
        "Ejecuta backfill_prices en SERIE para cada símbolo listado en un .txt "
        "(uno por línea; formato 'SYM' o 'SYM,Nombre'). "
        "Pasa --from y --to a cada corrida de backfill_prices."
    )

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Ruta del archivo .txt de tickers")
        parser.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
        parser.add_argument("--to", dest="date_to", required=False, help="YYYY-MM-DD")
        parser.add_argument("--sleep-between", type=float, default=1.0,
                            help="Segundos de espera entre símbolos (default 1.0)")

    def handle(self, *args, **opts):
        path = opts["file"]
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
        except FileNotFoundError:
            raise CommandError(f"No se encontró el archivo: {path}")

        date_from = opts["date_from"]
        date_to = opts.get("date_to")
        sleep_between = float(opts["sleep_between"])

        symbols = []
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):  # comentarios / vacías
                continue
            sym = line.split(",", 1)[0].strip().upper()
            if sym:
                symbols.append(sym)

        if not symbols:
            raise CommandError("No se encontraron símbolos en el archivo.")

        self.stdout.write(f"Backfill en serie de {len(symbols)} símbolos…")
        ok, failed = 0, 0

        for sym in symbols:
            try:
                self.stdout.write(self.style.NOTICE(f"[{sym}] backfill {date_from} → {date_to or '(últ. día hábil)'}"))
                if date_to:
                    call_command("backfill_prices", symbol=sym, **{"from": date_from, "to": date_to})
                else:
                    call_command("backfill_prices", symbol=sym, **{"from": date_from})
                ok += 1
            except Exception as e:
                failed += 1
                self.stderr.write(self.style.ERROR(f"[{sym}] ERROR: {e}"))
            time.sleep(sleep_between)

        self.stdout.write(self.style.SUCCESS(f"Terminado — OK: {ok}, ERROR: {failed}"))
