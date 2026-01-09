import sys
from django.core.management.base import BaseCommand, CommandError
from market.models import Ticker

class Command(BaseCommand):
    help = (
        "Crea/actualiza tickers desde un archivo .txt (uno por línea). "
        "Formato admitido: 'AAPL' o 'AAPL,Apple Inc.'. "
        "Líneas vacías o que empiezan con # se ignoran."
    )

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Ruta del archivo .txt")

        parser.add_argument(
            "--deactivate-missing",
            action="store_true",
            help="Marca is_active=False para tickers que no aparezcan en el archivo.",
        )

    def handle(self, *args, **opts):
        path = opts["file"]
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read().splitlines()
        except FileNotFoundError:
            raise CommandError(f"No se encontró el archivo: {path}")

        symbols_in_file = set()
        created, updated = 0, 0

        for line in raw:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Soporta "AAPL" o "AAPL,Apple Inc."
            parts = [p.strip() for p in line.split(",", 1)]
            symbol = parts[0].upper()
            name = parts[1] if len(parts) > 1 else ""

            symbols_in_file.add(symbol)

            obj, was_created = Ticker.objects.get_or_create(symbol=symbol)
            changed = False

            if name and obj.name != name:
                obj.name = name
                changed = True
            if obj.is_active is False:
                obj.is_active = True
                changed = True

            if was_created:
                created += 1
                obj.name = name
                obj.is_active = True
                obj.save()
            elif changed:
                updated += 1
                obj.save()

        deactivated = 0
        if opts["deactivate-missing"]:
            qs = Ticker.objects.filter(is_active=True).exclude(symbol__in=symbols_in_file)
            deactivated = qs.update(is_active=False)

        self.stdout.write(self.style.SUCCESS(
            f"Seed OK — creados: {created}, actualizados: {updated}, desactivados: {deactivated}"
        ))
