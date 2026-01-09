# activo/management/commands/load_company_profiles.py
from django.core.management.base import BaseCommand
from pathlib import Path
import pandas as pd
from market.models import Ticker
from activo.models import CompanyProfile

class Command(BaseCommand):
    help = "Carga/actualiza CompanyProfile desde CSV."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True)

    def handle(self, *args, **opts):
        df = pd.read_csv(opts["file"])
        df.columns = [c.strip() for c in df.columns]
        created, updated = 0, 0
        for row in df.to_dict(orient="records"):
            sym = str(row["symbol"]).upper().strip()
            t = Ticker.objects.get_or_create(symbol=sym)[0]
            defaults = {k: row.get(k) for k in [
                "name","country","exchange","currency","sector","industry","market_cap","shares_outstanding"
            ]}
            obj, was_created = CompanyProfile.objects.update_or_create(ticker=t, defaults=defaults)
            created += 1 if was_created else 0
            updated += 0 if was_created else 1
        self.stdout.write(self.style.SUCCESS(f"CompanyProfile ok. created={created} updated={updated}"))
