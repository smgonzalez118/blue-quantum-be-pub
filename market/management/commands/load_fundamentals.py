# activo/management/commands/load_fundamentals.py
from django.core.management.base import BaseCommand
from datetime import datetime
from market.models import Ticker
from activo.models import FundamentalMetric
import pandas as pd
import json

class Command(BaseCommand):
    help = "Carga fundamentales normalizados desde JSON o CSV."

    def add_arguments(self, parser):
        parser.add_argument("--symbol", required=True)
        parser.add_argument("--file", required=True, help="Archivo CSV o JSON con columnas/keys: metric,period_end,value")

    def handle(self, *args, **opts):
        sym = opts["symbol"].upper()
        t = Ticker.objects.get(symbol=sym)

        fp = opts["file"]
        if fp.lower().endswith(".json"):
            data = json.loads(Path(fp).read_text())
            rows = data if isinstance(data, list) else data["rows"]
            df = pd.DataFrame(rows)
        else:
            df = pd.read_csv(fp)

        df["period_end"] = pd.to_datetime(df["period_end"]).dt.date
        upserts = 0
        for r in df.to_dict(orient="records"):
            FundamentalMetric.objects.update_or_create(
                ticker=t,
                metric=r["metric"],
                period_end=r["period_end"],
                defaults={"value": r.get("value")},
            )
            upserts += 1
        self.stdout.write(self.style.SUCCESS(f"Fundamentals upsert={upserts} para {sym}"))
