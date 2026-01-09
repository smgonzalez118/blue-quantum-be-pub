# forecasting/management/commands/train_forecasting.py
from django.core.management.base import BaseCommand, CommandError
from forecasting.pipeline import train_all
from forecasting.config import CFG
import pathlib, glob

class Command(BaseCommand):
    help = "Entrena modelos con tickers dados o leyendo desde una carpeta/archivo"

    def add_arguments(self, parser):
        parser.add_argument(
            "--tickers",
            type=str,
            help="Tickers separados por coma (ej: AAPL,MSFT,NVDA)"
        )
        parser.add_argument(
            "--from-folder",
            dest="from_folder",
            metavar="DIR_OR_FILE",
            type=str,
            help="Carpeta con CSVs (1 por ticker) o archivo .txt con tickers (uno por línea)"
        )

    def handle(self, *args, **options):
        tickers_arg = options.get("tickers")
        from_folder = options.get("from_folder")

        tickers = []

        if tickers_arg:
            tickers = [t.strip().upper() for t in tickers_arg.split(",") if t.strip()]

        elif from_folder:
            p = pathlib.Path(from_folder)
            if not p.exists():
                raise CommandError(f"La ruta {from_folder} no existe")

            # Si es .txt -> leo tickers (no cambio DAILY_DIR)
            if p.is_file() and p.suffix.lower() == ".txt":
                tickers = [ln.strip().upper() for ln in p.read_text().splitlines() if ln.strip()]
                if not tickers:
                    raise CommandError(f"No se encontraron tickers en el archivo: {from_folder}")
                self.stdout.write(self.style.NOTICE(f"Tickers cargados desde {from_folder} ({len(tickers)})"))

            # Si es carpeta -> seteo DAILY_DIR a esa carpeta y detecto tickers por *.csv
            elif p.is_dir():
                csvs = sorted(glob.glob(str(p / "*.csv")))
                if not csvs:
                    raise CommandError(f"No hay CSVs en la carpeta: {from_folder}")
                tickers = [pathlib.Path(f).stem.upper() for f in csvs]

                # 🔧 Redirijo el loader de datos a esta carpeta
                CFG.DAILY_DIR = p
                self.stdout.write(self.style.NOTICE(f"Usando carpeta de datos: {p}"))
                self.stdout.write(self.style.NOTICE(f"Detectados {len(tickers)} tickers desde CSVs"))
            else:
                raise CommandError(f"{from_folder} no es ni un .txt válido ni una carpeta con CSVs")

        if not tickers:
            raise CommandError("No se proporcionaron tickers. Usa --tickers o --from-folder DIR/FILE")

        self.stdout.write(self.style.NOTICE(f"Entrenando {len(tickers)} tickers..."))
        metrics = train_all(tickers)
        self.stdout.write(self.style.SUCCESS("Entrenamiento completado"))
        self.stdout.write(str(metrics))
