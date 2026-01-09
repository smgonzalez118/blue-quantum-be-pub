# forecasting/config.py
import os
from pathlib import Path
from dataclasses import dataclass

_ALLOWED_H = (21, 63)

def _parse_horizons_env(val: str) -> list[int]:
    if not val:
        return list(_ALLOWED_H)
    raw = [x.strip().lower() for x in val.split(",") if x.strip()]
    out: list[int] = []
    for x in raw:
        if x.startswith("h"):
            x = x[1:]
        if x.isdigit():
            n = int(x)
            if n in _ALLOWED_H:
                out.append(n)
    return out or list(_ALLOWED_H)

@dataclass
class Config:
    # Paths de datos / artefactos
    DATA_ROOT: Path = Path("datasets")
    DAILY_DIR: Path = DATA_ROOT / "daily"
    BENCH_DIR: Path = DATA_ROOT / "benchmark"
    BENCH_FILE: str = "SPY.csv"
    ARTIFACTS_DIR: Path = Path(__file__).resolve().parent / "artifacts"

    # -------- API nueva --------
    def horizons_list(self) -> list[int]:
        env_val = os.getenv("FORECAST_HORIZONS", "").strip()
        return _parse_horizons_env(env_val)

    def horizons_dict(self) -> dict[str, int]:
        return {f"h{h:03d}": h for h in self.horizons_list()}

    # -------- Accesor híbrido (dict + callable) --------
    class _HorizonsAccessor(dict):
        def __init__(self, mapping: dict[str, int]):
            super().__init__(mapping)
        def __call__(self, *args, **kwargs):
            """
            Sin argumentos -> devuelve el dict completo.
            Con 1 argumento -> devuelve mapping.get(key, default) si lo pasan.
            """
            if not args:
                return dict(self)
            key = args[0]
            default = kwargs.get("default")
            return self.get(key, default)

    @property
    def horizons(self):
        """
        Compatibilidad doble:
        - Como atributo dict: CFG.horizons['h021'] -> 21
        - Como función:       CFG.horizons()      -> {'h021': 21, 'h063': 63}
                              CFG.horizons('h021') -> 21
        """
        return self._HorizonsAccessor(self.horizons_dict())

    # Alias adicional por si algún módulo usa otro nombre
    @property
    def horizons_map(self):
        return self.horizons  # mismo accesor

CFG = Config()

