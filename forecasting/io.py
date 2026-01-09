# forecasting/io.py
from __future__ import annotations
from pathlib import Path
import json
from functools import lru_cache
from typing import Dict
from .config import CFG
from .models import TrainedModel


def artifact_path(key: str) -> Path:
    return CFG.ARTIFACTS_DIR / f"model_{key}.pkl"


def model_exists(key: str) -> bool:
    """Devuelve True si existe el .pkl del modelo para esa key (hNNN)."""
    return artifact_path(key).exists()


def save_model(key: str, tm: TrainedModel):
    CFG.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    tm.save(str(artifact_path(key)))


def load_model(key: str) -> TrainedModel:
    """Carga directa (sin cache). Útil para scripts de entrenamiento."""
    p = artifact_path(key)
    if not p.exists():
        raise FileNotFoundError(f"Model artifact not found for key={key}: {p}")
    return TrainedModel.load(str(p))


@lru_cache(maxsize=1)  # mantener SOLO 1 modelo cacheado para reducir RAM
def get_model(key: str) -> TrainedModel:
    """
    Carga con cache en memoria (para predicción en el server).
    Con maxsize=1 evitás acumular modelos grandes en memoria.
    """
    p = artifact_path(key)
    if not p.exists():
        raise FileNotFoundError(f"Model artifact not found for key={key}: {p}")
    return TrainedModel.load(str(p))


def invalidate_model_cache():
    """Limpia el cache de modelos (útil luego de re-entrenar)."""
    try:
        get_model.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass


# alias más expresivo para usar después de cada símbolo en el endpoint time-boxed
def drop_model_cache():
    invalidate_model_cache()


def save_meta(meta: Dict):
    CFG.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    (CFG.ARTIFACTS_DIR / "meta.json").write_text(json.dumps(meta, indent=2))


def load_meta() -> Dict:
    p = CFG.ARTIFACTS_DIR / "meta.json"
    return json.loads(p.read_text()) if p.exists() else {}

