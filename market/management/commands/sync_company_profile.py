# market/management/commands/sync_company_profile.py
from __future__ import annotations
import time, json
from typing import Iterable, List, Dict, Any, Set
from pathlib import Path

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction

import finnhub
from market.models import Ticker
from activo.models import CompanyProfile  # tu modelo actual

DEFAULT_RPM = 100  # ajustá a tu plan (100 rpm ~ sleep 0.6)
DEFAULT_SLEEP = max(0.0, 60.0 / DEFAULT_RPM)
DEFAULT_RESUME = ".cache/sync_company_profile_progress.json"

def _mk_client() -> finnhub.Client:
    api_key = getattr(settings, "FINNHUB_APIKEY", None)
    if not api_key:
        raise RuntimeError("FINNHUB_APIKEY no configurada en settings/env.")
    return finnhub.Client(api_key=api_key)

def _normalize_profile(raw: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Devuelve dict con los campos que EXISTEN en tu CompanyProfile.
    Si Finnhub no tiene perfil (ETF/commodity), devuelve None.
    """
    if not raw or not isinstance(raw, dict):
        return None
    if not raw.get("ticker"):
        return None

    # Campos disponibles en tu modelo:
    # ['id','ticker','name','country','exchange','currency','sector','industry',
    #  'market_cap','shares_outstanding','updated_at']
    out: Dict[str, Any] = {
        "name": (raw.get("name") or "")[:128],
        "country": (raw.get("country") or "")[:64],
        "exchange": (raw.get("exchange") or "")[:64],
        "currency": (raw.get("currency") or "")[:16],
        "sector": (raw.get("finnhubIndustry") or "")[:64],
        "industry": (raw.get("industry") or "")[:128],
        "logo": raw.get("logo") or "",
        "weburl": raw.get("weburl") or ""

    }

    # Algunas fuentes devuelven marketCap/sharesOutstanding; si no están, dejalos en None.
    mc = raw.get("marketCapitalization")
    so = raw.get("shareOutstanding")
    try:
        out["market_cap"] = float(mc) if mc is not None else None
    except Exception:
        out["market_cap"] = None
    try:
        out["shares_outstanding"] = float(so) if so is not None else None
    except Exception:
        out["shares_outstanding"] = None

    return out

def _symbols_base(symbols: Iterable[str] | None) -> List[str]:
    if symbols:
        return sorted({s.strip().upper() for s in symbols if s and s.strip()})
    # solo activos marcados como is_active=True
    return list(Ticker.objects.filter(is_active=True).values_list("symbol", flat=True))

def _load_done_set(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data.get("done", []))
    except Exception:
        return set()

def _save_done_set(path: Path, done: Set[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"done": sorted(list(done))}, indent=2), encoding="utf-8")

class Command(BaseCommand):
    help = "Sincroniza Company Profile desde Finnhub. Reintenta con backoff y permite reanudar."

    def add_arguments(self, parser):
        parser.add_argument("--symbols", nargs="*", help="Símbolos explícitos (AAPL MSFT ...). Si se omite → Ticker.is_active.")
        parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Sleep base entre requests (segundos).")
        parser.add_argument("--max", type=int, default=0, help="Máximo de símbolos a procesar (0 = todos).")
        parser.add_argument("--resume-file", type=str, default=DEFAULT_RESUME, help="Archivo JSON con progreso para reanudar.")
        parser.add_argument("--reset", action="store_true", help="Ignora progreso previo (no reanuda).")
        parser.add_argument("--retries", type=int, default=5, help="Reintentos por símbolo ante 429 u otros errores transitorios.")

    def handle(self, *args, **opts):
        symbols = _symbols_base(opts.get("symbols"))
        max_n = int(opts.get("max") or 0)
        sleep_s = float(opts.get("sleep") or DEFAULT_SLEEP)
        resume_file = Path(str(opts.get("resume_file") or DEFAULT_RESUME))
        retries = int(opts.get("retries") or 5)
        reset = bool(opts.get("reset"))

        cli = _mk_client()

        done: Set[str] = set() if reset else _load_done_set(resume_file)
        pending = [s for s in symbols if s not in done]
        if max_n > 0:
            pending = pending[:max_n]

        self.stdout.write(self.style.NOTICE(f"Profiles: procesando {len(pending)} símbolos (total activos={len(symbols)}; ya hechos={len(done)})..."))

        ok = 0; skipped = 0; err = 0

        for sym in pending:
            # obtener FK al Ticker (para OneToOne)
            try:
                tk = Ticker.objects.get(symbol=sym)
            except Ticker.DoesNotExist:
                self.stdout.write(self.style.WARNING(f"[{sym}] Ticker inexistente en BD — skip"))
                skipped += 1
                done.add(sym); _save_done_set(resume_file, done)
                continue

            # reintentos con backoff
            attempt = 0
            backoff = sleep_s
            while True:
                try:
                    raw = cli.company_profile2(symbol=sym)
                    norm = _normalize_profile(raw)
                    if not norm:
                        # ETF/commodity/sin data — no error, lo registramos vacío si querés,
                        # o simplemente lo consideramos skipped
                        skipped += 1
                    else:
                        with transaction.atomic():
                            # buscamos Ticker FK
                            t = Ticker.objects.get(symbol=sym)
                            CompanyProfile.objects.update_or_create(
                                ticker=t,                    # FK
                                defaults={k: v for k, v in norm.items() if k != "ticker"}
                            )
                        ok += 1

                    # éxito o skip → marcamos como hecho y salimos del loop de reintentos
                    done.add(sym)
                    _save_done_set(resume_file, done)
                    time.sleep(sleep_s)
                    break

                except finnhub.FinnhubAPIException as e:
                    msg = str(e) or ""
                    # 429 → rate limit: backoff exponencial
                    if "429" in msg or "limit" in msg.lower():
                        attempt += 1
                        if attempt > retries:
                            self.stdout.write(self.style.ERROR(f"[{sym}] 429 tras {retries} reintentos — pauso y continúo con el siguiente"))
                            err += 1
                            # no marcamos done para reintentar luego en una corrida futura
                            time.sleep(backoff)
                            break
                        self.stdout.write(self.style.WARNING(f"[{sym}] 429 rate limit — retry {attempt}/{retries} en {backoff:.1f}s"))
                        time.sleep(backoff)
                        backoff = min(backoff * 2.0, 60.0)  # cap de 60s
                        continue
                    else:
                        # otro error de API — contamos error y seguimos
                        self.stdout.write(self.style.ERROR(f"[{sym}] FinnhubAPIException: {e}"))
                        err += 1
                        # no marcamos done para reintentar en la próxima ejecución
                        time.sleep(sleep_s)
                        break

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"[{sym}] {type(e).__name__}: {e}"))
                    err += 1
                    # no marcamos done para reintentar en la próxima
                    time.sleep(sleep_s)
                    break

        self.stdout.write(self.style.SUCCESS(f"Profiles OK={ok} skipped={skipped} errors={err} — progreso guardado en {resume_file}"))



