import datetime as dt
from django.db import transaction
from ..models import Ticker, PriceDaily
from .polygon_client import PolygonClient
from .trading_days import last_us_trading_day

def upsert_price_daily(ticker: Ticker, date: dt.date, row: dict) -> int:
    """
    Inserta o actualiza la vela (ticker, date). Retorna 1 si insertó/actualizó, 0 si no había datos.
    """
    if not row:
        return 0

    # Opción A (Django >=4.1): upsert masivo de 1 elemento
    obj = PriceDaily(
        ticker=ticker,
        date=date,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        adj_close=row["adj_close"],
        volume=row["volume"],
    )
    with transaction.atomic():
        PriceDaily.objects.bulk_create(
            [obj],
            update_conflicts=True,
            update_fields=["open","high","low","close","adj_close","volume"],
            unique_fields=["ticker","date"],
        )
    return 1

    # Opción B (si usás Django <4.1): descomentá esto y borra lo de arriba
    # PriceDaily.objects.update_or_create(
    #     ticker=ticker, date=date,
    #     defaults=row
    # )
    # return 1


def fetch_and_store_eod(symbol: str, date: dt.date | None) -> int:
    target_date = date or last_us_trading_day()
    t = Ticker.objects.get(symbol=symbol)
    cli = PolygonClient()
    row = cli.eod_bar(symbol, target_date)
    if not row:
        return 0
    obj = PriceDaily(
        ticker=t,
        date=target_date,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        adj_close=row["adj_close"],
        volume=row["volume"],
    )
    with transaction.atomic():
        PriceDaily.objects.bulk_create(
            [obj],
            update_conflicts=True,
            update_fields=["open","high","low","close","adj_close","volume"],
            unique_fields=["ticker","date"],
        )
    return 1


def upsert_prices_bulk(ticker: Ticker, rows: list[dict]) -> int:
    """
    Inserta/actualiza muchas velas de una (requiere Django >= 4.1 para update_conflicts).
    rows: [{"date", "open","high","low","close","adj_close","volume"}, ...]
    """
    if not rows:
        return 0
    objs = [
        PriceDaily(
            ticker=ticker,
            date=r["date"],
            open=r["open"], high=r["high"], low=r["low"],
            close=r["close"], adj_close=r["adj_close"], volume=r["volume"],
        ) for r in rows
    ]
    with transaction.atomic():
        PriceDaily.objects.bulk_create(
            objs, batch_size=1000,
            update_conflicts=True,
            update_fields=["open","high","low","close","adj_close","volume"],
            unique_fields=["ticker","date"],
        )
    return len(objs)

def upsert_grouped_day(rows: list[dict], universe: set[str]) -> int:
    # Filtrá sólo los símbolos que te interesan
    by_sym: dict[str, list[dict]] = {}
    for r in rows:
        s = r["symbol"].upper()
        if s in universe:
            by_sym.setdefault(s, []).append({
                "date": r["date"], "open": r["open"], "high": r["high"],
                "low": r["low"], "close": r["close"], "adj_close": r["adj_close"],
                "volume": r["volume"],
            })
    inserted = 0
    for sym, lst in by_sym.items():
        t, _ = Ticker.objects.get_or_create(symbol=sym)
        inserted += upsert_prices_bulk(t, lst)
    return inserted