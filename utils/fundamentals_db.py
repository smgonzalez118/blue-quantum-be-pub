# utils/fundamentals_db.py
from datetime import date
from collections import defaultdict
from activo.models import FundamentalMetric, CompanyProfile
from market.models import Ticker

def fundamentals_from_db(symbol: str):
    t = Ticker.objects.filter(symbol=symbol.upper()).first()
    if not t:
        return {"snapshot": {}, "history": {}}

    rows = FundamentalMetric.objects.filter(ticker=t).order_by("metric", "period_end")
    hist = defaultdict(list)
    for r in rows:
        hist[r.metric].append({"period": r.period_end.isoformat(), "value": r.value})

    # snapshot simple = último valor por métrica + promedios móviles
    snap = {}
    for m, series in hist.items():
        vals = [x["value"] for x in series if x["value"] is not None]
        if not vals:
            continue
        def avg_last(n): 
            s = vals[-n:] if len(vals) >= n else vals
            return sum(s)/len(s) if s else None
        snap[m] = {
            "ultimo": vals[-1],
            "prom_4": avg_last(4),
            "prom_8": avg_last(8),
            "prom_12": avg_last(12),
        }
    return {"snapshot": snap, "history": hist}
