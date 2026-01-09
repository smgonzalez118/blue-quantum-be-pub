
def get_fundamentals(ticker: str):
    import statistics
    import finnhub
    from decouple import config
    
    FINNHUB_APIKEY = config('FINNHUB_APIKEY')
    client = finnhub.Client(api_key=FINNHUB_APIKEY)
    
    METRICAS_QUARTER = [
        "eps", "fcfMargin", "grossMargin", "totalDebtToTotalAsset",
        "netMargin", "operatingMargin", "peTTM", "roaTTM", "roeTTM", "roicTTM",
    ]
    MAX_Q = 12  # últimos 12 trimestres

    def avg_last_n(serie12, n):
        """serie12: lista de dicts [{period, v}] ordenada asc.
        Devuelve 0 si no hay suficientes puntos.
        """
        if len(serie12) < n:
            return 0
        vals = [x["v"] for x in serie12[-n:] if isinstance(x, dict) and "v" in x]
        return round(sum(vals) / len(vals), 4) if len(vals) == n else 0

    data = client.company_basic_financials(ticker, "all")

    if not data or "series" not in data or "quarterly" not in data["series"]:
        return {"snapshot": {}, "history": {}}

    quarterly = data["series"]["quarterly"]

    snapshot = {}
    history = {}

    for m in METRICAS_QUARTER:
        values = quarterly.get(m, []) or []

        # normalizo y ordeno cronológicamente (más viejo -> más nuevo)
        values = [v for v in values if isinstance(v, dict) and "v" in v and "period" in v]
        values.sort(key=lambda x: x["period"])

        # me quedo con los últimos 12
        serie12 = values[-MAX_Q:]

        # history SOLO 12 para el sparkline
        history[m] = [{"period": q["period"], "value": q["v"]} for q in serie12]

        if serie12:
            snapshot[m] = {
                "ultimo":  round(serie12[-1]["v"], 4),
                "prom_4":  avg_last_n(serie12, 4),
                "prom_8":  avg_last_n(serie12, 8),
                "prom_12": avg_last_n(serie12, 12),
            }
        else:
            snapshot[m] = {"ultimo": 0, "prom_4": 0, "prom_8": 0, "prom_12": 0}

    return {"snapshot": snapshot, "history": history}