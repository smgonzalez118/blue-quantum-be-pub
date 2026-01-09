import datetime as dt

def last_us_trading_day(ref: dt.date | None = None) -> dt.date:
    """Devuelve el último día hábil simple (L-V). No contempla feriados."""
    d = ref or dt.date.today()
    # Si es sábado/domingo, retrocede
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d = d - dt.timedelta(days=1)
    # Si es lunes y querías 'hoy' un domingo, esto ya te va a dar el viernes anterior
    return d


# Más adelante, si querés exactitud con feriados US, metemos pandas_market_calendars.