from django.db import models

class Ticker(models.Model):
    symbol = models.CharField(max_length=16, unique=True, db_index=True)
    name = models.CharField(max_length=128, blank=True, default="")
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return self.symbol


class PriceDaily(models.Model):
    """
    Precios End-Of-Day (ajustados). Un registro por (ticker, date).
    """
    ticker = models.ForeignKey(Ticker, on_delete=models.CASCADE, related_name="prices_daily")
    date = models.DateField()

    open = models.DecimalField(max_digits=16, decimal_places=6)
    high = models.DecimalField(max_digits=16, decimal_places=6)
    low  = models.DecimalField(max_digits=16, decimal_places=6)
    close = models.DecimalField(max_digits=16, decimal_places=6)
    adj_close = models.DecimalField(max_digits=16, decimal_places=6)
    volume = models.BigIntegerField()

    class Meta:
        unique_together = (("ticker", "date"),)
        indexes = [
            models.Index(fields=["ticker", "-date"]),
            models.Index(fields=["-date"]),
        ]

    def __str__(self):
        return f"{self.ticker.symbol} {self.date}"
