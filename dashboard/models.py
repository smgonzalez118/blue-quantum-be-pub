from django.db import models
from django.conf import settings

from django.db import models

class AtributoDashboard(models.Model):
    TF_CHOICES = (("D", "daily"), ("W", "weekly"))

    ticker = models.CharField(max_length=50, db_index=True)
    activo = models.CharField(max_length=150, null=True, blank=True)

    timeframe = models.CharField(max_length=1, choices=TF_CHOICES, default="D", db_index=True)

    precio = models.FloatField(null=True, blank=True)
    macd = models.CharField(max_length=10, null=True, blank=True)
    pmm5 = models.CharField(max_length=10, null=True, blank=True)
    pmm10 = models.CharField(max_length=10, null=True, blank=True)
    pmm20 = models.CharField(max_length=10, null=True, blank=True)
    pmm30 = models.CharField(max_length=10, null=True, blank=True)
    mm5_10 = models.CharField(max_length=10, null=True, blank=True)
    mm10_20 = models.CharField(max_length=10, null=True, blank=True)
    tripleCruce = models.CharField(max_length=10, null=True, blank=True)
    pmm100 = models.CharField(max_length=10, null=True, blank=True)
    rsi = models.CharField(max_length=10, null=True, blank=True)
    dmi = models.CharField(max_length=10, null=True, blank=True)
    adx = models.CharField(max_length=10, null=True, blank=True)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [models.Index(fields=["ticker", "timeframe"])]
        constraints = [
            models.UniqueConstraint(fields=["ticker", "timeframe"], name="uniq_dash_ticker_tf")
        ]
        verbose_name = "Atributos de activo"
        verbose_name_plural = "Conjunto de atributos de activo"



class Favorite(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="favorites")
    ticker = models.CharField(max_length=50)  # ej: "AAPL", "BTC-USD"
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("user", "ticker")
        indexes = [models.Index(fields=["user", "ticker"])]
        constraints = [
            models.UniqueConstraint(fields=["user", "ticker"], name="uniq_user_ticker")
        ]

    def save(self, *args, **kwargs):
        if self.ticker:
            self.ticker = self.ticker.upper().strip()
        super().save(*args, **kwargs)