from django.db import models
from django.conf import settings
from market.models import Ticker


class Timeframe(models.TextChoices):
    D = "D", "Daily"
    W = "W", "Weekly"

class ReporteTecnico(models.Model):
    ticker = models.CharField(max_length=20)
    activo = models.CharField(max_length=150, null=True, blank=True)
    timeframe = models.CharField(max_length=1, choices=Timeframe.choices, default=Timeframe.D, db_index=True)
    precio = models.FloatField(null = True, blank = True)
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


    def __str__(self):
        return f"{self.activo} - {self.timeframe}"
    
    class Meta:
        verbose_name = "Reporte tecnico"
        verbose_name_plural = "Reportes tecnicos"


class CompanyProfile(models.Model):
    """
    Perfil estático/semiestático por ticker: lo usamos para info de empresa
    y para sacar comparables por sector/industria.
    """
    ticker = models.OneToOneField(Ticker, on_delete=models.CASCADE, related_name="profile")
    # Identidad
    name = models.CharField(max_length=128, blank=True, default="")
    country = models.CharField(max_length=64, blank=True, default="")
    exchange = models.CharField(max_length=64, blank=True, default="")
    currency = models.CharField(max_length=16, blank=True, default="")

    logo = models.URLField(blank=True, default="")      # <---
    weburl = models.URLField(blank=True, default="")    # <---

    # Clasificación
    sector = models.CharField(max_length=64, blank=True, default="")
    industry = models.CharField(max_length=128, blank=True, default="")

    # Métricas clave (snapshot; opcional guardar aquí para lecturas rápidas)
    market_cap = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    shares_outstanding = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)

    # Auditoría
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.ticker.symbol} - {self.name or self.ticker.symbol}"

    class Meta:
        indexes = [
            models.Index(fields=["sector"]),
            models.Index(fields=["industry"]),
        ]


class FundamentalMetric(models.Model):
    """
    Historial de fundamentales normalizado: un registro por (ticker, métrica, período).
    Período suele ser trimestre (YYYY-Q#) o fecha de fin de trimestre (DateField).
    """
    METRIC_CHOICES = [
        ("eps", "EPS"),
        ("fcfMargin", "FCF Margin"),
        ("grossMargin", "Gross Margin"),
        ("totalDebtToTotalAsset", "Debt/Assets"),
        ("netMargin", "Net Margin"),
        ("operatingMargin", "Operating Margin"),
        ("peTTM", "P/E TTM"),
        ("roaTTM", "ROA TTM"),
        ("roeTTM", "ROE TTM"),
        ("roicTTM", "ROIC TTM"),
        # podés agregar más libremente
    ]

    ticker = models.ForeignKey(Ticker, on_delete=models.CASCADE, related_name="fundamentals")
    metric = models.CharField(max_length=64, choices=METRIC_CHOICES, db_index=True)
    period_end = models.DateField(db_index=True)  # fin de trimestre (ej: 2024-12-31)
    value = models.FloatField(null=True, blank=True)

    # Auditoría
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("ticker", "metric", "period_end"),)
        indexes = [
            models.Index(fields=["ticker", "metric"]),
            models.Index(fields=["metric", "-period_end"]),
        ]

    def __str__(self):
        return f"{self.ticker.symbol} {self.metric} @ {self.period_end}: {self.value}"
    

class ComparablePeer(models.Model):
    """
    Relación de comparables curada. Útil para “whitelist” de pares por ticker.
    """
    base = models.ForeignKey(Ticker, on_delete=models.CASCADE, related_name="peers_base")
    peer = models.ForeignKey(Ticker, on_delete=models.CASCADE, related_name="peers_of")
    rank = models.PositiveIntegerField(default=0)  # para ordenar (opcional)

    class Meta:
        unique_together = (("base", "peer"),)
        indexes = [models.Index(fields=["base", "rank"])]

    def __str__(self):
        return f"{self.base.symbol} ~ {self.peer.symbol}"