from django.db import models


class SenalTecnica(models.Model):
    """
    Señal técnica EOD/Semanal.
    Nota: guardar en `activo` SOLO el ticker (ej: "AAPL"), no "AAPL (Apple Inc.)".
    """
    TIPO_SENAL = [('BUY', 'Buy'), ('SELL', 'Sell')]
    TF_CHOICES = (("D", "daily"), ("W", "weekly"))

    # Claves
    activo = models.CharField(max_length=64, db_index=True)  # ticker en MAYÚSCULAS
    timeframe = models.CharField(max_length=10, choices=TF_CHOICES, default="D", db_index=True)

    # Atributos de la señal
    tipo = models.CharField(max_length=10, choices=TIPO_SENAL)            # BUY/SELL
    fecha = models.DateTimeField(db_index=True)                           # EOD (D) o cierre semana (W)
    indicador = models.CharField(max_length=50, db_index=True)            # ej: EMA10/EMA20, PRICE/EMA5

    # Datos complementarios
    precio_senal = models.FloatField(null=True, blank=True)
    precio_actual = models.FloatField(null=True, blank=True)
    detalle = models.TextField(blank=True, null=True)
    rendimiento = models.FloatField(null=True, blank=True)

    class Meta:
        ordering = ['-fecha']
        constraints = [
            # Unicidad dura: evita duplicados “visualmente iguales”
            models.UniqueConstraint(
                fields=['activo', 'timeframe', 'indicador', 'tipo', 'fecha'],
                name='uniq_signal_activo_tf_indicador_tipo_fecha',
            ),
        ]
        # Índice compuesto útil para listados/paginación y filtros por tf/fecha
        indexes = [
            models.Index(fields=['activo', 'timeframe', 'fecha'], name='signals_idx'),
        ]
        verbose_name = "Señal técnica"
        verbose_name_plural = "Señales técnicas"

    def __str__(self) -> str:
        return f"{self.activo} {self.timeframe} {self.tipo} {self.indicador} @ {self.fecha:%Y-%m-%d}"



