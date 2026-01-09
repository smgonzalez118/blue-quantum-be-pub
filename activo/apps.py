# activo/apps.py
from django.apps import AppConfig
from django.db.backends.signals import connection_created
from django.dispatch import receiver

class ActivoConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "activo"

    def ready(self):
        pass

@receiver(connection_created)
def _set_sqlite_pragmas(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    try:
        with connection.cursor() as c:
            # Mejora concurrencia R/W
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")

            c.execute("PRAGMA temp_store=MEMORY;")
            c.execute("PRAGMA cache_size=-10000;")  # ~10 MB de cache
    except Exception:
        # Nunca rompamos el arranque por un PRAGMA
        pass
