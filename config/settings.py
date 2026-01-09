"""
Django settings for config project.
"""

from pathlib import Path
from datetime import timedelta
from decouple import config, Csv
import dj_database_url

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent

# ──────────────────────────────────────────────────────────────────────────────
# Core / Security
# ──────────────────────────────────────────────────────────────────────────────
SECRET_KEY = config("SECRET_KEY")  # definir en ENV de Render
DEBUG = config("DEBUG", cast=bool, default=False)

# Hosts y CORS/CSRF confiables (separados por coma en ENV)
ALLOWED_HOSTS = config("ALLOWED_HOSTS", default="").split(",") if config("ALLOWED_HOSTS", default="") else ["*"]
CORS_ALLOWED_ORIGINS = [o for o in config("CORS_ALLOWED_ORIGINS", default="").split(",") if o]
CSRF_TRUSTED_ORIGINS = [o for o in config("CSRF_TRUSTED_ORIGINS", default="").split(",") if o]

# API Keys
POLYGON_API_KEY = config("POLYGON_API_KEY", default="")
FINNHUB_APIKEY = config("FINNHUB_APIKEY", default="")

# ──────────────────────────────────────────────────────────────────────────────
# Apps
# ──────────────────────────────────────────────────────────────────────────────
CORE = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
]

THIRD_PARTY = [
    "rest_framework",
    "rest_framework_simplejwt",
    "corsheaders",
]

APPS = [
    "usuarios",
    "senales",
    "dashboard",
    "activo",
    "portafolio",
    "forecasting",
    "market",
]

INSTALLED_APPS = CORE + THIRD_PARTY + APPS

# ──────────────────────────────────────────────────────────────────────────────
# Middleware (WhiteNoise + CORS)
# ──────────────────────────────────────────────────────────────────────────────
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # <— estáticos en Render
    "corsheaders.middleware.CorsMiddleware",       # <— CORS antes de Common
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

# ──────────────────────────────────────────────────────────────────────────────
# Database: Postgres en prod (DATABASE_URL) y SQLite local como fallback
# ──────────────────────────────────────────────────────────────────────────────
DATABASES = {
    "default": dj_database_url.parse(
        config("DATABASE_URL", default=f"sqlite:///{BASE_DIR/'db.sqlite3'}"),
        conn_max_age=600,
        ssl_require=False,
    )
}

# Habilitamos transacciones por request (útil tanto para SQLite como Postgres)
DATABASES["default"]["ATOMIC_REQUESTS"] = True

# Ajustes específicos si estamos usando SQLite
if DATABASES["default"]["ENGINE"].endswith("sqlite3"):
    # Aumenta el tiempo de espera de bloqueo (segundos) para reducir "database is locked"
    DATABASES["default"].setdefault("OPTIONS", {})
    DATABASES["default"]["OPTIONS"].setdefault("timeout", 20)

    # Aplica PRAGMAs al crear la conexión: WAL, sync NORMAL, FK ON, cache razonable
    # (Django no tiene soporte directo en settings; lo hacemos vía señal)
    from django.db.backends.signals import connection_created
    from django.dispatch import receiver

    @receiver(connection_created)
    def _sqlite_config(sender, connection, **kwargs):
        if connection.vendor != "sqlite":
            return
        cursor = connection.cursor()
        # WAL mejora concurrencia; NORMAL reduce fsyncs
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        # seguridad referencial
        cursor.execute("PRAGMA foreign_keys=ON;")
        # cache_size en páginas negativas => KB; -20000 ≈ ~20 MB de cache
        cursor.execute("PRAGMA cache_size=-20000;")
        cursor.close()

# ──────────────────────────────────────────────────────────────────────────────
# Password validation
# ──────────────────────────────────────────────────────────────────────────────
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# ──────────────────────────────────────────────────────────────────────────────
# I18N / TZ
# ──────────────────────────────────────────────────────────────────────────────
LANGUAGE_CODE = "es-ar"
TIME_ZONE = "America/Argentina/Buenos_Aires"
USE_I18N = True
USE_TZ = True

# ──────────────────────────────────────────────────────────────────────────────
# Static files: WhiteNoise
# ──────────────────────────────────────────────────────────────────────────────
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

# Al usar Django 5.2, se recomienda STORAGES en lugar de STATICFILES_STORAGE
STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"
    }
}

# ──────────────────────────────────────────────────────────────────────────────
# Seguridad detrás de proxy TLS (Render) y redirects
# ──────────────────────────────────────────────────────────────────────────────
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SECURE_SSL_REDIRECT = not DEBUG

# ──────────────────────────────────────────────────────────────────────────────
# DRF + JWT
# ──────────────────────────────────────────────────────────────────────────────
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    )
}

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(hours=1),
    "AUTH_HEADER_TYPES": ("Bearer",),
}

# ──────────────────────────────────────────────────────────────────────────────
# CORS (si no usás lista, dejá todo abierto sólo en dev)
# ──────────────────────────────────────────────────────────────────────────────

CORS_ALLOWED_ORIGINS = [
    "https://blue-quantum-capital.vercel.app",
    "https://senales-mercado-frontend.vercel.app",
    "http://localhost:5173"
]

if CORS_ALLOWED_ORIGINS:
    CORS_ALLOW_ALL_ORIGINS = False
else:
    # Útil en desarrollo local; en prod conviene lista explícita
    CORS_ALLOW_ALL_ORIGINS = True

# ──────────────────────────────────────────────────────────────────────────────
# Feature toggles: CSV/DB y universo
# ──────────────────────────────────────────────────────────────────────────────
USE_CSV_FALLBACK = config("USE_CSV_FALLBACK", cast=bool, default=True)
UNIVERSE_MODE = config("UNIVERSE_MODE", default="files")  # "db" | "files"
CSV_ROOT = Path(config("CSV_ROOT", default=str(BASE_DIR)))

# ──────────────────────────────────────────────────────────────────────────────
# Otros
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
JOB_RUN_TOKEN = config("JOB_RUN_TOKEN", default="")



INTERNAL_API_TOKEN = config("INTERNAL_API_TOKEN", default="")


CONN_MAX_AGE = 0  # conexiones cortas, evita 'database locked' con cargas

# ──────────────────────────────────────────────────────────────────────────────
# FORECASTING
# ──────────────────────────────────────────────────────────────────────────────



ML_DIR = BASE_DIR / "forecasting" / "artifacts"

# Directorio de modelos: si no seteás la env, usa forecasting/artifacts
FORECAST_MODELS_DIR = Path(config("FORECAST_MODELS_DIR", default=str(ML_DIR)))

# Etiqueta del modelo en uso
FORECAST_MODEL_NAME = config("FORECAST_MODEL_NAME", default="rf")

# Horizontes (solo 21 y 63). Se leen de env: FORECAST_HORIZONS="21,63"
FORECAST_HORIZONS = Csv(int)(config("FORECAST_HORIZONS", default="21,63"))

# Time-box del endpoint de precompute
FORECAST_MAX_SECONDS = config("FORECAST_MAX_SECONDS", default=40.0, cast=float)
FORECAST_BURST       = config("FORECAST_BURST",       default=8,    cast=int)
FORECAST_SLEEP       = config("FORECAST_SLEEP",       default=0.10, cast=float)