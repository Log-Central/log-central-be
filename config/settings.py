from decimal import ROUND_HALF_UP, DefaultContext
from pathlib import Path

import environ
from kombu import Exchange, Queue

# Initialize environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
env = environ.Env()
environ.Env.read_env(env_file=BASE_DIR / ".env")


# Environment Settings
ENV = env("ENV", default="localhost")
SECRET_KEY = env("SECRET_KEY", default="")
DEBUG = True
ALLOWED_HOSTS = ["*"]


# Logging Settings
LOG_FILE = BASE_DIR / "logs/debug.log"
LOG_FILE.parent.mkdir(exist_ok=True)
LOG_FILE.touch(exist_ok=True)
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} || {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "verbose"},
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "verbose",
            "filename": LOG_FILE,
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 10,
            "encoding": "utf-8",
        },
    },
    "loggers": {
        "django": {"handlers": ["file", "console"], "level": "INFO", "propagate": True},
    },
}


# swagger
REST_FRAMEWORK = {"DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema"}


# Installed Apps
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "django_celery_results",
    "user",
    "common",
    "monitoring",
]
# Middleware
MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]
CORS_ALLOW_ALL_ORIGINS = True

# URL Configuration
ROOT_URLCONF = "config.urls"

# Templates
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# WSGI Application
WSGI_APPLICATION = "config.wsgi.application"

# Database Configuration

# PostgreSQL Configuration
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("DB_NAME", default="mylogbe"),
        "USER": "postgres",
        "PASSWORD": env("DB_PASSWORD", default=""),
        "HOST": env("DB_HOST", default=""),
        "PORT": "5432",
        "OPTIONS": {
            "options": "-c search_path=public,content",
        },
        "POOL_OPTIONS": {
            "POOL_SIZE": 30,
            "MAX_OVERFLOW": 10,
            "RECYCLE": 90,
            "PRE_PING": True,
        },
    }
}


# Static Files
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

# Language and Time Zone
LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Seoul"
USE_I18N = True
USE_TZ = True

# Celery Configuration
CELERY_RESULT_BACKEND = "django-db"
CELERY_TASK_IGNORE_RESULT = True
CELERY_BROKER_URL = env(
    "CELERY_BROKER_URL", default="amqp://guest:guest@localhost:5672/%2F"
)

# redis configuration
REDIS_HOST = env("REDIS_HOST", default="localhost")
REDIS_PORT = env.int("REDIS_PORT", default=6379)
REDIS_DB = env.int("REDIS_DB", default=0)


# Default Primary Key
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
DefaultContext.rounding = ROUND_HALF_UP
JWT_SECRET = env("JWT_SECRET", default="")
GOOGLE_CLIENT_ID = env("GOOGLE_CLIENT_ID", default="GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = env("GOOGLE_CLIENT_SECRET", default="GOOGLE_CLIENT_SECRET")
BASE_URL = env("BASE_URL", default="http://localhost:8000")
DB_NAME = env("DB_NAME", default="mylogbe")
GRAFANA_URL = env("GRAFANA_URL", default="http://localhost:3000")
S3_BUCKET_NAME = env("S3_BUCKET_NAME", default="mylogbe")
S3_AWS_REGION = env("AWS_REGION", default="ap-northeast-2")
S3_AWS_ACCESS_KEY_ID = env("S3_AWS_ACCESS_KEY_ID", default="")
S3_AWS_SECRET_ACCESS_KEY = env("S3_AWS_SECRET_ACCESS_KEY", default="")
GRAFANA_URL = env("GRAFANA_URL", default="http://localhost:3000")
GRAFANA_ADMIN_API_KEY = env("GRAFANA_ADMIN_API_KEY", default="")
