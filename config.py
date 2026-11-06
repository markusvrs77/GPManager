import os


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(INSTANCE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

SQLITE_DB_PATH = os.path.join(INSTANCE_DIR, "gp_reorganize_center.sqlite3")

def _env_flag(name, default):
    raw = os.environ.get(name)

    if raw is None or raw.strip() == "":
        return default

    return raw.strip().lower() in ("1", "true", "yes", "on")


# Переопределяются окружением (systemd: /etc/opsentri/opsentri.env).
# Без переменных — как раньше: 0.0.0.0:8080, debug включён (разработка).
APP_HOST = os.environ.get("OPSENTRI_HOST") or "0.0.0.0"
APP_PORT = int(os.environ.get("OPSENTRI_PORT") or 8080)
APP_DEBUG = _env_flag("OPSENTRI_DEBUG", True)

# Cookie сессии помечается Secure только за HTTPS: иначе браузер её
# просто не сохранит, и вход по http перестанет работать.
COOKIE_SECURE = os.environ.get("OPSENTRI_COOKIE_SECURE", "0") == "1"
