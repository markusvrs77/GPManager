import os


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(INSTANCE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

SQLITE_DB_PATH = os.path.join(INSTANCE_DIR, "gp_reorganize_center.sqlite3")

APP_HOST = "0.0.0.0"
APP_PORT = 8080
APP_DEBUG = True
