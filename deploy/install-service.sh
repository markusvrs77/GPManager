#!/bin/bash
# Установка Opsentri как systemd-сервиса (RHEL 8/9).
#
#   sudo ./deploy/install-service.sh [--user gpadmin] [--python /path/to/python3]
#                                    [--port 8080] [--gphome /usr/local/gpdb/greenplum-db]
#
# Повторный запуск безопасен: обновляет unit, /etc/opsentri/opsentri.env
# не перезаписывает (там твои настройки).

set -euo pipefail

NAME=opsentri
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_USER=gpadmin
PYTHON=""
PORT=8080
GPHOME_DIR="${GPHOME:-/usr/local/gpdb/greenplum-db}"
ENV_DIR=/etc/opsentri
ENV_FILE="$ENV_DIR/$NAME.env"
UNIT_FILE="/etc/systemd/system/$NAME.service"

die() { echo "ОШИБКА: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --user)   RUN_USER="$2"; shift 2 ;;
        --python) PYTHON="$2"; shift 2 ;;
        --port)   PORT="$2"; shift 2 ;;
        --gphome) GPHOME_DIR="$2"; shift 2 ;;
        -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
        *) die "неизвестный параметр: $1" ;;
    esac
done

[ "$(id -u)" -eq 0 ] || die "запусти через sudo"
id "$RUN_USER" >/dev/null 2>&1 || die "пользователя $RUN_USER нет"
[[ "$PORT" =~ ^[0-9]+$ ]] || die "порт должен быть числом: $PORT"
[ -f "$APP_DIR/app.py" ] || die "не найден $APP_DIR/app.py"

RUN_GROUP="$(id -gn "$RUN_USER")"

# python: --python, затем venv рядом с приложением, затем системный
if [ -z "$PYTHON" ]; then
    for candidate in "$APP_DIR/venv/bin/python" "$APP_DIR/.venv/bin/python" \
                     "$(command -v python3.11 || true)" "$(command -v python3 || true)"; do
        if [ -n "$candidate" ] && [ -x "$candidate" ]; then
            PYTHON="$candidate"
            break
        fi
    done
fi

[ -n "$PYTHON" ] && [ -x "$PYTHON" ] || die "не найден python, укажи --python"

# зависимости проверяем под тем же пользователем, под которым пойдёт сервис
if ! sudo -u "$RUN_USER" "$PYTHON" -c "import flask, psycopg2, yaml, openpyxl, croniter" 2>/dev/null; then
    die "у $PYTHON нет зависимостей (под $RUN_USER): $PYTHON -m pip install -r $APP_DIR/requirements.txt"
fi

[ -d "$GPHOME_DIR" ] || echo "ВНИМАНИЕ: $GPHOME_DIR не найден — поправь GPHOME в $ENV_FILE"

# каталоги, куда приложение пишет: база задач и логи gpcopy/gpbackup
for dir in instance logs; do
    mkdir -p "$APP_DIR/$dir"

    if ! sudo -u "$RUN_USER" test -w "$APP_DIR/$dir"; then
        echo "Отдаю $APP_DIR/$dir пользователю $RUN_USER"
        chown -R "$RUN_USER:$RUN_GROUP" "$APP_DIR/$dir"
    fi
done

# настройки: только если их ещё нет
mkdir -p "$ENV_DIR"

if [ ! -f "$ENV_FILE" ]; then
    sed -e "s|@GPHOME@|$GPHOME_DIR|g" \
        -e "s|^OPSENTRI_PORT=.*|OPSENTRI_PORT=$PORT|" \
        "$APP_DIR/deploy/$NAME.env.example" > "$ENV_FILE"
    chown "root:$RUN_GROUP" "$ENV_FILE"
    chmod 0640 "$ENV_FILE"
    echo "Создан $ENV_FILE"
else
    echo "Оставлен как есть $ENV_FILE"
fi

sed -e "s|@USER@|$RUN_USER|g" \
    -e "s|@GROUP@|$RUN_GROUP|g" \
    -e "s|@APP_DIR@|$APP_DIR|g" \
    -e "s|@PYTHON@|$PYTHON|g" \
    "$APP_DIR/deploy/$NAME.service" > "$UNIT_FILE"
chmod 0644 "$UNIT_FILE"

systemctl daemon-reload
systemctl enable "$NAME" >/dev/null 2>&1
echo "Установлен $UNIT_FILE (автозапуск при загрузке включён)"

# приложение, запущенное руками, держит порт — сервис бы не поднялся
CURRENT_PORT="$(sed -n 's/^OPSENTRI_PORT=\([0-9]*\).*/\1/p' "$ENV_FILE")"
CURRENT_PORT="${CURRENT_PORT:-8080}"

if ! systemctl is-active --quiet "$NAME" && \
        ss -ltnH "sport = :$CURRENT_PORT" 2>/dev/null | grep -q .; then
    echo
    echo "Порт $CURRENT_PORT уже занят (приложение запущено вручную?)."
    echo "Останови его (Ctrl+C или kill), затем: sudo systemctl start $NAME"
    exit 0
fi

systemctl restart "$NAME"
sleep 2
systemctl --no-pager --lines=5 status "$NAME" || true

cat <<EOF

Готово:
  sudo systemctl restart $NAME   # перезапуск (gpcopy/gpbackup не прерываются)
  sudo systemctl stop $NAME
  sudo systemctl status $NAME
  journalctl -u $NAME -f         # лог
  настройки: $ENV_FILE
EOF
