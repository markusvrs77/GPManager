"""
Пользователи, роли и права Opsentri.

До этого модуля в приложении не было аутентификации вообще: кто дотянулся
до порта, тот получал доступ к учётным данным кластеров и к операциям,
которые удаляют и переписывают данные.

Модель прав двухслойная. Роль задаёт обычный набор возможностей, а
точечная запись в user_permissions его переопределяет — так оператору
можно добавить «Гранты — изменять», не выдумывая четвёртую роль. Отдельно
от возможностей идёт привязка к кластерам: право «запускать перенос» не
значит «запускать перенос на PROD».

Пароли хэшируются scrypt из стандартной библиотеки. План фазы называл
Argon2id, но argon2-cffi — C-расширение, а приложению предстоит жить на
RHEL, возможно без доступа к колёсам. scrypt тоже memory-hard и для
паролей подходит, а новых зависимостей не требует.
"""

import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta

from db import sqlite_cursor


# ------------------------------------------------------------
# Каталог возможностей
# ------------------------------------------------------------

# (код, что видит человек в списке прав). Просмотр и действие разведены
# намеренно: посмотреть расхождение партиций безопасно, запустить перенос —
# нет, и это разные права.
CAPABILITIES = [
    ("dashboard.view", "Dashboard — просмотр"),
    ("health.view", "Здоровье БД — просмотр"),
    ("connections.view", "Подключения — просмотр"),
    ("connections.edit", "Подключения — изменять"),
    ("objects.view", "Объекты — просмотр"),
    ("jobs.view", "Задачи — просмотр"),
    ("jobs.stop", "Задачи — останавливать"),
    ("sync.view", "Синхронизация — просмотр"),
    ("sync.run", "Синхронизация — запускать перенос"),
    ("maintenance.view", "Maintenance — просмотр"),
    ("maintenance.run", "Maintenance — запускать"),
    ("vacuum.view", "Vacuum / Analyze — просмотр"),
    ("vacuum.run", "Vacuum / Analyze — запускать"),
    ("backups.view", "Резервные копии — просмотр"),
    ("backups.run", "Резервные копии — запускать"),
    ("grants.view", "Гранты — просмотр"),
    ("grants.edit", "Гранты — изменять"),
    ("schedules.view", "Расписания — просмотр"),
    ("schedules.edit", "Расписания — изменять"),
    ("kafka.view", "Kafka — просмотр"),
    ("kafka.edit", "Kafka — изменять"),
    ("users.manage", "Пользователи — управление"),
]

CAPABILITY_CODES = frozenset(code for code, _label in CAPABILITIES)

VIEW_CAPS = frozenset(c for c in CAPABILITY_CODES if c.endswith(".view"))

ROLES = ("viewer", "operator", "admin")

# Оператор запускает задачи, но не трогает подключения и гранты: доступы к
# кластерам и права в самом Greenplum — вотчина администратора.
ROLE_DEFAULTS = {
    "viewer": VIEW_CAPS,
    "operator": VIEW_CAPS | frozenset({
        "sync.run", "maintenance.run", "vacuum.run", "backups.run",
        "schedules.edit", "jobs.stop",
    }),
    "admin": CAPABILITY_CODES,
}


def capability_label(code):
    for item_code, label in CAPABILITIES:
        if item_code == code:
            return label
    return code


# ------------------------------------------------------------
# Пароли
# ------------------------------------------------------------

_SCRYPT_N = 2 ** 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_DKLEN = 32


MIN_PASSWORD_LENGTH = 10


def check_password_policy(password):
    """Единственное требование — длина.

    Требования вида «цифра, заглавная и спецсимвол» гонят людей к
    «Parol123!» и к бумажке под клавиатурой; длина даёт больше стойкости
    и не мешает пользоваться менеджером паролей.
    """
    if not password or len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(
            "Пароль короче {} символов".format(MIN_PASSWORD_LENGTH))


def hash_password(password):
    """scrypt$n$r$p$соль$хэш — параметры хранятся рядом, чтобы их можно
    было поднять позже, не ломая старые хэши."""
    if not password:
        raise ValueError("Пароль пуст")

    salt = os.urandom(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"), salt=salt,
        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_DKLEN,
    )

    return "scrypt${}${}${}${}${}".format(
        _SCRYPT_N, _SCRYPT_R, _SCRYPT_P,
        salt.hex(), digest.hex(),
    )


def verify_password(password, stored):
    """Сравнение постоянного времени: иначе по задержке ответа можно
    подбирать хэш побайтно."""
    if not password or not stored:
        return False

    try:
        scheme, n, r, p, salt_hex, digest_hex = stored.split("$")
        if scheme != "scrypt":
            return False

        digest = hashlib.scrypt(
            password.encode("utf-8"), salt=bytes.fromhex(salt_hex),
            n=int(n), r=int(r), p=int(p), dklen=len(digest_hex) // 2,
        )
    except (ValueError, TypeError):
        return False

    return hmac.compare_digest(digest.hex(), digest_hex)


# ------------------------------------------------------------
# Пользователи
# ------------------------------------------------------------

def normalize_username(username):
    return (username or "").strip().lower()


def _utc_now():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def create_user(username, password, role, must_change_password=True):
    name = normalize_username(username)

    if not name:
        raise ValueError("Имя пользователя пусто")

    if role not in ROLES:
        raise ValueError("Неизвестная роль: {}".format(role))

    with sqlite_cursor(commit=True) as cur:
        cur.execute("SELECT id FROM users WHERE username = ?", (name,))
        if cur.fetchone():
            raise ValueError("Пользователь уже существует: {}".format(name))

        cur.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active,
                               must_change_password, failed_logins,
                               locked_until, created_at)
            VALUES (?, ?, ?, 1, ?, 0, NULL, ?)
            """,
            (name, hash_password(password), role,
             1 if must_change_password else 0, _utc_now()),
        )
        return cur.lastrowid


def get_user_by_name(username):
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT * FROM users WHERE username = ?",
            (normalize_username(username),),
        )
        return _row_to_user(cur.fetchone())


def get_user(user_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id = ?", (int(user_id),))
        return _row_to_user(cur.fetchone())


def list_users():
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM users ORDER BY username")
        return [_row_to_user(row) for row in cur.fetchall()]


def _row_to_user(row):
    if row is None:
        return None

    user = dict(row)
    user["is_active"] = bool(user.get("is_active"))
    user["must_change_password"] = bool(user.get("must_change_password"))
    return user


def set_user_active(user_id, active):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE users SET is_active = ? WHERE id = ?",
            (1 if active else 0, int(user_id)),
        )
    # выключенный пользователь не должен доработать текущую сессию
    if not active:
        destroy_user_sessions(user_id)


def set_user_role(user_id, role):
    if role not in ROLES:
        raise ValueError("Неизвестная роль: {}".format(role))

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE users SET role = ? WHERE id = ?", (role, int(user_id)),
        )
    # права изменились — старую сессию продлевать нельзя
    destroy_user_sessions(user_id)


def set_user_password(user_id, password, must_change=False):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE users
               SET password_hash = ?, must_change_password = ?,
                   failed_logins = 0, locked_until = NULL
             WHERE id = ?
            """,
            (hash_password(password), 1 if must_change else 0, int(user_id)),
        )
    destroy_user_sessions(user_id)


def delete_user(user_id):
    with sqlite_cursor(commit=True) as cur:
        uid = int(user_id)
        cur.execute("DELETE FROM user_permissions WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM user_connections WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM user_sessions WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM users WHERE id = ?", (uid,))


# ------------------------------------------------------------
# Возможности: роль плюс точечные исключения
# ------------------------------------------------------------

def get_overrides(user_id):
    """{возможность: разрешена} — только явно заданные, роль сюда не входит."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT capability, allowed FROM user_permissions WHERE user_id = ?",
            (int(user_id),),
        )
        return {row["capability"]: bool(row["allowed"]) for row in cur.fetchall()}


def set_override(user_id, capability, allowed):
    """allowed=None убирает исключение и возвращает возможность под роль."""
    if capability not in CAPABILITY_CODES:
        raise ValueError("Неизвестная возможность: {}".format(capability))

    uid = int(user_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM user_permissions WHERE user_id = ? AND capability = ?",
            (uid, capability),
        )
        if allowed is not None:
            cur.execute(
                """
                INSERT INTO user_permissions (user_id, capability, allowed)
                VALUES (?, ?, ?)
                """,
                (uid, capability, 1 if allowed else 0),
            )
    destroy_user_sessions(user_id)


def effective_capabilities(user):
    """
    Что пользователю реально доступно.

    Администратор получает всё и исключениями не урезается: иначе можно
    отобрать у последнего админа управление пользователями и запереть
    систему без единого входа.
    """
    if not user or not user.get("is_active"):
        return frozenset()

    role = user.get("role")

    if role == "admin":
        return CAPABILITY_CODES

    caps = set(ROLE_DEFAULTS.get(role, frozenset()))

    for capability, allowed in get_overrides(user["id"]).items():
        if allowed:
            caps.add(capability)
        else:
            caps.discard(capability)

    return frozenset(caps)


def has_capability(user, capability):
    return capability in effective_capabilities(user)


# ------------------------------------------------------------
# Привязка к кластерам
# ------------------------------------------------------------

def set_user_connections(user_id, connection_ids):
    uid = int(user_id)

    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM user_connections WHERE user_id = ?", (uid,))
        for connection_id in connection_ids or []:
            cur.execute(
                "INSERT INTO user_connections (user_id, connection_id) VALUES (?, ?)",
                (uid, int(connection_id)),
            )


def allowed_connection_ids(user):
    """
    None означает «все кластеры» — так отвечает только администратор.

    Для остальных пустой список означает пустой список: пользователь без
    выданных кластеров не работает ни с одним. Обратное правило («пусто =
    всё») удобно ровно до первого забытого пользователя с доступом к PROD.
    """
    if not user or not user.get("is_active"):
        return frozenset()

    if user.get("role") == "admin":
        return None

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT connection_id FROM user_connections WHERE user_id = ?",
            (int(user["id"]),),
        )
        return frozenset(row["connection_id"] for row in cur.fetchall())


def can_use_connection(user, connection_id):
    if connection_id is None:
        return True

    allowed = allowed_connection_ids(user)

    if allowed is None:
        return True

    try:
        return int(connection_id) in allowed
    except (TypeError, ValueError):
        return False


# ------------------------------------------------------------
# Сессии
# ------------------------------------------------------------

SESSION_TTL_HOURS = 12
MAX_FAILED_LOGINS = 5
LOCKOUT_MINUTES = 15


def _token_hash(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_session(user_id, ip=None):
    """
    В базе лежит только хэш токена: утечка файла БД не должна давать
    возможность войти чужой сессией.
    """
    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO user_sessions (token_hash, user_id, created_at,
                                       expires_at, ip)
            VALUES (?, ?, ?, ?, ?)
            """,
            (_token_hash(token), int(user_id), _utc_now(),
             expires.strftime("%Y-%m-%d %H:%M:%S"), ip),
        )

    return token


def resolve_session(token):
    if not token:
        return None

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT user_id, expires_at FROM user_sessions WHERE token_hash = ?",
            (_token_hash(token),),
        )
        row = cur.fetchone()

    if not row:
        return None

    if row["expires_at"] <= _utc_now():
        destroy_session(token)
        return None

    user = get_user(row["user_id"])
    return user if user and user["is_active"] else None


def destroy_session(token):
    if not token:
        return

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM user_sessions WHERE token_hash = ?", (_token_hash(token),),
        )


def destroy_user_sessions(user_id):
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM user_sessions WHERE user_id = ?", (int(user_id),),
        )


def purge_expired_sessions():
    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "DELETE FROM user_sessions WHERE expires_at <= ?", (_utc_now(),),
        )


# ------------------------------------------------------------
# Вход
# ------------------------------------------------------------

class AuthError(Exception):
    """Причина для человека; наружу отдаётся один и тот же текст."""


def authenticate(username, password):
    """
    Возвращает пользователя либо бросает AuthError.

    Текст ошибки одинаков для несуществующего имени и неверного пароля:
    иначе форма входа превращается в справочник существующих учёток.
    """
    user = get_user_by_name(username)

    if user is None:
        # считаем пароль вхолостую, чтобы по времени ответа нельзя было
        # отличить несуществующее имя от неверного пароля
        verify_password(password or "x", hash_password("dummy"))
        raise AuthError("Неверное имя пользователя или пароль")

    if not user["is_active"]:
        raise AuthError("Учётная запись отключена")

    locked_until = user.get("locked_until")

    if locked_until and locked_until > _utc_now():
        raise AuthError("Вход временно заблокирован, попробуйте позже")

    if not verify_password(password, user["password_hash"]):
        _register_failed_login(user)
        raise AuthError("Неверное имя пользователя или пароль")

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE users SET failed_logins = 0, locked_until = NULL WHERE id = ?",
            (user["id"],),
        )

    return get_user(user["id"])


def _register_failed_login(user):
    failed = int(user.get("failed_logins") or 0) + 1
    locked_until = None

    if failed >= MAX_FAILED_LOGINS:
        locked_until = (
            datetime.utcnow() + timedelta(minutes=LOCKOUT_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        failed = 0

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE users SET failed_logins = ?, locked_until = ? WHERE id = ?",
            (failed, locked_until, user["id"]),
        )


def admin_exists():
    """Есть ли хоть один живой администратор — от этого зависит, показывать
    ли первичную настройку."""
    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND is_active = 1"
        )
        return int(cur.fetchone()["n"]) > 0
