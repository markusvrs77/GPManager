# -*- coding: utf-8 -*-
"""
Пользователи Kafka — те самые SCRAM-учётки, под которыми клиенты
подключаются к брокерам.

Учётка сама по себе не даёт ничего: пока принципалу не выданы правила на
вкладке «Доступы», он сможет только пройти аутентификацию. Поэтому
завести пользователя и выдать права — два разных шага, и путать их не
стоит.

Пароль здесь живёт ровно один раз: приходит в запросе, уходит на брокер
и на этом кончается. Ни в SQLite, ни в журнал действий он не попадает —
журнал хранит имя и механизм, но не то, чем человек войдёт.
"""

import re


# SHA-512 первым: он же и по умолчанию. SHA-256 оставлен для кластеров,
# где механизм уже выбран и менять его поздно.
MECHANISMS = (
    ("SCRAM-SHA-512", "SCRAM-SHA-512"),
    ("SCRAM-SHA-256", "SCRAM-SHA-256"),
)

MECHANISM_NAMES = tuple(name for name, _label in MECHANISMS)
DEFAULT_MECHANISM = "SCRAM-SHA-512"

# Kafka отвергает меньше 4096; 8192 заметно дороже для перебора и всё
# ещё незаметно при входе.
MIN_ITERATIONS = 4096
DEFAULT_ITERATIONS = 8192
MAX_ITERATIONS = 2 ** 20

MIN_PASSWORD_LENGTH = 12

# Имя уходит в принципал ACL и в конфигурацию клиентов, поэтому без
# пробелов, двоеточий и прочего, что там придётся экранировать.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$")


def validate_username(text):
    clean = str(text or "").strip()

    if not clean:
        raise ValueError("Укажите имя пользователя, например svc_etl")

    # человек мог скопировать принципал из таблицы правил
    if clean.lower().startswith("user:"):
        clean = clean.partition(":")[2].strip()

    if not _NAME_RE.match(clean):
        raise ValueError(
            "Имя может состоять из латиницы, цифр, точки, дефиса, "
            "подчёркивания и @, начинаться с буквы или цифры")

    return clean


def validate_password(text):
    password = str(text or "")

    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(
            "Пароль короче {} символов".format(MIN_PASSWORD_LENGTH))

    if password.strip() != password:
        raise ValueError("Пароль начинается или кончается пробелом")

    return password


def validate_mechanism(text):
    clean = str(text or "").strip().upper()

    if not clean:
        return DEFAULT_MECHANISM

    if clean not in MECHANISM_NAMES:
        raise ValueError("Неизвестный механизм: {}".format(text))

    return clean


def validate_iterations(value):
    if value in (None, ""):
        return DEFAULT_ITERATIONS

    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError("Число итераций должно быть целым")

    if number < MIN_ITERATIONS:
        raise ValueError(
            "Kafka не принимает меньше {} итераций".format(MIN_ITERATIONS))

    if number > MAX_ITERATIONS:
        raise ValueError(
            "Слишком много итераций: вход станет ощутимо медленным")

    return number


def build_user_spec(data):
    """Из тела запроса — проверенная заявка на создание или смену пароля."""
    body = data or {}

    return {
        "username": validate_username(body.get("username")),
        "password": validate_password(body.get("password")),
        "mechanism": validate_mechanism(body.get("mechanism")),
        "iterations": validate_iterations(body.get("iterations")),
    }


def audit_details(spec):
    """
    То же самое, но без пароля — для журнала.

    Отдельной функцией, а не `del spec["password"]` по месту: так забыть
    её сложнее, чем пропустить одну строчку среди прочих.
    """
    return {
        "username": spec["username"],
        "mechanism": spec["mechanism"],
        "iterations": spec["iterations"],
    }


def mechanism_name(value):
    """ScramMechanism.SCRAM_SHA_512 → 'SCRAM-SHA-512'."""
    name = getattr(value, "name", None)

    if name is None:
        name = str(value)

    return name.replace("_", "-")


def format_user(username, credential_infos):
    """Ответ describe_user_scram_credentials — в вид для страницы."""
    mechanisms = []

    for info in credential_infos or []:
        mechanisms.append({
            "mechanism": mechanism_name(info.get("mechanism")),
            "iterations": info.get("iterations"),
        })

    return {
        "username": username,
        # в правилах он будет выглядеть именно так
        "principal": "User:{}".format(username),
        "mechanisms": mechanisms,
    }
