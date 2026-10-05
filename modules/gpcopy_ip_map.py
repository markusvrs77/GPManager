"""
Карта «хост сегмента -> IP» для кластера-приёмника gpcopy.

gpcopy гонит данные напрямую между сегментами: сегменты источника
подключаются к сегментам приёмника по адресам из его
gp_segment_configuration. Если сегменты приёмника записаны там под
адресами внутренней сети (интерконнект), из другого кластера их не
видно: мастер отвечает, а копирование падает по таймауту.

На этот случай у gpcopy есть --dest-mapping-file — файл строк «хост,IP».
Тогда сегменты источника идут на сегменты приёмника по указанным IP.
Карта хранится у подключения и подставляется каждый раз, когда это
подключение выступает приёмником. Когда оно источник, карта не нужна:
тогда подключаются к сегментам другого кластера.
"""

import ipaddress
import json
import os
import re
import tempfile

from db import sqlite_cursor


MAPPING_FLAG = "--dest-mapping-file"

# имя хоста или адрес из gp_segment_configuration; запятая и пробелы
# сломали бы формат файла
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,252}$")


def _clean_host(value):
    host = str(value or "").strip()
    return host if HOST_RE.match(host) else None


def _clean_ip(value):
    try:
        return str(ipaddress.ip_address(str(value or "").strip()))
    except ValueError:
        return None


def normalize_entries(raw):
    """
    Проверяет записи карты и возвращает их в виде
    [{"host", "addresses"?, "ip"}]. Строки без IP пропускаются:
    в редакторе это хосты, которые ещё не заполнены.

    Ошибка в имени хоста или в IP — ValueError: такую карту gpcopy
    не примет, а молча выкинутая строка дала бы тот же таймаут.
    """
    if not isinstance(raw, list):
        raise ValueError("entries должен быть списком")

    result = []
    seen = set()

    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Запись карты должна быть объектом")

        ip_raw = str(item.get("ip") or "").strip()
        if not ip_raw:
            continue

        host = _clean_host(item.get("host"))
        if not host:
            raise ValueError(
                "Недопустимое имя хоста: {!r}".format(item.get("host")))

        ip = _clean_ip(ip_raw)
        if not ip:
            raise ValueError(
                "Недопустимый IP для {}: {!r}".format(host, ip_raw))

        if host in seen:
            raise ValueError("Хост {} указан дважды".format(host))
        seen.add(host)

        # адреса из gp_segment_configuration.address: у хоста с
        # несколькими интерфейсами их бывает больше одного
        addresses = []
        raw_addresses = item.get("addresses")
        if not isinstance(raw_addresses, list):
            raw_addresses = []

        for value in raw_addresses:
            address = _clean_host(value)
            if address and address != host and address not in addresses:
                addresses.append(address)

        entry = {"host": host, "ip": ip}
        if addresses:
            entry["addresses"] = addresses
        result.append(entry)

    return result


def get_ip_map(connection_id):
    """Сохранённая карта подключения; пустой список, если её нет."""
    if connection_id in (None, ""):
        return []

    with sqlite_cursor() as cur:
        cur.execute(
            "SELECT segment_ip_map FROM connections WHERE id = ?",
            (int(connection_id),),
        )
        row = cur.fetchone()

    if not row or not row["segment_ip_map"]:
        return []

    try:
        return normalize_entries(json.loads(row["segment_ip_map"]))
    except (TypeError, ValueError):
        return []


def save_ip_map(connection_id, raw_entries):
    """Проверяет и сохраняет карту. Пустой список удаляет её."""
    entries = normalize_entries(raw_entries)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            UPDATE connections
            SET segment_ip_map = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (json.dumps(entries) if entries else None, int(connection_id)),
        )

        if cur.rowcount == 0:
            raise LookupError("Подключение не найдено")

    return entries


def mapping_lines(entries):
    """
    Строки файла для gpcopy.

    Хост пишется и под именем (hostname), и под каждым адресом (address),
    если они различаются: в gp_segment_configuration есть оба, и карта
    должна сработать, по какому бы из них gpcopy ни искал сегмент.
    """
    lines = []
    keys = set()

    for entry in entries:
        for key in [entry["host"]] + list(entry.get("addresses") or []):
            if key not in keys:
                keys.add(key)
                lines.append("{},{}".format(key, entry["ip"]))

    return lines


def write_dest_mapping_file(dest_cfg):
    """
    Файл карты для подключения-приёмника или None, если карты нет.

    Без карты команда gpcopy остаётся прежней: флаг добавляется только
    для кластеров, где карту заполнили.
    """
    entries = get_ip_map((dest_cfg or {}).get("id"))

    if not entries:
        return None

    fd, path = tempfile.mkstemp(prefix="gpcopy_dest_map_", suffix=".txt")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("\n".join(mapping_lines(entries)) + "\n")

    print("[gpcopy] карта IP сегментов приёмника: {} ({} хостов)".format(
        path, len(entries)))

    return path


def list_segment_hosts(conn):
    """
    Хосты сегментов кластера: имя, адреса и сколько сегментов на них.

    Только чтение каталога. Мастер (content = -1) не нужен: к нему
    gpcopy идёт по --dest-host.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT hostname, address,
                   sum(CASE WHEN role = 'p' THEN 1 ELSE 0 END) AS primaries,
                   sum(CASE WHEN role = 'm' THEN 1 ELSE 0 END) AS mirrors
            FROM gp_segment_configuration
            WHERE content >= 0
            GROUP BY hostname, address
            ORDER BY hostname, address
            """
        )
        rows = cur.fetchall()

    hosts = {}

    for hostname, address, primaries, mirrors in rows:
        host = hosts.setdefault(hostname, {
            "host": hostname, "addresses": [], "primaries": 0, "mirrors": 0,
        })
        if address and address != hostname and address not in host["addresses"]:
            host["addresses"].append(address)
        host["primaries"] += int(primaries or 0)
        host["mirrors"] += int(mirrors or 0)

    return list(hosts.values())
