"""
Куда грузить таблицу: карта «таблица источника -> таблица приёмника».

Синхронизация по умолчанию пишет в одноимённую таблицу приёмника. Карта
`targets` в конфиге задачи позволяет направить таблицу в другую:

    {"src_schema.src_table": "dst_schema.dst_table"}

Значение без схемы ("table") берёт схему источника. Пустое значение и
цель, совпадающая с источником, из карты выбрасываются — для таких
таблиц всё остаётся как было.

Имя цели вводит человек, и дальше оно уходит в DDL, в DELETE окна и в
аргументы gpcopy. Поэтому формат узкий: строчные латинские буквы,
цифры, «_» и «$», без кавычек — такое имя одинаково понимают и
PostgreSQL без кавычек, и gpcopy.
"""

import re


PART_RE = re.compile(r"^[a-z_][a-z0-9_$]{0,62}$")


def _key(schema, table):
    return "{}.{}".format(schema, table)


def _split_source(name):
    name = str(name or "").strip()
    if name.count(".") != 1:
        raise ValueError("Таблица источника должна быть вида schema.table: "
                         "{!r}".format(name))
    schema, table = name.split(".")
    if not schema or not table:
        raise ValueError("Таблица источника должна быть вида schema.table: "
                         "{!r}".format(name))
    return schema, table


def parse_target(value, src_schema):
    """
    «schema.table» или «table» -> (schema, table). Пустое -> None.
    Ошибка формата — ValueError с текстом для человека.
    """
    text = str(value or "").strip()

    if not text:
        return None

    parts = text.split(".")

    if len(parts) not in (1, 2):
        raise ValueError(
            "Цель {!r}: нужно schema.table или table".format(text))

    # проверяем только то, что ввёл человек: схема, взятая из источника,
    # уже существует и дальше везде идёт в кавычках, даже если в ней
    # заглавные буквы
    for part in parts:
        if not PART_RE.match(part):
            raise ValueError(
                "Цель {!r}: имя {!r} — только строчные латинские буквы, "
                "цифры, _ и $, не с цифры, до 63 символов".format(text, part))

    if len(parts) == 1:
        return src_schema, parts[0]

    return parts[0], parts[1]


def _selected_keys(selected):
    """Выбранные таблицы в виде множества «schema.table»."""
    keys = set()

    for item in selected or []:
        if isinstance(item, dict):
            schema = (item.get("schema") or item.get("schema_name")
                      or item.get("source_schema"))
            table = (item.get("table") or item.get("table_name")
                     or item.get("source_table"))
            if schema and table:
                keys.add(_key(schema, table))
        elif item:
            keys.add(_key(*_split_source(item)))

    return keys


def normalize_targets(raw, selected):
    """
    Проверяет карту и возвращает {"schema.table": "schema.table"}.

    selected — выбранные таблицы задачи (словари schema/table или строки
    «schema.table»). Ключ вне выбора, две таблицы в одну цель, цель,
    в которую и так грузится другая выбранная таблица, — ValueError.
    """
    if raw in (None, "", {}):
        return {}

    if not isinstance(raw, dict):
        raise ValueError("targets должен быть объектом "
                         "{\"schema.table\": \"schema.table\"}")

    chosen = _selected_keys(selected)
    result = {}

    for src_name, value in raw.items():
        src_schema, src_table = _split_source(src_name)
        src_key = _key(src_schema, src_table)

        if src_key not in chosen:
            raise ValueError(
                "Таблица {} не выбрана, а для неё задана цель".format(src_key))

        parsed = parse_target(value, src_schema)

        if parsed is None or parsed == (src_schema, src_table):
            continue

        result[src_key] = _key(*parsed)

    # куда в итоге пишет каждая выбранная таблица
    owner = {}

    for src_key in sorted(chosen):
        dst_key = result.get(src_key, src_key)

        if dst_key in owner:
            raise ValueError(
                "В {} грузятся сразу {} и {} — у каждой таблицы должна "
                "быть своя цель".format(dst_key, owner[dst_key], src_key))

        owner[dst_key] = src_key

    return result


def target_of(targets, schema, table):
    """(схема, таблица) приёмника для таблицы источника."""
    value = (targets or {}).get(_key(schema, table))

    if not value:
        return schema, table

    dst_schema, dst_table = value.split(".", 1)
    return dst_schema, dst_table


def is_mapped(targets, schema, table):
    return target_of(targets, schema, table) != (schema, table)
