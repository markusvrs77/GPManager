# -*- coding: utf-8 -*-
"""
Правила доступа Kafka: разбор ответа, сборка правил и шаблоны.

Чистые функции: ни сети, ни базы. Имена операций и ресурсов держим
строками — перечисления библиотеки числовые, и таскать их по коду
значит завязать на неё половину приложения.
"""

# операции, которые имеет смысл выдавать из интерфейса
OPERATIONS = (
    ("ALL", "все операции"),
    ("READ", "чтение"),
    ("WRITE", "запись"),
    ("CREATE", "создание"),
    ("DELETE", "удаление"),
    ("ALTER", "изменение"),
    ("DESCRIBE", "просмотр"),
    ("DESCRIBE_CONFIGS", "просмотр конфигов"),
    ("ALTER_CONFIGS", "изменение конфигов"),
    ("IDEMPOTENT_WRITE", "идемпотентная запись"),
)

RESOURCE_TYPES = (
    ("TOPIC", "топик"),
    ("GROUP", "консьюмер-группа"),
    ("CLUSTER", "кластер"),
    ("TRANSACTIONAL_ID", "транзакционный id"),
)

PATTERN_TYPES = (
    ("LITERAL", "точное имя"),
    ("PREFIXED", "по префиксу"),
)

PERMISSIONS = (
    ("ALLOW", "разрешить"),
    ("DENY", "запретить"),
)

# у ресурса типа CLUSTER имя всегда одно и то же
CLUSTER_RESOURCE_NAME = "kafka-cluster"

_OPERATION_NAMES = set(name for name, _ in OPERATIONS)
_RESOURCE_NAMES = set(name for name, _ in RESOURCE_TYPES)
_PATTERN_NAMES = set(name for name, _ in PATTERN_TYPES)
_PERMISSION_NAMES = set(name for name, _ in PERMISSIONS)


def validate_principal(text):
    """'svc_etl' → 'User:svc_etl'. Чужой префикс не трогаем."""
    clean = str(text or "").strip()

    if not clean:
        raise ValueError("Укажите принципал, например User:svc_etl")

    if ":" in clean:
        prefix, _, name = clean.partition(":")

        if not prefix.strip() or not name.strip():
            raise ValueError(
                "Принципал выглядит как User:имя, например User:svc_etl")

        return "{}:{}".format(prefix.strip(), name.strip())

    return "User:{}".format(clean)


def _one_of(value, allowed, default, label):
    clean = str(value or "").strip().upper()

    if not clean:
        return default

    if clean not in allowed:
        raise ValueError("Неизвестное значение {}: {}".format(label, value))

    return clean


def build_acl_spec(data):
    """Форма выдачи прав → спецификация одного набора правил."""
    data = data or {}

    resource_type = _one_of(
        data.get("resource_type"), _RESOURCE_NAMES, "TOPIC", "типа ресурса")

    name = str(data.get("resource_name") or "").strip()

    if resource_type == "CLUSTER":
        name = CLUSTER_RESOURCE_NAME

    if not name:
        raise ValueError("Укажите имя ресурса")

    operations = []

    for item in data.get("operations") or []:
        operation = str(item or "").strip().upper()

        if operation not in _OPERATION_NAMES:
            raise ValueError("Неизвестная операция: {}".format(item))

        if operation not in operations:
            operations.append(operation)

    if not operations:
        raise ValueError("Выберите хотя бы одну операцию")

    return {
        "principal": validate_principal(data.get("principal")),
        "host": str(data.get("host") or "").strip() or "*",
        "resource_type": resource_type,
        "resource_name": name,
        "pattern_type": _one_of(data.get("pattern_type"), _PATTERN_NAMES,
                                "LITERAL", "типа шаблона"),
        "operations": operations,
        "permission": _one_of(data.get("permission"), _PERMISSION_NAMES,
                              "ALLOW", "разрешения"),
    }


def build_filter_spec(data):
    """
    Форма фильтра → спецификация поиска.

    Пустое поле означает ANY: фильтр должен уметь искать «всё подряд»,
    иначе список правил нельзя просто открыть.
    """
    data = data or {}
    principal = str(data.get("principal") or "").strip()
    name = str(data.get("resource_name") or "").strip()
    host = str(data.get("host") or "").strip()

    return {
        "principal": validate_principal(principal) if principal else None,
        "host": host or None,
        "resource_type": _one_of(data.get("resource_type"),
                                 _RESOURCE_NAMES | {"ANY"}, "ANY",
                                 "типа ресурса"),
        "resource_name": name or None,
        "pattern_type": _one_of(data.get("pattern_type"),
                                _PATTERN_NAMES | {"ANY", "MATCH"}, "ANY",
                                "типа шаблона"),
        "operation": _one_of(data.get("operation"),
                             _OPERATION_NAMES | {"ANY"}, "ANY", "операции"),
        "permission": _one_of(data.get("permission"),
                              _PERMISSION_NAMES | {"ANY"}, "ANY",
                              "разрешения"),
    }


# «читателю» нужны права и на топик, и на консьюмер-группу; про группу
# забывают чаще всего, и потребитель падает уже в проде
PRESETS = {
    "reader": "чтение топика",
    "writer": "запись в топик",
    "full": "полный доступ к топику",
}


def expand_preset(name, data):
    """Шаблон → набор спецификаций, готовых к созданию."""
    preset = str(name or "").strip().lower()

    if preset not in PRESETS:
        raise ValueError("Неизвестный шаблон: {}".format(name))

    data = data or {}
    topic = str(data.get("topic") or "").strip()

    if not topic:
        raise ValueError("Укажите топик для шаблона")

    principal = data.get("principal")
    host = data.get("host")
    pattern = data.get("pattern_type")

    def topic_rule(operations):
        return build_acl_spec({
            "principal": principal, "host": host,
            "resource_type": "TOPIC", "resource_name": topic,
            "pattern_type": pattern, "operations": operations,
            "permission": "ALLOW",
        })

    if preset == "writer":
        return [topic_rule(["WRITE", "DESCRIBE"])]

    if preset == "full":
        return [topic_rule(["ALL"])]

    group = str(data.get("group") or "").strip() or "*"

    return [
        topic_rule(["READ", "DESCRIBE"]),
        build_acl_spec({
            "principal": principal, "host": host,
            "resource_type": "GROUP", "resource_name": group,
            "pattern_type": pattern, "operations": ["READ"],
            "permission": "ALLOW",
        }),
    ]


def _enum_name(value):
    return getattr(value, "name", None) or str(value)


def format_acl(acl):
    """Объект правила из библиотеки → плоский словарь со строками."""
    pattern = getattr(acl, "resource_pattern", None)

    return {
        "principal": getattr(acl, "principal", None),
        "host": getattr(acl, "host", None),
        "operation": _enum_name(getattr(acl, "operation", None)),
        "permission": _enum_name(getattr(acl, "permission_type", None)),
        "resource_type": _enum_name(getattr(pattern, "resource_type", None)),
        "resource_name": getattr(pattern, "resource_name", None),
        "pattern_type": _enum_name(getattr(pattern, "pattern_type", None)),
    }
