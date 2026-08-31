# -*- coding: utf-8 -*-
"""
Единственное место, знающее про библиотеку kafka-python.

Всё остальное приложение работает со словарями и получает понятные ошибки,
а не NoBrokersAvailable из недр клиента. Импорт ленивый: без установленной
библиотеки приложение запускается, не открывается только вкладка Kafka.
"""

DEFAULT_TIMEOUT_MS = 15000

INSTALL_HINT = (
    "Библиотека kafka-python не установлена. На сервере: "
    "pip install -r requirements.txt и перезапуск app.py"
)


class KafkaUnavailable(Exception):
    """Кластер недоступен или библиотеки нет — с текстом для человека."""


def _import_kafka():
    """Вынесено отдельно, чтобы тесты могли подменить импорт."""
    import kafka

    return kafka


def library_available():
    try:
        _import_kafka()
    except ImportError:
        return False

    return True


def _servers(cluster):
    raw = str(cluster.get("bootstrap_servers") or "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def client_kwargs(cluster):
    """Аргументы, общие для админ-клиента и консьюмера."""
    timeout = int(cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS)
    protocol = str(cluster.get("security_protocol") or "PLAINTEXT").upper()

    kwargs = {
        "bootstrap_servers": _servers(cluster),
        "security_protocol": protocol,
        "request_timeout_ms": timeout,
        # без него клиент ждёт своих 30 секунд независимо от
        # request_timeout_ms — вкладка висела бы вдвое дольше обещанного
        "bootstrap_timeout_ms": timeout,
        "client_id": "opsentri",
    }

    if protocol.startswith("SASL"):
        kwargs["sasl_mechanism"] = cluster.get("sasl_mechanism") or "PLAIN"
        kwargs["sasl_plain_username"] = cluster.get("sasl_username")
        kwargs["sasl_plain_password"] = cluster.get("sasl_password")

    for field in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
        if cluster.get(field):
            kwargs[field] = cluster.get(field)

    return kwargs


def _fail(cluster, error):
    timeout = int(cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS)

    return KafkaUnavailable(
        "Кластер недоступен: {} не ответил за {} с ({})".format(
            ", ".join(_servers(cluster)) or "адрес не задан",
            round(timeout / 1000.0),
            error,
        )
    )


def _request_failed(cluster, error):
    """Связь есть, но запрос не удался — про таймаут врать не надо."""
    return KafkaUnavailable(
        "Кластер {} ответил ошибкой: {}".format(
            ", ".join(_servers(cluster)) or "адрес не задан", error
        )
    )


def open_admin(cluster):
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    try:
        return kafka.KafkaAdminClient(**client_kwargs(cluster))
    except Exception as error:
        raise _fail(cluster, error)


def open_consumer(cluster):
    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    try:
        return kafka.KafkaConsumer(
            enable_auto_commit=False,
            consumer_timeout_ms=int(
                cluster.get("request_timeout_ms") or DEFAULT_TIMEOUT_MS),
            **client_kwargs(cluster)
        )
    except Exception as error:
        raise _fail(cluster, error)


def ping(cluster):
    """Проверка связи для кнопки в интерфейсе. Никогда не бросает."""
    try:
        admin = open_admin(cluster)
    except KafkaUnavailable as error:
        return {"ok": False, "message": str(error), "brokers": 0}

    try:
        meta = admin.describe_cluster()
        brokers = len(meta.get("brokers") or [])

        return {
            "ok": True,
            "message": "Связь есть, брокеров: {}".format(brokers),
            "brokers": brokers,
        }
    except Exception as error:
        return {"ok": False, "message": str(_request_failed(cluster, error)),
                "brokers": 0}
    finally:
        try:
            admin.close()
        except Exception:
            pass


def _close(client):
    try:
        if client is not None:
            client.close()
    except Exception:
        pass


def fetch_cluster_meta(cluster):
    """(описание кластера, список топиков) сырыми структурами библиотеки."""
    admin = open_admin(cluster)
    consumer = None

    try:
        cluster_meta = admin.describe_cluster()

        # имена топиков спрашиваем у консьюмера: admin.list_topics() и
        # describe_topics() без аргумента шлют MetadataRequest с topics=None,
        # а часть брокеров такой запрос отвергает —
        # «All topics must not be None»
        consumer = open_consumer(cluster)
        names = sorted(consumer.topics() or [])

        topics_meta = admin.describe_topics(names) if names else []

        # 3.x отдаёт весь ответ метаданных, 2.x — сразу список топиков
        if isinstance(topics_meta, dict):
            topics_meta = topics_meta.get("topics") or []

        return cluster_meta, topics_meta
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)
        _close(consumer)


def fetch_offsets(cluster, pairs):
    """
    Границы оффсетов по списку [(topic, partition), ...].

    Спрашиваем одним вызовом на все партиции: по одной это десятки
    round-trip'ов даже на маленьком кластере.
    """
    if not pairs:
        return {}, {}

    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    consumer = open_consumer(cluster)

    try:
        tps = [kafka.TopicPartition(topic, part) for topic, part in pairs]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)

        return (
            {(tp.topic, tp.partition): int(v or 0)
             for tp, v in begin.items()},
            {(tp.topic, tp.partition): int(v or 0) for tp, v in end.items()},
        )
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(consumer)


def fetch_groups(cluster):
    """
    Описания всех консьюмер-групп кластера.

    list_groups даёт только имена и состояние; участники и протокол
    приходят из describe_groups.
    """
    admin = open_admin(cluster)

    try:
        listed = admin.list_groups() or []
        ids = [g.get("group_id") for g in listed if g.get("group_id")]

        if not ids:
            return []

        described = admin.describe_groups(ids) or {}
        groups = []

        for group_id in ids:
            row = described.get(group_id)

            if row:
                groups.append(dict(row))
                continue

            # брокер не описал группу — показываем хотя бы состояние
            base = [g for g in listed if g.get("group_id") == group_id][0]
            groups.append({
                "group_id": group_id,
                "group_state": base.get("group_state"),
                "protocol_data": base.get("protocol_type"),
                "members": [],
            })

        return groups
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def fetch_group_offsets(cluster, group_ids):
    """
    {(группа, топик, партиция): оффсет или None}.

    Kafka отдаёт -1, когда коммита по партиции не было; наружу это
    уходит как None, иначе «не читали» превратится в «нулевой оффсет»
    и лаг посчитается неверно.
    """
    if not group_ids:
        return {}

    admin = open_admin(cluster)

    try:
        specs = {}

        for group_id in group_ids:
            specs[group_id] = None

        answer = admin.list_group_offsets(specs) or {}
        out = {}

        for group_id, partitions in answer.items():
            for tp, meta in (partitions or {}).items():
                offset = getattr(meta, "offset", meta)
                offset = int(offset) if offset is not None else None

                if offset is not None and offset < 0:
                    offset = None

                out[(group_id, tp.topic, tp.partition)] = offset

        return out
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def _offset_value(mode, value):
    """(режим, значение) -> то, что понимает reset_group_offsets."""
    from kafka.admin import OffsetSpec, OffsetTimestamp

    if mode == "earliest":
        return OffsetSpec.EARLIEST

    if mode == "latest":
        return OffsetSpec.LATEST

    if mode == "timestamp":
        return OffsetTimestamp(int(value))

    raise KafkaUnavailable("Неизвестный режим сброса: {}".format(mode))


def reset_offsets(cluster, group_id, specs):
    """
    specs — {(топик, партиция): (режим, значение)} из build_reset_specs.
    Возвращает {(топик, партиция): текст ошибки или None}.
    """
    if not specs:
        return {}

    try:
        kafka = _import_kafka()
    except ImportError:
        raise KafkaUnavailable(INSTALL_HINT)

    admin = open_admin(cluster)

    try:
        request = {}

        for (topic, part), (mode, value) in specs.items():
            request[kafka.TopicPartition(topic, part)] = _offset_value(
                mode, value)

        answer = admin.reset_group_offsets(group_id, request) or {}
        out = {}

        for tp, row in answer.items():
            error = (row or {}).get("error")
            out[(tp.topic, tp.partition)] = str(error) if error else None

        return out
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)


def delete_group(cluster, group_id):
    admin = open_admin(cluster)

    try:
        admin.delete_groups([group_id])
    except KafkaUnavailable:
        raise
    except Exception as error:
        raise _request_failed(cluster, error)
    finally:
        _close(admin)
