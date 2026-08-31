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
        return {"ok": False, "message": str(_fail(cluster, error)),
                "brokers": 0}
    finally:
        try:
            admin.close()
        except Exception:
            pass


def fetch_cluster_meta(cluster):
    """(описание кластера, список топиков) сырыми структурами библиотеки."""
    admin = open_admin(cluster)

    try:
        return admin.describe_cluster(), admin.describe_topics()
    except Exception as error:
        raise _fail(cluster, error)
    finally:
        try:
            admin.close()
        except Exception:
            pass


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
    except Exception as error:
        raise _fail(cluster, error)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
