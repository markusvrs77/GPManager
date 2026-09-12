# Kafka Manager, этап 1: подключения и обзор кластера

**Дата:** 2026-08-31
**Направление:** Data Flow → Kafka
**Статус:** утверждено, готово к планированию

## Цель

Дать в Opsentri вкладку Kafka, где видно состояние кластера: брокеры, топики,
партиции, репликация и объём данных. Этап 1 — фундамент: подключения к
кластерам и обзор. Управление топиками, консьюмер-группы и просмотр сообщений
идут отдельными этапами поверх этого же слоя.

## Условия

Выяснено у заказчика:

- на сервере с Opsentri есть pip — можно ставить Python-зависимости;
- кластер работает по **PLAINTEXT**, без авторизации;
- масштаб — **десятки** топиков и консьюмер-групп;
- опасные операции (удаление, сброс оффсетов) разрешены, но только с
  подтверждением и записью в журнал; в этапе 1 их ещё нет.

## Выбранный подход

Свой модуль на Python-клиенте `kafka-python` плюс срез в SQLite.

Отвергнуто:

- **встроить AKHQ / kafka-ui в iframe** — отдельный Java-сервис рядом, чужой
  интерфейс внутри Opsentri, никакой связи с расписаниями и журналом действий;
- **каждое действие через `job_manager`** — для десятков топиков список должен
  открываться мгновенно, а не заводить фоновую задачу.

`kafka-python` вместо `confluent-kafka`: чистый Python, ставится без
компилятора и бинарных wheel, админ-API покрывает все четыре этапа. На десятках
топиков разница в скорости незаметна.

## Порядок этапов

1. **Подключения и обзор кластера** — этот документ.
2. Консьюмер-группы и лаг.
3. Управление топиками: создание, удаление, retention, число партиций.
4. Просмотр и отправка сообщений.

Каждый этап получает свой спек и свой план.

## Архитектура

```
templates/kafka.html  static/js/kafka.js
            │
     kafka_routes.py            Flask Blueprint, в app.py одна строка
            │
   ┌────────┴─────────┐
modules/               modules/
kafka_clusters.py      kafka_overview.py     CRUD и сбор среза
   └────────┬─────────┘
            │
   modules/kafka_client.py      единственное место с import kafka
            │
          Kafka
```

Ключевая граница — `kafka_client.py`. Только он знает про библиотеку; всё
остальное работает со словарями. Смена библиотеки затрагивает один файл.

`kafka_routes.py` вынесен в Blueprint намеренно: `app.py` уже 3340 строк, и
класть туда ещё один раздел значит делать файл, который невозможно держать в
голове целиком.

## Данные

Новые таблицы создаются в `db.init_db()` рядом с остальными.

```sql
CREATE TABLE IF NOT EXISTS kafka_clusters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    bootstrap_servers TEXT NOT NULL,
    security_protocol TEXT NOT NULL DEFAULT 'PLAINTEXT',
    sasl_mechanism TEXT,
    sasl_username TEXT,
    sasl_password TEXT,
    ssl_cafile TEXT,
    ssl_certfile TEXT,
    ssl_keyfile TEXT,
    request_timeout_ms INTEGER NOT NULL DEFAULT 15000,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS kafka_snapshots (
    cluster_id INTEGER PRIMARY KEY,
    taken_at TEXT NOT NULL,
    payload BLOB NOT NULL,
    brokers_total INTEGER NOT NULL DEFAULT 0,
    topics_total INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kafka_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id INTEGER,
    action TEXT NOT NULL,
    target TEXT,
    details_json TEXT,
    result TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

Почему отдельная таблица, а не расширение `connections`: у Kafka нет базы,
пользователя и одного порта в том же смысле — есть список bootstrap-серверов.
Подмешивание дало бы десяток NULL-колонок и потребовало бы править все
существующие выборки из `connections`, включая gpcopy и гранты.

Поля SASL и SSL заводятся сразу, хотя сегодня кластер PLAINTEXT: в форме они
спрятаны под «Расширенные». Это дешевле, чем миграция позже.

`kafka_audit` пустует до этапа 3, но схема ставится сейчас по той же причине.

Даты везде — строки `YYYY-MM-DD HH:MM:SS`, как в остальном приложении
(`job_manager.now_str()`).

### Формат среза

`payload` — `zlib.compress(json.dumps(...).encode("utf-8"))`, тот же приём, что
в `modules/grants.py` (там 11.2 МБ сжались до 0.6 МБ).

```json
{
  "cluster_id": "MkU3OEVBNTcwNTJENDM2Qk",
  "controller_id": 1,
  "brokers": [
    {"id": 1, "host": "kfk1", "port": 9092, "rack": null}
  ],
  "topics": [
    {
      "name": "orders",
      "internal": false,
      "partitions": 6,
      "replication": 3,
      "messages": 1250340,
      "under_replicated": false,
      "parts": [
        {"p": 0, "leader": 1, "replicas": [1, 2, 3], "isr": [1, 2, 3],
         "begin": 0, "end": 208390}
      ]
    }
  ]
}
```

`messages` — сумма `end - begin` по партициям. Это не точное число сообщений
(компакция и удаление сегментов его занижают), поэтому в интерфейсе колонка
называется «сообщений (оценка)».

`under_replicated` — `true`, если хоть у одной партиции `len(isr) <
len(replicas)`.

## Модули

### `modules/kafka_client.py`

Единственный файл с `import kafka`. Публичный интерфейс:

- `KafkaUnavailable(Exception)` — своя ошибка с человеческим текстом;
- `library_available()` → `bool`, есть ли `kafka-python`;
- `client_kwargs(cluster)` → dict аргументов для клиентов
  (`bootstrap_servers`, `security_protocol`, таймауты, SASL/SSL при наличии);
- `open_admin(cluster)` → `KafkaAdminClient`;
- `open_consumer(cluster)` → `KafkaConsumer` без группы;
- `ping(cluster)` → `{"ok": bool, "message": str, "brokers": int}`.

Все исключения библиотеки перехватываются и превращаются в `KafkaUnavailable`
с текстом вида «Кластер недоступен: kfk1:9092 не ответил за 15 с». Наружу
никогда не летит `NoBrokersAvailable`.

### `modules/kafka_clusters.py`

CRUD по образцу `modules/connections.py`:

- `list_clusters()`, `get_cluster(cluster_id)`;
- `create_cluster(data)`, `update_cluster(cluster_id, data)`,
  `delete_cluster(cluster_id)` (каскадом чистит `kafka_snapshots`);
- `normalize_bootstrap(value)` — чистая функция: строка или список →
  `"kfk1:9092,kfk2:9092"`; убирает пробелы и пустые элементы, дописывает порт
  9092, если не указан, схлопывает дубли с сохранением порядка.

Пароль `sasl_password` хранится так же, как пароли БД, и никогда не
возвращается наружу в списках — только флаг «задан».

### `modules/kafka_overview.py`

- `build_overview(metadata, offsets)` — **чистая функция**: объекты метаданных
  и словарь границ оффсетов → структура среза выше. Тестируется без брокера.
- `collect_overview(cluster_id, force=False)` — без `force` отдаёт срез из БД,
  с `force` идёт в кластер, собирает и сохраняет.
- `load_snapshot(cluster_id)` / `save_snapshot(cluster_id, data)` /
  `empty_overview()` — по образцу снапшотов в `modules/grants.py`.

Сбор из кластера: `admin.describe_cluster()` для брокеров и контроллера,
`consumer.topics()` + `consumer.partitions_for_topic()` для топиков,
`consumer.beginning_offsets()` / `consumer.end_offsets()` для границ — одним
вызовом на все партиции сразу, а не по партиции.

### `kafka_routes.py`

Blueprint `kafka_bp`:

| Метод | Путь | Назначение |
|-------|------|-----------|
| GET | `/kafka` | страница |
| GET | `/api/kafka/clusters` | список подключений |
| POST | `/api/kafka/clusters` | создать |
| PUT | `/api/kafka/clusters/<id>` | изменить |
| DELETE | `/api/kafka/clusters/<id>` | удалить |
| POST | `/api/kafka/clusters/<id>/ping` | проверить связь |
| GET | `/api/kafka/clusters/<id>/overview` | срез из БД |
| POST | `/api/kafka/clusters/<id>/overview/refresh` | опросить кластер |

Единственная строка в `app.py`: `app.register_blueprint(kafka_bp)`.

## Экран

`templates/kafka.html`, стиль и разметка — как у `grants.html`.

Шапка: выбор кластера, кнопка «Проверить связь», справа бейдж «Срез от
2026-08-31 19:04» и кнопка «Обновить срез». Автообновления нет ни на одном
экране — кластер опрашивается только по нажатию.

Карточка «Брокеры»: id, host:port, rack, отметка контроллера.

Карточка «Топики»: поиск по имени, галка «показывать системные» (по умолчанию
выключена, прячет `__consumer_offsets` и прочие `internal`), колонки — имя,
партиций, RF, сообщений (оценка). Топик с `under_replicated` подсвечивается
цветом `--crit`. Клик по строке разворачивает партиции: номер, лидер, реплики,
ISR, границы оффсетов.

Список топиков рендерится через `window.gpKeepScroll`, чтобы разворачивание
строки не сбрасывало прокрутку — то же требование, что и в синхронизации.

Управление подключениями — модальное окно с полями «Имя» и «Bootstrap-серверы»;
протокол, SASL и SSL спрятаны под «Расширенные».

Пустое состояние: «Среза ещё нет — нажмите «Обновить срез»», по образцу
`showNoSnapshot()` в грантах.

Меню: заглушка «Kafka скоро» в `templates/base.html` (блок `secBody-flow`)
заменяется ссылкой на `/kafka`.

## Ошибки

| Ситуация | Поведение |
|----------|-----------|
| `kafka-python` не установлен | Страница открывается, сверху баннер с командой `pip install -r requirements.txt` и напоминанием перезапустить app.py. Кнопка обновления заблокирована. |
| Кластер недоступен | API отвечает 502 с текстом ошибки, в интерфейсе красный баннер с адресом и таймаутом. Прежний срез остаётся на экране — его не затирают. |
| Кластер отвечает, но топиков нет | Обычное пустое состояние, не ошибка. |
| Срез не сохранился | Данные показываются, снизу предупреждение «Срез не сохранён» — обзор важнее кэша. |

Таймаут по умолчанию 15 секунд (`request_timeout_ms`), настраивается на
подключении.

## Тесты

Без работающего брокера — все на чистых функциях и подменах:

1. `build_overview` на фикстурах метаданных: считает `messages`, находит
   under-replicated, помечает `internal`.
2. `normalize_bootstrap`: дописывание порта, обрезка пробелов, схлопывание
   дублей, отказ на пустой строке.
3. Срез: `save_snapshot` → `load_snapshot` возвращает то же самое; пустой срез
   даёт `empty_overview()`; удаление кластера чистит срез.
4. CRUD кластеров: создание, изменение, удаление, пароль не утекает в список.
5. `client_kwargs`: PLAINTEXT не тянет SASL-поля; при заданном SASL они
   появляются.
6. Роуты через `flask.test_client` с подменённым `collect_overview`: `/kafka`
   отдаёт 200, `overview` возвращает срез, `refresh` зовёт сбор с `force=True`,
   недоступный кластер даёт 502 с текстом ошибки, а не трейс.

`kafka-python` в тестовом окружении не нужен: `library_available()` в тестах
подменяется, а `kafka_client` импортирует библиотеку лениво внутри функций.

## Зависимости

В `requirements.txt` добавляется `kafka-python>=2.0.2`. Импорт ленивый, поэтому
приложение стартует и без неё — не открывается только вкладка Kafka.

## Развёртывание

`git pull`, `pip install -r requirements.txt`, ручной перезапуск app.py на
сервере, Ctrl+F5 в браузере.
