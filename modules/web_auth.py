"""
Охрана маршрутов: кто вошёл, что ему можно и к каким кластерам.

Права проверяются не декораторами на каждом маршруте, а одним хуком
before_request по карте POLICY. Причина простая: маршрутов сто двадцать,
и декоратор, который забыли поставить, выглядит точно так же, как
маршрут, которому права не нужны. Карта же проверяется тестом —
незакрытый маршрут роняет сборку, а не уходит в production открытым.

Поэтому и умолчание здесь запрещающее: endpoint, которого нет в POLICY,
отдаёт 403. Новый маршрут придётся внести в карту осознанно.
"""

from flask import (
    g, has_request_context, jsonify, redirect, render_template, request, url_for,
)

import modules.security as sec


SESSION_COOKIE = "opsentri_session"

# Маркеры вместо кода возможности.
PUBLIC = "__public__"                # до входа: форма входа, первичная настройка
AUTHENTICATED = "__authenticated__"  # вошёл — и достаточно


# ------------------------------------------------------------
# Карта «endpoint -> что требуется»
# ------------------------------------------------------------
#
# Значение — либо код возможности на все методы, либо словарь
# {метод: возможность}, когда GET и POST по одному адресу стоят разного.

POLICY = {
    # -------------------------------------------------- служебное
    "static": PUBLIC,
    "auth.login_page": PUBLIC,
    "auth.login_submit": PUBLIC,
    "auth.setup_page": PUBLIC,
    "auth.setup_submit": PUBLIC,
    "auth.logout": AUTHENTICATED,
    "auth.password_page": AUTHENTICATED,
    "auth.password_submit": AUTHENTICATED,

    # -------------------------------------------------- пользователи
    "users.users_page": "users.manage",
    "users.api_users": "users.manage",
    "users.api_user_item": "users.manage",
    "users.api_user_password": "users.manage",

    # -------------------------------------------------- dashboard
    "dashboard_page": "dashboard.view",
    "api_dashboard_session_limits": "dashboard.view",

    # -------------------------------------------------- задачи
    "api_get_active_jobs": "jobs.view",
    "api_jobs_recent": "jobs.view",
    "api_get_job": "jobs.view",
    "api_job_status": "jobs.view",
    "api_get_job_items": "jobs.view",
    "api_job_log": "jobs.view",
    "api_job_log_download": "jobs.view",
    "api_stop_job": "jobs.stop",

    # -------------------------------------------------- подключения
    "connections_page": "connections.view",
    "api_connections": "connections.view",
    "api_test_connection": "connections.view",
    "add_connection": "connections.edit",
    "remove_connection": "connections.edit",

    # -------------------------------------------------- объекты
    "objects_page": "objects.view",
    "api_objects_tree": "objects.view",

    # -------------------------------------------------- здоровье
    "health_page": "health.view",
    "api_health_overview": "health.view",

    # -------------------------------------------------- maintenance
    "maintenance_page": "maintenance.view",
    "skew_page": "maintenance.view",
    "reorganize_page": "maintenance.view",
    "api_skew_results": "maintenance.view",
    "api_get_job_skew_results": "maintenance.view",
    "api_get_latest_skew_job": "maintenance.view",
    "api_get_skew_result_segments": "maintenance.view",
    "api_export_skew_job_excel": "maintenance.view",
    "api_reorganize_recommendation": "maintenance.view",
    "api_skew_analyze": "maintenance.run",
    "api_skew_start": "maintenance.run",
    "api_start_reorganize": "maintenance.run",
    # меняет распределение таблицы — не просмотр
    "api_reorganize_apply_distribution": "maintenance.run",

    # -------------------------------------------------- vacuum
    "vacuum_page": "vacuum.view",
    "api_vacuum_advisor": "vacuum.view",
    "api_vacuum_start": "vacuum.run",

    # -------------------------------------------------- синхронизация
    "gpcopy_page": "sync.view",
    "api_gpcopy_precheck": "sync.view",
    "api_gpcopy_date_columns": "sync.view",
    "api_gpcopy_preview_date_json": "sync.view",
    "api_gpcopy_strategies": "sync.view",
    "api_gpcopy_window_preview": "sync.view",
    "api_gpcopy_increment_preview": "sync.view",
    "api_gpcopy_partition_diff_preview": "sync.view",
    "api_gpcopy_partition_diff_preview_bulk": "sync.view",
    "api_gpcopy_sync_preview": "sync.view",
    "api_gpcopy_start": "sync.run",
    "api_gpcopy_start_date": "sync.run",
    "api_gpcopy_retry_failed": "sync.run",
    "api_gpcopy_increment_start": "sync.run",
    "api_gpcopy_partition_diff_start": "sync.run",
    "api_gpcopy_sync_apply": "sync.run",
    # правка структуры приёмника: CREATE, ALTER, DROP
    "api_gpcopy_add_columns": "sync.run",
    "api_gpcopy_create_tables": "sync.run",
    "api_gpcopy_rename_columns": "sync.run",
    "api_gpcopy_recreate_tables": "sync.run",
    "api_gpcopy_fix_deps": "sync.run",

    # -------------------------------------------------- каталог
    "api_catalog": "sync.view",
    "api_catalog_search": "sync.view",
    "api_catalog_schema_tables": "sync.view",
    "api_catalog_expand_mask": "sync.view",
    "api_catalog_resolve_list": "sync.view",
    "api_catalog_resolve_columns": "sync.view",
    "api_catalog_primary_keys": "sync.view",
    "api_catalog_resolve_keys": "sync.view",
    "api_catalog_compute_unique": "sync.view",
    "api_catalog_sync_keys": "sync.run",
    "api_table_sets": {"GET": "sync.view", "POST": "sync.run"},
    "api_table_set_item": {"GET": "sync.view", "DELETE": "sync.run"},

    # -------------------------------------------------- резервные копии
    "backups_page": "backups.view",
    "api_backup_list": "backups.view",
    "pg_backups_page": "backups.view",
    "api_pg_databases": "backups.view",
    "api_backup_start": "backups.run",
    "api_backup_restore": "backups.run",
    "api_backup_sync_disk": "backups.run",
    "api_backup_report": "backups.run",
    "api_backup_delete": "backups.run",
    "api_pg_backup_start": "backups.run",
    "api_pg_backup_restore": "backups.run",

    # -------------------------------------------------- гранты
    "grants_page": "grants.view",
    "api_grants_overview": "grants.view",
    "api_grants_schema_matrix": "grants.view",

    # -------------------------------------------------- расписания
    "schedules_page": "schedules.view",
    "api_schedule_runs": "schedules.view",
    "api_schedules_preview": "schedules.view",
    "api_schedules": {"GET": "schedules.view", "POST": "schedules.edit"},
    "api_schedule_item": {"PUT": "schedules.edit", "DELETE": "schedules.edit"},
    "api_schedule_toggle": "schedules.edit",
    "api_schedule_run_now": "schedules.edit",
    "api_notification_channels": {
        "GET": "schedules.view", "POST": "schedules.edit",
    },
    "api_notification_channel_item": {
        "PUT": "schedules.edit", "DELETE": "schedules.edit",
    },
    "api_notification_channel_test": "schedules.edit",

    # -------------------------------------------------- kafka
    "kafka.kafka_page": "kafka.view",
    "kafka.kafka_connections_page": "kafka.view",
    "kafka.kafka_groups_page": "kafka.view",
    "kafka.kafka_messages_page": "kafka.view",
    "kafka.kafka_acl_page": "kafka.view",
    "kafka.api_kafka_clusters": "kafka.view",
    "kafka.api_kafka_ping": "kafka.view",
    "kafka.api_kafka_overview": "kafka.view",
    "kafka.api_kafka_overview_refresh": "kafka.view",
    "kafka.api_kafka_groups": "kafka.view",
    "kafka.api_kafka_groups_refresh": "kafka.view",
    "kafka.api_kafka_audit": "kafka.view",
    "kafka.api_kafka_topic_configs": "kafka.view",
    "kafka.api_kafka_messages_read": "kafka.view",
    "kafka.api_kafka_acls_list": "kafka.view",
    "kafka.api_kafka_cluster_create": "kafka.edit",
    "kafka.api_kafka_cluster_update": "kafka.edit",
    "kafka.api_kafka_cluster_delete": "kafka.edit",
    "kafka.api_kafka_group_delete": "kafka.edit",
    "kafka.api_kafka_group_reset": "kafka.edit",
    "kafka.api_kafka_topic_create": "kafka.edit",
    "kafka.api_kafka_topic_delete": "kafka.edit",
    "kafka.api_kafka_topic_configs_update": "kafka.edit",
    "kafka.api_kafka_topic_partitions": "kafka.edit",
    "kafka.api_kafka_message_send": "kafka.edit",
    "kafka.api_kafka_acl_grant": "kafka.edit",
    "kafka.api_kafka_acl_revoke": "kafka.edit",
}


def required_capability(endpoint, method):
    """
    Что нужно для этого запроса, или None — если маршрут закрыт наглухо.

    None отдаётся и для незнакомого endpoint, и для метода, не описанного
    в словаре: умолчание запрещающее.
    """
    rule = POLICY.get(endpoint)

    if rule is None:
        return None

    if isinstance(rule, dict):
        return rule.get((method or "").upper())

    return rule


# ------------------------------------------------------------
# Текущий пользователь
# ------------------------------------------------------------

def current_user():
    # вне запроса пользователя нет: планировщик и консоль работают сами
    # по себе, и обращение к g там просто упало бы
    if not has_request_context():
        return None

    return getattr(g, "user", None)


def current_capabilities():
    """Возможности текущего запроса; считаются один раз на запрос."""
    caps = getattr(g, "user_caps", None)

    if caps is None:
        caps = sec.effective_capabilities(current_user())
        g.user_caps = caps

    return caps


def user_can(capability):
    return capability in current_capabilities()


# ------------------------------------------------------------
# Куда человеку можно
# ------------------------------------------------------------

# (возможность, адрес, название). Порядок задаёт и точку входа: человек
# без Dashboard попадает в первый открытый ему раздел, а не в отказ.
SECTIONS = (
    ("dashboard.view", "/", "Dashboard"),
    ("health.view", "/health", "Здоровье БД"),
    ("connections.view", "/connections", "Подключения"),
    ("objects.view", "/objects", "Объекты"),
    ("sync.view", "/gpcopy", "Синхронизация"),
    ("maintenance.view", "/maintenance", "Maintenance"),
    ("vacuum.view", "/vacuum", "Vacuum / Analyze"),
    ("backups.view", "/backups", "Резервные копии"),
    ("grants.view", "/grants", "Гранты"),
    ("schedules.view", "/schedules", "Расписания"),
    ("kafka.view", "/kafka", "Kafka"),
    ("users.manage", "/users", "Пользователи"),
)

# Какие возможности оживляют набор инструментов в боковом меню. Набор,
# из которого не открыть ни одной страницы, показывать незачем.
TOOLKIT_CAPS = {
    "gp": ("dashboard.view", "health.view", "connections.view",
           "objects.view", "sync.view", "maintenance.view", "vacuum.view",
           "backups.view", "grants.view", "schedules.view"),
    "pg": ("connections.view", "sync.view", "backups.view",
           "schedules.view"),
    "kafka": ("kafka.view",),
}


def available_sections():
    """Разделы, открытые текущему пользователю."""
    caps = current_capabilities()

    return [(path, label) for code, path, label in SECTIONS if code in caps]


def landing_path():
    """
    Первая страница, куда человека можно пустить, или None.

    Нужна потому, что «/» — общая точка входа, а Dashboard есть не у
    всех: без этого пользователь с одной только Kafka упирался в отказ,
    из которого единственная кнопка вела обратно в тот же отказ.
    """
    caps = current_capabilities()

    for code, path, _label in SECTIONS:
        if code in caps:
            return path

    return None


def can_toolkit(name):
    caps = current_capabilities()

    return any(code in caps for code in TOOLKIT_CAPS.get(name, ()))


def scope_connections(connections):
    """
    Оставляет только те кластеры, что выданы пользователю.

    Через это проходят все списки подключений в интерфейсе: если PROD не
    выдан, он не появится ни в выпадающем списке, ни в JSON. Скрытие не
    заменяет проверку на входе в маршрут — оно избавляет от соблазна её
    обойти подстановкой чужого id.
    """
    # вне запроса ограничивать некого — иначе планировщик остался бы
    # вовсе без кластеров
    if not has_request_context():
        return connections

    allowed = sec.allowed_connection_ids(current_user())

    if allowed is None:
        return connections

    return [c for c in connections if c.get("id") in allowed]


# ------------------------------------------------------------
# Проверка доступа к кластеру в теле запроса
# ------------------------------------------------------------

# Под этими именами id кластера приходит в маршруты. Kafka сюда не
# входит: там cluster_id — это своя таблица, а не подключение Greenplum.
CONNECTION_KEYS = (
    "connection_id",
    "source_connection_id",
    "dest_connection_id",
    "destination_connection_id",
    "target_connection_id",
)


def requested_connection_ids():
    """Все id кластеров, названные в запросе: путь, строка запроса, тело."""
    found = []

    for source in (request.view_args or {}, request.args):
        for key in CONNECTION_KEYS:
            value = source.get(key)
            if value not in (None, ""):
                found.append(value)

    if request.is_json:
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            for key in CONNECTION_KEYS:
                value = body.get(key)
                if value not in (None, ""):
                    found.append(value)

    if request.form:
        for key in CONNECTION_KEYS:
            value = request.form.get(key)
            if value not in (None, ""):
                found.append(value)

    return found


# ------------------------------------------------------------
# Хук
# ------------------------------------------------------------

def _wants_json():
    """API отвечает кодом, страница — редиректом на форму входа."""
    if request.path.startswith("/api/"):
        return True

    return request.accept_mimetypes.best == "application/json"


def _deny(status, message, login_redirect=False):
    if _wants_json():
        return jsonify({"ok": False, "message": message}), status

    if login_redirect:
        return redirect(url_for("auth.login_page", next=request.path))

    # render_template, а не jinja_env напрямую: странице отказа нужны
    # и текущий пользователь, и список открытых ему разделов
    return render_template(
        "forbidden.html", message=message, sections=available_sections(),
    ), status


def install_auth(app):
    """Вешает охрану на приложение. Вызывается один раз при сборке app."""

    @app.before_request
    def _guard():
        endpoint = request.endpoint

        # несуществующий адрес — пусть Flask сам отдаст 404
        if endpoint is None:
            return None

        g.user = sec.resolve_session(request.cookies.get(SESSION_COOKIE))

        required = required_capability(endpoint, request.method)

        if required == PUBLIC:
            return None

        # свежая установка: пока нет ни одного администратора, вести
        # некуда, кроме первичной настройки
        if not sec.admin_exists():
            if _wants_json():
                return jsonify({
                    "ok": False,
                    "message": "Приложение не настроено: нет администратора",
                }), 503
            return redirect(url_for("auth.setup_page"))

        if g.user is None:
            return _deny(401, "Требуется вход", login_redirect=True)

        # пароль выдан администратором — до смены дальше не пускаем
        if g.user["must_change_password"] and endpoint not in (
                "auth.password_page", "auth.password_submit", "auth.logout"):
            if _wants_json():
                return jsonify({
                    "ok": False, "message": "Требуется смена пароля",
                }), 403
            return redirect(url_for("auth.password_page"))

        if required == AUTHENTICATED:
            return None

        if required is None:
            # маршрута нет в карте прав — закрыт до тех пор, пока его
            # осознанно туда не внесут
            return _deny(403, "Доступ к этому разделу не настроен")

        if not user_can(required):
            # «/» открывают все и всегда — значит, он обязан вести туда,
            # куда этому человеку можно, а не в отказ
            if endpoint == "dashboard_page" and not _wants_json():
                target = landing_path()

                if target and target != request.path:
                    return redirect(target)

            return _deny(403, "Недостаточно прав: {}".format(
                sec.capability_label(required)))

        for connection_id in requested_connection_ids():
            if not sec.can_use_connection(g.user, connection_id):
                return _deny(403, "Нет доступа к этому кластеру")

        return None

    @app.context_processor
    def _inject_user():
        """Шаблоны спрашивают can('sync.run'), а не разбирают роли сами."""
        return {
            "current_user": current_user(),
            "can": user_can,
            "can_toolkit": can_toolkit,
        }

    return app
