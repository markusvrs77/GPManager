"""
Управление пользователями: кого завели, что ему можно, куда пускают.

Отдельным Blueprint по той же причине, что и Kafka: app.py уже
разросся. Здесь же сосредоточены запреты, которые нельзя доверить
интерфейсу — их обходит любой, кто умеет отправить POST руками.
"""

from flask import Blueprint, jsonify, render_template, request

import modules.security as sec
from modules.connections import list_connections
from modules.web_auth import current_user


users_bp = Blueprint("users", __name__)


def _last_active_admin(user_id):
    """
    Он ли единственный живой администратор.

    Понижение, отключение или удаление последнего администратора
    оставляет систему без единого входа в управление людьми — дальше
    только правка SQLite руками.
    """
    admins = [
        u for u in sec.list_users()
        if u["role"] == "admin" and u["is_active"]
    ]

    return len(admins) == 1 and admins[0]["id"] == int(user_id)


def _user_view(user):
    """Пользователь в том виде, в каком его показывает страница."""
    allowed = sec.allowed_connection_ids(user)

    return {
        "id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "is_active": user["is_active"],
        "must_change_password": user["must_change_password"],
        "created_at": user.get("created_at"),
        "overrides": sec.get_overrides(user["id"]),
        "capabilities": sorted(sec.effective_capabilities(user)),
        # None у администратора означает «все кластеры»
        "connection_ids": None if allowed is None else sorted(allowed),
    }


@users_bp.route("/users")
def users_page():
    return render_template(
        "users.html",
        users=[_user_view(u) for u in sec.list_users()],
        capabilities=sec.CAPABILITIES,
        roles=sec.ROLES,
        role_defaults={r: sorted(c) for r, c in sec.ROLE_DEFAULTS.items()},
        # список не фильтруется по правам: чтобы выдать кластер, его
        # надо видеть, а страницу открывает только users.manage
        connections=[
            {
                "id": c["id"],
                "name": c["name"],
                "host": c.get("host"),
                "database": c.get("database_name"),
                "db_type": c.get("db_type"),
            }
            for c in list_connections()
        ],
        me=current_user(),
        min_password_length=sec.MIN_PASSWORD_LENGTH,
    )


@users_bp.route("/api/users", methods=["GET", "POST"])
def api_users():
    if request.method == "GET":
        return jsonify({
            "ok": True,
            "users": [_user_view(u) for u in sec.list_users()],
        })

    data = request.get_json(silent=True) or {}

    try:
        sec.check_password_policy(data.get("password") or "")
        user_id = sec.create_user(
            data.get("username"),
            data.get("password"),
            data.get("role") or "viewer",
            # пароль придумал администратор, значит он его и знает —
            # первый вход обязан его сменить
            must_change_password=True,
        )
        _apply_grants(user_id, data)
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    return jsonify({"ok": True, "user": _user_view(sec.get_user(user_id))})


@users_bp.route("/api/users/<int:user_id>", methods=["PUT", "DELETE"])
def api_user_item(user_id):
    user = sec.get_user(user_id)

    if user is None:
        return jsonify({"ok": False, "message": "Пользователь не найден"}), 404

    me = current_user()

    if request.method == "DELETE":
        if me and me["id"] == user_id:
            return jsonify({
                "ok": False, "message": "Нельзя удалить самого себя",
            }), 400

        if _last_active_admin(user_id):
            return jsonify({
                "ok": False, "message": "Это последний администратор",
            }), 400

        sec.delete_user(user_id)
        return jsonify({"ok": True})

    data = request.get_json(silent=True) or {}

    role = data.get("role")
    active = data.get("is_active")

    losing_admin = (
        (role is not None and role != "admin")
        or (active is not None and not active)
    )

    if losing_admin and _last_active_admin(user_id):
        return jsonify({
            "ok": False,
            "message": "Это последний администратор: сначала назначьте другого",
        }), 400

    try:
        if role is not None and role != user["role"]:
            sec.set_user_role(user_id, role)

        if active is not None and bool(active) != user["is_active"]:
            sec.set_user_active(user_id, bool(active))

        _apply_grants(user_id, data)
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    return jsonify({"ok": True, "user": _user_view(sec.get_user(user_id))})


@users_bp.route("/api/users/<int:user_id>/password", methods=["POST"])
def api_user_password(user_id):
    if sec.get_user(user_id) is None:
        return jsonify({"ok": False, "message": "Пользователь не найден"}), 404

    data = request.get_json(silent=True) or {}
    password = data.get("password") or ""

    try:
        sec.check_password_policy(password)
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    # администратор задал пароль — значит, знает его; владелец обязан
    # сменить при первом же входе
    sec.set_user_password(user_id, password, must_change=True)

    return jsonify({"ok": True})


def _apply_grants(user_id, data):
    """Точечные права и список кластеров — что пришло, то и ставим."""
    if "overrides" in data:
        overrides = data.get("overrides") or {}

        # заданное в прошлый раз, но не присланное сейчас, снимается:
        # иначе снятую галочку невозможно было бы вернуть под роль
        for capability in sec.get_overrides(user_id):
            if capability not in overrides:
                sec.set_override(user_id, capability, None)

        for capability, allowed in overrides.items():
            sec.set_override(user_id, capability,
                             None if allowed is None else bool(allowed))

    if "connection_ids" in data:
        sec.set_user_connections(user_id, data.get("connection_ids") or [])
