"""
Вход, выход, первичная настройка и смена своего пароля.

Самостоятельной регистрации нет: учётные записи заводит администратор.
Инструмент ходит в кластеры под сохранёнными учётными данными — форма
«зарегистрироваться» означала бы здесь «выдать себе доступ к PROD».
"""

from flask import (
    Blueprint, redirect, render_template, request, url_for,
)

import config
import modules.security as sec
from modules.web_auth import SESSION_COOKIE, current_user


auth_bp = Blueprint("auth", __name__)


def _safe_next(value):
    """
    Куда вернуть после входа.

    Пускаем только свои пути: адрес вида //evil.example превратил бы
    форму входа в открытый редирект и удобную заготовку для фишинга.
    """
    if not value or not value.startswith("/") or value.startswith("//"):
        return "/"

    return value


def _set_session_cookie(response, token):
    response.set_cookie(
        SESSION_COOKIE, token,
        httponly=True,           # недоступна из JavaScript
        samesite="Lax",          # чужой сайт не отправит её своим POST
        secure=config.COOKIE_SECURE,
        path="/",
        max_age=sec.SESSION_TTL_HOURS * 3600,
    )
    return response


# ------------------------------------------------------------
# Первичная настройка
# ------------------------------------------------------------

@auth_bp.route("/setup")
def setup_page():
    if sec.admin_exists():
        return redirect(url_for("auth.login_page"))

    return render_template("setup.html")


@auth_bp.route("/setup", methods=["POST"])
def setup_submit():
    # окно открыто ровно до появления первого администратора
    if sec.admin_exists():
        return redirect(url_for("auth.login_page"))

    username = request.form.get("username") or ""
    password = request.form.get("password") or ""
    repeat = request.form.get("password2") or ""

    try:
        if password != repeat:
            raise ValueError("Пароли не совпадают")

        sec.check_password_policy(password)
        user_id = sec.create_user(username, password, "admin",
                                  must_change_password=False)
    except ValueError as e:
        return render_template("setup.html", error=str(e),
                               username=username), 400

    token = sec.create_session(user_id, ip=request.remote_addr)
    return _set_session_cookie(redirect("/"), token)


# ------------------------------------------------------------
# Вход и выход
# ------------------------------------------------------------

@auth_bp.route("/login")
def login_page():
    if current_user() is not None:
        return redirect(_safe_next(request.args.get("next")))

    if not sec.admin_exists():
        return redirect(url_for("auth.setup_page"))

    return render_template("login.html", next=request.args.get("next") or "")


@auth_bp.route("/login", methods=["POST"])
def login_submit():
    username = request.form.get("username") or ""
    password = request.form.get("password") or ""
    target = _safe_next(request.form.get("next"))

    try:
        user = sec.authenticate(username, password)
    except sec.AuthError as e:
        return render_template(
            "login.html", error=str(e), username=username,
            next=request.form.get("next") or "",
        ), 401

    token = sec.create_session(user["id"], ip=request.remote_addr)

    if user["must_change_password"]:
        target = url_for("auth.password_page")

    return _set_session_cookie(redirect(target), token)


@auth_bp.route("/logout", methods=["POST"])
def logout():
    sec.destroy_session(request.cookies.get(SESSION_COOKIE))

    response = redirect(url_for("auth.login_page"))
    response.delete_cookie(SESSION_COOKIE, path="/")
    return response


# ------------------------------------------------------------
# Свой пароль
# ------------------------------------------------------------

@auth_bp.route("/account/password")
def password_page():
    user = current_user()
    return render_template("account_password.html",
                           forced=user["must_change_password"])


@auth_bp.route("/account/password", methods=["POST"])
def password_submit():
    user = current_user()

    current = request.form.get("current_password") or ""
    password = request.form.get("password") or ""
    repeat = request.form.get("password2") or ""

    try:
        # текущий пароль спрашиваем всегда: иначе отлучившийся от
        # рабочего места человек дарит свою учётку насовсем
        if not sec.verify_password(current, user["password_hash"]):
            raise ValueError("Текущий пароль неверен")

        if password != repeat:
            raise ValueError("Пароли не совпадают")

        if password == current:
            raise ValueError("Новый пароль совпадает со старым")

        sec.check_password_policy(password)
    except ValueError as e:
        return render_template(
            "account_password.html", error=str(e),
            forced=user["must_change_password"],
        ), 400

    # смена пароля закрывает все сессии, включая текущую — выдаём новую,
    # чтобы человек не оказался выкинут сразу после успешной смены
    sec.set_user_password(user["id"], password, must_change=False)
    token = sec.create_session(user["id"], ip=request.remote_addr)

    return _set_session_cookie(redirect("/"), token)
