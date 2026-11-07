# -*- coding: utf-8 -*-
"""
Сторож блокировок синхронизации прод -> тест (GPCOPY, синхронизация по ключу).

Синхронизация не должна ни ждать чужие блокировки, ни задерживать ETL:

* наша сессия ждёт блокировку дольше wait_seconds — или таблицу занимает
  приоритетная сессия — её запрос отменяется (pg_cancel_backend своей же
  сессии), таблица/партиция откладывается и догружается в конце задачи,
  не больше attempts раз;
* приоритетная сессия (учётка и/или IP из настроек) ждёт нашу блокировку —
  начатые партиции этой таблицы доделываются, новые не начинаются (сразу
  откладываются): ETL проходит, таблица потом продолжается с оставшихся
  партиций.

Сторож только читает pg_locks / pg_stat_activity и отменяет запросы своих
сессий (та же учётка, сессия открыта после старта прогона). Чужие сессии
он не трогает никогда.
"""

import ipaddress
import json
import re
import threading
import time
from collections import defaultdict
from datetime import datetime

from db import sqlite_cursor

APP_NAME = "opsentri-lockguard"
POLL_SECONDS = 3
CANCEL_REPEAT_SECONDS = 15

DEFAULTS = {
    "enabled": True,
    "wait_seconds": 30,       # сколько наш запрос может ждать чужую блокировку
    "attempts": 3,            # сколько раз догружать отложенное в конце
    "retry_minutes": 5,       # пауза перед каждой догрузкой
    "max_wait_minutes": 60,   # сколько ждать освобождения таблиц перед догрузкой
    "priority": [],           # [{"user": "etl", "ip": "10.1.2.0/24"}]
}

_INT_FIELDS = {
    "wait_seconds": (5, 3600, "Ждать блокировку, сек"),
    "attempts": (0, 20, "Попыток догрузки"),
    "retry_minutes": (0, 720, "Пауза перед догрузкой, мин"),
    "max_wait_minutes": (0, 1440, "Ждать освобождения, мин"),
}

_USER_RE = re.compile(r"^[A-Za-z0-9_$.\-]{1,63}$")


class LockDeferred(Exception):
    """Таблица отложена: занята блокировкой или нужна приоритетному ETL."""


def is_lock_timeout(error):
    """Ошибка psycopg2 — истёк lock_timeout (SQLSTATE 55P03)."""
    return (getattr(error, "pgcode", None) == "55P03"
            or "lock timeout" in str(error).lower())


# ------------------------------------------------------------------ настройки

def parse_rule(text):
    """'etl@10.1.2.3' | 'etl' | '@10.1.2.0/24' | '10.1.2.3' -> {user, ip}."""
    s = str(text or "").strip()

    if not s:
        return None

    user, ip = "", ""

    if "@" in s:
        user, ip = (part.strip() for part in s.rsplit("@", 1))
    else:
        try:
            ipaddress.ip_network(s, strict=False)
            ip = s
        except ValueError:
            user = s

    if user and not _USER_RE.match(user):
        raise ValueError("Учётка «{}»: только латиница, цифры и _ $ . -".format(user))

    if ip:
        try:
            net = ipaddress.ip_network(ip, strict=False)
        except ValueError:
            raise ValueError("IP «{}» не распознан (пример: 10.1.2.3 или "
                             "10.1.2.0/24)".format(ip))
        ip = str(net) if net.num_addresses > 1 else str(net.network_address)

    if not user and not ip:
        raise ValueError("Пустое правило «{}»".format(s))

    return {"user": user, "ip": ip}


def parse_rules(value):
    """Строка (по строке/запятой на правило) или список -> [{user, ip}]."""
    if isinstance(value, str):
        value = re.split(r"[\n,;]+", value)

    rules = []

    for item in value or []:
        if isinstance(item, dict):
            item = "{}@{}".format(item.get("user") or "", item.get("ip") or "") \
                if item.get("ip") else (item.get("user") or "")

        rule = parse_rule(item)

        if rule and rule not in rules:
            rules.append(rule)

    return rules


def rule_label(rule):
    if rule["user"] and rule["ip"]:
        return "{}@{}".format(rule["user"], rule["ip"])
    return rule["user"] or "@" + rule["ip"]


def match_priority(rules, usename, client_addr):
    """Правило, под которое попадает сессия, или None."""
    for rule in rules or []:
        if rule.get("user") and rule["user"].lower() != str(usename or "").lower():
            continue

        if rule.get("ip"):
            try:
                if ipaddress.ip_address(str(client_addr or "")) not in \
                        ipaddress.ip_network(rule["ip"], strict=False):
                    continue
            except ValueError:
                continue

        return rule

    return None


def normalize_settings(data):
    """Проверенные настройки; ValueError — что не так."""
    data = data or {}
    out = dict(DEFAULTS)
    out["enabled"] = bool(data.get("enabled", DEFAULTS["enabled"]))

    for key, (low, high, label) in _INT_FIELDS.items():
        raw = data.get(key, DEFAULTS[key])

        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise ValueError("{}: нужно целое число".format(label))

        if not low <= value <= high:
            raise ValueError("{}: от {} до {}".format(label, low, high))

        out[key] = value

    out["priority"] = parse_rules(data.get("priority") or [])
    return out


def load_settings():
    try:
        with sqlite_cursor() as cur:
            cur.execute("SELECT settings_json FROM sync_lock_settings WHERE id = 1")
            row = cur.fetchone()
    except Exception:
        row = None

    if not row:
        return normalize_settings({})

    try:
        return normalize_settings(json.loads(row[0] or "{}"))
    except Exception:
        return normalize_settings({})


def save_settings(data, user=None):
    settings = normalize_settings(data)

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            "INSERT INTO sync_lock_settings (id, settings_json, updated_at, updated_by) "
            "VALUES (1, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "settings_json = excluded.settings_json, updated_at = excluded.updated_at, "
            "updated_by = excluded.updated_by",
            (json.dumps(settings, ensure_ascii=False),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user),
        )

    return settings


# ------------------------------------------------------------ блокировки

_MODES = ("AccessShareLock", "RowShareLock", "RowExclusiveLock",
          "ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock",
          "ExclusiveLock", "AccessExclusiveLock")

_CONFLICTS = {
    "AccessShareLock": {"AccessExclusiveLock"},
    "RowShareLock": {"ExclusiveLock", "AccessExclusiveLock"},
    "RowExclusiveLock": {"ShareLock", "ShareRowExclusiveLock", "ExclusiveLock",
                         "AccessExclusiveLock"},
    "ShareUpdateExclusiveLock": {"ShareUpdateExclusiveLock", "ShareLock",
                                 "ShareRowExclusiveLock", "ExclusiveLock",
                                 "AccessExclusiveLock"},
    "ShareLock": {"RowExclusiveLock", "ShareUpdateExclusiveLock",
                  "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"},
    "ShareRowExclusiveLock": {"RowExclusiveLock", "ShareUpdateExclusiveLock",
                              "ShareLock", "ShareRowExclusiveLock",
                              "ExclusiveLock", "AccessExclusiveLock"},
    "ExclusiveLock": set(_MODES) - {"AccessShareLock"},
    "AccessExclusiveLock": set(_MODES),
}


def conflicts(mode_a, mode_b):
    """Конфликтуют ли режимы; незнакомый режим — считаем, что да."""
    known = _CONFLICTS.get(mode_a)
    return True if known is None or mode_b not in _MODES else mode_b in known


def describe(row):
    """'etl@10.1.2.3 (pid 123, AccessExclusive: TRUNCATE …)'."""
    who = "{}@{}".format(row.get("user") or "?", row.get("ip") or "local")
    mode = str(row.get("mode") or "").replace("Lock", "")
    query = " ".join(str(row.get("query") or "").split())[:80]
    return "{} (pid {}, {}{})".format(who, row.get("pid"), mode,
                                      ": " + query if query else "")


_SELECTED_SQL = """
SELECT c.oid::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN unnest(%s::text[], %s::text[]) AS sel(s, t)
  ON sel.s = n.nspname AND sel.t = c.relname
"""

_PARENT_SQL = ("SELECT inhrelid::bigint, inhparent::bigint FROM pg_inherits "
               "WHERE inhrelid = ANY(%s::oid[])")

# выбранные таблицы и все их партиции; корень — верхний родитель (задача
# догрузки выбирает отдельные партиции — пауза всё равно по всей таблице)
_FAMILY_SQL = """
WITH RECURSIVE fam(oid, root) AS (
    SELECT * FROM unnest(%s::oid[], %s::oid[])
    UNION ALL
    SELECT i.inhrelid, f.root
    FROM pg_inherits i
    JOIN fam f ON i.inhparent = f.oid
)
SELECT DISTINCT f.oid::bigint, rn.nspname, rc.relname, n.nspname, c.relname
FROM fam f
JOIN pg_class c ON c.oid = f.oid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class rc ON rc.oid = f.root
JOIN pg_namespace rn ON rn.oid = rc.relnamespace
"""

_LOCKS_SQL = """
SELECT l.pid, l.relation::bigint, l.mode, l.granted, a.usename,
       host(a.client_addr), COALESCE(a.application_name, ''),
       LEFT(COALESCE(a.query, ''), 200)
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'relation'
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.relation = ANY(%s::oid[])
  AND l.pid <> pg_backend_pid()
  {seg}
"""


class Side(object):
    """Одна база (источник или приёмник): свои сессии, таблицы, блокировки."""

    def __init__(self, label, connect, pairs):
        self.label = label
        self.connect = connect
        self.pairs = sorted({(str(s), str(t)) for s, t in pairs or []})
        self.conn = None
        self.user = None
        self.client_ip = None
        self.rules = []
        self.baseline = set()
        self.family = {}    # oid -> (root (schema, table), leaf (schema, table))
        self.seg_filter = ""
        self.error = None

    def open(self):
        conn = self.connect()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SET application_name = %s", (APP_NAME,))
        cur.execute("SET statement_timeout = '15s'")
        cur.execute("SELECT session_user, host(inet_client_addr())")
        self.user, self.client_ip = cur.fetchone()
        cur.execute("SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_catalog.pg_locks'::regclass "
                    "AND attname = 'gp_segment_id'")
        # в Greenplum pg_locks отдаёт и сегменты — нужны блокировки координатора
        self.seg_filter = "AND l.gp_segment_id = -1" if cur.fetchone() else ""
        cur.execute(_SELECTED_SQL, ([s for s, _t in self.pairs], [t for _s, t in self.pairs]))
        tops = {int(r[0]): int(r[0]) for r in cur.fetchall()}

        for _depth in range(8):
            cur.execute(_PARENT_SQL, (sorted(set(tops.values())),))
            parent = {int(c): int(p) for c, p in cur.fetchall()}
            moved = {o: parent.get(t, t) for o, t in tops.items()}

            if moved == tops:
                break
            tops = moved

        self.family = {}

        if tops:
            cur.execute(_FAMILY_SQL, (list(tops), list(tops.values())))
            self.family = {int(oid): ((rs, rt), (s, t))
                           for oid, rs, rt, s, t in cur.fetchall()}

            # сам корень (ETL берёт блокировку на него), даже если выбраны
            # только партиции — без остальных его партиций
            missing = sorted(set(tops.values()) - set(self.family))

            if missing:
                cur.execute("SELECT c.oid::bigint, n.nspname, c.relname FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "WHERE c.oid = ANY(%s::oid[])", (missing,))

                for oid, s, t in cur.fetchall():
                    self.family[int(oid)] = ((s, t), (s, t))
        self.conn = conn
        self.error = None
        return self

    def close(self):
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        self.conn = None

    def _cur(self):
        if self.conn is None or getattr(self.conn, "closed", 0):
            self.open()
        return self.conn.cursor()

    def rebaseline(self):
        """Все сессии, открытые до старта прогона, — не наши (их не трогаем)."""
        cur = self._cur()
        cur.execute("SELECT pid FROM pg_stat_activity")
        self.baseline = {int(r[0]) for r in cur.fetchall()}

    def rows(self):
        if not self.family:
            return []

        cur = self._cur()
        cur.execute(_LOCKS_SQL.format(seg=self.seg_filter), (list(self.family),))
        return [
            {"pid": int(pid), "oid": int(oid), "mode": mode, "granted": bool(granted),
             "user": user, "ip": ip, "app": app, "query": query}
            for pid, oid, mode, granted, user, ip, app, query in cur.fetchall()
        ]

    def cancel(self, pid):
        cur = self._cur()
        cur.execute("SELECT pg_cancel_backend(%s)", (int(pid),))
        return bool(cur.fetchone()[0])

    def is_ours(self, row):
        """
        Сессия прогона: открыта после его старта, с адреса этого хоста (gpcopy
        запускается здесь; учётка на источнике у него может быть другой) и
        не сторож. Приоритетная сессия своей не бывает никогда — сомнение
        всегда в пользу «чужая, не трогать».
        """
        if int(row["pid"]) in self.baseline or row.get("app") == APP_NAME:
            return False

        if match_priority(self.rules, row.get("user"), row.get("ip")):
            return False

        if self.client_ip:
            return row.get("ip") == self.client_ip

        return not row.get("ip") and row.get("user") == self.user

    def root_of(self, oid):
        return (self.family.get(oid) or (None, None))[0]


class LockGuard(object):
    """
    Следит за блокировками таблиц прогона на обеих базах и отменяет
    собственные запросы, которые ждут чужое или мешают приоритетному ETL.
    """

    def __init__(self, sides, settings, on_change=None, poll=POLL_SECONDS,
                 clock=time.time):
        self.sides = [s for s in sides if s.pairs]
        self.rules = settings.get("priority") or []

        for side in self.sides:
            side.rules = self.rules

        self.wait = int(settings.get("wait_seconds") or DEFAULTS["wait_seconds"])
        self.on_change = on_change
        self.poll = poll
        self.clock = clock
        self.lock = threading.RLock()
        self.first_wait = {}
        self.cancelled = {}
        self.last_rows = {}
        self.paused = {}       # root -> {reason, since, allowed}
        self.deferred = {}     # root -> {reason, leaves: set, side}
        self.round_roots = set()
        self.warnings = []
        self._stop = threading.Event()
        self._thread = None

    # ---------------------------------------------------------- жизнь

    def open(self):
        """Подключиться к базам; не вышло — сторож без этой базы (с причиной)."""
        alive = []

        for side in self.sides:
            try:
                side.open()
                alive.append(side)
            except Exception as e:
                side.close()
                self.warnings.append("{}: сторож блокировок не подключился — {}".format(
                    side.label, str(e).strip()[:200]))

        self.sides = alive
        return bool(alive)

    def close(self):
        self.stop_watch()

        for side in self.sides:
            side.close()

    def new_round(self):
        """Перед каждым запуском: снимок чужих сессий, пауза ETL сбрасывается."""
        with self.lock:
            self.paused = {}
            self.first_wait = {}
            self.round_roots = set()

        for side in self.sides:
            try:
                side.rebaseline()
            except Exception as e:
                side.error = str(e)[:200]

    def start_watch(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop_watch(self):
        self._stop.set()

        if self._thread is not None:
            self._thread.join(timeout=self.poll + 20)
            self._thread = None

    def _loop(self):
        while not self._stop.wait(self.poll):
            self.tick()

    # ---------------------------------------------------------- решения

    def tick(self):
        changed = False

        for side in self.sides:
            try:
                rows = side.rows()
            except Exception as e:
                side.error = str(e)[:200]
                side.close()   # переподключится на следующем опросе
                continue

            with self.lock:
                self.last_rows[side.label] = rows
                actions = self.evaluate(side, rows, self.clock())

            for row, reason in actions:
                try:
                    side.cancel(row["pid"])
                except Exception as e:
                    side.error = str(e)[:200]
                    continue

                self._record(side, row, reason)
                changed = True

        if changed and self.on_change:
            try:
                self.on_change(self)
            except Exception:
                pass

    def evaluate(self, side, rows, now):
        """[(строка нашей сессии, причина)] — что отменить. Базу не трогает."""
        def prio(r):
            return (not side.is_ours(r)) and match_priority(
                self.rules, r.get("user"), r.get("ip"))

        by_oid = defaultdict(list)

        for r in rows:
            by_oid[r["oid"]].append(r)

        actions = {}
        waiting_now = set()

        def act(r, reason):
            key = (side.label, r["pid"], r["oid"])
            last = self.cancelled.get(key)

            if last is not None and now - last < CANCEL_REPEAT_SECONDS:
                return
            actions.setdefault(key, (r, reason))

        for oid, rs in by_oid.items():
            holders = [r for r in rs if r["granted"]]
            prios = [r for r in rs if prio(r)]

            for w in rs:
                if w["granted"]:
                    continue

                if side.is_ours(w):
                    key = (side.label, w["pid"], oid)
                    waiting_now.add(key)
                    first = self.first_wait.setdefault(key, now)

                    if prios:
                        act(w, "таблицу занимает приоритетная сессия " + describe(prios[0]))
                    elif now - first >= self.wait:
                        blockers = [h for h in holders
                                    if h["pid"] != w["pid"] and not side.is_ours(h)
                                    and conflicts(w["mode"], h["mode"])]
                        act(w, "ждала блокировку {} с, держит {}".format(
                            int(now - first),
                            describe(blockers[0]) if blockers else "очередь блокировок"))
                elif prio(w):
                    blocking = [h for h in holders
                                if side.is_ours(h) and conflicts(w["mode"], h["mode"])]
                    root = side.root_of(oid)

                    if blocking and root and root not in self.paused:
                        self.paused[root] = {
                            "reason": "её ждёт приоритетная сессия " + describe(w),
                            "since": now,
                            "allowed": self._started(root),
                        }

        self.first_wait = {k: v for k, v in self.first_wait.items()
                           if k[0] != side.label or k in waiting_now}

        # таблица на паузе: начатое доделывается, новое не начинается
        for r in rows:
            if not side.is_ours(r):
                continue

            pause = self.paused.get(side.root_of(r["oid"]))

            if pause and (side.label, r["pid"], r["oid"]) not in pause["allowed"]:
                act(r, "пауза ради ETL: " + pause["reason"])

        return list(actions.values())

    def _started(self, root):
        """Что уже взято нашими сессиями по таблице (на обеих базах)."""
        allowed = set()

        for side in self.sides:
            for r in self.last_rows.get(side.label) or []:
                if r["granted"] and side.is_ours(r) and side.root_of(r["oid"]) == root:
                    allowed.add((side.label, r["pid"], r["oid"]))

        return allowed

    def _record(self, side, row, reason):
        root, leaf = side.family.get(row["oid"]) or ((None, None), (None, None))

        with self.lock:
            self.cancelled[(side.label, row["pid"], row["oid"])] = self.clock()
            entry = self.deferred.setdefault(root, {"leaves": set(), "side": side.label})
            entry["reason"] = "{}: {}".format(side.label, reason)
            entry["leaves"].add(leaf)
            self.round_roots.add(root)

        print("[lock_guard] {} отложено {}.{}: {}".format(
            side.label, leaf[0], leaf[1], reason))

    # ---------------------------------------------------------- для прогона

    def round_reason(self):
        """Причина отложенного в текущем прогоне (для сообщения таблицы)."""
        with self.lock:
            reasons = [self.deferred[r].get("reason") for r in self.round_roots
                       if r in self.deferred]
        return "; ".join(r for r in reasons if r)[:600]

    def cancel_ours(self):
        """Отменить все запросы наших сессий по таблицам прогона (прерываем его)."""
        for side in self.sides:
            try:
                pids = {r["pid"] for r in side.rows() if side.is_ours(r)}

                for pid in pids:
                    side.cancel(pid)
            except Exception as e:
                side.error = str(e)[:200]

    def root_of_name(self, schema, table):
        """(schema, table) партиции или таблицы -> корень, если известен."""
        for side in self.sides:
            for root, leaf in side.family.values():
                if leaf == (schema, table):
                    return root
        return None

    def busy(self, roots, need):
        """
        {root: причина} — таблицы, за которые сейчас браться бесполезно:
        на них работает приоритетная сессия или чужая блокировка конфликтует
        с нужной (need: {label: режим}).
        """
        roots = set(roots)
        out = {}

        for side in self.sides:
            mode = need.get(side.label)

            try:
                rows = side.rows()
            except Exception as e:
                side.error = str(e)[:200]
                side.close()
                continue

            for r in rows:
                root = side.root_of(r["oid"])

                if root not in roots or root in out or side.is_ours(r):
                    continue

                if match_priority(self.rules, r.get("user"), r.get("ip")):
                    out[root] = "{}: работает приоритетная сессия {}".format(
                        side.label, describe(r))
                elif mode and r["granted"] and conflicts(mode, r["mode"]):
                    out[root] = "{}: блокировку держит {}".format(side.label, describe(r))

        return out

    def summary(self):
        """Для карточки задачи: отложенные таблицы, паузы, предупреждения."""
        with self.lock:
            return {
                "deferred": [
                    {"table": "{}.{}".format(*root), "parts": len(v["leaves"]),
                     "reason": v.get("reason") or ""}
                    for root, v in sorted(self.deferred.items(), key=lambda kv: str(kv[0]))
                    if root and root[0]
                ],
                "paused": [
                    {"table": "{}.{}".format(*root), "reason": v["reason"]}
                    for root, v in self.paused.items() if root
                ],
                "warnings": list(self.warnings),
            }


def wait_until_free(guard, roots, need, max_wait_seconds, should_stop,
                    poll=20, on_wait=None, sleep=time.sleep, clock=time.time):
    """
    Ждать, пока таблицы освободятся (не дольше max_wait_seconds).
    -> {root: причина} того, что всё ещё занято; None — остановлено.
    """
    deadline = clock() + max(0, int(max_wait_seconds))

    while True:
        busy = guard.busy(roots, need)

        if not busy or clock() >= deadline:
            return busy

        if on_wait:
            on_wait(busy)

        step_end = min(deadline, clock() + poll)

        while clock() < step_end:
            if should_stop():
                return None
            sleep(min(3, max(0.0, step_end - clock())))


def sleep_unless_stopped(seconds, should_stop, sleep=time.sleep, clock=time.time):
    """Пауза с проверкой «Стоп» каждые 3 с. -> False, если остановили."""
    end = clock() + max(0, seconds)

    while clock() < end:
        if should_stop():
            return False
        sleep(min(3, max(0.0, end - clock())))

    return not should_stop()
