"""Страница /gpcopy: режим «Сравнение и разница» есть только в Postgres Toolkit."""


def _page(client, url):
    resp = client.get(url)
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def test_pg_toolkit_has_mode_switch_and_script(client):
    html = _page(client, "/gpcopy?toolkit=pg")

    assert 'id="pgcmpModeSwitch"' in html
    assert "Сравнение и разница" in html
    assert "js/pg_compare.js" in html
    assert 'id="pgcmpPanel"' in html


def test_pg_toolkit_opens_transfer_by_default(client):
    html = _page(client, "/gpcopy?toolkit=pg")

    # «Перенос» отмечен, панель сравнения скрыта до переключения
    assert 'data-pgcmp-mode="transfer" aria-pressed="true"' in html
    assert 'data-pgcmp-mode="compare" aria-pressed="false"' in html
    assert 'id="pgcmpPanel" hidden' in html
    # мастер «Перенос» на месте
    assert 'id="gppGo"' in html


def test_gp_toolkit_has_no_compare_mode(client):
    html = _page(client, "/gpcopy")

    assert "pgcmp" not in html
    assert "pg_compare.js" not in html
    assert "Сравнение и разница" not in html
    assert 'id="gppGo"' in html


def _static(client, path):
    resp = client.get(path)
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    resp.close()
    return text


def test_pg_compare_script_globals_are_prefixed(client):
    import re

    js = _static(client, "/static/js/pg_compare.js")

    # если режим что-то выставит в window, то только с префиксом pgcmp
    exported = re.findall(r"window\.([A-Za-z_$][\w$]*)\s*=", js)
    assert all(name.startswith("pgcmp") for name in exported), exported

    # чужие id режим только читает: селекторы шапки и ничего больше
    ids = set(re.findall(r'\$\("([^"]+)"\)', js))
    foreign = {i for i in ids if not i.startswith("pgcmp")}
    assert foreign <= {"gppSrc", "gppDst"}, foreign


def test_pg_runs_feed_includes_compare_and_load(client):
    js = _static(client, "/static/js/gpcopy_pipeline.js")

    assert '"copy_pipe,pg_compare,pg_diff_load"' in js


def test_dangerous_checkboxes_start_unchecked(client):
    import re

    html = _page(client, "/gpcopy?toolkit=pg")

    # браузер не должен восстанавливать галку после перезагрузки
    for box_id in ("pgcmpDeleteMissing", "pgcmpAllMissing"):
        tag = re.search(r'<input[^>]*id="%s"[^>]*>' % box_id, html).group(0)
        assert "checked" not in tag
        assert 'autocomplete="off"' in tag


def test_pg_runs_feed_has_human_labels(client):
    js = _static(client, "/static/js/gpcopy_pipeline.js")

    # лента «Запуски» подписывает задачи режима по-человечески, а не сырым типом
    assert 'pg_compare: "Сравнение баз"' in js
    assert 'pg_diff_load: "Загрузка разницы"' in js


def test_pg_compare_script_shows_range_progress(client):
    html = _page(client, "/gpcopy?toolkit=pg")
    js = _static(client, "/static/js/pg_compare.js")

    # новая версия скрипта, чтобы браузер не держал старый из кеша
    assert "js/pg_compare.js?v=5" in html
    # прогресс и итог сравнения по диапазонам берутся из поля chunked
    assert ".chunked" in js
    assert "диапазонов " in js
    assert "по диапазонам: проверено " in js
    # итог — только из chunked: строку message JS не собирает и не сверяет
    assert "dupMsg" not in js
