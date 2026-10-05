"""Страница /gpcopy: блок «Куда грузить» (карта targets) в обоих тулкитах."""

import re


def _get(client, url):
    resp = client.get(url)
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    resp.close()
    return text


def test_gp_page_has_target_block_with_partitions_note(client):
    html = _get(client, "/gpcopy")

    assert 'id="gppTgt"' in html
    assert 'id="gppTgtList"' in html
    assert 'id="gppTgtFilter"' in html
    assert 'id="gppTgtOnlyMapped"' in html
    assert "Для режима партиций загрузка" in html
    # список прокручивается в своём контейнере, шапка закреплена
    css = re.search(r"\.gpp-tgt-list\s*\{([^}]*)\}", html).group(1)
    assert "max-height" in css and "overflow-y" in css
    assert "js/gpcopy_pipeline.js?v=21" in html


def test_pg_page_has_target_block_in_both_modes(client):
    html = _get(client, "/gpcopy?toolkit=pg")

    assert 'id="gppTgt"' in html        # «Перенос»
    assert 'id="pgcmpTgt"' in html      # «Сравнение и разница»
    panel = html.index('id="pgcmpPanel"')
    assert panel < html.index('id="pgcmpTgt"') < html.index('id="pgcmpCompareBtn"')


def test_scripts_mirror_server_name_rule_and_send_targets(client):
    gpp = _get(client, "/static/js/gpcopy_pipeline.js")
    pgcmp = _get(client, "/static/js/pg_compare.js")

    # то же правило имени, что в modules/sync_targets.py
    from modules.sync_targets import PART_RE

    for js in (gpp, pgcmp):
        assert "/" + PART_RE.pattern + "/" in js
        assert "toLowerCase()" in js

    # карта уходит в запуски; маршрут партиций её не получает
    assert "tgtAttach(body, tables)" in gpp
    part_call = gpp.index('api("/api/gpcopy/partition-diff/start"')
    part_body = gpp.rindex("var body = {", 0, part_call)
    assert "tgtAttach" not in gpp[part_body:part_call]
    assert "body.targets = tgt.payload" in pgcmp
