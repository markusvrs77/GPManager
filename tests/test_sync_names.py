import pytest

from modules.gpcopy_sync import resolve_sync_names


def test_classic_source_target_passthrough():
    src, tgt = resolve_sync_names({"source": "s.a", "target": "d.b"})
    assert (src, tgt) == ("s.a", "d.b")


def test_pipeline_schema_table_format():
    src, tgt = resolve_sync_names({"schema": "dq", "table": "dq_table_list",
                                   "key_columns": ["id"]})
    assert src == "dq.dq_table_list"
    assert tgt == src  # target по умолчанию = source


def test_missing_names_raise():
    with pytest.raises(Exception):
        resolve_sync_names({"key_columns": ["id"]})
