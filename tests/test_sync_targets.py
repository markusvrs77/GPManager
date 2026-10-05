# -*- coding: utf-8 -*-
"""
Карта «таблица источника -> таблица приёмника» (modules/sync_targets.py):
правила проверки, общие для всех маршрутов запуска.
"""

import pytest

from modules.sync_targets import is_mapped, normalize_targets, target_of

SEL = [{"schema": "a", "table": "x"}, {"schema": "a", "table": "y"},
       {"schema": "b", "table": "z"}]


def test_no_targets_is_empty_map():
    assert normalize_targets(None, SEL) == {}
    assert normalize_targets({}, SEL) == {}
    assert normalize_targets("", SEL) == {}


def test_table_without_schema_takes_source_schema():
    assert normalize_targets({"b.z": "z_copy"}, SEL) == {"b.z": "b.z_copy"}


def test_full_name_is_kept():
    assert normalize_targets({"b.z": "arch.z"}, SEL) == {"b.z": "arch.z"}


def test_empty_value_and_same_as_source_are_dropped():
    raw = {"a.x": "", "a.y": "a.y", "b.z": "  "}

    assert normalize_targets(raw, SEL) == {}


def test_swap_between_selected_tables_is_allowed():
    raw = {"a.x": "a.y", "a.y": "a.x"}

    assert normalize_targets(raw, SEL) == raw


def test_target_taken_by_unmapped_selected_table_is_rejected():
    """a.x -> a.y, а a.y выбрана без карты и сама грузится в a.y."""
    with pytest.raises(ValueError) as error:
        normalize_targets({"a.x": "a.y"}, SEL)

    assert "a.y" in str(error.value)


def test_two_sources_into_one_target_are_rejected():
    with pytest.raises(ValueError):
        normalize_targets({"a.x": "arch.t", "b.z": "arch.t"}, SEL)


@pytest.mark.parametrize("bad", [
    "Arch.t", "arch.T", "arch.t x", '"arch".t', "arch.t,b", "a.b.c",
    "1abc", "arch.", ".t",
])
def test_bad_names_are_rejected(bad):
    with pytest.raises(ValueError):
        normalize_targets({"b.z": bad}, SEL)


def test_dollar_and_underscore_are_allowed():
    assert normalize_targets({"b.z": "_arch.t$1"}, SEL) == {"b.z": "_arch.t$1"}


def test_key_outside_selection_is_rejected():
    with pytest.raises(ValueError) as error:
        normalize_targets({"c.q": "c.r"}, SEL)

    assert "не выбрана" in str(error.value)


def test_non_object_is_rejected():
    with pytest.raises(ValueError):
        normalize_targets(["a.x"], SEL)


def test_selection_as_strings():
    assert normalize_targets({"a.x": "x2"}, ["a.x"]) == {"a.x": "a.x2"}


def test_target_of_and_is_mapped():
    targets = {"a.x": "arch.x_copy"}

    assert target_of(targets, "a", "x") == ("arch", "x_copy")
    assert target_of(targets, "a", "y") == ("a", "y")
    assert target_of(None, "a", "y") == ("a", "y")
    assert is_mapped(targets, "a", "x") is True
    assert is_mapped(targets, "a", "y") is False


def test_short_form_takes_uppercase_source_schema_as_is():
    """Схема из источника не проверяется — её ввёл не человек."""
    sel = [{"schema": "Sales", "table": "x"}]

    assert normalize_targets({"Sales.x": "y"}, sel) == {"Sales.x": "Sales.y"}


def test_typed_uppercase_schema_is_still_rejected():
    sel = [{"schema": "Sales", "table": "x"}]

    with pytest.raises(ValueError):
        normalize_targets({"Sales.x": "Sales.y"}, sel)

    with pytest.raises(ValueError):
        normalize_targets({"Sales.x": "Y"}, sel)
