# -*- coding: utf-8 -*-
"""Найденные ключи синхронизации хранятся в базе и переиспользуются."""

from modules.table_catalog import (
    forget_sync_key,
    load_sync_keys,
    save_sync_key,
)


def test_save_load_forget():
    assert load_sync_keys(777) == {}

    save_sync_key(777, "dwh_stage", "orders", ["order_id"], "computed")
    save_sync_key(777, "dwh_stage", "clients", ["id", "dt"], "manual")

    saved = load_sync_keys(777)

    assert saved[("dwh_stage", "orders")]["columns"] == ["order_id"]
    assert saved[("dwh_stage", "orders")]["source"] == "computed"
    assert saved[("dwh_stage", "clients")]["columns"] == ["id", "dt"]
    assert saved[("dwh_stage", "orders")]["found_at"]

    # повторное сохранение обновляет, а не плодит строки
    save_sync_key(777, "dwh_stage", "orders", ["uuid"], "sampled")
    saved = load_sync_keys(777)

    assert len(saved) == 2
    assert saved[("dwh_stage", "orders")]["columns"] == ["uuid"]
    assert saved[("dwh_stage", "orders")]["source"] == "sampled"

    # выборка ограничивается списком таблиц
    only = load_sync_keys(777, [("dwh_stage", "clients")])

    assert list(only) == [("dwh_stage", "clients")]

    assert forget_sync_key(777, "dwh_stage", "orders") == 1
    assert ("dwh_stage", "orders") not in load_sync_keys(777)

    forget_sync_key(777, "dwh_stage", "clients")


def test_keys_are_per_connection():
    save_sync_key(778, "s", "t", ["a"], "pk")
    save_sync_key(779, "s", "t", ["b"], "pk")

    assert load_sync_keys(778)[("s", "t")]["columns"] == ["a"]
    assert load_sync_keys(779)[("s", "t")]["columns"] == ["b"]

    forget_sync_key(778, "s", "t")
    forget_sync_key(779, "s", "t")


def test_empty_columns_not_saved():
    save_sync_key(780, "s", "t", [], "computed")

    assert load_sync_keys(780) == {}
