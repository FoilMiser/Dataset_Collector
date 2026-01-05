from __future__ import annotations

from types import SimpleNamespace

import pytest

from collector_core.pipeline_driver_base import (
    apply_denylist_bucket,
    build_evidence_headers,
    resolve_retry_config,
    sort_queue_rows,
)


def test_build_evidence_headers_filters_invalid() -> None:
    headers = build_evidence_headers(["Authorization=token", "X-Test=1", "invalid"])
    assert headers == {"Authorization": "token", "X-Test": "1"}


def test_resolve_retry_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    args = SimpleNamespace(retry_max=None, retry_backoff=None, max_retries=None)
    monkeypatch.setenv("PIPELINE_RETRY_MAX", "5")
    monkeypatch.setenv("PIPELINE_RETRY_BACKOFF", "4.5")
    retry_max, retry_backoff = resolve_retry_config(args)
    assert retry_max == 5
    assert retry_backoff == 4.5


def test_sort_queue_rows_orders_by_priority_and_id() -> None:
    rows = [
        {"id": "b", "priority": 1},
        {"id": "a", "priority": 10},
        {"id": "c"},
    ]
    sorted_rows = sort_queue_rows(rows)
    assert [row["id"] for row in sorted_rows] == ["a", "b", "c"]


def test_apply_denylist_bucket_prioritizes_red() -> None:
    hits = [{"severity": "force_yellow"}, {"severity": "hard_red"}]
    assert apply_denylist_bucket(hits, "GREEN") == "RED"
