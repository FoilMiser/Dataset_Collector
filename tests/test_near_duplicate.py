from __future__ import annotations

import pytest

from collector_core.checks.near_duplicate import create_detector


def test_python_detector_reports_duplicates_and_stats() -> None:
    detector = create_detector(backend="python", threshold=0.7, shingle_size=2)
    detector.add("doc-1", "the quick brown fox")
    result = detector.query("the quick brown fox jumps")

    assert result.is_duplicate is True
    assert result.match_id == "doc-1"
    assert result.backend == "python"
    assert detector.stats.indexed == 1
    assert detector.stats.queries == 1
    assert detector.stats.duplicates == 1
    assert detector.stats.last_query_ms is not None


def test_create_detector_python_backend() -> None:
    detector = create_detector(backend="python")
    assert detector.backend == "python"


def test_datasketch_detector_if_available() -> None:
    pytest.importorskip("datasketch")
    detector = create_detector(backend="datasketch", threshold=0.7, num_perm=64, shingle_size=2)
    detector.add("doc-1", "the quick brown fox")
    result = detector.query("the quick brown fox jumps")

    assert result.is_duplicate is True
    assert result.backend == "datasketch"


def test_exact_duplicates_are_detected_property_based() -> None:
    hypothesis = pytest.importorskip("hypothesis")
    from hypothesis import given, strategies as st

    @given(
        st.lists(
            st.text(alphabet="abcd", min_size=1, max_size=6),
            min_size=2,
            max_size=6,
        )
    )
    def _inner(words: list[str]) -> None:
        text = " ".join(words)
        detector = create_detector(backend="python", threshold=0.9, shingle_size=1)
        detector.add("doc", text)
        result = detector.query(text)
        assert result.is_duplicate is True
        assert result.score == 1.0

    _inner()
