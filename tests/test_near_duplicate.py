from __future__ import annotations

import pytest
from hypothesis import given, strategies as st

from collector_core.checks.near_duplicate import _build_shingles, _jaccard, create_detector


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


def test_create_detector_rejects_unknown_backend() -> None:
    with pytest.raises(ValueError, match="Unsupported near-duplicate backend"):
        create_detector(backend="unsupported")


@given(
    st.lists(st.text(alphabet="abcd", min_size=1, max_size=4), max_size=8),
    st.lists(st.text(alphabet="abcd", min_size=1, max_size=4), max_size=8),
)
def test_jaccard_similarity_is_symmetric(lhs: list[str], rhs: list[str]) -> None:
    score_lr = _jaccard(lhs, rhs)
    score_rl = _jaccard(rhs, lhs)
    assert 0.0 <= score_lr <= 1.0
    assert score_lr == score_rl


@given(
    st.lists(st.text(alphabet="abcd", min_size=1, max_size=4), min_size=1, max_size=8),
    st.integers(min_value=1, max_value=6),
)
def test_build_shingles_size_matches_tokens(tokens: list[str], size: int) -> None:
    shingles = _build_shingles(tokens, size)
    if size <= 1 or len(tokens) <= size:
        assert shingles == tokens
    else:
        assert len(shingles) == len(tokens) - size + 1
