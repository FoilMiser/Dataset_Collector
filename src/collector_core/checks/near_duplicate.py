from __future__ import annotations

import importlib.util
import re
import time
from dataclasses import dataclass
from typing import Iterable

from collector_core.stability import stable_api

DEFAULT_THRESHOLD = 0.85
DEFAULT_NUM_PERM = 128
DEFAULT_SHINGLE_SIZE = 3
DEFAULT_MAX_TOKENS = 2000
DEFAULT_MAX_CANDIDATES = 50


@stable_api
@dataclass(frozen=True)
class DuplicateResult:
    is_duplicate: bool
    score: float
    match_id: str | None
    backend: str
    elapsed_ms: float
    candidates_checked: int


@stable_api
@dataclass
class DetectorStats:
    indexed: int = 0
    queries: int = 0
    duplicates: int = 0
    total_index_ms: float = 0.0
    total_query_ms: float = 0.0
    last_index_ms: float | None = None
    last_query_ms: float | None = None

    def record_index(self, elapsed_ms: float) -> None:
        self.indexed += 1
        self.total_index_ms += elapsed_ms
        self.last_index_ms = elapsed_ms

    def record_query(self, elapsed_ms: float, *, is_duplicate: bool) -> None:
        self.queries += 1
        self.total_query_ms += elapsed_ms
        self.last_query_ms = elapsed_ms
        if is_duplicate:
            self.duplicates += 1

    def to_dict(self) -> dict[str, float | int | None]:
        avg_index = self.total_index_ms / self.indexed if self.indexed else 0.0
        avg_query = self.total_query_ms / self.queries if self.queries else 0.0
        return {
            "indexed": self.indexed,
            "queries": self.queries,
            "duplicates": self.duplicates,
            "total_index_ms": round(self.total_index_ms, 3),
            "total_query_ms": round(self.total_query_ms, 3),
            "avg_index_ms": round(avg_index, 3),
            "avg_query_ms": round(avg_query, 3),
            "last_index_ms": None if self.last_index_ms is None else round(self.last_index_ms, 3),
            "last_query_ms": None if self.last_query_ms is None else round(self.last_query_ms, 3),
        }


def _tokenize(text: str, *, max_tokens: int) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    if max_tokens > 0:
        return tokens[:max_tokens]
    return tokens


def _build_shingles(tokens: list[str], size: int) -> list[str]:
    if not tokens:
        return []
    if size <= 1 or len(tokens) <= size:
        return tokens
    return [" ".join(tokens[idx : idx + size]) for idx in range(len(tokens) - size + 1)]


def _jaccard(lhs: Iterable[str], rhs: Iterable[str]) -> float:
    lhs_set = set(lhs)
    rhs_set = set(rhs)
    if not lhs_set and not rhs_set:
        return 1.0
    if not lhs_set or not rhs_set:
        return 0.0
    return len(lhs_set & rhs_set) / len(lhs_set | rhs_set)


def _datasketch_available() -> bool:
    return importlib.util.find_spec("datasketch") is not None


@stable_api
class NearDuplicateDetector:
    """
    Near-duplicate detector optimized for large corpora.

    Performance target (<1ms per query on ~100K docs) assumes:
      - Datasketch MinHashLSH backend
      - max_candidates capped (default: 50)
      - tokens capped (default: 2000) and short shingles
    The pure-Python fallback is intended for small corpora or testing only.
    """

    def __init__(
        self,
        *,
        backend: str,
        threshold: float = DEFAULT_THRESHOLD,
        num_perm: int = DEFAULT_NUM_PERM,
        shingle_size: int = DEFAULT_SHINGLE_SIZE,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        max_candidates: int = DEFAULT_MAX_CANDIDATES,
    ) -> None:
        self.backend = backend
        self.threshold = threshold
        self.num_perm = num_perm
        self.shingle_size = shingle_size
        self.max_tokens = max_tokens
        self.max_candidates = max_candidates
        self.stats = DetectorStats()
        self._token_sets: dict[str, set[str]] = {}
        if backend == "datasketch":
            from datasketch import MinHashLSH

            self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        else:
            self._lsh = None

    def _prepare_tokens(self, text: str) -> list[str]:
        tokens = _tokenize(text, max_tokens=self.max_tokens)
        return _build_shingles(tokens, self.shingle_size)

    def _build_minhash(self, tokens: list[str]):
        from datasketch import MinHash

        minhash = MinHash(num_perm=self.num_perm)
        for token in tokens:
            minhash.update(token.encode("utf-8"))
        return minhash

    def add(self, doc_id: str, text: str) -> None:
        if doc_id in self._token_sets:
            return
        start = time.perf_counter()
        tokens = self._prepare_tokens(text)
        if not tokens:
            return
        token_set = set(tokens)
        self._token_sets[doc_id] = token_set
        if self._lsh is not None:
            minhash = self._build_minhash(tokens)
            self._lsh.insert(doc_id, minhash)
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.stats.record_index(elapsed_ms)

    def query(self, text: str) -> DuplicateResult:
        start = time.perf_counter()
        tokens = self._prepare_tokens(text)
        if not tokens:
            elapsed_ms = (time.perf_counter() - start) * 1000
            result = DuplicateResult(
                is_duplicate=False,
                score=0.0,
                match_id=None,
                backend=self.backend,
                elapsed_ms=elapsed_ms,
                candidates_checked=0,
            )
            self.stats.record_query(elapsed_ms, is_duplicate=False)
            return result

        best_score = 0.0
        best_id: str | None = None
        candidates: list[str]
        if self._lsh is not None:
            minhash = self._build_minhash(tokens)
            raw_candidates = self._lsh.query(minhash)
            candidates = sorted(raw_candidates)
        else:
            candidates = list(self._token_sets.keys())
        if self.max_candidates and len(candidates) > self.max_candidates:
            candidates = candidates[: self.max_candidates]

        for candidate_id in candidates:
            candidate_tokens = self._token_sets.get(candidate_id)
            if not candidate_tokens:
                continue
            score = _jaccard(tokens, candidate_tokens)
            if score > best_score:
                best_score = score
                best_id = candidate_id
            if best_score >= 1.0:
                break

        elapsed_ms = (time.perf_counter() - start) * 1000
        is_duplicate = best_score >= self.threshold
        result = DuplicateResult(
            is_duplicate=is_duplicate,
            score=best_score,
            match_id=best_id,
            backend=self.backend,
            elapsed_ms=elapsed_ms,
            candidates_checked=len(candidates),
        )
        self.stats.record_query(elapsed_ms, is_duplicate=is_duplicate)
        return result


@stable_api
def create_detector(
    *,
    backend: str | None = None,
    threshold: float = DEFAULT_THRESHOLD,
    num_perm: int = DEFAULT_NUM_PERM,
    shingle_size: int = DEFAULT_SHINGLE_SIZE,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> NearDuplicateDetector:
    resolved_backend = backend or ("datasketch" if _datasketch_available() else "python")
    if resolved_backend not in {"datasketch", "python"}:
        raise ValueError(f"Unsupported near-duplicate backend: {resolved_backend}")
    if resolved_backend == "datasketch" and not _datasketch_available():
        resolved_backend = "python"
    return NearDuplicateDetector(
        backend=resolved_backend,
        threshold=threshold,
        num_perm=num_perm,
        shingle_size=shingle_size,
        max_tokens=max_tokens,
        max_candidates=max_candidates,
    )
