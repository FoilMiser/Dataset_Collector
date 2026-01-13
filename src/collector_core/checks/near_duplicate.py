"""Near-duplicate detection using MinHash LSH.

This module provides efficient near-duplicate detection for text documents
using MinHash signatures and Locality-Sensitive Hashing (LSH).

The implementation supports both the datasketch library (if installed) and
a pure Python fallback for environments without optional dependencies.

Example:
    detector = NearDuplicateDetector(threshold=0.8)
    
    # Add documents to index
    detector.add("doc1", "This is the first document about machine learning.")
    detector.add("doc2", "This is the second document about deep learning.")
    
    # Check for duplicates
    result = detector.query("This is the first document about machine learning!")
    if result.is_duplicate:
        print(f"Found duplicate: {result.matched_ids}")
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

# Try to import datasketch for production use
try:
    import datasketch
    HAS_DATASKETCH = True
except ImportError:
    datasketch = None  # type: ignore
    HAS_DATASKETCH = False


@dataclass
class DuplicateResult:
    """Result of a near-duplicate query.
    
    Attributes:
        is_duplicate: Whether the query document is a near-duplicate
        similarity: Jaccard similarity with best match (0.0-1.0)
        matched_ids: List of document IDs that match above threshold
        query_time_ms: Time taken for query in milliseconds
    """
    is_duplicate: bool
    similarity: float
    matched_ids: list[str] = field(default_factory=list)
    query_time_ms: float = 0.0


@dataclass
class DetectorStats:
    """Statistics about the near-duplicate detector.
    
    Attributes:
        document_count: Number of documents indexed
        total_shingles: Total shingles across all documents
        avg_shingles_per_doc: Average shingles per document
        memory_estimate_mb: Estimated memory usage in MB
    """
    document_count: int
    total_shingles: int
    avg_shingles_per_doc: float
    memory_estimate_mb: float


class _PureMinHash:
    """Pure Python MinHash implementation.
    
    This is a fallback when datasketch is not installed.
    Uses the same algorithm but may be slower for large datasets.
    """
    
    _MERSENNE_PRIME = (1 << 61) - 1
    _MAX_HASH = (1 << 32) - 1
    
    def __init__(self, num_perm: int = 128, seed: int = 1):
        self.num_perm = num_perm
        self.seed = seed
        self.hashvalues = [self._MAX_HASH] * num_perm
        
        import random
        gen = random.Random(seed)
        self._a = [gen.randint(1, self._MERSENNE_PRIME - 1) for _ in range(num_perm)]
        self._b = [gen.randint(0, self._MERSENNE_PRIME - 1) for _ in range(num_perm)]
    
    def update(self, data: bytes) -> None:
        """Update MinHash with a new element."""
        h = int(hashlib.sha1(data).hexdigest()[:16], 16)
        for i in range(self.num_perm):
            hv = ((self._a[i] * h + self._b[i]) % self._MERSENNE_PRIME) & self._MAX_HASH
            if hv < self.hashvalues[i]:
                self.hashvalues[i] = hv
    
    def jaccard(self, other: "_PureMinHash") -> float:
        """Estimate Jaccard similarity with another MinHash."""
        if self.num_perm != other.num_perm:
            raise ValueError("MinHash must have same num_perm")
        matches = sum(1 for a, b in zip(self.hashvalues, other.hashvalues) if a == b)
        return matches / self.num_perm


class _PureMinHashLSH:
    """Pure Python MinHash LSH implementation."""
    
    def __init__(self, threshold: float = 0.5, num_perm: int = 128):
        self.threshold = threshold
        self.num_perm = num_perm
        self.b, self.r = self._optimal_params(threshold, num_perm)
        self.hashtables: list[dict[int, list[str]]] = [{} for _ in range(self.b)]
    
    def _optimal_params(self, threshold: float, num_perm: int) -> tuple[int, int]:
        """Find optimal band/row configuration for threshold."""
        best_b, best_r = 1, num_perm
        best_error = float("inf")
        
        for b in range(1, num_perm + 1):
            if num_perm % b != 0:
                continue
            r = num_perm // b
            p = 1 - (1 - threshold**r)**b
            error = abs(p - threshold)
            if error < best_error:
                best_error = error
                best_b, best_r = b, r
        
        return best_b, best_r
    
    def _hash_band(self, hashvalues: list[int], band_idx: int) -> int:
        """Hash a band of the MinHash signature."""
        start = band_idx * self.r
        end = start + self.r
        band = tuple(hashvalues[start:end])
        return hash(band)
    
    def insert(self, key: str, minhash: _PureMinHash) -> None:
        """Insert a document into the LSH index."""
        for i in range(self.b):
            band_hash = self._hash_band(minhash.hashvalues, i)
            if band_hash not in self.hashtables[i]:
                self.hashtables[i][band_hash] = []
            self.hashtables[i][band_hash].append(key)
    
    def query(self, minhash: _PureMinHash) -> list[str]:
        """Query for candidate duplicates."""
        candidates: set[str] = set()
        for i in range(self.b):
            band_hash = self._hash_band(minhash.hashvalues, i)
            if band_hash in self.hashtables[i]:
                candidates.update(self.hashtables[i][band_hash])
        return list(candidates)


class NearDuplicateDetector:
    """Near-duplicate detector using MinHash LSH.
    
    This class provides efficient near-duplicate detection for text documents.
    It uses MinHash signatures to create compact document representations and
    LSH (Locality-Sensitive Hashing) for fast candidate retrieval.
    
    Args:
        num_perm: Number of permutations for MinHash (more = more accurate but slower)
        threshold: Jaccard similarity threshold for duplicate detection (0.0-1.0)
        shingle_size: Size of n-gram shingles (words)
        
    Example:
        detector = NearDuplicateDetector(threshold=0.8)
        detector.add("doc1", "Machine learning is a subset of artificial intelligence.")
        
        result = detector.query("Machine learning is part of artificial intelligence.")
        print(f"Is duplicate: {result.is_duplicate}")  # True
        print(f"Similarity: {result.similarity:.2f}")  # ~0.85
    """
    
    def __init__(
        self,
        num_perm: int = 128,
        threshold: float = 0.8,
        shingle_size: int = 3,
    ):
        if not 0.0 < threshold <= 1.0:
            raise ValueError("threshold must be between 0 and 1")
        if num_perm < 16:
            raise ValueError("num_perm must be at least 16")
        if shingle_size < 1:
            raise ValueError("shingle_size must be at least 1")
        
        self.num_perm = num_perm
        self.threshold = threshold
        self.shingle_size = shingle_size
        self._use_datasketch = HAS_DATASKETCH
        
        if self._use_datasketch:
            self._lsh = datasketch.MinHashLSH(threshold=threshold, num_perm=num_perm)
        else:
            self._lsh = _PureMinHashLSH(threshold=threshold, num_perm=num_perm)
        
        self._signatures: dict[str, Any] = {}
        self._total_shingles = 0
    
    def _tokenize(self, text: str) -> Iterator[str]:
        """Generate word n-gram shingles from text."""
        text = text.lower()
        words = text.split()
        for i in range(len(words) - self.shingle_size + 1):
            yield " ".join(words[i:i + self.shingle_size])
    
    def _create_minhash(self, text: str) -> Any:
        """Create MinHash signature for text."""
        if self._use_datasketch:
            mh = datasketch.MinHash(num_perm=self.num_perm)
        else:
            mh = _PureMinHash(num_perm=self.num_perm)
        
        shingle_count = 0
        for shingle in self._tokenize(text):
            mh.update(shingle.encode("utf-8"))
            shingle_count += 1
        
        self._total_shingles += shingle_count
        return mh
    
    def add(self, doc_id: str, text: str) -> None:
        """Add a document to the index."""
        if doc_id in self._signatures:
            raise ValueError(f"Document {doc_id} already in index")
        
        mh = self._create_minhash(text)
        self._lsh.insert(doc_id, mh)
        self._signatures[doc_id] = mh
    
    def query(self, text: str) -> DuplicateResult:
        """Check if text is a near-duplicate of indexed documents."""
        start = time.perf_counter()
        
        mh = self._create_minhash(text)
        candidates = self._lsh.query(mh)
        
        if not candidates:
            elapsed = (time.perf_counter() - start) * 1000
            return DuplicateResult(
                is_duplicate=False, similarity=0.0, matched_ids=[], query_time_ms=elapsed,
            )
        
        matches = []
        best_similarity = 0.0
        
        for cand_id in candidates:
            cand_mh = self._signatures[cand_id]
            similarity = mh.jaccard(cand_mh)
            
            if similarity >= self.threshold:
                matches.append(cand_id)
                best_similarity = max(best_similarity, similarity)
        
        elapsed = (time.perf_counter() - start) * 1000
        
        return DuplicateResult(
            is_duplicate=len(matches) > 0,
            similarity=best_similarity,
            matched_ids=matches,
            query_time_ms=elapsed,
        )
    
    def contains(self, doc_id: str) -> bool:
        """Check if document ID is in the index."""
        return doc_id in self._signatures
    
    def remove(self, doc_id: str) -> bool:
        """Remove a document from the index."""
        if doc_id in self._signatures:
            del self._signatures[doc_id]
            return True
        return False
    
    def get_stats(self) -> DetectorStats:
        """Get statistics about the detector."""
        doc_count = len(self._signatures)
        bytes_per_sig = self.num_perm * 4
        memory_mb = (doc_count * bytes_per_sig * 3) / (1024 * 1024)
        
        return DetectorStats(
            document_count=doc_count,
            total_shingles=self._total_shingles,
            avg_shingles_per_doc=self._total_shingles / max(1, doc_count),
            memory_estimate_mb=memory_mb,
        )
    
    def clear(self) -> None:
        """Clear all documents from the index."""
        if self._use_datasketch:
            self._lsh = datasketch.MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        else:
            self._lsh = _PureMinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        self._signatures.clear()
        self._total_shingles = 0


def create_detector(threshold: float = 0.8, num_perm: int = 128) -> NearDuplicateDetector:
    """Create a near-duplicate detector with sensible defaults."""
    return NearDuplicateDetector(threshold=threshold, num_perm=num_perm, shingle_size=3)


__all__ = [
    "NearDuplicateDetector",
    "DuplicateResult",
    "DetectorStats",
    "create_detector",
]
