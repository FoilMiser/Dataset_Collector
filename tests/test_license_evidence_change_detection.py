"""
test_license_evidence_change_detection.py

Regression tests for license evidence change detection.

Covers:
- Timestamp/date changes only → should NOT flip classification (cosmetic change)
- HTML formatting-only changes → should be ignored if normalization strips it
- Added/removed restriction keywords ("no ai", "no machine learning") → should be detected
- Policy variations: raw, normalized, either
- Cosmetic change handling with treat_as_changed policy

All tests are local (no network calls).
"""

from __future__ import annotations

from collector_core.pipeline_driver_base import (
    compute_normalized_text_hash,
    compute_signoff_mismatches,
    html_to_text,
    normalize_evidence_text,
    resolve_evidence_change,
)
from collector_core.utils import contains_any

# =============================================================================
# resolve_evidence_change policy tests
# =============================================================================


class TestResolveEvidenceChange:
    """Tests for the resolve_evidence_change function."""

    def test_no_change_returns_false(self) -> None:
        """When nothing changed, should return False for all policies."""
        for policy in ("raw", "normalized", "either"):
            result = resolve_evidence_change(
                raw_changed=False,
                normalized_changed=False,
                cosmetic_change=False,
                evidence_policy=policy,
                cosmetic_policy="warn_only",
            )
            assert result is False, f"Policy {policy} should return False when nothing changed"

    def test_raw_policy_only_considers_raw_changes(self) -> None:
        """With raw policy, only raw_changed matters."""
        # Raw changed, normalized unchanged
        assert (
            resolve_evidence_change(
                raw_changed=True,
                normalized_changed=False,
                cosmetic_change=True,
                evidence_policy="raw",
                cosmetic_policy="warn_only",
            )
            is True
        )

        # Raw unchanged, normalized changed
        assert (
            resolve_evidence_change(
                raw_changed=False,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy="raw",
                cosmetic_policy="warn_only",
            )
            is False
        )

    def test_normalized_policy_only_considers_normalized_changes(self) -> None:
        """With normalized policy, only normalized_changed matters."""
        # Raw changed, normalized unchanged (cosmetic change)
        assert (
            resolve_evidence_change(
                raw_changed=True,
                normalized_changed=False,
                cosmetic_change=True,
                evidence_policy="normalized",
                cosmetic_policy="warn_only",
            )
            is False
        )

        # Both changed
        assert (
            resolve_evidence_change(
                raw_changed=True,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy="normalized",
                cosmetic_policy="warn_only",
            )
            is True
        )

        # Only normalized changed (edge case)
        assert (
            resolve_evidence_change(
                raw_changed=False,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy="normalized",
                cosmetic_policy="warn_only",
            )
            is True
        )

    def test_either_policy_considers_both(self) -> None:
        """With either policy, either raw_changed or normalized_changed triggers."""
        # Only raw changed
        assert (
            resolve_evidence_change(
                raw_changed=True,
                normalized_changed=False,
                cosmetic_change=True,
                evidence_policy="either",
                cosmetic_policy="warn_only",
            )
            is True
        )

        # Only normalized changed
        assert (
            resolve_evidence_change(
                raw_changed=False,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy="either",
                cosmetic_policy="warn_only",
            )
            is True
        )

        # Both changed
        assert (
            resolve_evidence_change(
                raw_changed=True,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy="either",
                cosmetic_policy="warn_only",
            )
            is True
        )

    def test_cosmetic_change_warn_only_does_not_trigger(self) -> None:
        """With warn_only policy, cosmetic changes don't trigger evidence change."""
        result = resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        assert result is False

    def test_cosmetic_change_treat_as_changed_triggers(self) -> None:
        """With treat_as_changed policy, cosmetic changes trigger evidence change."""
        result = resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            evidence_policy="normalized",
            cosmetic_policy="treat_as_changed",
        )
        assert result is True

    def test_cosmetic_change_treat_as_changed_even_with_normalized_policy(self) -> None:
        """Cosmetic changes with treat_as_changed override normalized policy."""
        # Even if normalized policy would say no change, cosmetic_policy forces it
        result = resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            evidence_policy="normalized",
            cosmetic_policy="treat_as_changed",
        )
        assert result is True


# =============================================================================
# compute_signoff_mismatches tests
# =============================================================================


class TestComputeSignoffMismatches:
    """Tests for the compute_signoff_mismatches function."""

    def test_no_previous_signoff_no_mismatch(self) -> None:
        """When no previous signoff exists, should not report mismatch."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha=None,
            signoff_normalized_sha=None,
            current_raw_sha="abc123",
            current_normalized_sha="def456",
            text_extraction_failed=False,
        )
        assert raw_mismatch is False
        assert normalized_mismatch is False
        assert cosmetic is False

    def test_same_hashes_no_mismatch(self) -> None:
        """When hashes match, should not report mismatch."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="abc123",
            current_normalized_sha="def456",
            text_extraction_failed=False,
        )
        assert raw_mismatch is False
        assert normalized_mismatch is False
        assert cosmetic is False

    def test_raw_differs_normalized_same_is_cosmetic(self) -> None:
        """When raw differs but normalized same, it's a cosmetic change."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="xyz789",  # Different raw
            current_normalized_sha="def456",  # Same normalized
            text_extraction_failed=False,
        )
        assert raw_mismatch is True
        assert normalized_mismatch is False
        assert cosmetic is True

    def test_both_differ_not_cosmetic(self) -> None:
        """When both raw and normalized differ, it's not cosmetic."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="xyz789",  # Different
            current_normalized_sha="uvw012",  # Different
            text_extraction_failed=False,
        )
        assert raw_mismatch is True
        assert normalized_mismatch is True
        assert cosmetic is False

    def test_text_extraction_failed_forces_normalized_mismatch(self) -> None:
        """When text extraction fails and raw differs, normalized is also marked as mismatched."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="xyz789",
            current_normalized_sha="def456",  # Would normally match
            text_extraction_failed=True,  # But extraction failed
        )
        assert raw_mismatch is True
        assert normalized_mismatch is True  # Forced due to extraction failure
        assert cosmetic is False

    def test_text_extraction_failed_prevents_cosmetic(self) -> None:
        """When text extraction fails, we cannot determine cosmetic change."""
        raw_mismatch, normalized_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="abc123",
            signoff_normalized_sha="def456",
            current_raw_sha="xyz789",
            current_normalized_sha="def456",
            text_extraction_failed=True,
        )
        # Cannot be cosmetic if extraction failed
        assert cosmetic is False


# =============================================================================
# normalize_evidence_text tests (timestamp/date removal)
# =============================================================================


class TestNormalizeEvidenceText:
    """Tests for text normalization that should strip timestamps/dates."""

    def test_iso_timestamp_removed(self) -> None:
        """ISO timestamps should be normalized away."""
        text = "Last updated: 2024-01-15T10:30:00Z"
        normalized = normalize_evidence_text(text)
        assert "2024-01-15" not in normalized
        assert "10:30:00" not in normalized
        assert "Last updated:" in normalized

    def test_date_only_removed(self) -> None:
        """Date-only strings should be normalized away."""
        text = "Effective date: 2024-01-15"
        normalized = normalize_evidence_text(text)
        assert "2024-01-15" not in normalized
        assert "Effective date:" in normalized

    def test_us_date_format_removed(self) -> None:
        """US date format (MM/DD/YYYY) should be normalized away."""
        text = "Published on 1/15/2024"
        normalized = normalize_evidence_text(text)
        assert "1/15/2024" not in normalized
        assert "Published on" in normalized

    def test_time_only_removed(self) -> None:
        """Time-only strings should be normalized away."""
        text = "Generated at 14:30:00"
        normalized = normalize_evidence_text(text)
        assert "14:30:00" not in normalized
        assert "Generated at" in normalized

    def test_url_querystrings_stripped(self) -> None:
        """URL query strings should be stripped."""
        text = "See https://example.com/terms?v=123&t=456 for details"
        normalized = normalize_evidence_text(text)
        assert "https://example.com/terms" in normalized
        assert "?v=123" not in normalized

    def test_whitespace_normalized(self) -> None:
        """Multiple whitespace should be collapsed."""
        text = "Multiple    spaces   and\n\nnewlines"
        normalized = normalize_evidence_text(text)
        # Should have normalized whitespace
        assert "    " not in normalized

    def test_content_preserved(self) -> None:
        """Actual content should be preserved after normalization."""
        text = "You may not use this software for machine learning purposes."
        normalized = normalize_evidence_text(text)
        assert "machine learning" in normalized
        assert "may not use" in normalized

    def test_timestamp_change_produces_same_normalized_hash(self) -> None:
        """Two texts differing only in timestamp should have same normalized hash."""
        text_v1 = "Terms of Service\nLast updated: 2024-01-01T00:00:00Z\nNo commercial use."
        text_v2 = "Terms of Service\nLast updated: 2024-06-15T12:30:00Z\nNo commercial use."

        hash_v1 = compute_normalized_text_hash(text_v1)
        hash_v2 = compute_normalized_text_hash(text_v2)

        assert hash_v1 == hash_v2, "Timestamp-only changes should produce same normalized hash"


# =============================================================================
# html_to_text tests (HTML stripping)
# =============================================================================


class TestHtmlToText:
    """Tests for HTML-to-text conversion."""

    def test_script_tags_removed(self) -> None:
        """Script tags and their content should be removed."""
        html = "<html><script>alert('hi');</script>Content</html>"
        text = html_to_text(html)
        assert "alert" not in text
        assert "Content" in text

    def test_style_tags_removed(self) -> None:
        """Style tags and their content should be removed."""
        html = "<html><style>.foo { color: red; }</style>Content</html>"
        text = html_to_text(html)
        assert "color" not in text
        assert "Content" in text

    def test_html_comments_removed(self) -> None:
        """HTML comments should be removed."""
        html = "<html><!-- This is a comment -->Content</html>"
        text = html_to_text(html)
        assert "comment" not in text
        assert "Content" in text

    def test_html_tags_removed(self) -> None:
        """HTML tags should be stripped, keeping text content."""
        html = "<div><p>Hello <strong>World</strong></p></div>"
        text = html_to_text(html)
        assert "<" not in text
        assert "Hello" in text
        assert "World" in text

    def test_html_entities_unescaped(self) -> None:
        """HTML entities should be unescaped."""
        html = "Terms &amp; Conditions &copy; 2024"
        text = html_to_text(html)
        assert "&amp;" not in text
        assert "&" in text
        assert "©" in text

    def test_html_formatting_change_produces_same_normalized_hash(self) -> None:
        """Two HTML documents with same text but different formatting should have same hash."""
        html_v1 = "<html><body><p>No commercial use allowed.</p></body></html>"
        html_v2 = "<html><body><div><span>No commercial use allowed.</span></div></body></html>"

        text_v1 = html_to_text(html_v1)
        text_v2 = html_to_text(html_v2)

        hash_v1 = compute_normalized_text_hash(text_v1)
        hash_v2 = compute_normalized_text_hash(text_v2)

        assert hash_v1 == hash_v2, (
            "HTML formatting-only changes should produce same normalized hash"
        )


# =============================================================================
# contains_any restriction phrase detection tests
# =============================================================================


class TestContainsAny:
    """Tests for restriction phrase detection."""

    def test_finds_exact_phrase(self) -> None:
        """Should find exact phrase matches."""
        text = "This data may not be used for machine learning."
        needles = ["machine learning", "ai training"]
        found = contains_any(text, needles)
        assert "machine learning" in found
        assert "ai training" not in found

    def test_finds_multiple_phrases(self) -> None:
        """Should find multiple matching phrases."""
        text = "No machine learning or AI training allowed."
        needles = ["machine learning", "ai training", "deep learning"]
        found = contains_any(text, needles)
        assert "machine learning" in found
        assert "ai training" in found
        assert "deep learning" not in found

    def test_case_insensitive(self) -> None:
        """Should match case-insensitively."""
        text = "NO MACHINE LEARNING ALLOWED"
        needles = ["machine learning"]
        found = contains_any(text, needles)
        assert "machine learning" in found

    def test_no_matches_returns_empty(self) -> None:
        """Should return empty list when no matches."""
        text = "You may use this freely."
        needles = ["machine learning", "no ai"]
        found = contains_any(text, needles)
        assert found == []

    def test_empty_needles_ignored(self) -> None:
        """Should handle empty strings in needles list."""
        text = "Some content"
        needles = ["", "missing", ""]
        found = contains_any(text, needles)
        assert found == []


# =============================================================================
# Integration scenarios from checklist requirements
# =============================================================================


class TestEvidenceChangeIntegrationScenarios:
    """Integration tests for real-world evidence change scenarios."""

    def test_timestamp_only_change_not_detected_with_normalized_policy(self) -> None:
        """
        Scenario: License terms page only changed its 'last updated' timestamp.
        Expected: Should NOT flip classification with normalized policy.
        """
        old_terms = """
        Terms of Service
        Last updated: 2024-01-01T00:00:00Z

        1. You may use this data for research purposes.
        2. Commercial use requires a license.
        """

        new_terms = """
        Terms of Service
        Last updated: 2024-06-15T14:30:00Z

        1. You may use this data for research purposes.
        2. Commercial use requires a license.
        """

        # Compute normalized hashes
        old_normalized_hash = compute_normalized_text_hash(old_terms)
        new_normalized_hash = compute_normalized_text_hash(new_terms)

        # Raw bytes would differ (timestamp is different) in real scenario
        # For this test, we simulate raw bytes differing via compute_signoff_mismatches

        # But normalized should be same (timestamp removed)
        normalized_changed = old_normalized_hash != new_normalized_hash
        assert normalized_changed is False, "Normalized hashes should match after timestamp removal"

        # Compute if this is cosmetic
        raw_mismatch, norm_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="old_raw",
            signoff_normalized_sha=old_normalized_hash,
            current_raw_sha="new_raw",  # Different
            current_normalized_sha=new_normalized_hash,  # Same
            text_extraction_failed=False,
        )

        assert raw_mismatch is True
        assert norm_mismatch is False
        assert cosmetic is True

        # With normalized policy, should NOT trigger evidence change
        changed = resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        assert changed is False, "Timestamp-only change should not trigger with normalized policy"

    def test_restriction_phrase_added_detected(self) -> None:
        """
        Scenario: New restriction phrase 'no machine learning' added to terms.
        Expected: Should detect the restriction.
        """
        old_terms = "You may use this data for any purpose."
        new_terms = "You may use this data for any purpose except machine learning training."

        restriction_phrases = ["no ai", "no machine learning", "machine learning training"]

        old_hits = contains_any(old_terms, restriction_phrases)
        new_hits = contains_any(new_terms, restriction_phrases)

        assert old_hits == [], "Old terms should have no restriction hits"
        assert "machine learning training" in new_hits, "New terms should detect restriction"

    def test_restriction_phrase_removed_detected(self) -> None:
        """
        Scenario: Restriction phrase removed from terms.
        Expected: Should detect the change (restriction no longer present).
        """
        old_terms = "This data may not be used for AI training purposes."
        new_terms = "This data may be used freely."

        restriction_phrases = ["no ai", "ai training", "machine learning"]

        old_hits = contains_any(old_terms, restriction_phrases)
        new_hits = contains_any(new_terms, restriction_phrases)

        assert "ai training" in old_hits, "Old terms should have restriction hit"
        assert new_hits == [], "New terms should have no restriction hits"

        # The restriction was removed - this should be detected as a real change
        old_normalized = compute_normalized_text_hash(old_terms)
        new_normalized = compute_normalized_text_hash(new_terms)

        assert old_normalized != new_normalized, (
            "Content change should produce different normalized hashes"
        )

    def test_html_formatting_only_change_not_detected(self) -> None:
        """
        Scenario: HTML page reformatted with different tags but same text.
        Expected: Should NOT flip classification (cosmetic change).
        """
        old_html = """
        <html>
        <head><title>Terms</title></head>
        <body>
        <h1>Terms of Service</h1>
        <p>You may use this data for research.</p>
        </body>
        </html>
        """

        new_html = """
        <html>
        <head><title>Terms</title></head>
        <body>
        <div class="header"><span>Terms of Service</span></div>
        <article><section>You may use this data for research.</section></article>
        </body>
        </html>
        """

        old_text = html_to_text(old_html)
        new_text = html_to_text(new_html)

        old_normalized = compute_normalized_text_hash(old_text)
        new_normalized = compute_normalized_text_hash(new_text)

        assert old_normalized == new_normalized, (
            "HTML formatting changes should not affect normalized hash"
        )

        # Simulate raw bytes being different
        raw_mismatch, norm_mismatch, cosmetic = compute_signoff_mismatches(
            signoff_raw_sha="old_raw",
            signoff_normalized_sha=old_normalized,
            current_raw_sha="new_raw",
            current_normalized_sha=new_normalized,
            text_extraction_failed=False,
        )

        assert raw_mismatch is True
        assert norm_mismatch is False
        assert cosmetic is True

        # With normalized policy, should NOT trigger
        changed = resolve_evidence_change(
            raw_changed=True,
            normalized_changed=False,
            cosmetic_change=True,
            evidence_policy="normalized",
            cosmetic_policy="warn_only",
        )
        assert changed is False

    def test_real_content_change_detected(self) -> None:
        """
        Scenario: Actual terms content changed (new restrictions added).
        Expected: Should be detected as evidence change.
        """
        old_terms = """
        Data License Agreement
        Version 1.0

        You may use this data for any purpose.
        """

        new_terms = """
        Data License Agreement
        Version 2.0

        You may use this data for any purpose EXCEPT:
        - Machine learning model training
        - AI system development
        """

        old_normalized = compute_normalized_text_hash(old_terms)
        new_normalized = compute_normalized_text_hash(new_terms)

        assert old_normalized != new_normalized, (
            "Real content changes should produce different hashes"
        )

        # This should trigger with any policy
        for policy in ("raw", "normalized", "either"):
            changed = resolve_evidence_change(
                raw_changed=True,
                normalized_changed=True,
                cosmetic_change=False,
                evidence_policy=policy,
                cosmetic_policy="warn_only",
            )
            assert changed is True, f"Real content change should trigger with {policy} policy"

    def test_mixed_cosmetic_and_content_change(self) -> None:
        """
        Scenario: Both timestamp updated AND new restriction added.
        Expected: Should detect as real change.
        """
        old_terms = """
        Terms
        Updated: 2024-01-01
        You may use freely.
        """

        new_terms = """
        Terms
        Updated: 2024-06-01
        You may NOT use for machine learning.
        """

        old_normalized = compute_normalized_text_hash(old_terms)
        new_normalized = compute_normalized_text_hash(new_terms)

        # Despite timestamp being normalized out, content is different
        assert old_normalized != new_normalized

        restriction_phrases = ["machine learning", "no ai"]
        old_hits = contains_any(old_terms, restriction_phrases)
        new_hits = contains_any(new_terms, restriction_phrases)

        assert old_hits == []
        assert "machine learning" in new_hits


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    """Edge case tests for evidence change detection."""

    def test_empty_text_handling(self) -> None:
        """Empty text should be handled gracefully."""
        normalized = normalize_evidence_text("")
        assert normalized == ""

        html_text = html_to_text("")
        assert html_text == ""

        found = contains_any("", ["test"])
        assert found == []

    def test_none_handling_in_normalization(self) -> None:
        """None values should be handled gracefully."""
        # normalize_evidence_text handles None via `or ""`
        normalized = normalize_evidence_text(None)  # type: ignore
        assert normalized == ""

    def test_special_characters_preserved(self) -> None:
        """Special characters in restrictions should be handled."""
        text = "No use in AI/ML systems"
        needles = ["ai/ml"]
        found = contains_any(text, needles)
        assert "ai/ml" in found

    def test_multiple_timestamps_removed(self) -> None:
        """Multiple different timestamp formats should all be removed."""
        text = """
        Created: 2024-01-15
        Updated: 2024-06-20T10:30:00Z
        Last access: 12:30:45
        Published: 3/15/2024
        Content remains.
        """
        normalized = normalize_evidence_text(text)

        # All dates/times should be gone
        assert "2024-01-15" not in normalized
        assert "2024-06-20" not in normalized
        assert "10:30:00" not in normalized
        assert "12:30:45" not in normalized
        assert "3/15/2024" not in normalized

        # Content should remain
        assert "Content remains" in normalized
