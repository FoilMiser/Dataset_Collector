"""Tests for collector_core/denylist_matcher.py.

P3.1E: Tests for denylist matching functionality including:
- extract_domain() with malformed URLs
- _domain_matches() subdomain logic
- denylist_hits() regex patterns
"""

from __future__ import annotations

import pytest

from collector_core.denylist_matcher import (
    extract_domain,
    _domain_matches,
    _normalize_denylist,
    denylist_hits,
    build_denylist_haystack,
    _iter_hay_values,
)


class TestExtractDomain:
    """Tests for extract_domain function."""

    def test_simple_url(self):
        """Should extract domain from simple URL."""
        assert extract_domain("https://example.com/path") == "example.com"

    def test_url_with_port(self):
        """Should extract domain from URL with port."""
        assert extract_domain("https://example.com:8080/path") == "example.com"

    def test_url_with_subdomain(self):
        """Should extract full domain including subdomain."""
        assert extract_domain("https://sub.example.com/path") == "sub.example.com"

    def test_ftp_url(self):
        """Should extract domain from FTP URL."""
        assert extract_domain("ftp://files.example.com/data.txt") == "files.example.com"

    def test_no_scheme(self):
        """Should handle URL without scheme."""
        # urlparse treats this differently
        result = extract_domain("example.com/path")
        assert result == "" or result == "example.com"

    def test_malformed_url_empty(self):
        """Should return empty string for empty input."""
        assert extract_domain("") == ""

    def test_malformed_url_just_scheme(self):
        """Should return empty string for just scheme."""
        assert extract_domain("https://") == ""

    def test_malformed_url_garbage(self):
        """Should handle garbage input gracefully."""
        assert extract_domain("not a url at all") == ""

    def test_ip_address(self):
        """Should extract IP address as domain."""
        assert extract_domain("http://192.168.1.1/path") == "192.168.1.1"

    def test_localhost(self):
        """Should handle localhost."""
        assert extract_domain("http://localhost:3000/") == "localhost"


class TestDomainMatches:
    """Tests for _domain_matches function."""

    def test_exact_match(self):
        """Should match exact domain."""
        assert _domain_matches("example.com", "example.com") is True

    def test_exact_match_case_insensitive(self):
        """Should match case-insensitively."""
        assert _domain_matches("EXAMPLE.COM", "example.com") is True
        assert _domain_matches("example.com", "EXAMPLE.COM") is True

    def test_subdomain_match(self):
        """Should match subdomain of target."""
        assert _domain_matches("sub.example.com", "example.com") is True

    def test_deep_subdomain_match(self):
        """Should match deep subdomain."""
        assert _domain_matches("deep.sub.example.com", "example.com") is True

    def test_no_match_different_domain(self):
        """Should not match different domain."""
        assert _domain_matches("other.com", "example.com") is False

    def test_no_match_partial(self):
        """Should not match partial domain names."""
        # notexample.com should NOT match example.com
        assert _domain_matches("notexample.com", "example.com") is False

    def test_no_match_suffix_without_dot(self):
        """Should not match when suffix doesn't have dot separator."""
        assert _domain_matches("fakeexample.com", "example.com") is False

    def test_empty_host(self):
        """Should return False for empty host."""
        assert _domain_matches("", "example.com") is False

    def test_empty_target(self):
        """Should return False for empty target."""
        assert _domain_matches("example.com", "") is False

    def test_both_empty(self):
        """Should return False when both empty."""
        assert _domain_matches("", "") is False


class TestNormalizeDenylist:
    """Tests for _normalize_denylist function."""

    def test_empty_data(self):
        """Should handle empty input."""
        result = _normalize_denylist({})
        assert result["patterns"] == []
        assert result["domain_patterns"] == []
        assert result["publisher_patterns"] == []

    def test_normalize_patterns(self):
        """Should normalize pattern entries."""
        data = {
            "patterns": [
                {"type": "substring", "value": "test", "severity": "hard_red"},
            ]
        }
        result = _normalize_denylist(data)
        assert len(result["patterns"]) == 1
        assert result["patterns"][0]["type"] == "substring"
        assert result["patterns"][0]["value"] == "test"
        assert result["patterns"][0]["severity"] == "hard_red"

    def test_normalize_domain_patterns(self):
        """Should normalize domain pattern entries."""
        data = {
            "domain_patterns": [
                {"domain": "example.com", "severity": "hard_red"},
            ]
        }
        result = _normalize_denylist(data)
        assert len(result["domain_patterns"]) == 1
        assert result["domain_patterns"][0]["domain"] == "example.com"

    def test_skip_invalid_patterns(self):
        """Should skip non-dict entries."""
        data = {
            "patterns": [
                "string entry",
                {"type": "substring", "value": "valid"},
            ]
        }
        result = _normalize_denylist(data)
        assert len(result["patterns"]) == 1

    def test_skip_empty_value(self):
        """Should skip patterns with empty value."""
        data = {
            "patterns": [
                {"type": "substring", "value": ""},
                {"type": "substring", "value": "valid"},
            ]
        }
        result = _normalize_denylist(data)
        assert len(result["patterns"]) == 1

    def test_default_fields(self):
        """Should set default fields when not provided."""
        data = {
            "patterns": [
                {"type": "substring", "value": "test"},
            ]
        }
        result = _normalize_denylist(data)
        expected_fields = ["id", "name", "license_evidence_url", "download_urls", "download_blob"]
        assert result["patterns"][0]["fields"] == expected_fields


class TestDenylistHits:
    """Tests for denylist_hits function."""

    def test_substring_match(self):
        """Should detect substring matches."""
        denylist = {
            "patterns": [
                {
                    "type": "substring",
                    "value": "sci-hub",
                    "fields": ["id", "name"],
                    "severity": "hard_red",
                    "reason": "Piracy",
                }
            ]
        }
        hay = {"id": "sci-hub-dataset", "name": "Test"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1
        assert hits[0]["field"] == "id"
        assert hits[0]["pattern"] == "sci-hub"

    def test_no_match(self):
        """Should return empty list when no matches."""
        denylist = {
            "patterns": [
                {"type": "substring", "value": "sci-hub", "fields": ["id"], "severity": "hard_red"}
            ]
        }
        hay = {"id": "legitimate-dataset"}
        hits = denylist_hits(denylist, hay)
        assert hits == []

    def test_regex_match(self):
        """Should detect regex matches."""
        denylist = {
            "patterns": [
                {
                    "type": "regex",
                    "value": r"pirat(e|ed|ing)",
                    "fields": ["name"],
                    "severity": "hard_red",
                }
            ]
        }
        hay = {"name": "Pirated Books Collection"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1

    def test_invalid_regex(self):
        """Should skip invalid regex patterns."""
        denylist = {
            "patterns": [
                {"type": "regex", "value": "[invalid", "fields": ["name"], "severity": "hard_red"}
            ]
        }
        hay = {"name": "some text"}
        # Should not raise
        hits = denylist_hits(denylist, hay)
        assert hits == []

    def test_domain_pattern(self):
        """Should detect domain pattern matches."""
        denylist = {
            "domain_patterns": [
                {"domain": "sci-hub.se", "severity": "hard_red"}
            ]
        }
        hay = {"license_evidence_url": "https://sci-hub.se/paper.pdf"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1
        assert hits[0]["type"] == "domain"

    def test_subdomain_pattern(self):
        """Should match subdomains in domain patterns."""
        denylist = {
            "domain_patterns": [
                {"domain": "sci-hub.se", "severity": "hard_red"}
            ]
        }
        hay = {"license_evidence_url": "https://www.sci-hub.se/paper.pdf"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1

    def test_publisher_pattern(self):
        """Should detect publisher pattern matches."""
        denylist = {
            "publisher_patterns": [
                {"publisher": "Pirate Press", "severity": "hard_red"}
            ]
        }
        hay = {"publisher": "Pirate Press Publishing"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1

    def test_case_insensitive_substring(self):
        """Substring matching should be case-insensitive."""
        denylist = {
            "patterns": [
                {"type": "substring", "value": "SCI-HUB", "fields": ["name"], "severity": "hard_red"}
            ]
        }
        hay = {"name": "sci-hub mirror"}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1

    def test_empty_denylist(self):
        """Should handle empty/None denylist."""
        assert denylist_hits({}, {"id": "test"}) == []
        assert denylist_hits(None, {"id": "test"}) == []

    def test_list_field_values(self):
        """Should handle list values in hay fields."""
        denylist = {
            "patterns": [
                {"type": "substring", "value": "pirate", "fields": ["download_urls"], "severity": "hard_red"}
            ]
        }
        hay = {"download_urls": ["https://good.com", "https://pirate.com/file"]}
        hits = denylist_hits(denylist, hay)
        assert len(hits) == 1


class TestIterHayValues:
    """Tests for _iter_hay_values helper."""

    def test_string_value(self):
        """Should return single string in list."""
        assert _iter_hay_values("test") == ["test"]

    def test_list_value(self):
        """Should return list as strings."""
        assert _iter_hay_values(["a", "b"]) == ["a", "b"]

    def test_empty_string(self):
        """Should return empty list for empty string."""
        assert _iter_hay_values("") == []

    def test_none(self):
        """Should return empty list for None."""
        assert _iter_hay_values(None) == []

    def test_mixed_list(self):
        """Should filter None values from list."""
        assert _iter_hay_values(["a", None, "b"]) == ["a", "b"]


class TestBuildDenylistHaystack:
    """Tests for build_denylist_haystack function."""

    def test_basic_build(self):
        """Should build haystack from target metadata."""
        target = {"publisher": "Test Publisher"}
        result = build_denylist_haystack(
            tid="test-id",
            name="Test Dataset",
            evidence_url="https://example.com/license",
            download_urls=["https://example.com/data.zip"],
            target=target,
        )
        assert result["id"] == "test-id"
        assert result["name"] == "Test Dataset"
        assert result["license_evidence_url"] == "https://example.com/license"
        assert result["download_urls"] == ["https://example.com/data.zip"]
        assert result["publisher"] == "Test Publisher"
        assert "data.zip" in result["download_blob"]

    def test_empty_publisher(self):
        """Should handle empty publisher."""
        result = build_denylist_haystack(
            tid="test", name="Test", evidence_url="", download_urls=[], target={}
        )
        assert result["publisher"] == ""

    def test_multiple_download_urls(self):
        """Should join download URLs into blob."""
        urls = ["https://a.com/1", "https://b.com/2"]
        result = build_denylist_haystack(
            tid="test", name="Test", evidence_url="", download_urls=urls, target={}
        )
        assert "a.com" in result["download_blob"]
        assert "b.com" in result["download_blob"]
