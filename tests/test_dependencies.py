from __future__ import annotations

import types

from collector_core import dependencies


def test_try_import_missing_module_returns_none() -> None:
    assert dependencies._try_import("module_that_does_not_exist") is None


def test_try_import_attribute_lookup() -> None:
    resolved = dependencies._try_import("types", "SimpleNamespace")
    assert resolved is types.SimpleNamespace


def test_requires_returns_hint_for_missing_dependency() -> None:
    message = dependencies.requires("requests", None, install="pip install requests")
    assert message == "missing dependency: requests (install: pip install requests)"


def test_requires_returns_none_when_dependency_present() -> None:
    assert dependencies.requires("requests", object(), install="pip install requests") is None
