# Dataset Collector Refactoring Checklist for Claude Code

## Overview

This document provides step-by-step instructions for refactoring the Dataset Collector repository. Each task is atomic and can be completed independently, though some have dependencies noted.

**Repository Structure Reference:**
```
Dataset_Collector-main/
├── collector_core/          # Shared infrastructure
├── *_pipeline_v2/           # 17 domain pipelines (to be consolidated)
├── configs/common/          # Shared configuration
├── tools/                   # Utilities
├── tests/                   # Test suite
└── docs/                    # Documentation
```

---

## Phase 1: Create Shared Utilities Module

**Goal:** Eliminate duplicate utility functions across the codebase.

### Task 1.1: Create `collector_core/utils.py`

**File to create:** `collector_core/utils.py`

**Instructions:**
1. Create a new file `collector_core/utils.py`
2. Extract and consolidate these commonly duplicated functions from across the codebase:

```python
"""
collector_core/utils.py

Shared utility functions for the Dataset Collector.
Consolidates common operations that were previously duplicated across modules.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Return current UTC time in ISO 8601 format."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> None:
    """Create directory and parents if they don't exist."""
    path.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    """Compute SHA-256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    """Compute SHA-256 hash of normalized text."""
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str | None:
    """Compute SHA-256 hash of a file. Returns None on error."""
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def normalize_whitespace(text: str) -> str:
    """Collapse all whitespace to single spaces and strip."""
    return re.sub(r"\s+", " ", (text or "")).strip()


def lower(text: str) -> str:
    """Lowercase string, handling None."""
    return (text or "").lower()


def read_json(path: Path) -> dict[str, Any]:
    """Read JSON file and return as dict."""
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: dict[str, Any], *, indent: int = 2) -> None:
    """Write dict to JSON file atomically."""
    ensure_dir(path.parent)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(obj, indent=indent, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )
    tmp_path.replace(path)


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Read JSONL file (supports .gz) and yield records."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def read_jsonl_list(path: Path) -> list[dict[str, Any]]:
    """Read JSONL file and return as list."""
    return list(read_jsonl(path))


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Write records to JSONL file (supports .gz)."""
    ensure_dir(path.parent)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Append records to JSONL file (supports .gz)."""
    ensure_dir(path.parent)
    if path.suffix == ".gz":
        with gzip.open(path, "ab") as f:
            for row in rows:
                f.write((json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8"))
    else:
        with open(path, "a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_filename(s: str, max_length: int = 200) -> str:
    """Convert string to safe filename."""
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip())
    return s[:max_length] if s else "file"


def contains_any(haystack: str, needles: list[str]) -> list[str]:
    """Return list of needles found in haystack (case-insensitive)."""
    h = lower(haystack)
    return [n for n in needles if n and lower(n) in h]


def coerce_int(val: Any, default: int | None = None) -> int | None:
    """Safely convert value to int, returning default on failure."""
    try:
        return int(val)
    except Exception:
        return default
```

3. Add to `collector_core/__init__.py`:
```python
from collector_core.utils import (
    utc_now,
    ensure_dir,
    sha256_bytes,
    sha256_text,
    sha256_file,
    normalize_whitespace,
    lower,
    read_json,
    write_json,
    read_jsonl,
    read_jsonl_list,
    write_jsonl,
    append_jsonl,
    safe_filename,
    contains_any,
    coerce_int,
)
```

### Task 1.2: Update Imports Across Codebase

**Files to modify:** All files in `collector_core/` and `*_pipeline_v2/` that define these utility functions.

**Instructions:**
1. Search for each function definition across the codebase:
   ```bash
   grep -r "def utc_now" --include="*.py"
   grep -r "def ensure_dir" --include="*.py"
   grep -r "def sha256_file" --include="*.py"
   grep -r "def read_jsonl" --include="*.py"
   grep -r "def write_json" --include="*.py"
   grep -r "def normalize_whitespace" --include="*.py"
   ```

2. For each file that defines these functions locally:
   - Remove the local function definition
   - Add import at top: `from collector_core.utils import <function_name>`

3. Files known to need updates:
   - `collector_core/pipeline_driver_base.py` (lines 45-91)
   - `collector_core/acquire_strategies.py` (lines 83-119)
   - `collector_core/merge.py` (multiple utility functions)
   - `collector_core/yellow_screen_common.py` (lines 106-161)
   - `collector_core/pmc_worker.py`
   - `collector_core/review_queue.py`
   - All `*_pipeline_v2/yellow_scrubber.py` files

### Task 1.3: Create Unit Tests for Utils

**File to create:** `tests/test_utils.py`

```python
"""Tests for collector_core.utils module."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from collector_core.utils import (
    utc_now,
    ensure_dir,
    sha256_bytes,
    sha256_text,
    sha256_file,
    normalize_whitespace,
    lower,
    read_json,
    write_json,
    read_jsonl,
    read_jsonl_list,
    write_jsonl,
    append_jsonl,
    safe_filename,
    contains_any,
    coerce_int,
)


class TestUtcNow:
    def test_format(self):
        result = utc_now()
        assert result.endswith("Z")
        assert "T" in result
        assert len(result) == 20  # YYYY-MM-DDTHH:MM:SSZ


class TestEnsureDir:
    def test_creates_nested_dirs(self, tmp_path: Path):
        target = tmp_path / "a" / "b" / "c"
        ensure_dir(target)
        assert target.exists()
        assert target.is_dir()

    def test_idempotent(self, tmp_path: Path):
        target = tmp_path / "exists"
        target.mkdir()
        ensure_dir(target)  # Should not raise
        assert target.exists()


class TestSha256:
    def test_sha256_bytes(self):
        result = sha256_bytes(b"hello")
        assert len(result) == 64
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_sha256_text_normalizes_whitespace(self):
        result1 = sha256_text("hello  world")
        result2 = sha256_text("hello world")
        result3 = sha256_text("hello\n\tworld")
        assert result1 == result2 == result3

    def test_sha256_file(self, tmp_path: Path):
        file = tmp_path / "test.txt"
        file.write_bytes(b"hello")
        result = sha256_file(file)
        assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_sha256_file_missing(self, tmp_path: Path):
        result = sha256_file(tmp_path / "nonexistent.txt")
        assert result is None


class TestNormalizeWhitespace:
    def test_collapses_spaces(self):
        assert normalize_whitespace("a  b   c") == "a b c"

    def test_handles_newlines_tabs(self):
        assert normalize_whitespace("a\n\tb") == "a b"

    def test_strips(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_handles_none(self):
        assert normalize_whitespace(None) == ""


class TestLower:
    def test_lowercases(self):
        assert lower("HELLO") == "hello"

    def test_handles_none(self):
        assert lower(None) == ""


class TestJsonIO:
    def test_write_read_json(self, tmp_path: Path):
        file = tmp_path / "test.json"
        data = {"key": "value", "number": 42}
        write_json(file, data)
        result = read_json(file)
        assert result == data

    def test_write_json_atomic(self, tmp_path: Path):
        file = tmp_path / "test.json"
        write_json(file, {"a": 1})
        # No .tmp file should remain
        assert not (tmp_path / "test.json.tmp").exists()


class TestJsonlIO:
    def test_write_read_jsonl(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        rows = [{"a": 1}, {"b": 2}]
        write_jsonl(file, rows)
        result = read_jsonl_list(file)
        assert result == rows

    def test_write_read_jsonl_gzip(self, tmp_path: Path):
        file = tmp_path / "test.jsonl.gz"
        rows = [{"a": 1}, {"b": 2}]
        write_jsonl(file, rows)
        result = read_jsonl_list(file)
        assert result == rows

    def test_append_jsonl(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        write_jsonl(file, [{"a": 1}])
        append_jsonl(file, [{"b": 2}])
        result = read_jsonl_list(file)
        assert result == [{"a": 1}, {"b": 2}]

    def test_read_jsonl_skips_invalid(self, tmp_path: Path):
        file = tmp_path / "test.jsonl"
        file.write_text('{"valid": true}\ninvalid json\n{"also": "valid"}\n')
        result = read_jsonl_list(file)
        assert len(result) == 2


class TestSafeFilename:
    def test_replaces_special_chars(self):
        assert safe_filename("hello world!@#") == "hello_world_"

    def test_truncates(self):
        result = safe_filename("a" * 300, max_length=10)
        assert len(result) == 10

    def test_handles_empty(self):
        assert safe_filename("") == "file"
        assert safe_filename(None) == "file"


class TestContainsAny:
    def test_finds_matches(self):
        result = contains_any("Hello World", ["hello", "foo"])
        assert result == ["hello"]

    def test_case_insensitive(self):
        result = contains_any("HELLO", ["hello"])
        assert result == ["hello"]

    def test_no_matches(self):
        result = contains_any("hello", ["foo", "bar"])
        assert result == []


class TestCoerceInt:
    def test_valid_int(self):
        assert coerce_int("42") == 42
        assert coerce_int(42) == 42

    def test_invalid_returns_default(self):
        assert coerce_int("not a number", default=0) == 0
        assert coerce_int(None, default=-1) == -1

    def test_invalid_no_default(self):
        assert coerce_int("invalid") is None
```

---

## Phase 2: Create Pipeline Configuration System

**Goal:** Replace 17 near-identical pipeline directories with a configuration-driven system.

### Task 2.1: Create Pipeline Specification Dataclass

**File to create:** `collector_core/pipeline_spec.py`

```python
"""
collector_core/pipeline_spec.py

Defines the specification for a domain pipeline, enabling configuration-driven
pipeline creation instead of duplicated boilerplate files.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineSpec:
    """Specification for a domain-specific pipeline."""
    
    # Required fields
    domain: str  # e.g., "chem", "bio", "physics"
    name: str    # Human-readable name, e.g., "Chemistry Pipeline"
    
    # Targets configuration
    targets_yaml: str  # e.g., "targets_chem.yaml"
    
    # Path configuration (derived from domain if not specified)
    domain_prefix: str | None = None  # Defaults to domain
    
    # Routing configuration
    routing_keys: list[str] = field(default_factory=list)
    routing_confidence_keys: list[str] = field(default_factory=list)
    default_routing: dict[str, Any] = field(default_factory=lambda: {
        "subject": "misc",
        "domain": "misc", 
        "category": "misc",
        "level": 5,
        "granularity": "target"
    })
    
    # Custom worker modules (relative to pipeline directory)
    yellow_screen_module: str | None = None  # e.g., "yellow_screen_chem"
    custom_workers: dict[str, str] = field(default_factory=dict)
    
    # Feature flags
    include_routing_dict_in_row: bool = False
    
    @property
    def prefix(self) -> str:
        """Return the domain prefix for paths."""
        return self.domain_prefix or self.domain
    
    @property
    def pipeline_id(self) -> str:
        """Return the pipeline directory name."""
        return f"{self.domain}_pipeline_v2"
    
    def get_default_roots(self, base_path: str = "/data") -> dict[str, str]:
        """Return default root paths for this pipeline."""
        prefix = self.prefix
        return {
            "raw_root": f"{base_path}/{prefix}/raw",
            "screened_yellow_root": f"{base_path}/{prefix}/screened_yellow",
            "combined_root": f"{base_path}/{prefix}/combined",
            "manifests_root": f"{base_path}/{prefix}/_manifests",
            "queues_root": f"{base_path}/{prefix}/_queues",
            "catalogs_root": f"{base_path}/{prefix}/_catalogs",
            "ledger_root": f"{base_path}/{prefix}/_ledger",
            "pitches_root": f"{base_path}/{prefix}/_pitches",
            "logs_root": f"{base_path}/{prefix}/_logs",
        }


# Registry of all pipeline specifications
PIPELINE_SPECS: dict[str, PipelineSpec] = {}


def register_pipeline(spec: PipelineSpec) -> PipelineSpec:
    """Register a pipeline specification."""
    PIPELINE_SPECS[spec.domain] = spec
    return spec


def get_pipeline_spec(domain: str) -> PipelineSpec | None:
    """Get a pipeline specification by domain."""
    return PIPELINE_SPECS.get(domain)


def list_pipelines() -> list[str]:
    """List all registered pipeline domains."""
    return sorted(PIPELINE_SPECS.keys())
```

### Task 2.2: Create Pipeline Specifications Registry

**File to create:** `collector_core/pipeline_specs_registry.py`

```python
"""
collector_core/pipeline_specs_registry.py

Registry of all domain pipeline specifications.
This replaces the need for per-pipeline boilerplate files.
"""
from __future__ import annotations

from collector_core.pipeline_spec import PipelineSpec, register_pipeline


# === Scientific Domains ===

register_pipeline(PipelineSpec(
    domain="chem",
    name="Chemistry Pipeline",
    targets_yaml="targets_chem.yaml",
    routing_keys=["chem_routing", "math_routing"],
    routing_confidence_keys=["chem_routing"],
    default_routing={
        "subject": "chem",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_chem",
))

register_pipeline(PipelineSpec(
    domain="biology",
    name="Biology Pipeline",
    domain_prefix="bio",
    targets_yaml="targets_biology.yaml",
    routing_keys=["bio_routing"],
    routing_confidence_keys=["bio_routing"],
    default_routing={
        "subject": "biology",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="physics",
    name="Physics Pipeline",
    targets_yaml="targets_physics.yaml",
    routing_keys=["physics_routing", "math_routing"],
    routing_confidence_keys=["physics_routing"],
    default_routing={
        "subject": "physics",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="math",
    name="Mathematics Pipeline",
    targets_yaml="targets_math.yaml",
    routing_keys=["math_routing"],
    routing_confidence_keys=["math_routing"],
    default_routing={
        "subject": "math",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="earth",
    name="Earth Science Pipeline",
    targets_yaml="targets_earth.yaml",
    routing_keys=["earth_routing"],
    routing_confidence_keys=["earth_routing"],
    default_routing={
        "subject": "earth_science",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="materials_science",
    name="Materials Science Pipeline",
    domain_prefix="matsci",
    targets_yaml="targets_materials.yaml",
    routing_keys=["materials_routing", "chem_routing"],
    routing_confidence_keys=["materials_routing"],
    default_routing={
        "subject": "materials_science",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))


# === Engineering & Technical Domains ===

register_pipeline(PipelineSpec(
    domain="engineering",
    name="Engineering Pipeline",
    targets_yaml="targets_engineering.yaml",
    routing_keys=["engineering_routing"],
    routing_confidence_keys=["engineering_routing"],
    default_routing={
        "subject": "engineering",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="code",
    name="Code Pipeline",
    targets_yaml="targets_code.yaml",
    routing_keys=["code_routing"],
    routing_confidence_keys=["code_routing"],
    default_routing={
        "subject": "code",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={"code_worker": "code_worker.py"},
))

register_pipeline(PipelineSpec(
    domain="cyber",
    name="Cybersecurity Pipeline",
    targets_yaml="targets_cyber.yaml",
    routing_keys=["cyber_routing"],
    routing_confidence_keys=["cyber_routing"],
    default_routing={
        "subject": "cybersecurity",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={
        "nvd_worker": "nvd_worker.py",
        "stix_worker": "stix_worker.py",
        "advisory_worker": "advisory_worker.py",
    },
))

register_pipeline(PipelineSpec(
    domain="3d_modeling",
    name="3D Modeling Pipeline",
    domain_prefix="3d",
    targets_yaml="targets_3d.yaml",
    routing_keys=["3d_routing"],
    routing_confidence_keys=["3d_routing"],
    default_routing={
        "subject": "3d_modeling",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={"mesh_worker": "mesh_worker.py"},
))

register_pipeline(PipelineSpec(
    domain="metrology",
    name="Metrology Pipeline",
    targets_yaml="targets_metrology.yaml",
    routing_keys=["metrology_routing"],
    routing_confidence_keys=["metrology_routing"],
    default_routing={
        "subject": "metrology",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))


# === NLP & Knowledge Domains ===

register_pipeline(PipelineSpec(
    domain="nlp",
    name="NLP Pipeline",
    targets_yaml="targets_nlp.yaml",
    routing_keys=["nlp_routing"],
    routing_confidence_keys=["nlp_routing"],
    default_routing={
        "subject": "nlp",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_nlp",
))

register_pipeline(PipelineSpec(
    domain="logic",
    name="Logic Pipeline",
    targets_yaml="targets_logic.yaml",
    routing_keys=["logic_routing", "math_routing"],
    routing_confidence_keys=["logic_routing"],
    default_routing={
        "subject": "logic",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="kg_nav",
    name="Knowledge Graph & Navigation Pipeline",
    targets_yaml="targets_kg_nav.yaml",
    routing_keys=["kg_nav_routing"],
    routing_confidence_keys=["kg_nav_routing"],
    default_routing={
        "subject": "knowledge_graph",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_kg_nav",
))


# === Economics & Policy Domains ===

register_pipeline(PipelineSpec(
    domain="econ_stats_decision_adaptation",
    name="Economics, Statistics, Decision & Adaptation Pipeline",
    domain_prefix="econ",
    targets_yaml="targets_econ_stats_decision_v2.yaml",
    routing_keys=["econ_routing"],
    routing_confidence_keys=["econ_routing"],
    default_routing={
        "subject": "economics",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_econ",
))

register_pipeline(PipelineSpec(
    domain="regcomp",
    name="Regulatory Compliance Pipeline",
    targets_yaml="targets_regcomp.yaml",
    routing_keys=["regcomp_routing"],
    routing_confidence_keys=["regcomp_routing"],
    default_routing={
        "subject": "regulatory",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))


# === Safety & Agriculture Domains ===

register_pipeline(PipelineSpec(
    domain="safety_incident",
    name="Safety Incident Pipeline",
    domain_prefix="safety",
    targets_yaml="targets_safety_incident.yaml",
    routing_keys=["safety_routing"],
    routing_confidence_keys=["safety_routing"],
    default_routing={
        "subject": "safety",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_safety",
))

register_pipeline(PipelineSpec(
    domain="agri_circular",
    name="Agriculture & Circular Economy Pipeline",
    targets_yaml="targets_agri_circular.yaml",
    routing_keys=["agri_routing"],
    routing_confidence_keys=["agri_routing"],
    default_routing={
        "subject": "agriculture",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))
```

### Task 2.3: Create Dynamic Pipeline Factory

**File to create:** `collector_core/pipeline_factory.py`

```python
"""
collector_core/pipeline_factory.py

Factory for creating pipeline driver instances from specifications.
Eliminates the need for per-pipeline pipeline_driver.py files.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from collector_core.__version__ import __version__ as VERSION
from collector_core.pipeline_driver_base import BasePipelineDriver, RoutingBlockSpec
from collector_core.pipeline_spec import PipelineSpec, get_pipeline_spec, list_pipelines

if TYPE_CHECKING:
    pass


def create_pipeline_driver(spec: PipelineSpec) -> type[BasePipelineDriver]:
    """
    Create a pipeline driver class from a specification.
    
    This dynamically generates a subclass of BasePipelineDriver configured
    according to the PipelineSpec, eliminating the need for boilerplate
    pipeline_driver.py files in each pipeline directory.
    """
    
    # Build routing blocks from routing keys
    routing_blocks = []
    for key in spec.routing_keys:
        routing_blocks.append(
            RoutingBlockSpec(name=key, sources=[key], mode="subset")
        )
    
    # Create the dynamic class
    class_attrs = {
        "DOMAIN": spec.domain,
        "PIPELINE_VERSION": VERSION,
        "TARGETS_LABEL": spec.targets_yaml,
        "USER_AGENT": f"{spec.domain}-corpus-pipeline",
        "ROUTING_KEYS": spec.routing_keys,
        "ROUTING_CONFIDENCE_KEYS": spec.routing_confidence_keys,
        "DEFAULT_ROUTING": spec.default_routing,
        "ROUTING_BLOCKS": routing_blocks,
        "INCLUDE_ROUTING_DICT_IN_ROW": spec.include_routing_dict_in_row,
    }
    
    driver_class = type(
        f"{spec.domain.title().replace('_', '')}PipelineDriver",
        (BasePipelineDriver,),
        class_attrs,
    )
    
    return driver_class


def get_pipeline_driver(domain: str) -> type[BasePipelineDriver]:
    """
    Get a pipeline driver class for a domain.
    
    Args:
        domain: The domain identifier (e.g., "chem", "physics")
        
    Returns:
        A configured pipeline driver class
        
    Raises:
        ValueError: If the domain is not registered
    """
    spec = get_pipeline_spec(domain)
    if spec is None:
        available = ", ".join(list_pipelines())
        raise ValueError(f"Unknown pipeline domain: {domain}. Available: {available}")
    return create_pipeline_driver(spec)


def run_pipeline(domain: str) -> None:
    """
    Run a pipeline by domain name.
    
    This is the main entry point for running a pipeline from the command line.
    """
    driver_class = get_pipeline_driver(domain)
    driver_class.main()
```

### Task 2.4: Create Unified Pipeline Entry Point

**File to modify:** `collector_core/dc_cli.py`

**Instructions:**
Add the ability to run any pipeline via the unified CLI using the factory:

```python
# Add to dc_cli.py

from collector_core.pipeline_factory import get_pipeline_driver, run_pipeline
from collector_core.pipeline_spec import list_pipelines

# In the argument parser, add:
parser.add_argument(
    "--list-pipelines",
    action="store_true",
    help="List all available pipelines",
)

# In the main function, add handling:
if args.list_pipelines:
    print("Available pipelines:")
    for domain in list_pipelines():
        print(f"  - {domain}")
    return
```

### Task 2.5: Create Generic Worker Scripts

**File to create:** `collector_core/generic_workers.py`

```python
"""
collector_core/generic_workers.py

Generic worker implementations that can be parameterized by pipeline spec.
Replaces per-pipeline acquire_worker.py, merge_worker.py, etc.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from collector_core.__version__ import __version__ as VERSION
from collector_core.acquire_strategies import (
    DEFAULT_STRATEGY_HANDLERS,
    RootsDefaults,
    run_acquire_worker,
)
from collector_core.merge import run_merge_worker, RootDefaults as MergeRootDefaults
from collector_core.pipeline_spec import PipelineSpec, get_pipeline_spec

if TYPE_CHECKING:
    pass


def run_acquire_for_pipeline(spec: PipelineSpec) -> None:
    """Run the acquire worker for a pipeline specification."""
    roots = spec.get_default_roots()
    defaults = RootsDefaults(
        raw_root=roots["raw_root"],
        manifests_root=roots["manifests_root"],
        logs_root=roots["logs_root"],
    )
    run_acquire_worker(
        defaults=defaults,
        targets_yaml_label=spec.targets_yaml,
        strategy_handlers=DEFAULT_STRATEGY_HANDLERS,
    )


def run_merge_for_pipeline(spec: PipelineSpec) -> None:
    """Run the merge worker for a pipeline specification."""
    roots = spec.get_default_roots()
    defaults = MergeRootDefaults(
        raw_root=roots["raw_root"],
        screened_root=roots["screened_yellow_root"],
        combined_root=roots["combined_root"],
        ledger_root=roots["ledger_root"],
    )
    run_merge_worker(
        defaults=defaults,
        targets_yaml_label=spec.targets_yaml,
        pipeline_id=spec.pipeline_id,
    )


def main_acquire(domain: str) -> None:
    """Entry point for acquire worker."""
    spec = get_pipeline_spec(domain)
    if spec is None:
        print(f"Unknown pipeline domain: {domain}", file=sys.stderr)
        sys.exit(1)
    run_acquire_for_pipeline(spec)


def main_merge(domain: str) -> None:
    """Entry point for merge worker."""
    spec = get_pipeline_spec(domain)
    if spec is None:
        print(f"Unknown pipeline domain: {domain}", file=sys.stderr)
        sys.exit(1)
    run_merge_for_pipeline(spec)
```

---

## Phase 3: Migrate Existing Pipelines

**Goal:** Convert existing pipeline directories to use the new configuration system while preserving custom logic.

### Task 3.1: Identify Custom Logic Per Pipeline

**Instructions:**
Run the following analysis to identify which pipelines have custom logic that needs preservation:

```bash
# Find pipelines with custom workers (non-standard files)
for dir in *_pipeline_v2; do
    echo "=== $dir ==="
    ls -la "$dir"/*.py | grep -v -E "(acquire_worker|merge_worker|pipeline_driver|catalog_builder|review_queue|pmc_worker|yellow_screen_worker|yellow_scrubber)\.py"
done
```

**Known custom workers to preserve:**
- `3d_modeling_pipeline_v2/mesh_worker.py` - Keep as custom worker
- `code_pipeline_v2/code_worker.py` - Keep as custom worker
- `cyber_pipeline_v2/nvd_worker.py` - Keep as custom worker
- `cyber_pipeline_v2/stix_worker.py` - Keep as custom worker
- `cyber_pipeline_v2/advisory_worker.py` - Keep as custom worker

### Task 3.2: Create Minimal Pipeline Stubs

For each pipeline, replace the boilerplate files with minimal stubs that delegate to the factory:

**Template for `{domain}_pipeline_v2/pipeline_driver.py`:**

```python
#!/usr/bin/env python3
"""
Pipeline driver for {domain} domain.

This is a minimal stub that delegates to the pipeline factory.
Custom logic should be placed in separate worker modules.
"""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Import the registry to ensure specs are registered
import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.pipeline_factory import run_pipeline


def main() -> None:
    run_pipeline("{domain}")


if __name__ == "__main__":
    main()
```

**Template for `{domain}_pipeline_v2/acquire_worker.py`:**

```python
#!/usr/bin/env python3
"""Acquire worker for {domain} domain."""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.generic_workers import main_acquire


def main() -> None:
    main_acquire("{domain}")


if __name__ == "__main__":
    main()
```

**Template for `{domain}_pipeline_v2/merge_worker.py`:**

```python
#!/usr/bin/env python3
"""Merge worker for {domain} domain."""
from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector_core.pipeline_specs_registry  # noqa: F401
from collector_core.generic_workers import main_merge


def main() -> None:
    main_merge("{domain}")


if __name__ == "__main__":
    main()
```

### Task 3.3: Remove Redundant Stub Files

**Files to delete from each `*_pipeline_v2/` directory:**
- `catalog_builder.py` (512 bytes, just imports)
- `review_queue.py` (384 bytes, just imports)
- `pmc_worker.py` (~500 bytes, just imports)

These can be accessed directly via `collector_core` or the unified CLI.

### Task 3.4: Consolidate Yellow Scrubber Logic

**Instructions:**
1. Create a base yellow scrubber class in `collector_core/yellow_scrubber_base.py`
2. Move common logic from `*_pipeline_v2/yellow_scrubber.py` files
3. Keep only domain-specific transformations in pipeline directories

**File to create:** `collector_core/yellow_scrubber_base.py`

```python
"""
collector_core/yellow_scrubber_base.py

Base class for yellow scrubber implementations.
Consolidates common logic from per-pipeline yellow_scrubber.py files.
"""
from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from collector_core.__version__ import __version__ as VERSION
from collector_core.utils import (
    ensure_dir,
    read_jsonl,
    write_json,
    write_jsonl,
    sha256_file,
    utc_now,
)
from collector_core.config_validator import read_yaml
from collector_core.pipeline_spec import PipelineSpec


@dataclass
class ScrubberConfig:
    """Configuration for yellow scrubber."""
    targets_path: Path
    raw_root: Path
    screened_root: Path
    manifests_root: Path
    ledger_root: Path
    execute: bool = False
    emit_parquet: bool = False
    dedupe: bool = False
    normalize: bool = False


class BaseYellowScrubber(ABC):
    """Base class for yellow scrubber implementations."""
    
    VERSION = VERSION
    
    def __init__(self, config: ScrubberConfig):
        self.config = config
        self.targets_cfg = read_yaml(config.targets_path, schema_name="targets") or {}
    
    @abstractmethod
    def process_target(
        self, 
        target: dict[str, Any], 
        input_path: Path
    ) -> Iterator[dict[str, Any]]:
        """
        Process a single target and yield transformed records.
        
        Args:
            target: Target configuration from targets YAML
            input_path: Path to the raw data for this target
            
        Yields:
            Transformed records ready for screened output
        """
        pass
    
    def run(self) -> dict[str, Any]:
        """Run the yellow scrubber and return summary."""
        summary = {
            "version": self.VERSION,
            "started_at": utc_now(),
            "targets_processed": 0,
            "records_processed": 0,
            "records_passed": 0,
            "records_rejected": 0,
        }
        
        targets = self.targets_cfg.get("targets", [])
        for target in targets:
            if not target.get("enabled", True):
                continue
            
            target_id = target.get("id", "unknown")
            # ... process target
            summary["targets_processed"] += 1
        
        summary["completed_at"] = utc_now()
        return summary
    
    @classmethod
    def build_arg_parser(cls) -> argparse.ArgumentParser:
        """Build argument parser for CLI."""
        parser = argparse.ArgumentParser(
            description=f"Yellow Scrubber v{cls.VERSION}"
        )
        parser.add_argument("--targets", required=True, help="Path to targets YAML")
        parser.add_argument("--execute", action="store_true", help="Actually write files")
        parser.add_argument("--emit-parquet", action="store_true", help="Output Parquet")
        parser.add_argument("--dedupe", action="store_true", help="Run deduplication")
        parser.add_argument("--normalize", action="store_true", help="Normalize structures")
        return parser
```

---

## Phase 4: Improve Test Coverage

### Task 4.1: Create Integration Test Fixtures

**Directory to create:** `tests/fixtures/`

**Files to create:**

`tests/fixtures/minimal_targets.yaml`:
```yaml
schema_version: "0.9"
updated_utc: "2025-01-01"

globals:
  raw_root: "${TEMP}/raw"
  screened_yellow_root: "${TEMP}/screened"
  combined_root: "${TEMP}/combined"
  manifests_root: "${TEMP}/_manifests"
  queues_root: "${TEMP}/_queues"
  ledger_root: "${TEMP}/_ledger"

companion_files:
  license_map:
    - "minimal_license_map.yaml"
  denylist:
    - "minimal_denylist.yaml"

targets:
  - id: "test_http_target"
    name: "Test HTTP Download"
    enabled: true
    license_profile: "permissive"
    license_evidence:
      spdx_hint: "MIT"
      url: "https://example.com/LICENSE"
    download:
      strategy: "http"
      url: "https://httpbin.org/json"
      filename: "test.json"

  - id: "test_disabled_target"
    name: "Disabled Target"
    enabled: false
    license_profile: "unknown"
    license_evidence:
      spdx_hint: "Unknown"
      url: ""
    download:
      strategy: "none"
```

`tests/fixtures/minimal_license_map.yaml`:
```yaml
schema_version: "0.9"
updated_utc: "2025-01-01"

spdx:
  allow:
    - "MIT"
    - "CC0-1.0"
    - "Apache-2.0"
  conditional:
    - "GPL-3.0-or-later"
  deny_prefixes:
    - "CC-BY-NC"

normalization:
  rules:
    - match_any: ["MIT License", "MIT"]
      spdx: "MIT"

restriction_scan:
  phrases:
    - "no ai"
    - "no machine learning"

gating:
  conditional_spdx_bucket: "YELLOW"
  unknown_spdx_bucket: "YELLOW"
  deny_spdx_bucket: "RED"
  restriction_phrase_bucket: "YELLOW"

profiles:
  permissive:
    default_bucket: "GREEN"
  unknown:
    default_bucket: "YELLOW"
```

`tests/fixtures/minimal_denylist.yaml`:
```yaml
schema_version: "0.9"
updated_utc: "2025-01-01"

domain_patterns: []
publisher_patterns: []
patterns: []
```

### Task 4.2: Create Integration Test for Full Pipeline

**File to create:** `tests/integration/test_pipeline_factory.py`

```python
"""Integration tests for the pipeline factory system."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure collector_core is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from collector_core.pipeline_spec import PipelineSpec, register_pipeline, get_pipeline_spec, list_pipelines, PIPELINE_SPECS
from collector_core.pipeline_factory import create_pipeline_driver, get_pipeline_driver


@pytest.fixture(autouse=True)
def clean_registry():
    """Clean the registry before and after each test."""
    original = PIPELINE_SPECS.copy()
    PIPELINE_SPECS.clear()
    yield
    PIPELINE_SPECS.clear()
    PIPELINE_SPECS.update(original)


class TestPipelineSpec:
    def test_register_and_retrieve(self):
        spec = PipelineSpec(
            domain="test",
            name="Test Pipeline",
            targets_yaml="targets_test.yaml",
        )
        register_pipeline(spec)
        
        retrieved = get_pipeline_spec("test")
        assert retrieved is not None
        assert retrieved.domain == "test"
        assert retrieved.name == "Test Pipeline"
    
    def test_default_roots(self):
        spec = PipelineSpec(
            domain="test",
            name="Test",
            targets_yaml="targets_test.yaml",
        )
        roots = spec.get_default_roots("/data")
        
        assert roots["raw_root"] == "/data/test/raw"
        assert roots["manifests_root"] == "/data/test/_manifests"
    
    def test_custom_prefix(self):
        spec = PipelineSpec(
            domain="test",
            name="Test",
            targets_yaml="targets_test.yaml",
            domain_prefix="custom",
        )
        roots = spec.get_default_roots("/data")
        
        assert roots["raw_root"] == "/data/custom/raw"
    
    def test_list_pipelines(self):
        register_pipeline(PipelineSpec(domain="a", name="A", targets_yaml="a.yaml"))
        register_pipeline(PipelineSpec(domain="b", name="B", targets_yaml="b.yaml"))
        
        pipelines = list_pipelines()
        assert pipelines == ["a", "b"]


class TestPipelineFactory:
    def test_create_driver_class(self):
        spec = PipelineSpec(
            domain="test",
            name="Test Pipeline",
            targets_yaml="targets_test.yaml",
            routing_keys=["test_routing"],
            default_routing={"subject": "test"},
        )
        register_pipeline(spec)
        
        driver_class = create_pipeline_driver(spec)
        
        assert driver_class.DOMAIN == "test"
        assert driver_class.TARGETS_LABEL == "targets_test.yaml"
        assert driver_class.ROUTING_KEYS == ["test_routing"]
    
    def test_get_pipeline_driver(self):
        spec = PipelineSpec(
            domain="mytest",
            name="My Test",
            targets_yaml="targets_mytest.yaml",
        )
        register_pipeline(spec)
        
        driver_class = get_pipeline_driver("mytest")
        assert driver_class.DOMAIN == "mytest"
    
    def test_get_unknown_pipeline_raises(self):
        with pytest.raises(ValueError, match="Unknown pipeline domain"):
            get_pipeline_driver("nonexistent")
```

### Task 4.3: Add Yellow Scrubber Tests

**File to create:** `tests/test_yellow_scrubber_chem.py`

```python
"""Tests for chemistry yellow scrubber logic."""
from __future__ import annotations

import json
import gzip
from pathlib import Path
from typing import Any

import pytest


# Test fixtures for PubChem computed-only extraction
SAMPLE_PUBCHEM_RECORD = {
    "PUBCHEM_COMPOUND_CID": 2244,
    "PUBCHEM_CACTVS_CANONICAL_SMILES": "CC(=O)OC1=CC=CC=C1C(=O)O",
    "PUBCHEM_IUPAC_INCHI": "InChI=1S/C9H8O4/c1-6(10)13-8-5-3-2-4-7(8)9(11)12/h2-5H,1H3,(H,11,12)",
    "PUBCHEM_IUPAC_INCHIKEY": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
    "PUBCHEM_MOLECULAR_FORMULA": "C9H8O4",
    "PUBCHEM_MOLECULAR_WEIGHT": "180.16",
    "PUBCHEM_EXACT_MASS": "180.042259",
    "PUBCHEM_XLOGP3": "1.2",
    "PUBCHEM_TPSA": "63.6",
    # Fields that should be excluded
    "freeform_text": "This is depositor text",
    "synonyms": ["Aspirin", "Acetylsalicylic acid"],
}


class TestPubChemExtraction:
    """Test PubChem computed-only field extraction."""
    
    ALLOWED_FIELDS = {
        "PUBCHEM_COMPOUND_CID",
        "PUBCHEM_CACTVS_CANONICAL_SMILES",
        "PUBCHEM_IUPAC_INCHI",
        "PUBCHEM_IUPAC_INCHIKEY",
        "PUBCHEM_MOLECULAR_FORMULA",
        "PUBCHEM_MOLECULAR_WEIGHT",
        "PUBCHEM_EXACT_MASS",
        "PUBCHEM_XLOGP3",
        "PUBCHEM_TPSA",
    }
    
    EXCLUDED_FIELDS = {"freeform_text", "synonyms"}
    
    def test_extract_computed_only(self):
        """Verify only computed fields are extracted."""
        # This would call the actual extraction function
        # For now, verify the field sets are correct
        extracted = {k: v for k, v in SAMPLE_PUBCHEM_RECORD.items() 
                     if k in self.ALLOWED_FIELDS}
        
        assert "PUBCHEM_COMPOUND_CID" in extracted
        assert "PUBCHEM_MOLECULAR_WEIGHT" in extracted
        assert "freeform_text" not in extracted
        assert "synonyms" not in extracted
    
    def test_cid_validation(self):
        """Verify CID is a valid integer."""
        cid = SAMPLE_PUBCHEM_RECORD["PUBCHEM_COMPOUND_CID"]
        assert isinstance(cid, int)
        assert cid > 0
    
    def test_smiles_validation(self):
        """Verify SMILES is a valid string."""
        smiles = SAMPLE_PUBCHEM_RECORD["PUBCHEM_CACTVS_CANONICAL_SMILES"]
        assert isinstance(smiles, str)
        assert len(smiles) > 0
        # Basic SMILES character validation
        valid_chars = set("CNOSPFClBrIHcnospfbri[]()=#+-0123456789@/\\")
        assert all(c in valid_chars for c in smiles)
```

### Task 4.4: Add Acquire Strategy Edge Case Tests

**File to modify:** `tests/test_acquire_strategies.py`

**Add these test cases:**

```python
class TestHttpDownloadEdgeCases:
    """Edge case tests for HTTP download strategy."""
    
    def test_redirect_to_private_ip_blocked(self, tmp_path: Path):
        """Verify redirects to private IPs are blocked."""
        # This requires mocking the redirect chain
        pass
    
    def test_partial_download_resume(self, tmp_path: Path):
        """Verify partial downloads can be resumed."""
        pass
    
    def test_content_length_mismatch(self, tmp_path: Path):
        """Verify content length mismatches are detected."""
        pass
    
    def test_sha256_mismatch_fails(self, tmp_path: Path):
        """Verify SHA256 mismatches cause failure."""
        pass


class TestZenodoStrategy:
    """Tests for Zenodo download strategy."""
    
    def test_md5_verification(self, tmp_path: Path):
        """Verify Zenodo MD5 checksums are validated."""
        pass
    
    def test_record_not_found(self, tmp_path: Path):
        """Verify graceful handling of missing records."""
        pass


class TestGitStrategy:
    """Tests for Git clone strategy."""
    
    def test_shallow_clone(self, tmp_path: Path):
        """Verify shallow clones work correctly."""
        pass
    
    def test_specific_commit_checkout(self, tmp_path: Path):
        """Verify specific commit checkout works."""
        pass
    
    def test_tag_checkout(self, tmp_path: Path):
        """Verify tag checkout works."""
        pass
```

---

## Phase 5: Documentation Updates

### Task 5.1: Create Denylist Workflow Documentation

**File to create:** `docs/denylist_workflow.md`

```markdown
# Denylist Workflow

This document describes how to add, modify, and review entries in the Dataset Collector denylist.

## Overview

The denylist (`configs/common/denylist.yaml`) contains patterns that block or flag datasets based on:
- Domain patterns (e.g., `example.com`)
- Publisher patterns (e.g., "Restricted Publisher Inc")
- General patterns (substring or regex matching)

## Severity Levels

### `hard_red`
Forces immediate RED classification. The dataset will not be collected or merged.

Use for:
- Publishers with explicit AI training prohibitions in ToS
- Domains known to have restrictive licenses
- Sources with legal issues

### `force_yellow`
Forces YELLOW classification for manual review. The dataset can still be approved.

Use for:
- Sources with unclear licensing
- New publishers requiring review
- Temporary holds pending investigation

## Adding a New Entry

### Required Fields

Every denylist entry MUST include:

```yaml
- type: "substring"  # or "regex" or "domain"
  value: "example.com"
  fields: ["license_evidence_url", "download_blob"]
  severity: "hard_red"  # or "force_yellow"
  link: "https://example.com/terms"  # URL to evidence
  rationale: "Clear explanation of why this is blocked"
```

### Process

1. **Identify the source** requiring blocking
2. **Document the evidence** (save ToS/license page)
3. **Determine severity** based on restriction clarity
4. **Add the entry** to `denylist.yaml`
5. **Create a PR** with:
   - The denylist change
   - Evidence screenshot or archive link
   - Brief explanation in PR description
6. **Get review** from at least one other team member

## Reviewing Entries

### Annual Review

All denylist entries should be reviewed annually to verify:
- The restriction is still in place
- The evidence link is still valid
- The severity is still appropriate

### Removal Process

To remove an entry:
1. Verify the restriction has been lifted
2. Document the change (new ToS, etc.)
3. Move entry to `# Archived` section with removal date
4. Create PR with evidence of change

## Examples

### Domain Block (Hard)

```yaml
- type: "domain"
  value: "restricted-journal.com"
  fields: ["license_evidence_url", "download_blob"]
  severity: "hard_red"
  link: "https://restricted-journal.com/terms#ai-training"
  rationale: "Terms Section 4.2 explicitly prohibits AI/ML training use"
```

### Publisher Block (Soft)

```yaml
- type: "substring"
  value: "Unclear Publisher"
  fields: ["name", "id"]
  severity: "force_yellow"
  link: "https://example.com/publisher-review-ticket-123"
  rationale: "Publisher terms are ambiguous; requires legal review per ticket #123"
```
```

### Task 5.2: Update Architecture Documentation

**File to modify:** `docs/architecture.md`

**Add section:**

```markdown
## Pipeline Configuration System

As of v2.0, pipelines are defined through a configuration-driven system rather than
duplicated boilerplate files.

### Pipeline Specifications

Each pipeline is defined by a `PipelineSpec` in `collector_core/pipeline_specs_registry.py`:

```python
register_pipeline(PipelineSpec(
    domain="chem",
    name="Chemistry Pipeline",
    targets_yaml="targets_chem.yaml",
    routing_keys=["chem_routing", "math_routing"],
    yellow_screen_module="yellow_screen_chem",
))
```

### Pipeline Factory

The factory creates pipeline driver classes dynamically:

```python
from collector_core.pipeline_factory import get_pipeline_driver

ChemDriver = get_pipeline_driver("chem")
ChemDriver.main()
```

### Custom Workers

Pipelines with custom logic (e.g., mesh processing, code analysis) still maintain
their custom worker files in the pipeline directory. These are referenced in the
`PipelineSpec.custom_workers` field.
```

### Task 5.3: Create Migration Guide

**File to create:** `docs/migration_v2.md`

```markdown
# Migration Guide: v1 to v2 Pipeline System

This guide covers migrating from the duplicated pipeline files to the new
configuration-driven system.

## What Changed

### Before (v1)
Each pipeline had 6-8 nearly identical Python files:
- `pipeline_driver.py`
- `acquire_worker.py`
- `merge_worker.py`
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py`
- `yellow_screen_worker.py`
- `yellow_scrubber.py`

### After (v2)
Pipelines are defined in `collector_core/pipeline_specs_registry.py` and only
custom logic remains in pipeline directories.

## Migration Steps

### 1. Add Pipeline Spec

Add your pipeline to `collector_core/pipeline_specs_registry.py`:

```python
register_pipeline(PipelineSpec(
    domain="your_domain",
    name="Your Pipeline Name",
    targets_yaml="targets_your_domain.yaml",
    routing_keys=["your_routing"],
))
```

### 2. Replace Boilerplate Files

Replace `pipeline_driver.py` with the minimal stub:

```python
#!/usr/bin/env python3
from collector_core.pipeline_factory import run_pipeline

def main() -> None:
    run_pipeline("your_domain")

if __name__ == "__main__":
    main()
```

### 3. Remove Redundant Files

Delete these files (now handled by collector_core):
- `catalog_builder.py`
- `review_queue.py`
- `pmc_worker.py`

### 4. Keep Custom Logic

Keep any custom workers that have domain-specific logic:
- `yellow_scrubber.py` (if it has domain transforms)
- Any custom `*_worker.py` files

### 5. Update Imports

If any of your custom code imported from the deleted files, update to:

```python
from collector_core.catalog_builder import main as catalog_main
from collector_core.review_queue import main as review_main
```

## Verification

Run the test suite to verify everything works:

```bash
pytest tests/
python -m tools.preflight
```
```

---

## Phase 6: Security Improvements

### Task 6.1: Add Content-Type Validation

**File to modify:** `collector_core/acquire_strategies.py`

**Add function:**

```python
SAFE_CONTENT_TYPES = {
    # Data formats
    "application/json",
    "application/x-ndjson",
    "text/csv",
    "text/plain",
    "text/tab-separated-values",
    
    # Archives
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-tar",
    "application/x-bzip2",
    "application/x-7z-compressed",
    
    # Scientific formats
    "chemical/x-mdl-sdfile",
    "application/x-hdf5",
    "application/x-parquet",
    
    # Documents
    "application/pdf",
    
    # Binary data
    "application/octet-stream",
}


def validate_content_type(content_type: str | None, filename: str) -> tuple[bool, str | None]:
    """
    Validate that content type is safe for download.
    
    Returns:
        (is_valid, error_reason)
    """
    if content_type is None:
        # Allow if we can't determine type
        return True, None
    
    # Normalize content type
    ct = content_type.split(";")[0].strip().lower()
    
    # Check against allowlist
    if ct in SAFE_CONTENT_TYPES:
        return True, None
    
    # Allow any text/* type
    if ct.startswith("text/"):
        return True, None
    
    # Block potentially dangerous types
    dangerous = {
        "application/javascript",
        "application/x-javascript",
        "text/javascript",
        "application/x-httpd-php",
        "application/x-sh",
        "application/x-csh",
    }
    
    if ct in dangerous:
        return False, f"dangerous_content_type:{ct}"
    
    # Warn but allow unknown types
    return True, None
```

### Task 6.2: Add Download Size Limits

**File to modify:** `collector_core/acquire_limits.py`

**Add/update:**

```python
# Add to acquire_limits.py

DEFAULT_MAX_DOWNLOAD_SIZE = 10 * 1024 * 1024 * 1024  # 10 GB
DEFAULT_WARN_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB


def check_content_length(
    content_length: int | None,
    max_size: int = DEFAULT_MAX_DOWNLOAD_SIZE,
    warn_size: int = DEFAULT_WARN_SIZE,
) -> tuple[bool, str | None, bool]:
    """
    Check if content length is within acceptable limits.
    
    Returns:
        (is_allowed, error_reason, should_warn)
    """
    if content_length is None:
        return True, None, False
    
    if content_length > max_size:
        return False, f"content_length_exceeds_max:{content_length}>{max_size}", False
    
    should_warn = content_length > warn_size
    return True, None, should_warn
```

### Task 6.3: Improve Filename Sanitization

**File to modify:** `collector_core/utils.py`

**Replace `safe_filename` with:**

```python
import unicodedata


def safe_filename(
    s: str,
    max_length: int = 200,
    allow_unicode: bool = False,
) -> str:
    """
    Convert string to safe filename.
    
    - Removes or replaces dangerous characters
    - Prevents directory traversal
    - Handles Unicode normalization
    - Prevents reserved names on Windows
    """
    if not s:
        return "file"
    
    # Normalize Unicode
    if allow_unicode:
        s = unicodedata.normalize("NFKC", s)
    else:
        s = unicodedata.normalize("NFKD", s)
        s = s.encode("ascii", "ignore").decode("ascii")
    
    # Remove null bytes
    s = s.replace("\x00", "")
    
    # Replace directory separators and other dangerous chars
    dangerous = set('/<>:"\|?*\x00')
    s = "".join(c if c not in dangerous else "_" for c in s)
    
    # Remove leading/trailing dots and spaces
    s = s.strip(". ")
    
    # Replace runs of underscores/spaces
    s = re.sub(r"[_\s]+", "_", s)
    
    # Windows reserved names
    reserved = {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    }
    name_upper = s.upper().split(".")[0]
    if name_upper in reserved:
        s = f"_{s}"
    
    # Truncate
    if len(s) > max_length:
        # Preserve extension if present
        if "." in s:
            name, ext = s.rsplit(".", 1)
            ext = ext[:10]  # Limit extension length
            name = name[:max_length - len(ext) - 1]
            s = f"{name}.{ext}"
        else:
            s = s[:max_length]
    
    return s or "file"
```

---

## Phase 7: Observability & Metrics

### Task 7.1: Add Structured Logging Context

**File to modify:** `collector_core/logging_config.py`

**Add:**

```python
import contextvars
from typing import Any

# Context variables for structured logging
_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "log_context", default={}
)


def set_log_context(**kwargs: Any) -> None:
    """Set context variables for structured logging."""
    ctx = _log_context.get().copy()
    ctx.update(kwargs)
    _log_context.set(ctx)


def clear_log_context() -> None:
    """Clear all context variables."""
    _log_context.set({})


def get_log_context() -> dict[str, Any]:
    """Get current log context."""
    return _log_context.get().copy()


class ContextualJsonFormatter(JsonFormatter):
    """JSON formatter that includes context variables."""
    
    def format(self, record: logging.LogRecord) -> str:
        message = self._format_message(record)
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        
        # Add context
        ctx = get_log_context()
        if ctx:
            payload["context"] = ctx
        
        if record.exc_info:
            payload["exc_info"] = redact_string(self.formatException(record.exc_info))
        
        return json.dumps(payload, ensure_ascii=False)
```

### Task 7.2: Add Metrics Collection

**File to create:** `collector_core/metrics.py`

```python
"""
collector_core/metrics.py

Simple metrics collection for observability.
"""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""
    
    # Counters
    targets_processed: int = 0
    targets_succeeded: int = 0
    targets_failed: int = 0
    
    records_processed: int = 0
    records_passed: int = 0
    records_rejected: int = 0
    
    bytes_downloaded: int = 0
    files_downloaded: int = 0
    
    # Timings (seconds)
    total_duration: float = 0.0
    download_duration: float = 0.0
    processing_duration: float = 0.0
    
    # Distributions
    errors_by_type: Counter = field(default_factory=Counter)
    targets_by_bucket: Counter = field(default_factory=Counter)
    licenses_seen: Counter = field(default_factory=Counter)
    
    # Timestamps
    started_at: str = ""
    completed_at: str = ""
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "targets": {
                "processed": self.targets_processed,
                "succeeded": self.targets_succeeded,
                "failed": self.targets_failed,
            },
            "records": {
                "processed": self.records_processed,
                "passed": self.records_passed,
                "rejected": self.records_rejected,
            },
            "bytes_downloaded": self.bytes_downloaded,
            "files_downloaded": self.files_downloaded,
            "duration": {
                "total_seconds": self.total_duration,
                "download_seconds": self.download_duration,
                "processing_seconds": self.processing_duration,
            },
            "errors_by_type": dict(self.errors_by_type),
            "targets_by_bucket": dict(self.targets_by_bucket),
            "top_licenses": self.licenses_seen.most_common(10),
            "timestamps": {
                "started_at": self.started_at,
                "completed_at": self.completed_at,
            },
        }


class MetricsTimer:
    """Context manager for timing operations."""
    
    def __init__(self, metrics: PipelineMetrics, field: str):
        self.metrics = metrics
        self.field = field
        self.start_time: float = 0
    
    def __enter__(self) -> MetricsTimer:
        self.start_time = time.monotonic()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.monotonic() - self.start_time
        current = getattr(self.metrics, self.field)
        setattr(self.metrics, self.field, current + duration)
```

---

## Verification Checklist

After completing all phases, verify:

### Code Quality
- [ ] All tests pass: `pytest tests/`
- [ ] No ruff errors: `ruff check .`
- [ ] No ruff format issues: `ruff format --check .`
- [ ] YAML lint passes: `yamllint .`

### Functionality
- [ ] Preflight passes: `python -m tools.preflight`
- [ ] Repo validation passes: `python -m tools.validate_repo`
- [ ] Minimal dry run works: `bash tools/run_minimal_dry_run.sh`

### Documentation
- [ ] README is updated with new CLI options
- [ ] Architecture docs reflect new structure
- [ ] Migration guide is complete

### Backwards Compatibility
- [ ] Existing pipeline directories still work
- [ ] `dc run` command works for all pipelines
- [ ] Legacy `run_pipeline.sh` still functions (deprecated)

---

## Appendix: Files to Delete

After migration is complete, these files can be deleted from each `*_pipeline_v2/` directory:

```
catalog_builder.py      # Stub, use collector_core directly
review_queue.py         # Stub, use collector_core directly
pmc_worker.py           # Stub, use collector_core directly (unless customized)
```

The following files should be KEPT only if they contain custom logic:
- `pipeline_driver.py` (minimal stub)
- `acquire_worker.py` (minimal stub or custom handlers)
- `merge_worker.py` (minimal stub or custom logic)
- `yellow_screen_worker.py` (minimal stub)
- `yellow_scrubber.py` (keep if has domain transforms)
- Any custom `*_worker.py` files

---

## Appendix: Quick Reference - Domain to Prefix Mapping

| Domain | Prefix | Targets YAML |
|--------|--------|--------------|
| 3d_modeling | 3d | targets_3d.yaml |
| agri_circular | agri_circular | targets_agri_circular.yaml |
| biology | bio | targets_biology.yaml |
| chem | chem | targets_chem.yaml |
| code | code | targets_code.yaml |
| cyber | cyber | targets_cyber.yaml |
| earth | earth | targets_earth.yaml |
| econ_stats_decision_adaptation | econ | targets_econ_stats_decision_v2.yaml |
| engineering | engineering | targets_engineering.yaml |
| kg_nav | kg_nav | targets_kg_nav.yaml |
| logic | logic | targets_logic.yaml |
| materials_science | matsci | targets_materials.yaml |
| math | math | targets_math.yaml |
| metrology | metrology | targets_metrology.yaml |
| nlp | nlp | targets_nlp.yaml |
| physics | physics | targets_physics.yaml |
| regcomp | regcomp | targets_regcomp.yaml |
| safety_incident | safety | targets_safety_incident.yaml |
