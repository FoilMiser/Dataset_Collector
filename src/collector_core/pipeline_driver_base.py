from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
from collections import Counter
from pathlib import Path
from typing import Any

from collector_core.__version__ import __version__ as VERSION
from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.checks.actions import (
    normalize_content_check_actions,
    resolve_content_check_action,
)
from collector_core.checks.runner import generate_run_id, run_checks_for_target
from collector_core.classification.logic import (
    apply_yellow_signoff_requirement,
    build_bucket_signals,
    resolve_effective_bucket,
    resolve_output_pool,
    resolve_spdx_with_confidence,
)
from collector_core.companion_files import read_license_maps, resolve_companion_paths
from collector_core.config_validator import read_yaml

# Denylist functions extracted to denylist_matcher.py for modularity - re-exported for compatibility
from collector_core.denylist_matcher import (
    build_denylist_haystack,
    denylist_hits,
    load_denylist,
)
from collector_core.dataset_root import resolve_dataset_root
from collector_core.exceptions import ConfigValidationError
from collector_core.logging_config import add_logging_args, configure_logging
from collector_core.metrics import MetricsCollector, clear_collector, set_collector
from collector_core.policy_snapshot import build_policy_snapshot
from collector_core.queue.emission import emit_queues
from collector_core.utils.io import write_json
from collector_core.utils.logging import utc_now
from collector_core.utils.paths import ensure_dir
from collector_core.utils.text import coerce_int, contains_any

from collector_core.evidence.change_detection import (
    compute_signoff_mismatches,
    normalize_cosmetic_change_policy,
    normalize_evidence_change_policy,
    resolve_evidence_change,
)
from collector_core.evidence.fetching import (
    fetch_evidence,
    fetch_evidence_batch,
    fetch_url_with_retry,
    redact_headers_for_manifest,
    snapshot_evidence,
)

logger = logging.getLogger(__name__)

SUPPORTED_LICENSE_GATES = {
    "manual_legal_review",
    "manual_review",
    "no_restrictions",
    "restriction_phrase_scan",
    "snapshot_terms",
}
SUPPORTED_CONTENT_CHECKS = {
    "code_and_docs_chunking",
    "collect_statistics",
    "computed_only_extract",
    "distribution_statement_scan",
    "domain_filter_arxiv_math",
    "domain_filter_math_stackexchange",
    "domain_filter_math_textbooks",
    "dual_use_instruction_scan",
    "dual_use_scan",
    "exploit_code_scan",
    "emit_attribution_bundle",
    "emit_training_manifest",
    "extract_latex_aware_chunks",
    "extract_text_chunks",
    "formal_chunk_by_entry",
    "formal_chunk_by_lemma",
    "formal_chunk_by_module",
    "formal_chunk_by_theorem",
    "html_crawl",
    "license_metadata_validate",
    "mesh_extract_metadata",
    "mesh_geometry_dedupe",
    "mesh_render_thumbnails",
    "mesh_sanitize",
    "mesh_validate",
    "near_duplicate_detection",
    "pdf_extract",
    "pii_phi_scan",
    "pii_scan",
    "pii_scan_and_redact",
    "pii_scan_and_redact_strict",
    "record_level_filter",
    "secret_scan",
    "segregate_copyleft_pool",
    "strip_third_party_media",
    "validate_schema",
    "weapon_trademark_filter",
}


@dataclasses.dataclass
class LicenseMap:
    allow: list[str]
    conditional: list[str]
    deny_prefixes: list[str]
    normalization_rules: list[dict[str, Any]]
    restriction_phrases: list[str]
    gating: dict[str, str]
    profiles: dict[str, dict[str, Any]]
    evidence_change_policy: str
    cosmetic_change_policy: str


@dataclasses.dataclass(frozen=True)
class DriverConfig:
    args: argparse.Namespace
    retry_max: int
    retry_backoff: float
    headers: dict[str, str]
    targets_path: Path
    targets_cfg: dict[str, Any]
    license_map_path: list[Path]
    license_map: LicenseMap
    denylist_path: list[Path]
    denylist: dict[str, Any]
    manifests_root: Path
    queues_root: Path
    ledger_root: Path
    default_license_gates: list[str]
    default_content_checks: list[str]
    targets: list[dict[str, Any]]
    require_yellow_signoff: bool
    checks_run_id: str
    content_check_actions: dict[str, str]


@dataclasses.dataclass(frozen=True)
class EvidenceResult:
    snapshot: dict[str, Any]
    text: str
    license_change_detected: bool
    no_fetch_missing_evidence: bool


@dataclasses.dataclass(frozen=True)
class TargetContext:
    target: dict[str, Any]
    tid: str
    name: str
    profile: str
    evidence_url: str
    spdx_hint: str
    download_blob: str
    review_required: bool
    license_gates: list[str]
    content_checks: list[str]
    content_check_actions: dict[str, str]
    target_manifest_dir: Path
    signoff: dict[str, Any]
    review_status: str
    promote_to: str
    routing: dict[str, Any]
    dl_hits: list[dict[str, Any]]
    enabled: bool
    split_group_id: str


@dataclasses.dataclass
class ClassificationResult:
    green_rows: list[dict[str, Any]]
    yellow_rows: list[dict[str, Any]]
    red_rows: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    evidence_bytes: int = 0
    evidence_fetches: int = 0
    evidence_fetch_ok: int = 0
    evidence_fetch_errors: int = 0
    evidence_fetch_skipped: int = 0


def load_license_map(paths: Path | list[Path]) -> LicenseMap:
    path_list = paths if isinstance(paths, list) else [paths]
    m = read_license_maps(path_list)
    spdx = m.get("spdx", {}) or {}
    normalization = m.get("normalization", {}) or {}
    restriction_scan = m.get("restriction_scan", {}) or {}
    gating = m.get("gating", {}) or {}
    profiles = m.get("profiles", {}) or {}
    evidence_change_policy = normalize_evidence_change_policy(m.get("evidence_change_policy"))
    cosmetic_change_policy = normalize_cosmetic_change_policy(m.get("cosmetic_change_policy"))

    return LicenseMap(
        allow=spdx.get("allow", []),
        conditional=spdx.get("conditional", []),
        deny_prefixes=spdx.get("deny_prefixes", []),
        normalization_rules=normalization.get("rules", []),
        restriction_phrases=restriction_scan.get("phrases", []),
        gating=gating,
        profiles=profiles,
        evidence_change_policy=evidence_change_policy,
        cosmetic_change_policy=cosmetic_change_policy,
    )


def resolve_retry_config(
    args: argparse.Namespace, globals_cfg: dict[str, Any]
) -> tuple[int, float]:
    retry_max_env = os.getenv("PIPELINE_RETRY_MAX")
    retry_backoff_env = os.getenv("PIPELINE_RETRY_BACKOFF")
    retry_cfg = globals_cfg.get("retry", {}) or {}
    retry_max = args.retry_max if args.retry_max is not None else args.max_retries
    if retry_max is None:
        retry_max = (
            int(retry_cfg.get("max"))
            if retry_cfg.get("max") is not None
            else (int(retry_max_env) if retry_max_env else 3)
        )
    retry_backoff = (
        args.retry_backoff
        if args.retry_backoff is not None
        else (
            float(retry_cfg.get("backoff"))
            if retry_cfg.get("backoff") is not None
            else (float(retry_backoff_env) if retry_backoff_env else 2.0)
        )
    )
    return retry_max, retry_backoff


def build_evidence_headers(raw_headers: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw in raw_headers:
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key.strip():
            headers[key.strip()] = value.strip()
    return headers


def resolve_output_roots(
    args: argparse.Namespace, globals_cfg: dict[str, Any]
) -> tuple[Path, Path, Path]:
    dataset_root = resolve_dataset_root(args.dataset_root)
    manifests_override = args.manifests_root or args.out_manifests
    queues_override = args.queues_root or args.out_queues
    ledger_override = args.ledger_root
    if manifests_override:
        manifests_root = Path(manifests_override).expanduser().resolve()
    elif dataset_root:
        manifests_root = (dataset_root / "_manifests").resolve()
    else:
        manifests_root = (
            Path(globals_cfg.get("manifests_root", "./manifests")).expanduser().resolve()
        )
    if queues_override:
        queues_root = Path(queues_override).expanduser().resolve()
    elif dataset_root:
        queues_root = (dataset_root / "_queues").resolve()
    else:
        queues_root = Path(globals_cfg.get("queues_root", "./queues")).expanduser().resolve()
    if ledger_override:
        ledger_root = Path(ledger_override).expanduser().resolve()
    elif dataset_root:
        ledger_root = (dataset_root / "_ledger").resolve()
    else:
        ledger_root = Path(globals_cfg.get("ledger_root", "./_ledger")).expanduser().resolve()
    return manifests_root, queues_root, ledger_root


def load_driver_config(args: argparse.Namespace) -> DriverConfig:
    headers = build_evidence_headers(args.evidence_header)
    targets_path = Path(args.targets).resolve()
    targets_cfg = read_yaml(targets_path, schema_name="targets")
    globals_cfg = targets_cfg.get("globals", {}) or {}
    content_check_actions = normalize_content_check_actions(
        globals_cfg.get("content_check_actions", {})
    )
    retry_max, retry_backoff = resolve_retry_config(args, globals_cfg)
    companion = targets_cfg.get("companion_files", {}) or {}
    license_map_value = (
        args.license_map if args.license_map is not None else companion.get("license_map")
    )
    license_map_paths = resolve_companion_paths(
        targets_path, license_map_value, "./license_map.yaml"
    )
    license_map = load_license_map(license_map_paths)
    denylist_paths = resolve_companion_paths(
        targets_path, companion.get("denylist"), "./denylist.yaml"
    )
    denylist = load_denylist(denylist_paths)
    manifests_root, queues_root, ledger_root = resolve_output_roots(args, globals_cfg)
    ensure_dir(manifests_root)
    ensure_dir(queues_root)
    ensure_dir(ledger_root)
    return DriverConfig(
        args=args,
        retry_max=retry_max,
        retry_backoff=retry_backoff,
        headers=headers,
        targets_path=targets_path,
        targets_cfg=targets_cfg,
        license_map_path=license_map_paths,
        license_map=license_map,
        denylist_path=denylist_paths,
        denylist=denylist,
        manifests_root=manifests_root,
        queues_root=queues_root,
        ledger_root=ledger_root,
        default_license_gates=globals_cfg.get("default_license_gates", []) or [],
        default_content_checks=globals_cfg.get("default_content_checks", []) or [],
        targets=targets_cfg.get("targets", []) or [],
        require_yellow_signoff=bool(globals_cfg.get("require_yellow_signoff", False)),
        checks_run_id=generate_run_id("classification"),
        content_check_actions=content_check_actions,
    )


def build_target_identity(
    target: dict[str, Any],
    license_map: LicenseMap,
) -> tuple[str, str, str, bool, list[dict[str, Any]]]:
    enabled = bool(target.get("enabled", True))
    tid = str(target.get("id", "")).strip() or "unknown_id"
    name = str(target.get("name", tid))
    profile = str(target.get("license_profile", "unknown"))
    warnings: list[dict[str, Any]] = []
    if profile not in license_map.profiles:
        warnings.append(
            {
                "type": "unknown_license_profile",
                "target_id": tid,
                "license_profile": profile,
                "known_profiles": sorted(license_map.profiles.keys()),
                "message": f"Target {tid} uses license_profile '{profile}' not present in license_map profiles.",
            }
        )
    return tid, name, profile, enabled, warnings


def validate_target_values(
    values: list[str],
    target_id: str,
    *,
    field: str,
    supported_values: set[str],
    strict: bool,
) -> list[dict[str, Any]]:
    unknown_values = sorted({value for value in values if value not in supported_values})
    if not unknown_values:
        return []
    message = (
        f"Target {target_id} uses unsupported {field}: {', '.join(unknown_values)}."
    )
    warning = {
        "type": f"unknown_{field}",
        "target_id": target_id,
        "unknown_values": unknown_values,
        "supported_values": sorted(supported_values),
        "message": message,
    }
    if strict:
        raise ConfigValidationError(
            message,
            context={
                "target_id": target_id,
                "unknown_values": unknown_values,
                "supported_values": sorted(supported_values),
            },
        )
    return [warning]


def extract_evidence_fields(target: dict[str, Any]) -> tuple[str, str]:
    evidence = target.get("license_evidence", {}) or {}
    spdx_hint = str(evidence.get("spdx_hint", "UNKNOWN"))
    evidence_url = str(evidence.get("url", ""))
    return spdx_hint, evidence_url


def _merge_download_config(download: dict[str, Any]) -> dict[str, Any]:
    download_cfg = dict(download or {})
    cfg = download_cfg.get("config")
    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in download_cfg.items() if k != "config"})
        return merged
    return download_cfg


def _is_probable_url(value: str) -> bool:
    if not value:
        return False
    try:
        from urllib.parse import urlparse

        parsed = urlparse(value)
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        logger.debug("Failed to parse URL: %r", value)
        return False


def _collect_urls(value: Any, urls: list[str], seen: set[str]) -> None:
    if isinstance(value, str):
        if _is_probable_url(value) and value not in seen:
            seen.add(value)
            urls.append(value)
        return
    if isinstance(value, dict):
        for item in value.values():
            _collect_urls(item, urls, seen)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_urls(item, urls, seen)


def extract_download_urls(target: dict[str, Any]) -> list[str]:
    download_cfg = _merge_download_config(target.get("download", {}) or {})
    urls: list[str] = []
    _collect_urls(download_cfg, urls, set())
    return urls


# NOTE: build_denylist_haystack, extract_domain, load_denylist, denylist_hits
# moved to collector_core/denylist_matcher.py - re-exported above for compatibility


def read_review_signoff(manifest_dir: Path) -> dict[str, Any]:
    """Read review_signoff.json if present."""
    p = manifest_dir / "review_signoff.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to read review signoff from %s", p, exc_info=True)
        return {}


def merge_override(default_values: list[str], overrides: dict[str, Any]) -> list[str]:
    """Merge default values with overrides from target config."""
    merged = list(default_values)
    add = overrides.get("add", []) or []
    remove = overrides.get("remove", []) or []

    for g in add:
        if g not in merged:
            merged.append(g)
    for g in remove:
        if g in merged:
            merged.remove(g)
    return merged


def canonicalize_license_gates(gates: list[str]) -> list[str]:
    canonical_map = {
        "no_restrictions": "restriction_phrase_scan",
        "manual_review": "manual_legal_review",
    }
    canonicalized: list[str] = []
    for gate in gates:
        mapped = canonical_map.get(gate, gate)
        if mapped not in canonicalized:
            canonicalized.append(mapped)
    return canonicalized


def canonicalize_checks(checks: list[str]) -> list[str]:
    canonicalized: list[str] = []
    for check in checks:
        if check not in canonicalized:
            canonicalized.append(check)
    return canonicalized


def generate_dry_run_report(
    queues_root: Path,
    targets: list[dict[str, Any]],
    green_rows: list[dict[str, Any]],
    yellow_rows: list[dict[str, Any]],
    red_rows: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> str:
    lines = [
        "=" * 70,
        "PIPELINE DRIVER DRY-RUN SUMMARY",
        "=" * 70,
        "",
        f"Total targets: {len(targets)}",
        f"  GREEN (approved): {len(green_rows)}",
        f"  YELLOW (needs review): {len(yellow_rows)}",
        f"  RED (rejected): {len(red_rows)}",
        "",
    ]

    if warnings:
        lines.extend(
            [
                "WARNINGS",
                "-" * 40,
            ]
        )
        for w in warnings[:20]:
            lines.append(f"  ⚠ {w.get('message', str(w))}")
        if len(warnings) > 20:
            lines.append(f"  ... and {len(warnings) - 20} more")
        lines.append("")

    if green_rows:
        lines.extend(
            [
                "GREEN TARGETS (will be downloaded)",
                "-" * 40,
            ]
        )
        for r in green_rows[:20]:
            lines.append(f"  ✓ {r['id']}: {r['name'][:50]}")
            lines.append(f"      License: {r['resolved_spdx']}")
        if len(green_rows) > 20:
            lines.append(f"  ... and {len(green_rows) - 20} more")
        lines.append("")

    if yellow_rows:
        lines.extend(
            [
                "YELLOW TARGETS (need additional processing)",
                "-" * 40,
            ]
        )
        for r in yellow_rows[:20]:
            reason = (
                "record-level filtering"
                if r.get("license_profile") == "record_level"
                else "manual review"
            )
            if r.get("restriction_hits"):
                reason = f"restriction phrase: {r['restriction_hits'][0]}"
            lines.append(f"  ⚠ {r['id']}: {r['name'][:50]}")
            lines.append(f"      Reason: {reason}")
            lines.append(f"      Signoff SHA256: {r.get('signoff_evidence_sha256')}")
            lines.append(f"      Current SHA256: {r.get('current_evidence_sha256')}")
            lines.append(f"      Signoff stale: {r.get('signoff_is_stale')}")
        if len(yellow_rows) > 20:
            lines.append(f"  ... and {len(yellow_rows) - 20} more")
        lines.append("")

    if red_rows:
        lines.extend(
            [
                "RED TARGETS (rejected)",
                "-" * 40,
            ]
        )
        for r in red_rows[:10]:
            lines.append(f"  ✗ {r['id']}: {r['name'][:50]}")
            lines.append(f"      License: {r['resolved_spdx']}")
        if len(red_rows) > 10:
            lines.append(f"  ... and {len(red_rows) - 10} more")
        lines.append("")

    lines.extend(
        [
            "NEXT STEPS",
            "-" * 40,
            "  1. Review this summary for any unexpected classifications",
            "  2. Run dc run --pipeline <slug> --stage acquire --execute to download GREEN and YELLOW targets",
            "  3. Run dc run --pipeline <slug> --stage yellow_screen and merge per docs/PIPELINE_V2_REWORK_PLAN.md",
            "",
            "=" * 70,
        ]
    )

    report = "\n".join(lines)

    # Write report to file
    report_path = queues_root / "dry_run_report.txt"
    report_path.write_text(report, encoding="utf-8")

    return report


@dataclasses.dataclass(frozen=True)
class RoutingBlockSpec:
    name: str
    sources: list[str]
    mode: str = "subset"


class BasePipelineDriver:
    DOMAIN = "base"
    TARGETS_LABEL = "targets.yaml"
    USER_AGENT = "dataset-collector-pipeline"
    EVIDENCE_MAX_BYTES = 20 * 1024 * 1024
    ROUTING_KEYS: list[str] = []
    ROUTING_CONFIDENCE_KEYS: list[str] = []
    ROUTING_BLOCKS: list[RoutingBlockSpec] = []
    DEFAULT_ROUTING: dict[str, Any] = {"granularity": "target"}
    INCLUDE_ROUTING_DICT_IN_ROW = False

    def _routing_sources(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        routing = target.get("routing", {}) or {}
        return [routing] + [(target.get(key, {}) or {}) for key in self.ROUTING_KEYS]

    def _confidence_sources(self, target: dict[str, Any]) -> list[dict[str, Any]]:
        routing = target.get("routing", {}) or {}
        sources = [routing]
        for key in self.ROUTING_CONFIDENCE_KEYS:
            sources.append(target.get(key, {}) or {})
        return sources

    def _first_value(self, sources: list[dict[str, Any]], key: str) -> Any | None:
        for src in sources:
            val = src.get(key)
            if val not in (None, ""):
                return val
        return None

    def _first_level(self, sources: list[dict[str, Any]]) -> int | None:
        for src in sources:
            val = coerce_int(src.get("level"))
            if val is not None:
                return val
        return None

    def resolve_routing_fields(self, target: dict[str, Any]) -> dict[str, Any]:
        sources = self._routing_sources(target)
        confidence_sources = self._confidence_sources(target)

        subject = self._first_value(sources, "subject")
        domain = self._first_value(sources, "domain")
        category = self._first_value(sources, "category")
        level = self._first_level(sources)
        granularity = self._first_value(sources, "granularity")
        confidence = self._first_value(confidence_sources, "confidence")
        reason = self._first_value(confidence_sources, "reason")

        return {
            "subject": subject if subject is not None else self.DEFAULT_ROUTING.get("subject"),
            "domain": domain if domain is not None else self.DEFAULT_ROUTING.get("domain"),
            "category": category if category is not None else self.DEFAULT_ROUTING.get("category"),
            "level": level if level is not None else self.DEFAULT_ROUTING.get("level"),
            "granularity": granularity
            if granularity is not None
            else self.DEFAULT_ROUTING.get("granularity"),
            "confidence": confidence,
            "reason": reason,
        }

    def build_routing_block(self, target: dict[str, Any], spec: RoutingBlockSpec) -> dict[str, Any]:
        chosen: dict[str, Any] = {}
        for key in spec.sources:
            candidate = target.get(key, {}) or {}
            if candidate:
                chosen = candidate
                break
        if spec.mode == "raw":
            return chosen
        return {
            "domain": chosen.get("domain"),
            "category": chosen.get("category"),
            "level": chosen.get("level"),
            "granularity": chosen.get("granularity"),
        }

    def build_evaluation_extras(
        self, target: dict[str, Any], routing: dict[str, Any]
    ) -> dict[str, Any]:
        extras: dict[str, Any] = {}
        for spec in self.ROUTING_BLOCKS:
            extras[spec.name] = self.build_routing_block(target, spec)
        return extras

    def build_row_extras(self, target: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
        return {}

    def fetch_url_with_retry(
        self,
        url: str,
        timeout_s: float | tuple[float, float] = (15.0, 60.0),
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        max_bytes: int | None = None,
        allow_private_hosts: bool = False,
    ) -> tuple[bytes | None, str | None, dict[str, Any]]:
        return fetch_url_with_retry(
            url,
            user_agent=f"{self.USER_AGENT}/{VERSION}",
            timeout_s=timeout_s,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
            max_bytes=max_bytes,
            allow_private_hosts=allow_private_hosts,
        )

    def snapshot_evidence(
        self,
        manifest_dir: Path,
        url: str,
        *,
        evidence_change_policy: str = "normalized",
        cosmetic_change_policy: str = "warn_only",
        max_retries: int = 3,
        backoff_base: float = 2.0,
        headers: dict[str, str] | None = None,
        allow_private_hosts: bool = False,
    ) -> dict[str, Any]:
        return snapshot_evidence(
            manifest_dir,
            url,
            user_agent=f"{self.USER_AGENT}/{VERSION}",
            evidence_change_policy=evidence_change_policy,
            cosmetic_change_policy=cosmetic_change_policy,
            max_retries=max_retries,
            backoff_base=backoff_base,
            headers=headers,
            allow_private_hosts=allow_private_hosts,
            max_bytes=self.EVIDENCE_MAX_BYTES,
        )

    def run(self, args: argparse.Namespace) -> None:
        cfg = load_driver_config(args)
        run_ledger_root = cfg.ledger_root / cfg.checks_run_id
        ensure_dir(run_ledger_root)
        collector = MetricsCollector(self.DOMAIN)
        set_collector(collector)
        run_timer = collector.timer("run_total_ms").start()
        try:
            policy_timer = collector.timer("policy_snapshot_ms").start()
            policy_snapshot = build_policy_snapshot(
                run_id=cfg.checks_run_id,
                targets_path=cfg.targets_path,
                targets_cfg=cfg.targets_cfg,
                license_map_paths=cfg.license_map_path,
                denylist_paths=cfg.denylist_path,
                default_content_checks=cfg.default_content_checks,
                targets=cfg.targets,
            )
            policy_timer.stop()
            write_json(run_ledger_root / "policy_snapshot.json", policy_snapshot)
            classify_timer = collector.timer("classification_ms").start()
            results = self.classify_targets(cfg)
            classify_timer.stop()
            queues_timer = collector.timer("emit_queues_ms").start()
            self.emit_queues(cfg.queues_root, results)
            queues_timer.stop()
            summary_timer = collector.timer("emit_summary_ms").start()
            self.emit_summary(cfg, results)
            summary_timer.stop()
            report_timer = collector.timer("emit_report_ms").start()
            self.emit_report(cfg, results)
            report_timer.stop()
            run_timer.stop()
            metrics = self.build_metrics_payload(cfg, results, collector)
            write_json(run_ledger_root / "metrics.json", metrics)
        finally:
            clear_collector()

    def classify_targets(self, cfg: DriverConfig) -> ClassificationResult:
        results = ClassificationResult([], [], [], [])
        contexts: list[TargetContext] = []
        for target in cfg.targets:
            ctx, warnings = self.prepare_target_context(target, cfg)
            results.warnings.extend(warnings)
            contexts.append(ctx)
        evidence_results = self.fetch_evidence_batch(contexts, cfg)
        results.evidence_bytes = self._sum_evidence_bytes(evidence_results)
        (
            results.evidence_fetches,
            results.evidence_fetch_ok,
            results.evidence_fetch_errors,
            results.evidence_fetch_skipped,
        ) = self._count_evidence_fetches(evidence_results)
        for ctx, evidence in zip(contexts, evidence_results):
            evaluation, row = self.classify_target(ctx, cfg, evidence)
            write_json(ctx.target_manifest_dir / "evaluation.json", evaluation)
            if not ctx.enabled:
                continue
            if row["effective_bucket"] == "GREEN":
                results.green_rows.append(row)
            elif row["effective_bucket"] == "YELLOW":
                results.yellow_rows.append(row)
            else:
                results.red_rows.append(row)
        return results

    def prepare_target_context(
        self, target: dict[str, Any], cfg: DriverConfig
    ) -> tuple[TargetContext, list[dict[str, Any]]]:
        tid, name, profile, enabled, warnings = build_target_identity(target, cfg.license_map)
        spdx_hint, evidence_url = extract_evidence_fields(target)
        download_cfg = target.get("download", {}) or {}
        download_blob = json.dumps(download_cfg, ensure_ascii=False)
        download_urls = extract_download_urls(target)
        review_required = bool(target.get("review_required", False))
        merged_license_gates = merge_override(
            cfg.default_license_gates, target.get("license_gates", {}) or {}
        )
        merged_content_checks = merge_override(
            cfg.default_content_checks, target.get("content_checks", {}) or {}
        )
        warnings.extend(
            validate_target_values(
                merged_license_gates,
                tid,
                field="license_gates",
                supported_values=SUPPORTED_LICENSE_GATES,
                strict=cfg.args.strict,
            )
        )
        warnings.extend(
            validate_target_values(
                merged_content_checks,
                tid,
                field="content_checks",
                supported_values=SUPPORTED_CONTENT_CHECKS,
                strict=cfg.args.strict,
            )
        )
        content_check_actions = dict(cfg.content_check_actions)
        target_actions = normalize_content_check_actions(
            target.get("content_check_actions", {}) or {}
        )
        content_check_actions.update(target_actions)
        license_gates = canonicalize_license_gates(merged_license_gates)
        content_checks = canonicalize_checks(merged_content_checks)
        target_manifest_dir = cfg.manifests_root / tid
        ensure_dir(target_manifest_dir)
        signoff = read_review_signoff(target_manifest_dir)
        review_status = str(signoff.get("status", "") or "").lower()
        promote_to = str(signoff.get("promote_to", "") or "").upper()
        dl_hits = denylist_hits(
            cfg.denylist, build_denylist_haystack(tid, name, evidence_url, download_urls, target)
        )
        routing = self.resolve_routing_fields(target)
        split_group_id = str(target.get("split_group_id", "") or tid)
        ctx = TargetContext(
            target=target,
            tid=tid,
            name=name,
            profile=profile,
            evidence_url=evidence_url,
            spdx_hint=spdx_hint,
            download_blob=download_blob,
            review_required=review_required,
            license_gates=license_gates,
            content_checks=content_checks,
            content_check_actions=content_check_actions,
            target_manifest_dir=target_manifest_dir,
            signoff=signoff,
            review_status=review_status,
            promote_to=promote_to,
            routing=routing,
            dl_hits=dl_hits,
            enabled=enabled,
            split_group_id=split_group_id,
        )
        return ctx, warnings

    def classify_target(
        self, ctx: TargetContext, cfg: DriverConfig, evidence: EvidenceResult | None = None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if evidence is None:
            evidence = self.fetch_evidence(ctx, cfg)
        review_status = ctx.review_status
        promote_to = ctx.promote_to
        review_required = ctx.review_required
        evidence_raw_sha = evidence.snapshot.get("sha256_raw_bytes") or evidence.snapshot.get(
            "sha256"
        )
        evidence_normalized_sha = evidence.snapshot.get("sha256_normalized_text")
        signoff_raw_sha = ctx.signoff.get("license_evidence_sha256_raw_bytes") or ctx.signoff.get(
            "license_evidence_sha256"
        )
        signoff_normalized_sha = ctx.signoff.get("license_evidence_sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_raw_sha,
            signoff_normalized_sha=signoff_normalized_sha,
            current_raw_sha=evidence_raw_sha,
            current_normalized_sha=evidence_normalized_sha,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        change_requires_review = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            cfg.license_map.evidence_change_policy,
            cfg.license_map.cosmetic_change_policy,
        )
        if change_requires_review:
            review_status = "pending"
            promote_to = ""
            review_required = True
        restriction_hits = contains_any(evidence.text, cfg.license_map.restriction_phrases)
        resolved, resolved_confidence, confidence_reason = resolve_spdx_with_confidence(
            cfg.license_map, evidence.text, ctx.spdx_hint
        )
        eff_bucket = resolve_effective_bucket(
            cfg.license_map,
            ctx.license_gates,
            evidence,
            resolved,
            restriction_hits,
            cfg.args.min_license_confidence,
            resolved_confidence,
            review_required,
            review_status,
            promote_to,
            ctx.dl_hits,
            cfg.args.strict,
        )
        review_required = apply_yellow_signoff_requirement(
            eff_bucket,
            review_status,
            review_required,
            cfg.require_yellow_signoff,
        )
        out_pool = resolve_output_pool(ctx.profile, eff_bucket, ctx.target)
        evaluation = self.build_evaluation(
            ctx,
            cfg,
            evidence,
            restriction_hits,
            resolved,
            resolved_confidence,
            confidence_reason,
            eff_bucket,
            review_required,
            review_status,
            out_pool,
        )
        row = self.build_row(
            ctx,
            cfg.license_map,
            evidence,
            restriction_hits,
            resolved,
            resolved_confidence,
            eff_bucket,
            review_required,
            out_pool,
        )
        check_results = run_checks_for_target(
            content_checks=ctx.content_checks,
            ledger_root=cfg.ledger_root,
            run_id=cfg.checks_run_id,
            target_id=ctx.tid,
            stage="classification",
            target=ctx.target,
            row=row,
            extra={"content_check_actions": ctx.content_check_actions},
        )
        action, action_checks = resolve_content_check_action(check_results)
        row["content_check_action"] = action or "ok"
        row["content_check_action_checks"] = action_checks
        evaluation["content_check_action"] = action or "ok"
        evaluation["content_check_action_checks"] = action_checks
        if action == "block":
            row["effective_bucket"] = "RED"
            row["queue_bucket"] = "RED"
            row["output_pool"] = "quarantine"
            evaluation["effective_bucket"] = "RED"
            evaluation["queue_bucket"] = "RED"
            evaluation["output_pool"] = "quarantine"
        elif action == "quarantine":
            if row["effective_bucket"] == "GREEN":
                row["effective_bucket"] = "YELLOW"
                row["queue_bucket"] = "YELLOW"
                evaluation["effective_bucket"] = "YELLOW"
                evaluation["queue_bucket"] = "YELLOW"
            row["output_pool"] = "quarantine"
            evaluation["output_pool"] = "quarantine"
        bucket_reason, signals = build_bucket_signals(
            ctx=ctx,
            license_map=cfg.license_map,
            evidence=evidence,
            restriction_hits=restriction_hits,
            resolved=resolved,
            resolved_confidence=resolved_confidence,
            eff_bucket=row["effective_bucket"],
            review_required=review_required,
            review_status=review_status,
            promote_to=promote_to,
            min_confidence=cfg.args.min_license_confidence,
            require_yellow_signoff=cfg.require_yellow_signoff,
            action=action or "ok",
            action_checks=action_checks,
            strict_snapshot=cfg.args.strict,
        )
        row["bucket_reason"] = bucket_reason
        row["signals"] = signals
        return evaluation, row

    def fetch_evidence(self, ctx: TargetContext, cfg: DriverConfig) -> EvidenceResult:
        return fetch_evidence(
            ctx=ctx,
            cfg=cfg,
            user_agent=f"{self.USER_AGENT}/{VERSION}",
            max_bytes=self.EVIDENCE_MAX_BYTES,
        )

    def fetch_evidence_batch(
        self, ctxs: list[TargetContext], cfg: DriverConfig
    ) -> list[EvidenceResult]:
        return fetch_evidence_batch(
            ctxs=ctxs,
            cfg=cfg,
            user_agent=f"{self.USER_AGENT}/{VERSION}",
            max_bytes=self.EVIDENCE_MAX_BYTES,
        )

    def build_evaluation(
        self,
        ctx: TargetContext,
        cfg: DriverConfig,
        evidence: EvidenceResult,
        restriction_hits: list[str],
        resolved: str,
        resolved_confidence: float,
        confidence_reason: str,
        eff_bucket: str,
        review_required: bool,
        review_status: str,
        out_pool: str,
    ) -> dict[str, Any]:
        signoff_evidence_sha256 = (ctx.signoff or {}).get("license_evidence_sha256")
        signoff_evidence_sha256_raw = (ctx.signoff or {}).get(
            "license_evidence_sha256_raw_bytes"
        ) or signoff_evidence_sha256
        signoff_evidence_sha256_normalized = (ctx.signoff or {}).get(
            "license_evidence_sha256_normalized_text"
        )
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        current_evidence_sha256_raw = (
            evidence.snapshot.get("sha256_raw_bytes") or current_evidence_sha256
        )
        current_evidence_sha256_normalized = evidence.snapshot.get("sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_evidence_sha256_raw,
            signoff_normalized_sha=signoff_evidence_sha256_normalized,
            current_raw_sha=current_evidence_sha256_raw,
            current_normalized_sha=current_evidence_sha256_normalized,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        signoff_is_stale = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            cfg.license_map.evidence_change_policy,
            cfg.license_map.cosmetic_change_policy,
        )
        evaluation = {
            "id": ctx.tid,
            "name": ctx.name,
            "enabled": ctx.enabled,
            "evaluated_at_utc": utc_now(),
            "pipeline_version": VERSION,
            "review_required": review_required,
            "review_signoff": ctx.signoff or None,
            "review_status": review_status or "pending",
            "denylist_hits": ctx.dl_hits,
            "license_profile": ctx.profile,
            "spdx_hint": ctx.spdx_hint,
            "resolved_spdx": resolved,
            "resolved_spdx_confidence": resolved_confidence,
            "resolved_spdx_confidence_reason": confidence_reason,
            "restriction_hits": restriction_hits,
            "license_gates": ctx.license_gates,
            "content_checks": ctx.content_checks,
            "content_check_actions": ctx.content_check_actions,
            "effective_bucket": eff_bucket,
            "queue_bucket": eff_bucket,
            "license_evidence_url": ctx.evidence_url,
            "evidence_snapshot": evidence.snapshot,
            "evidence_headers_used": redact_headers_for_manifest(cfg.headers),
            "license_change_detected": evidence.license_change_detected,
            "signoff_evidence_sha256": signoff_evidence_sha256,
            "signoff_evidence_sha256_raw_bytes": signoff_evidence_sha256_raw,
            "signoff_evidence_sha256_normalized_text": signoff_evidence_sha256_normalized,
            "current_evidence_sha256": current_evidence_sha256,
            "current_evidence_sha256_raw_bytes": current_evidence_sha256_raw,
            "current_evidence_sha256_normalized_text": current_evidence_sha256_normalized,
            "signoff_is_stale": signoff_is_stale,
            "signoff_cosmetic_change": cosmetic_change,
            "evidence_raw_changed": evidence.snapshot.get("raw_changed_from_previous"),
            "evidence_normalized_changed": evidence.snapshot.get(
                "normalized_changed_from_previous"
            ),
            "evidence_cosmetic_change": evidence.snapshot.get("cosmetic_change"),
            "download": ctx.target.get("download", {}),
            "build": ctx.target.get("build", {}),
            "data_type": ctx.target.get("data_type", []),
            "priority": ctx.target.get("priority", None),
            "statistics": ctx.target.get("statistics", {}),
            "split_group_id": ctx.split_group_id,
            "no_fetch_missing_evidence": evidence.no_fetch_missing_evidence,
            "require_yellow_signoff": cfg.require_yellow_signoff,
            "output_pool": out_pool,
        }
        evaluation.update(self.build_evaluation_extras(ctx.target, ctx.routing))
        evaluation["routing"] = ctx.routing
        return evaluation

    def build_row(
        self,
        ctx: TargetContext,
        license_map: LicenseMap,
        evidence: EvidenceResult,
        restriction_hits: list[str],
        resolved: str,
        resolved_confidence: float,
        eff_bucket: str,
        review_required: bool,
        out_pool: str,
    ) -> dict[str, Any]:
        signoff_evidence_sha256 = (ctx.signoff or {}).get("license_evidence_sha256")
        signoff_evidence_sha256_raw = (ctx.signoff or {}).get(
            "license_evidence_sha256_raw_bytes"
        ) or signoff_evidence_sha256
        signoff_evidence_sha256_normalized = (ctx.signoff or {}).get(
            "license_evidence_sha256_normalized_text"
        )
        current_evidence_sha256 = evidence.snapshot.get("sha256")
        current_evidence_sha256_raw = (
            evidence.snapshot.get("sha256_raw_bytes") or current_evidence_sha256
        )
        current_evidence_sha256_normalized = evidence.snapshot.get("sha256_normalized_text")
        raw_mismatch, normalized_mismatch, cosmetic_change = compute_signoff_mismatches(
            signoff_raw_sha=signoff_evidence_sha256_raw,
            signoff_normalized_sha=signoff_evidence_sha256_normalized,
            current_raw_sha=current_evidence_sha256_raw,
            current_normalized_sha=current_evidence_sha256_normalized,
            text_extraction_failed=bool(evidence.snapshot.get("text_extraction_failed")),
        )
        signoff_is_stale = resolve_evidence_change(
            raw_mismatch,
            normalized_mismatch,
            cosmetic_change,
            license_map.evidence_change_policy,
            license_map.cosmetic_change_policy,
        )
        row = {
            "id": ctx.tid,
            "name": ctx.name,
            "effective_bucket": eff_bucket,
            "queue_bucket": eff_bucket,
            "license_profile": ctx.profile,
            "resolved_spdx": resolved,
            "resolved_spdx_confidence": resolved_confidence,
            "restriction_hits": restriction_hits,
            "license_evidence_url": ctx.evidence_url,
            "manifest_dir": str(ctx.target_manifest_dir),
            "download": ctx.target.get("download", {}),
            "build": ctx.target.get("build", {}),
            "data_type": ctx.target.get("data_type", []),
            "priority": ctx.target.get("priority", None),
            "enabled": ctx.enabled,
            "statistics": ctx.target.get("statistics", {}),
            "content_checks": ctx.content_checks,
            "content_check_actions": ctx.content_check_actions,
            "split_group_id": ctx.split_group_id,
            "denylist_hits": ctx.dl_hits,
            "review_required": review_required,
            "license_change_detected": evidence.license_change_detected,
            "signoff_evidence_sha256": signoff_evidence_sha256,
            "signoff_evidence_sha256_raw_bytes": signoff_evidence_sha256_raw,
            "signoff_evidence_sha256_normalized_text": signoff_evidence_sha256_normalized,
            "current_evidence_sha256": current_evidence_sha256,
            "current_evidence_sha256_raw_bytes": current_evidence_sha256_raw,
            "current_evidence_sha256_normalized_text": current_evidence_sha256_normalized,
            "signoff_is_stale": signoff_is_stale,
            "signoff_cosmetic_change": cosmetic_change,
            "evidence_raw_changed": evidence.snapshot.get("raw_changed_from_previous"),
            "evidence_normalized_changed": evidence.snapshot.get(
                "normalized_changed_from_previous"
            ),
            "evidence_cosmetic_change": evidence.snapshot.get("cosmetic_change"),
            "output_pool": out_pool,
            "routing_subject": ctx.routing.get("subject"),
            "routing_domain": ctx.routing.get("domain"),
            "routing_category": ctx.routing.get("category"),
            "routing_level": ctx.routing.get("level"),
            "routing_granularity": ctx.routing.get("granularity"),
            "routing_confidence": ctx.routing.get("confidence"),
            "routing_reason": ctx.routing.get("reason"),
        }
        row.update(self.build_row_extras(ctx.target, ctx.routing))
        if self.INCLUDE_ROUTING_DICT_IN_ROW and "routing" not in row:
            row["routing"] = ctx.routing
        return row

    def _sum_evidence_bytes(self, evidence_results: list[EvidenceResult]) -> int:
        total = 0
        for evidence in evidence_results:
            snapshot = evidence.snapshot or {}
            bytes_value = snapshot.get("bytes")
            if bytes_value is None:
                fetch_meta = snapshot.get("fetch_meta") or {}
                bytes_value = fetch_meta.get("bytes")
            if bytes_value:
                total += int(bytes_value)
        return total

    def _count_evidence_fetches(
        self, evidence_results: list[EvidenceResult]
    ) -> tuple[int, int, int, int]:
        fetches = 0
        ok = 0
        errors = 0
        skipped = 0
        for evidence in evidence_results:
            snapshot = evidence.snapshot or {}
            status = snapshot.get("status") or "unknown"
            if status in {"skipped", "no_url"}:
                skipped += 1
                continue
            fetches += 1
            if status == "ok":
                ok += 1
            else:
                errors += 1
        return fetches, ok, errors, skipped

    def build_metrics_payload(
        self,
        cfg: DriverConfig,
        results: ClassificationResult,
        collector: MetricsCollector,
    ) -> dict[str, Any]:
        timings_ms = {
            timer.name: timer.duration_ms
            for timer in collector.timers
            if timer.duration_ms is not None
        }
        targets_enabled = sum(1 for target in cfg.targets if target.get("enabled", True))
        counts = {
            "targets_total": len(cfg.targets),
            "targets_enabled": targets_enabled,
            "queued_green": len(results.green_rows),
            "queued_yellow": len(results.yellow_rows),
            "queued_red": len(results.red_rows),
            "warnings": len(results.warnings),
            "evidence_fetches": results.evidence_fetches,
            "evidence_fetch_ok": results.evidence_fetch_ok,
            "evidence_fetch_errors": results.evidence_fetch_errors,
            "evidence_fetch_skipped": results.evidence_fetch_skipped,
        }
        return {
            "run_id": cfg.checks_run_id,
            "pipeline_id": self.DOMAIN,
            "started_at_utc": collector.start_time,
            "ended_at_utc": utc_now(),
            "counts": counts,
            "bytes": {"evidence_fetched": results.evidence_bytes},
            "timings_ms": timings_ms,
        }

    def emit_queues(self, queues_root: Path, results: ClassificationResult) -> None:
        emit_queues(queues_root, results)

    def emit_summary(self, cfg: DriverConfig, results: ClassificationResult) -> None:
        failed_targets = [
            {
                "id": warning.get("target_id", "unknown"),
                "error": warning.get("message") or warning.get("type") or "warning",
            }
            for warning in results.warnings
        ]
        counts = Counter(
            {
                "targets_total": len(cfg.targets),
                "queued_green": len(results.green_rows),
                "queued_yellow": len(results.yellow_rows),
                "queued_red": len(results.red_rows),
                "warnings": len(results.warnings),
                "failed": len(failed_targets),
            }
        )
        summary = {
            "run_at_utc": utc_now(),
            "targets_total": len(cfg.targets),
            "queued_green": len(results.green_rows),
            "queued_yellow": len(results.yellow_rows),
            "queued_red": len(results.red_rows),
            "targets_path": str(cfg.targets_path),
            "license_map_path": [str(p) for p in cfg.license_map_path],
            "manifests_root": str(cfg.manifests_root),
            "queues_root": str(cfg.queues_root),
            "warnings": results.warnings,
            "counts": dict(counts),
            "failed_targets": failed_targets,
        }
        summary.update(build_artifact_metadata(written_at_utc=summary["run_at_utc"]))
        write_json(cfg.queues_root / "run_summary.json", summary)

    def emit_report(self, cfg: DriverConfig, results: ClassificationResult) -> None:
        report = generate_dry_run_report(
            queues_root=cfg.queues_root,
            targets=cfg.targets,
            green_rows=results.green_rows,
            yellow_rows=results.yellow_rows,
            red_rows=results.red_rows,
            warnings=results.warnings,
        )
        if not cfg.args.quiet:
            logger.info(report)

    @classmethod
    def build_arg_parser(cls) -> argparse.ArgumentParser:
        ap = argparse.ArgumentParser(description=f"Pipeline Driver v{VERSION}")
        ap.add_argument(
            "--targets",
            required=True,
            help=f"Path to {cls.TARGETS_LABEL} (v0.9)",
        )
        ap.add_argument(
            "--license-map",
            default=None,
            help="Path to license_map.yaml (defaults to companion_files.license_map)",
        )
        ap.add_argument(
            "--dataset-root",
            default=None,
            help="Override dataset root (sets manifests/queues defaults)",
        )
        ap.add_argument(
            "--manifests-root",
            default=None,
            help="Override manifests_root (alias: --out-manifests)",
        )
        ap.add_argument(
            "--queues-root", default=None, help="Override queues_root (alias: --out-queues)"
        )
        ap.add_argument("--ledger-root", default=None, help="Override ledger_root")
        ap.add_argument("--out-manifests", default=None, help=argparse.SUPPRESS)
        ap.add_argument("--out-queues", default=None, help=argparse.SUPPRESS)
        ap.add_argument(
            "--no-fetch",
            action="store_true",
            help="Do not fetch evidence URLs (offline mode - v0.9: forces YELLOW if no snapshot)",
        )
        ap.add_argument(
            "--retry-max", type=int, default=None, help="Max retries for evidence fetching"
        )
        ap.add_argument(
            "--retry-backoff", type=float, default=None, help="Backoff base for evidence fetching"
        )
        ap.add_argument("--max-retries", type=int, default=None, help=argparse.SUPPRESS)
        ap.add_argument(
            "--min-license-confidence",
            type=float,
            default=0.6,
            help="Minimum SPDX confidence required before GREEN classification",
        )
        ap.add_argument(
            "--evidence-header",
            action="append",
            default=[],
            help="Custom header for evidence fetcher (KEY=VALUE). Useful for license-gated pages",
        )
        ap.add_argument(
            "--allow-private-evidence-hosts",
            action="store_true",
            help="Allow evidence URLs that resolve to private, loopback, or link-local IPs (unsafe).",
        )
        ap.add_argument(
            "--strict",
            action="store_true",
            help="Treat config warnings (such as unknown license_gates/content_checks) as errors.",
        )
        ap.add_argument("--quiet", action="store_true", help="Suppress dry-run report output")
        add_logging_args(ap)
        return ap

    @classmethod
    def main(cls) -> None:
        args = cls.build_arg_parser().parse_args()
        configure_logging(level=args.log_level, fmt=args.log_format)
        cls().run(args)
