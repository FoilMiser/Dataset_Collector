from __future__ import annotations

import dataclasses
import ipaddress
from collections.abc import Callable
from pathlib import Path
from typing import Any

from collector_core.acquire_limits import RunByteBudget
from collector_core.stability import stable_api

StrategyHandler = Callable[["AcquireContext", dict[str, Any], Path], list[dict[str, Any]]]
PostProcessor = Callable[
    ["AcquireContext", dict[str, Any], Path, str, dict[str, Any]], dict[str, Any] | None
]


@stable_api
@dataclasses.dataclass(frozen=True)
class RootsDefaults:
    raw_root: str
    manifests_root: str
    ledger_root: str
    logs_root: str


@stable_api
@dataclasses.dataclass
class Roots:
    raw_root: Path
    manifests_root: Path
    ledger_root: Path
    logs_root: Path


@stable_api
@dataclasses.dataclass
class Limits:
    limit_targets: int | None
    limit_files: int | None
    max_bytes_per_target: int | None


@stable_api
@dataclasses.dataclass
class RunMode:
    execute: bool
    overwrite: bool
    verify_sha256: bool
    verify_zenodo_md5: bool
    enable_resume: bool
    workers: int


@stable_api
@dataclasses.dataclass
class RetryConfig:
    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0


@stable_api
@dataclasses.dataclass
class AcquireContext:
    roots: Roots
    limits: Limits
    mode: RunMode
    retry: RetryConfig
    run_budget: RunByteBudget | None = None
    allow_non_global_download_hosts: bool = False
    internal_mirror_allowlist: "InternalMirrorAllowlist" = dataclasses.field(
        default_factory=lambda: InternalMirrorAllowlist()
    )
    cfg: dict[str, Any] | None = None
    checks_run_id: str = ""


@stable_api
@dataclasses.dataclass(frozen=True)
class InternalMirrorAllowlist:
    hosts: frozenset[str] = frozenset()
    networks: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()

    def allows_host(self, hostname: str) -> bool:
        normalized = hostname.lower().rstrip(".")
        for host in self.hosts:
            if host.startswith("."):
                if normalized.endswith(host):
                    return True
            elif normalized == host:
                return True
        return False

    def allows_ip(self, ip_value: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        return any(ip_value in network for network in self.networks)


def _normalize_internal_mirror_allowlist(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _build_internal_mirror_allowlist(values: list[str]) -> InternalMirrorAllowlist:
    hosts: set[str] = set()
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for entry in values:
        text = entry.strip()
        if not text:
            continue
        try:
            network = ipaddress.ip_network(text, strict=False)
        except ValueError:
            hosts.add(text.lower().rstrip("."))
        else:
            networks.append(network)
    return InternalMirrorAllowlist(hosts=frozenset(hosts), networks=tuple(networks))
