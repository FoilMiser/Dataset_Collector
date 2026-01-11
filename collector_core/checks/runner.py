from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from collector_core.artifact_metadata import build_artifact_metadata
from collector_core.checks.base import BaseCheck, CheckContext
from collector_core.checks.loader import load_builtin_checks
from collector_core.checks.registry import get_check
from collector_core.utils import ensure_dir, safe_filename, utc_now, write_json


def generate_run_id(prefix: str | None = None) -> str:
    stamp = utc_now()
    token = uuid.uuid4().hex[:8]
    base = f"{stamp}_{token}"
    if prefix:
        base = f"{prefix}_{base}"
    return safe_filename(base)


def _resolve_check_path(
    ledger_root: Path, run_id: str, target_id: str, check_name: str
) -> Path:
    safe_run = safe_filename(run_id)
    safe_target = safe_filename(target_id)
    safe_check = safe_filename(check_name)
    return ledger_root / safe_run / safe_target / "checks" / f"{safe_check}.json"


def _build_payload(
    *,
    run_id: str,
    target_id: str,
    stage: str,
    check_name: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "run_id": run_id,
        "target_id": target_id,
        "stage": stage,
        "check": check_name,
        "status": status,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
    }
    if result:
        payload["result"] = result
    if error:
        payload["error"] = error
    payload.update(build_artifact_metadata(written_at_utc=finished_at_utc))
    return payload


def run_checks_for_target(
    *,
    content_checks: list[str],
    ledger_root: Path,
    run_id: str,
    target_id: str,
    stage: str,
    target: dict[str, Any] | None = None,
    row: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not content_checks:
        return results
    load_builtin_checks()
    for check_name in content_checks:
        started_at = utc_now()
        check_cls = get_check(check_name)
        if not check_cls:
            finished_at = utc_now()
            payload = _build_payload(
                run_id=run_id,
                target_id=target_id,
                stage=stage,
                check_name=check_name,
                status="missing",
                started_at_utc=started_at,
                finished_at_utc=finished_at,
                result={"reason": "unregistered_check"},
            )
        else:
            ctx = CheckContext(
                run_id=run_id,
                target_id=target_id,
                stage=stage,
                content_checks=content_checks,
                target=target,
                row=row,
                extra=extra,
            )
            try:
                check = check_cls()
                result = check.run(ctx) or {}
                status = str(result.pop("status", "ok") or "ok")
                finished_at = utc_now()
                payload = _build_payload(
                    run_id=run_id,
                    target_id=target_id,
                    stage=stage,
                    check_name=check_name,
                    status=status,
                    started_at_utc=started_at,
                    finished_at_utc=finished_at,
                    result=result,
                )
            except Exception as exc:  # pragma: no cover - defensive
                finished_at = utc_now()
                payload = _build_payload(
                    run_id=run_id,
                    target_id=target_id,
                    stage=stage,
                    check_name=check_name,
                    status="error",
                    started_at_utc=started_at,
                    finished_at_utc=finished_at,
                    error=repr(exc),
                )
        path = _resolve_check_path(ledger_root, run_id, target_id, check_name)
        ensure_dir(path.parent)
        write_json(path, payload)
        results.append(payload)
    return results
