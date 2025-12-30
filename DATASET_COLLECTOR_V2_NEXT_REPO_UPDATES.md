# Dataset Collector v2 — Next Repo Updates (Repo‑wide)

This is a **repo‑wide** update plan for the current `Dataset_Collector-main` snapshot you uploaded (“(5).zip”).

You’ve successfully landed the big “collector‑only” refactor goals:

- `final/` is gone from the layout (`tools/init_layout.py`) and from patched targets (`tools/patch_targets.py`).
- The notebook is wired to run through `tools/build_natural_corpus.py`.
- `tools/preflight.py` has the dynamic import crash fixed (module registered in `sys.modules` before exec).

What’s left is mostly **policy correctness (YELLOW gating)** + **consistency** + a couple of orchestrator “footguns”.

---

## What looks good right now

### Repo-level
- Clear output contract (`docs/output_contract.md`) centered on `raw/`, `screened_yellow/`, `combined/`, plus `_ledger/_catalogs/_queues/_pitches/_manifests/_logs`.
- `tools/pipeline_map.yaml` gives a single place to control which pipelines run and where they write outputs.
- `tools/build_natural_corpus.py` already supports:
  - `--execute` vs dry-run
  - per-pipeline staged execution
  - per-pipeline patched targets dropped into `_manifests/`

### Pipeline-level
- `pipeline_driver.py` in each pipeline is doing the right conceptual job:
  - fetch/store license evidence snapshots
  - resolve SPDX with confidence
  - apply restriction phrase + denylist gating
  - emit `green_download.jsonl`, `yellow_pipeline.jsonl`, `red_rejected.jsonl`

---

## Patch Set A — **Enforce YELLOW signoff gating repo‑wide** (highest priority)

### Why this is required
Right now, most pipelines will run `yellow_screen_worker.py` on **all** YELLOW queue rows, even when no human signoff exists. That defeats the core “license screening” promise: **YELLOW should mean “needs manual review before being allowed into screened/combined.”**

**Current state**
- `safety_incident_pipeline_v2/yellow_screen_worker.py` implements signoff gating.
- Most other `*_pipeline_v2/yellow_screen_worker.py` files do **not**.

### A1) Flip the global default: require signoff for YELLOW
For every `*_pipeline_v2/targets_*.yaml`, set:

```yaml
globals:
  require_yellow_signoff: true
```

(You already have this set to `true` in `targets_safety_incident.yaml`, but e.g. `targets_math.yaml` is still `false`.)

**Per-target escape hatch (optional)**
If you have a YELLOW target that is “yellow only because it’s record-level licensed” (i.e., you filter each record by SPDX) and you intentionally want to allow it without review:

```yaml
targets:
  - id: some_record_level_dataset
    yellow_screen:
      allow_without_signoff: true
```

…and implement that key in the worker (see A2).

### A2) Update **all** yellow_screen_worker.py to respect signoffs
Bring all pipelines in line with the safety incident implementation.

**Required behavior**
- If `globals.require_yellow_signoff: true`:
  - `review_signoff.json` must exist and `status == "approved"` to pass.
  - `status == "rejected"` should pitch/skip everything for that target.
  - Missing signoff should pitch/skip everything for that target.

**Where to patch**
- Every `*_pipeline_v2/yellow_screen_worker.py` **except** safety_incident (already has it).

**Minimal code pattern (drop-in)**
Add helpers:

```py
def load_signoff(manifest_dir: Path) -> Optional[Dict[str, Any]]:
    p = manifest_dir / "review_signoff.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
```

Inside `process_target(..., queue_row, ...)`:

```py
g = (cfg.get("globals", {}) or {})
require_signoff = bool(g.get("require_yellow_signoff", False))

target_cfg = next((t for t in cfg.get("targets", []) if t.get("id") == target_id), {})
allow_without_signoff = bool((target_cfg.get("yellow_screen", {}) or {}).get("allow_without_signoff", False))

manifest_dir = Path(queue_row.get("manifest_dir") or roots.manifests_root / target_id)
signoff = load_signoff(manifest_dir) or {}
status = str(signoff.get("status", "") or "").lower()

if require_signoff and not allow_without_signoff:
    if status == "rejected":
        # pitch/skip the whole target
        ...
    elif status != "approved":
        # pitch/skip the whole target
        ...
```

**Important detail:** your `pipeline_driver.py` already writes `manifest_dir` into the queue rows, so the worker can find signoffs without extra configuration.

### A3) Make “pitched” inspectable: write pitch samples to `_pitches/`
Most pipelines currently only append pitched rows to `_ledger/yellow_pitched.jsonl` (or not at all), which makes it hard to audit *why* things were rejected.

Standardize this:

- `_ledger/yellow_passed.jsonl`: one row per accepted record (with `output_shard`)
- `_ledger/yellow_pitched.jsonl`: lightweight pitched rows (reason + sample_id)
- `_pitches/yellow_pitch.jsonl`: **optional sample payloads** for debugging (cap it!)

Recommended cap pattern:

- Write at most **N samples per (target_id, reason)** per run (e.g., N=25), to avoid exploding disk usage.
- Store minimal fields (`id`, `text` excerpt, `source_url`, `reason`) rather than entire raw objects when possible.

### A4) Notebook should include the manual review step explicitly
Right now, the notebook goes straight from `classify -> acquire -> screen`. With signoff gating enabled, users need a clear “pause here” step.

Add a section like:

1) Run `classify`
2) Review YELLOW queue:
   - `python <pipeline>/review_queue.py list ...`
   - `python <pipeline>/review_queue.py approve/reject ...`
3) Then run `screen_yellow`

---

## Patch Set B — Fix remaining “difficulty” leftovers and output inconsistencies

### B1) Fix `safety_incident_pipeline_v2/catalog_builder.py` (difficulty leftovers)
`safety_incident_pipeline_v2/catalog_builder.py` still references difficulty artifacts (`difficulty_index.jsonl`, `difficulty_pitched.jsonl`). Those are no longer produced in collector‑only mode.

**Update required**
- Remove any “difficulty” ledger expectations from that catalog builder.
- Align it with the other pipelines’ `catalog_builder.py` behavior:
  - report on `raw`, `screened_yellow`, `combined`
  - report on the ledgers actually written by this pipeline

### B2) Standardize YELLOW ledger file naming across pipelines (recommended)
You currently have multiple conventions:

- Most pipelines: `_ledger/yellow_passed.jsonl` + `_ledger/yellow_pitched.jsonl`
- Safety incident: `_ledger/yellow_pass.jsonl` + `_pitches/yellow_pitch.jsonl`
- 3D modeling: writes `yellow_pitched.jsonl` into both `_ledger` **and** `_pitches`

Pick one convention and apply repo‑wide. The least disruptive option is:

- Keep `_ledger/yellow_passed.jsonl` and `_ledger/yellow_pitched.jsonl` (since most catalogs already expect these)
- Add `_pitches/yellow_pitch.jsonl` for sampled pitched records

Then update `docs/output_contract.md` to mention the canonical ledger filenames you’ve chosen.

---

## Patch Set C — Orchestrator robustness and footgun removal

### C1) Abort if preflight fails
In `tools/build_natural_corpus.py`, you call `run_preflight(...)` but do not stop if it returns non‑zero.

**Update required**
- If `run_preflight(...) != 0`, `sys.exit(1)` before running any pipelines.

This prevents “half runs” with confusing partial outputs.

### C2) Make pipeline_map environment-specific (Windows vs WSL)
`tools/pipeline_map.yaml` uses `E:/...` destinations. That’s correct for native Windows Python, but will create odd paths if someone runs under WSL/Linux.

**Update required** (choose one)

**Option 1 (simple):** two map files
- `tools/pipeline_map.windows.yaml` (E:/...)
- `tools/pipeline_map.wsl.yaml` (/mnt/e/...)

…and teach the notebook to select one.

**Option 2 (nicer):** allow `destination_root` to be templated
- e.g. `${DATASET_ROOT}` env var expansion in `build_natural_corpus.py`

### C3) Tighten stage defaults for a signoff-driven workflow
`tools/build_natural_corpus.py` defaults to:

```py
DEFAULT_STAGES = ["classify", "acquire_green", "acquire_yellow", "screen_yellow", "merge", "catalog"]
```

That’s not wrong, but once signoff gating is enabled, it’s common to want:

- “run up to acquire” in one session
- “review”
- “run screen+merge+catalog” later

**Update recommended**
- Add a `--mode` flag with presets:
  - `--mode collect` → `classify,acquire_green,acquire_yellow`
  - `--mode compile` → `screen_yellow,merge,catalog`
  - `--mode full` (current default)

…and make the notebook use `collect` then `compile`.

---

## Patch Set D — Reduce config duplication (quality-of-life, but worth it)

Several files are duplicated across pipelines with drift risk:

- `denylist.yaml`
- `license_map.yaml`

**Update recommended**
- Create `configs/shared/` and move shared files there:

```
configs/shared/denylist.yaml
configs/shared/license_map.yaml
```

Then in each targets YAML:

```yaml
companion_files:
  denylist: ../configs/shared/denylist.yaml
  license_map: ../configs/shared/license_map.yaml
```

This is “cheap consolidation”: no YAML includes and no merge semantics, just consistent paths.

---

## Patch Set E — Make installs / Jupyter usage smoother

### E1) Provide one “install everything you need” path
There’s `requirements.txt` + `requirements-dev.txt` at repo root, and per-pipeline `requirements.txt` files with slightly different optional deps.

**Update recommended**
- Add `requirements-all.txt` at repo root containing the union of:
  - root requirements
  - any per-pipeline deps actually used by enabled targets (e.g., `datasets`, `pyarrow`, `boto3` if those strategies are enabled)

Then the notebook can simply say: `pip install -r requirements-all.txt`.

### E2) Add a tiny smoke-test script
Add `tools/smoke_test.py` to run:

- preflight
- dry-run classify for 1–2 pipelines
- verify queue files exist

This reduces “it ran but did nothing” situations.

---

## Quick regression checklist (do this after patching)

1) **Preflight fails → orchestrator stops**
   - Break one pipeline’s `acquire_worker.py` import intentionally; confirm build script exits.

2) **YELLOW signoff required**
   - Set `globals.require_yellow_signoff: true`
   - Run `classify` to generate `yellow_pipeline.jsonl`
   - Run `screen_yellow` without any signoff → should pitch/skip everything (no screened shards)

3) **Approved YELLOW flows through**
   - Create `review_signoff.json` with `status: approved` for one target
   - Re-run `screen_yellow` → should produce `screened_yellow/<pool>/shards/*.jsonl.gz`

4) **Merge only includes approved**
   - Run `merge` and confirm combined shards are written

5) **Catalog no longer references difficulty**
   - Run `catalog` for safety incident and confirm it completes and does not mention difficulty ledgers

---

## “Do these first” short list

If you only do a few things next, do these in order:

1) **Repo‑wide signoff gating in `yellow_screen_worker.py`** + flip `require_yellow_signoff: true` in targets
2) Fix `safety_incident_pipeline_v2/catalog_builder.py` difficulty leftovers
3) Make `build_natural_corpus.py` abort on failed preflight
4) Split `pipeline_map` (Windows vs WSL) and update the notebook to pick one
