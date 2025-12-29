# Dataset Collector v2 — Remaining Fixes & Holes (collector-only)

This doc targets the repo snapshot in `Dataset_Collector-main (3).zip`.

Your current direction is correct: **collector-only** (acquire targeted datasets + license screening), runnable from **JupyterLab on native Windows** via the Windows-first notebook cell that calls `tools/build_natural_corpus.py`.

There are only a few remaining issues, but **one of them is a true run-blocker** when orchestrating *all pipelines*.

---

## TL;DR checklist

### Must-fix (run-blockers / correctness)
- [ ] **Unify queue file naming** across *all* pipelines (or add orchestrator fallback):
  - `green_download.jsonl`
  - `yellow_pipeline.jsonl`
  - `red_rejected.jsonl`
- [ ] Fix **regcomp** queue names (currently `*_queue.jsonl`) so it matches the contract/orchestrator
- [ ] Fix **metrology** red queue name (currently `red.jsonl`)

### Should-fix (silent no-ops / “looks successful but did nothing”)
- [ ] Disable **regcomp placeholder targets** that have `download.strategy: none` but `enabled: true`
- [ ] Add a lightweight **preflight validator** so enabled targets can’t drift into “strategy not implemented” again

### Nice-to-have (Windows/Jupyter polish)
- [ ] Document or enforce external tool requirements (`git`, `aws`, `aria2c`)
- [ ] Clarify that `screen_yellow/merge/catalog` are optional unless you also add normalization (JSONL canonicalization)

---

## 1) Run-blocker: queue filename mismatch vs orchestrator

### Symptom
When you run the **Windows-first notebook cell**:

```py
python tools/build_natural_corpus.py --stages classify,acquire_green,acquire_yellow ...
```

The orchestrator expects these queue filenames under each domain’s `_queues/`:
- `green_download.jsonl`
- `yellow_pipeline.jsonl`

However:

- `regcomp_pipeline_v2/pipeline_driver.py` writes:
  - `green_queue.jsonl`
  - `yellow_queue.jsonl`
  - `red_excluded.jsonl`

- `metrology_pipeline_v2/pipeline_driver.py` writes:
  - `red.jsonl` (instead of `red_rejected.jsonl`)

This causes “file not found” failures and inconsistent red queue semantics.

### Recommended fix
**Standardize queue naming repo-wide** so every pipeline emits the same filenames.

#### Standard queue filenames (repo-wide contract)
- GREEN queue: `green_download.jsonl`
- YELLOW queue: `yellow_pipeline.jsonl`
- RED queue: `red_rejected.jsonl`

---

## 2) Patch plan: standardize queue naming

### 2.1 regcomp_pipeline_v2 — rename queue outputs

**Files**
- `regcomp_pipeline_v2/pipeline_driver.py`
- `regcomp_pipeline_v2/run_pipeline.sh` (and review stage)
- (optional) any README/comments inside the regcomp pipeline

#### A) Update `pipeline_driver.py`
Change the output filenames to match the repo standard.

Search/replace:
- `green_queue.jsonl` → `green_download.jsonl`
- `yellow_queue.jsonl` → `yellow_pipeline.jsonl`
- `red_excluded.jsonl` → `red_rejected.jsonl`

Make sure you update:
- the actual `write_jsonl(...)` calls
- any docstrings / “Produces:” sections
- any places where the file names are referenced for summaries

#### B) Update `run_pipeline.sh`
Make regcomp’s `run_acquire()` match other pipelines (e.g., `math_pipeline_v2/run_pipeline.sh`).

**Current regcomp behavior**
- it looks for `"$QUEUES_ROOT/${bucket}_queue.jsonl"` for both green and yellow

**Target behavior**
- green uses `green_download.jsonl`
- yellow uses `yellow_pipeline.jsonl`

Concrete pattern to copy:

```bash
run_acquire() {
  local bucket="$1"
  local queue_file="$QUEUES_ROOT/${bucket}_download.jsonl"
  if [[ "$bucket" == "yellow" ]]; then
    queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
  fi
  ...
}
```

Also update:
- `run_review()` to point at `yellow_pipeline.jsonl`

---

### 2.2 metrology_pipeline_v2 — fix the red queue filename

**File**
- `metrology_pipeline_v2/pipeline_driver.py`

Change:
- `write_jsonl(queues_root / "red.jsonl", red_rows)`
to:
- `write_jsonl(queues_root / "red_rejected.jsonl", red_rows)`

Also update any docstring text that lists `red.jsonl`.

---

### 2.3 Optional resilience: make the orchestrator tolerant anyway
Even after standardizing, it’s worth making `tools/build_natural_corpus.py` tolerant to future drift.

**File**
- `tools/build_natural_corpus.py`

Add a helper:

```py
def pick_existing(queues_root: Path, candidates: list[str]) -> Path:
    for name in candidates:
        p = queues_root / name
        if p.exists():
            return p
    raise FileNotFoundError(
        f"None of {candidates} exist under {queues_root}. "
        "Did you run the classify stage first?"
    )
```

Then in `acquire_green`:

```py
queue = pick_existing(queues_root, ["green_download.jsonl", "green_queue.jsonl"])
```

In `acquire_yellow`:

```py
queue = pick_existing(queues_root, ["yellow_pipeline.jsonl", "yellow_queue.jsonl"])
```

For red (future):

```py
queue = pick_existing(queues_root, ["red_rejected.jsonl", "red_excluded.jsonl", "red.jsonl"])
```

This makes the tool robust even if one pipeline lags behind the naming contract.

---

## 3) Silent failure risk: regcomp has enabled placeholders with `strategy: none`

### Symptom
`regcomp_pipeline_v2/targets_regcomp.yaml` still contains **enabled** targets that cannot be acquired:

- `iso_iec_standards`
- `astm_standards`
- `nfpa_codes`
- `cis_benchmarks`
- `pci_dss`
- `aicpa_soc2`
- `ucca_ucc`
- `commercial_legal_publishers`
- `commercial_compliance_courseware`

They are marked `enabled: true` but use `download.strategy: none`.

This is a classic “pipeline ran successfully, but downloaded nothing” trap.

### Fix
Set these to `enabled: false`.

Also add an explicit note so future-you knows *why* they’re disabled:

```yaml
enabled: false
notes: "Commercial / paid standards or restricted distribution; not eligible for training data collection."
download:
  strategy: none
```

---

## 4) Add a lightweight preflight validator (prevents future drift)

Right now your safety net is “handler missing → noop”. That’s great for experimentation, but bad for reliability.

Add `tools/preflight.py` with checks like:

- every pipeline in `tools/pipeline_map.yaml` exists
- every `targets_yaml` exists
- every `enabled: true` target has:
  - `download.strategy` present and not `none`
  - strategy key exists in that pipeline’s `STRATEGY_HANDLERS`
- optionally: warn if a strategy requires external tools

### Suggested behavior
- **Fail hard** (exit non-zero) on:
  - enabled target with missing/none strategy
  - enabled target strategy not implemented in that pipeline
- **Warn only** on:
  - missing external tools (`git`, `aws`, `aria2c`) because users may not use those targets

### Where to run it
- At the start of `tools/build_natural_corpus.py`, before running stages
- Or as a separate notebook cell: `python tools/preflight.py`

---

## 5) External tool dependencies to document (Windows/Jupyter)

Some strategies depend on external CLIs:

- `download.strategy: git`
  - requires `git` installed and on PATH
- `download.strategy: torrent`
  - your workers reference `aria2c`
- `download.strategy: s3_sync` / `aws_requester_pays`
  - requires AWS CLI (`aws`) installed and on PATH
  - this affects `kg_nav_pipeline_v2` in particular

### Fix
Add a section to the root `README.md`:
- “Prereqs (Windows)”
- include minimal install links/notes:
  - Git for Windows
  - AWS CLI v2 installer (recommended) or `pip install awscli` (v1)
  - aria2 (windows build) or install via package manager if available

Also consider adding a preflight warning message when tools are missing.

---

## 6) Optional clarity: downstream stages are “best-effort” unless you add normalization

Your notebook correctly defaults to:

```py
STAGES = ["classify", "acquire_green", "acquire_yellow"]
```

Keep it that way for collector-only.

If you keep the advanced bash cell with `screen_yellow/merge/catalog`, add a comment like:

> These stages assume canonical `.jsonl(.gz)` inputs. Many acquisition strategies download archives/PDFs/repos/HF datasets which are not auto-normalized into JSONL yet.

If/when you want `merge/catalog` to represent actual train-ready shards, you’ll need a normalization stage (out of scope for collector-only).

---

## 7) Verification plan (do this after the patches)

### A) Smoke test: classify-only for all pipelines
From Jupyter:

- `EXECUTE = False`
- `STAGES = ["classify"]`

Verify per domain root:
- `_queues/green_download.jsonl` exists
- `_queues/yellow_pipeline.jsonl` exists
- `_queues/red_rejected.jsonl` exists (or is empty but present)

### B) Orchestrator run: green acquisition only
- `EXECUTE = True`
- `STAGES = ["classify", "acquire_green"]`

Verify:
- `raw/green/...` contains expected target folders
- `_ledger/...` updated
- acquisition logs written under `_logs/`

### C) YELLOW acquisition
- add `acquire_yellow`
- spot-check at least one yellow target downloads

### D) Preflight
Run `python tools/preflight.py` and ensure it fails if you intentionally enable a `strategy: none` target.

---

## Suggested commit order

1) **Fix queue naming**
   - regcomp driver + run script
   - metrology driver
   - (optional) build_natural_corpus fallback detection

2) **Disable regcomp placeholders**
   - edit `targets_regcomp.yaml`

3) **Add preflight**
   - new `tools/preflight.py`
   - wire into orchestrator or notebook

4) **Docs**
   - root README prerequisites section
   - brief note in notebook about downstream stages

---

## End state definition (what “done” looks like)

You can open JupyterLab on Windows, run the Windows-first cell with:

```py
STAGES = ["classify", "acquire_green", "acquire_yellow"]
EXECUTE = True
```

and it completes across all pipelines in `tools/pipeline_map.yaml` with:

- consistent queue naming per domain
- no enabled placeholders that noop
- clear warnings for missing external tools (if relevant)
- no “file not found” failures in `build_natural_corpus.py`
