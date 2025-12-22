# Code Pipeline v2 Adaptation Plan (from code_pipeline_v1 → code_pipeline_v2)

## Goal

Update **code_pipeline_v1** so its **end-to-end behavior and stage flow matches `math_pipeline_v2`**, i.e.:

1. **Classify** targets (GREEN/YELLOW/RED) + snapshot license/ToU evidence into manifests.
2. **Acquire** GREEN and YELLOW into a *v2 raw layout* (`raw/{green|yellow}/{license_pool}/{target_id}/...`).
3. **Screen YELLOW** into canonical records with strict “anything unclear is pitched” behavior.
4. **Merge** canonical GREEN + screened YELLOW into combined shards with dedup + ledgers.
5. **Difficulty**: final light screen + difficulty assignment into `final/{license_pool}/d01..d10/`.
6. **Catalog**: summarize outputs/ledgers across stages.

This plan assumes you want to keep the same *ethics-first license gating* pattern and the same *filesystem contract* as math v2 (queues/ledgers/manifests/pitches + sharded jsonl.gz outputs).

---

## Current state vs. math_pipeline_v2 (gap analysis)

### What code_pipeline_v1 already has
- `pipeline_driver.py` (v1.0) that emits GREEN/YELLOW/RED queues + per-target manifests.
- `review_queue.py` manual signoff helper.
- `download_worker.py` (v0.9) for GREEN acquisition (v1 layout).
- `yellow_scrubber.py` (planning helper, not a canonical screener).
- `field_schemas.yaml` already defines **code_file_v1.0.0** and **code_chunk_v1.0.0**.
- `code_worker.py` exists but is only a **stub** (no real extraction).

### What’s missing relative to math_pipeline_v2 behavior
- **v2 raw layout** (`raw_root`, `screened_yellow_root`, `combined_root`, `final_root`, plus `_ledger`, `_pitches`).
- **Acquire worker v2** (math uses `acquire_worker.py`).
- **YELLOW screening worker** that produces canonical shards + pass/pitch ledgers (`yellow_screen_worker.py`).
- **Merge worker** (`merge_worker.py`) for GREEN+YELLOW canonical merging.
- **Difficulty worker** (`difficulty_worker.py`) + a `difficulties_code.yaml` map.
- `run_pipeline.sh` stage orchestration updated to match v2 order and stage names.

---

## Target code_pipeline_v2 layout (match math v2)

Recommended roots (in `targets_code.yaml -> globals`):

```
/data/code/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
  _catalogs/*.json
  _logs/*.log
```

Sharding contract (same knobs as math v2):
- `globals.sharding.max_records_per_shard`
- `globals.sharding.compression` (`gzip`)

---

## Step-by-step adaptation tasks

### 1) Create `code_pipeline_v2/` by cloning math v2 structure
Start from `math_pipeline_v2` as the “golden behavior” template and port code-specific pieces:

**Add to code repo**
- `acquire_worker.py` (copy from math v2; rename help text to code)
- `yellow_screen_worker.py` (new code-specific implementation, see §3)
- `merge_worker.py` (copy from math v2)
- `difficulty_worker.py` (copy from math v2; adjust payload handling + heuristics)
- `difficulties_code.yaml` (new, see §4)
- Replace/refresh `catalog_builder.py` and `run_pipeline.sh` with v2-compatible versions

**Keep (but update as needed)**
- `pipeline_driver.py` (copy from math v2 then tweak defaults to “code”)
- `review_queue.py` (can copy verbatim from math v2; already compatible)

---

### 2) Update `targets_code.yaml` to the v2 global roots + screening/sharding knobs

#### Required edits
- Replace `globals.storage_root / pools` with math-v2-style roots:

```yaml
globals:
  raw_root: /data/code/raw
  screened_yellow_root: /data/code/screened_yellow
  combined_root: /data/code/combined
  final_root: /data/code/final
  ledger_root: /data/code/_ledger
  pitches_root: /data/code/_pitches
  manifests_root: /data/code/_manifests
  queues_root: /data/code/_queues
  catalogs_root: /data/code/_catalogs
  logs_root: /data/code/_logs
  require_yellow_signoff: true
  sharding:
    max_records_per_shard: 50000
    compression: gzip
  screening:
    # code payload bounds (tune)
    min_chars: 50
    max_chars: 20000
    text_field_candidates: [text, code, content]
    deny_phrases: [noai, "no tdm", "no machine learning"]
```

- Add `companion_files.difficulties_map: ./difficulties_code.yaml`
- Ensure each target includes a `routing:` block (or allow driver defaults):
  - `subject: code`
  - `domain: python|js|cpp|...` (or “multi”)
  - `category: algorithms|web|systems|...`
  - optional `level:` (coarse prior)

#### Optional (recommended) code-specific knobs (kept from v1)
- Keep `near_duplicate_detection` but move under `globals` and decide *where enforced*:
  - cheapest place: **merge** (sha256 dedupe) + optional “minhash grouping report”
  - stricter: also in **screen_yellow** (vendor stripping reduces dupes)

---

### 3) Implement real code canonicalization (replace stub `code_worker.py`)

This is the biggest delta between “math text” and “code”.

#### Worker contract (so downstream v2 stages work)
Every emitted canonical record must contain:
- a **payload field** used for screening/difficulty: prefer `text` (but can be `code`; see §5 updates)
- a **content hash** at `hash.content_sha256` (or `content_sha256`)
- a **source block** with:
  - `target_id`, `license_profile` (permissive/copyleft/quarantine), `source_url`
  - repo/dataset provenance: repo url, commit, path, etc.
- optional: `routing` (subject/domain/category) to drive difficulty mapping

#### Code-specific transforms (aligning with CODE_PIPELINE_ADAPTATION.md)
Implement these in `code_worker.py` (and reuse for YELLOW screening):

1) **Repo snapshot**
- capture: repo URL, commit hash, subdir, list of extracted files
- hash LICENSE/NOTICE content (store in manifests)

2) **Path + file filters (must-have)**
- drop vendor/build/minified/binary:
  - `node_modules/`, `vendor/`, `dist/`, `build/`, `target/`, `.git/`, `__pycache__/`
  - `*.min.js`, `*.map`, `*.png`, `*.pdf`, etc.

3) **Language detection**
- extension → normalized language (python/js/ts/go/rust/java/cpp/…)
- optionally fall back to shebang / tree-sitter

4) **Secrets scanning (must-have)**
- regex + entropy heuristics (AWS keys, GitHub tokens, JWTs, PEM blocks)
- policy:
  - GREEN: redact secrets + mark `secrets_redacted=true` (or pitch if you want ultra-strict)
  - YELLOW: **pitch by default** unless redaction rules are explicitly allowed

5) **Chunking**
- emit `code_chunk_v1.0.0` records
- chunk by:
  - file boundaries first
  - then AST blocks or line windows (e.g., 200–400 lines) with overlap
- maintain provenance fields: file path, start_line/end_line, repo+commit

6) **License attachment**
- repo-level SPDX from manifest evaluation
- per-file overrides if detected (SPDX headers)
- if ambiguous → pitch (especially for YELLOW)

Output format:
- write `*.jsonl.gz` under the target dir (e.g., `raw/green/.../{target_id}/shards/code_00000.jsonl.gz`)
- ensure every record includes `hash.content_sha256`

---

### 4) Add `yellow_screen_worker.py` for code (strict pitch behavior)

Match math v2 outputs exactly, but with code extraction semantics:

Outputs (same paths/names as math v2):
- `screened_yellow/{license_pool}/shards/yellow_shard_00000.jsonl.gz`
- `_ledger/yellow_passed.jsonl`
- `_ledger/yellow_pitched.jsonl`
- `_manifests/{target_id}/yellow_screen_done.json`

Implementation approach:
- Read `queues/yellow_pipeline.jsonl` rows.
- For each target:
  - locate raw input at `raw/yellow/{license_pool}/{target_id}/...`
  - invoke the same underlying extraction pipeline as GREEN (`code_worker` core), but with **stricter rules**:
    - deny unknown/mixed licenses
    - deny missing provenance (no commit hash)
    - deny secrets unless redaction policy explicitly enabled
    - deny binary/minified/vendor content
  - shard accepted records to `screened_yellow/{pool}/shards/…`
  - append pitched reasons + minimal context to `_ledger/yellow_pitched.jsonl`

This preserves the *math v2 contract* (“YELLOW becomes canonical shards + ledgers; unclear gets pitched”).

---

### 5) Port `merge_worker.py` + make it payload-agnostic

Copy math v2 `merge_worker.py` verbatim, then add one small robustness tweak:

- If `hash.content_sha256` is missing, compute it from the first available payload field:
  - `text` → `code` → `content`

This prevents silent drops for code records that store payload as `code`.

Also consider (optional) “near-duplicate reporting”:
- keep math’s exact-hash dedupe for correctness
- optionally compute minhash signatures and emit duplicate groups to `_ledger/near_duplicates.jsonl` when enabled in `targets_code.yaml -> globals.near_duplicate_detection`.

---

### 6) Port `difficulty_worker.py` + add code-specific difficulty heuristics

Copy math v2 `difficulty_worker.py`, then:
- Change screening payload lookup to use `text|code|content`.
- Replace `heuristic_level(text)` with code-aware features (still cheap + deterministic):
  - lines of code, average identifier length, nesting depth heuristics
  - presence of templates/generics/macros
  - imports count and “heaviness” (e.g., CUDA, LLVM, kernel headers)
  - build-system complexity (CMake, Bazel) as a weak signal
- Keep `level_from_routing()` so targets can set priors via `routing`.

Outputs (same as math v2):
- `final/{license_pool}/d01..d10/shards/*.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_pitches/final_pitched.jsonl`

---

### 7) Create `difficulties_code.yaml` (v2-style, compatible with difficulty_worker)

Structure should mirror `difficulties_math.yaml`:
- `schema_version: "2.0"`
- `globals.default_subject: code`
- `subjects.code.domains.<domain>.categories.<category>.level: <1-10>`

Recommended domain/category starter map:
- domains: `python`, `javascript`, `typescript`, `java`, `cpp`, `rust`, `go`, `csharp`, `sql`, `shell`, `multi`
- categories: `basics`, `algorithms`, `data_structures`, `web_backend`, `web_frontend`, `systems`, `ml`, `devops`, `security`, `graphics`, `embedded`

You can start coarse (defaults) and refine after you have actual corpus stats.

---

### 8) Update `run_pipeline.sh` to match math v2 stage names + order

Implement stages (same names as math v2):
- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`
(+ keep `review` as a helper stage if you want parity with code v1 UX)

Each stage calls the v2 scripts with roots taken from `targets_code.yaml` (allow overrides for quick experiments).

---

### 9) Refresh `catalog_builder.py` for v2 layout

Start from math v2 `catalog_builder.py` and add code-focused reporting:
- counts by language / extension
- average LOC per record (approx)
- top repositories by record count (from provenance)
- pitch rates by reason (from ledgers)

Write output to `_catalogs/catalog.json` (and optionally CSV).

---

### 10) Requirements + tooling

Minimum additions likely needed for full code extraction:
- `GitPython` or shelling to `git`
- `detect-secrets` (or your own regex set)
- `tree_sitter` + language grammars (optional but helpful)
- `datasketch` (optional, for minhash near-duplicate grouping)
- `chardet` or `charset-normalizer` for decoding edge-case files

Keep the baseline deps from math v2 (`pyyaml`, `requests`, etc.).

---

## Execution parity checklist (what “same behavior” means)

After the port, you should be able to run these commands and see the same kind of artifacts as math v2:

1) **Classify (dry-run)**  
- creates `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`
- creates `_manifests/{target_id}/evaluation.json` + `license_evidence.*`

2) **Acquire (execute)**  
- writes into `raw/green/...` and `raw/yellow/...`
- writes `_manifests/{target_id}/acquire_done.json`

3) **Screen YELLOW (execute)**  
- writes `screened_yellow/{pool}/shards/*.jsonl.gz`
- appends `_ledger/yellow_passed.jsonl` + `_ledger/yellow_pitched.jsonl`
- writes `_manifests/{target_id}/yellow_screen_done.json`

4) **Merge (execute)**  
- writes `combined/{pool}/shards/*.jsonl.gz`
- writes `_ledger/combined_index.jsonl` + `merge_summary.json`

5) **Difficulty (execute)**  
- writes `final/{pool}/dXX/shards/*.jsonl.gz`
- writes `_ledger/final_index.jsonl` + `difficulty_summary.json`
- optionally writes `_pitches/final_pitched.jsonl`

6) **Catalog**  
- writes `_catalogs/catalog.json` and summary counts

---

## Suggested incremental rollout (keeps risk low)

1) **Port scaffolding first** (targets globals + run_pipeline + acquire + merge + difficulty) using a tiny GREEN source that already ships jsonl.
2) Implement **code_worker** (real extraction) for *one strategy* (`git`) and one language (e.g., python).
3) Enable **screen_yellow** for YELLOW repos with strict pitch.
4) Expand language support + chunking.
5) Add per-file license parsing + attribution bundling.
6) Turn on optional minhash grouping.

---

## Notes on backward compatibility
- Keep `download_worker.py` in the repo only as a legacy reference; v2 should use `acquire_worker.py`.
- Keep `yellow_scrubber.py` if it’s useful, but it’s no longer required for correctness once `yellow_screen_worker.py` exists.
