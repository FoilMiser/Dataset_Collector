# Logic corpus pipeline adaptation plan

This document describes how to adapt the existing **chem_pipeline_v1** package into a **logic-specialized** corpus builder while staying conservative on licensing and ethical constraints.

The current pipeline already does the hard parts you want for safe dataset collection:
- snapshots license/ToS evidence
- normalizes licenses into an allow/conditional/deny policy
- scans for “no AI / no TDM / no training” restrictions
- produces GREEN/YELLOW/RED queues for downstream download/extract steps

What changes for “logic” is mostly **(a) target inventory** and **(b) extraction workers + schemas**.

---

## 1) Copy/rename the project

Recommended layout:

```
logic_pipeline_v1/
  pipeline_driver.py
  download_worker.py
  review_queue.py
  catalog_builder.py
  yellow_scrubber.py
  license_map.yaml
  denylist.yaml
  field_schemas.yaml
  targets_logic.yaml
  workers/
    proof_repo_worker.py
    benchmark_worker.py
    nl_logic_worker.py
  README.md
  todo.txt
```

Minimal renames:
- change paths from `/data/chem/...` to `/data/logic/...` in your new targets YAML (already done in `targets_logic.yaml`).
- update user agent strings (`logic-corpus-pipeline/...`).

---

## 2) Define “logic data types” explicitly

Treat logic as four distinct modalities. You’ll get better quality and fewer licensing surprises if each has its own extraction path and schema.

1) **Formal proof corpora**
   - Lean / Coq / Isabelle / HOL / Metamath
   - outputs: theorem statements, proof scripts, tactic traces, dependency graphs

2) **Benchmarks / solver formats**
   - SMT-LIB `.smt2`, SAT/DIMACS `.cnf`, (optionally) ATP formats
   - outputs: problem text + metadata (logic, status, expected result)

3) **NL logic datasets**
   - rule reasoning, entailment chains, synthetic proof chains
   - outputs: premises, question/hypothesis, label/answer, explanation/proof if provided

4) **Open educational texts**
   - open logic textbooks / lecture notes / exercises with clear licenses
   - outputs: chunked text with section structure + exercise blocks when parseable

---

## 3) Add logic-specific extraction workers

### A) `proof_repo_worker.py`

Purpose: extract “trainable records” from downloaded proof repos.

Inputs:
- a `git`-cloned repository directory
- a `target` entry from `targets_logic.yaml`

Core tasks:
- detect proof language by file extension and/or repo structure (`.lean`, `.v`, `.thy`, `.mm`, etc.)
- capture repository provenance:
  - repo URL
  - commit hash
  - file paths
  - license file(s)
- emit records at multiple granularities:
  - **theorem_statement** record
  - **proof_script** record
  - optionally **tactic_trace** record if derivable

Recommended record schema (JSONL):
- `record_type`: `theorem_statement | proof_script | lemma | definition`
- `proof_language`: `lean | coq | isabelle | hol | metamath | other`
- `repo_url`, `commit`, `path`
- `theorem_name`
- `statement`
- `proof`
- `imports` / `dependencies` (best-effort)
- `license_spdx` (resolved or per-file when available)
- `license_evidence_path` (pointer into manifests)

### B) `benchmark_worker.py`

Purpose: parse benchmark/problem libraries into “one-problem-per-record”.

Supported formats (start small):
- SMT-LIB v2: capture `(set-logic ...)`, `(assert ...)`, `(check-sat)`, `:status` when present
- DIMACS CNF: capture `p cnf`, clause count, variable count

Recommended record schema:
- `record_type`: `smt_problem | sat_problem`
- `format`: `smt2 | dimacs`
- `problem_text` (raw)
- `problem_meta`: parsed fields
- `expected_result` if included
- `license_spdx` + evidence pointer

### C) `nl_logic_worker.py`

Purpose: normalize NL logic datasets to a shared schema.

Recommended normalized fields:
- `premises: string[]`
- `question: string`
- `label` / `answer`
- `proof` / `explanation` (optional)
- `difficulty` (optional)
- `source_split` (train/val/test)
- `split_group_id` (critical for leak prevention)

---

## 4) Update `field_schemas.yaml` for logic

Chemistry schemas are molecule/structure-centric; logic needs proof/problem-centric schemas.

Add schema versions like:
- `logic_theorem_v1.0.0`
- `logic_proof_script_v1.0.0`
- `logic_smt_problem_v1.0.0`
- `logic_sat_problem_v1.0.0`
- `logic_nl_reasoning_v1.0.0`

Then wire them in each target under a future `extract:` stanza (or as part of the worker config).

---

## 5) License policy & safe handling rules

### A) Green vs Yellow vs Red rules (practical)

- **GREEN** (permissive): MIT/Apache/BSD/CC0/CC-BY/US-PD, with no restriction phrase hits.
- **YELLOW** (conditional/mixed): CC-BY-SA, GPL/LGPL/MPL, ODbL, or any “mixed submitter” benchmark pools.
- **RED**: NonCommercial/NoDerivatives/proprietary/custom restrictive terms, or sources that are operationally incompatible with automation.

### B) “Mixed licensing” in benchmark libraries

Benchmark collections often include submissions from multiple parties. Treat these as `license_profile: record_level` and require:
- per-file license metadata (header, accompanying LICENSE, dataset metadata)
- if absent, keep the record quarantined and do not train on it

### C) Copyleft segregation

If you decide to use share-alike/copyleft corpora:
- keep them in `/pools/copyleft`
- do not blend or deduplicate across pools
- always generate an attribution bundle

---

## 6) CommonPile integration (logic slice)

Because CommonPile is typically distributed as **large pre-sharded corpora**, treat it as a separate ingestion lane:

Planned worker: `commonpile_import_worker.py`
- reads local shards (JSONL/ZST)
- filters to logic topics (keyword + classifier optional)
- preserves per-record provenance and license
- emits `record_level_filter` decisions per record

Your `targets_logic.yaml` includes a stub entry (`commonpile_logic_slice`) with suggested include/exclude terms.

---

## 7) Quality gates specific to logic

Add/enable these gates in the extraction stage:

1) **Proof/benchmark normalization**
   - canonicalize unicode
   - normalize whitespace
   - remove obviously-generated duplicates via MinHash/LSH

2) **Leak prevention**
   - keep benchmark families together (`split_group_id`)
   - do not split files from the same repo commit across train/val/test

3) **Statement-only vs statement+proof**
   - produce two derived corpora:
     - `statement_only` (lower license/compliance risk, easier to dedup)
     - `statement_plus_proof` (higher training value, but more careful licensing)

---

## 8) Operational workflow (same as chem)

1) Run license evaluation + queue build:

```bash
python pipeline_driver.py --targets targets_logic.yaml
```

2) Download GREEN (and optionally YELLOW into quarantine):

```bash
python download_worker.py --queue /data/logic/_queues/green_download.jsonl --targets-yaml targets_logic.yaml --execute --workers 4
python download_worker.py --queue /data/logic/_queues/yellow_pipeline.jsonl --targets-yaml targets_logic.yaml --execute --workers 2
```

3) Run extraction (new workers you’ll add):

```bash
python workers/proof_repo_worker.py --inputs /data/logic/pools/permissive/lean_mathlib4
python workers/benchmark_worker.py  --inputs /data/logic/pools/quarantine/smtlib_benchmarks
python workers/nl_logic_worker.py   --inputs /data/logic/pools/permissive/<dataset>
```

---

## 9) Suggested next implementation steps

1) Implement `proof_repo_worker.py` for Lean + Metamath first (high value, easy licensing evidence).
2) Implement `benchmark_worker.py` for SMT-LIB `.smt2` parsing.
3) Add schema validation for the JSONL outputs.
4) Add a `statement-only` derived corpus builder.
5) Add the CommonPile import worker once your topic filter is stable.

---

## 10) What this draft does and does not do

Included now:
- `targets_logic.yaml` with conservative licensing defaults + a starter inventory
- a clear plan for the worker and schema changes needed

Not included (yet):
- the actual new worker implementations
- updated `field_schemas.yaml` logic schemas
- updated `denylist.yaml` entries specific to logic sites

