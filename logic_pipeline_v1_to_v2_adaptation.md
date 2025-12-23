# logic_pipeline_v1 → logic_pipeline_v2 adaptation plan (match math_pipeline_v2 behavior)

This document describes how to adapt **`logic_pipeline_v1`** to a new **`logic_pipeline_v2`** so it behaves the same way as **`math_pipeline_v2`**.

**Source bundles inspected:**
- `logic_pipeline_v1.zip` (files: `pipeline_driver.py`, `download_worker.py`, `catalog_builder.py`, `targets_logic.yaml`, etc.)
- `math_pipeline_v2.zip` (files: `pipeline_driver.py`, `acquire_worker.py`, `yellow_screen_worker.py`, `merge_worker.py`, `difficulty_worker.py`, `difficulties_math.yaml`, etc.)

---

## 1) What “same behavior as math_pipeline_v2” means

To match **math v2**, logic v2 must implement the same **stage contract**, directory layout, and record/ledger conventions:

### v2 stages (same names + semantics)
| Stage | Script | Purpose |
|---|---|---|
| `classify` | `pipeline_driver.py` | Classify targets into GREEN/YELLOW/RED queues using license evidence + denylist gates; write evaluation + queue JSONL. |
| `acquire_green` | `acquire_worker.py` | Download GREEN targets into `raw/green/{license_pool}/{target_id}/...` (dry-run by default; `--execute` writes). |
| `acquire_yellow` | `acquire_worker.py` | Download YELLOW targets into `raw/yellow/{license_pool}/{target_id}/...`. |
| `screen_yellow` | `yellow_screen_worker.py` | Convert raw YELLOW into canonical JSONL records; pitch unclear; write pass/pitch ledgers + done markers. |
| `merge` | `merge_worker.py` | Merge GREEN raw + screened YELLOW into `combined/{license_pool}/shards/*.jsonl.gz` with hash-based dedupe. |
| `difficulty` | `difficulty_worker.py` | Final screening + assign difficulty (1–10); write to `final/{license_pool}/d01..d10/shards/*.jsonl.gz`. |
| `catalog` | `catalog_builder.py` | Summarize counts/bytes + ledger references across stages. |

> **Important:** math v2’s `difficulty_worker.py` outputs to `final/{pool}/dXX/shards/…` (no domain/category subfolders). Match that exactly.

---

## 2) Target v2 directory layout for logic

Match math v2 roots, but under `/data/logic`:

```
/data/logic/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz

  _queues/
    green_download.jsonl
    yellow_pipeline.jsonl
    red_rejected.jsonl
    run_summary.json

  _manifests/{target_id}/...  # per-target snapshots + done markers
  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    final_index.jsonl
    difficulty_summary.json
  _pitches/
    final_pitched.jsonl
  _catalogs/
  _logs/
```

`license_pool` should remain consistent with math v2: typically `permissive`, `copyleft`, `quarantine`.

---

## 3) Repository/file strategy: “math v2 as template, swap in logic specialization”

### Recommended approach
1. Create a new folder: `logic_pipeline_v2/`
2. Copy these files from `math_pipeline_v2/` into `logic_pipeline_v2/` as the baseline:
   - `pipeline_driver.py`
   - `acquire_worker.py`
   - `yellow_screen_worker.py`
   - `yellow_scrubber.py`
   - `merge_worker.py`
   - `difficulty_worker.py`
   - `catalog_builder.py`
   - `review_queue.py`
   - `run_pipeline.sh`
   - `license_map.yaml`
   - `denylist.yaml`
   - `field_schemas.yaml` (then add any logic-specific schemas you want)
3. Bring over logic-specific workers if you still use them:
   - `pmc_worker.py` (optional; keep if you rely on PMC ingestion for logic-related material)
4. Replace math-specific configs with logic versions:
   - `targets_math.yaml` → `targets_logic.yaml` (updated to v2 global roots + screening/sharding defaults)
   - `difficulties_math.yaml` → **new** `difficulties_logic.yaml`

This yields identical stage semantics and output shape, while letting you customize logic targets + difficulty mapping.

---

## 4) Config changes: `targets_logic.yaml` must be converted from v1 globals → v2 globals

### 4.1 Replace v1 global roots with v2 root keys

In `logic_pipeline_v1/targets_logic.yaml`, `globals` currently contains:
- `storage_root`, `staging_root`, `pools.*`, `download_defaults`, etc.

In `logic_pipeline_v2/targets_logic.yaml`, mirror math v2’s `globals` keys:

```yaml
globals:
  raw_root: /data/logic/raw
  screened_yellow_root: /data/logic/screened_yellow
  combined_root: /data/logic/combined
  final_root: /data/logic/final
  ledger_root: /data/logic/_ledger
  pitches_root: /data/logic/_pitches
  manifests_root: /data/logic/_manifests
  queues_root: /data/logic/_queues
  catalogs_root: /data/logic/_catalogs
  logs_root: /data/logic/_logs

  require_yellow_signoff: false

  sharding:
    max_records_per_shard: 50000
    compression: gzip

  screening:
    min_chars: 200
    max_chars: 12000
    text_field_candidates: ["text", "content", "body", "statement", "problem", "prompt"]
    record_license_field_candidates: ["license", "license_spdx"]
    require_record_license: false
    allow_spdx: ["CC-BY-4.0", "CC0-1.0", "MIT", "Apache-2.0", "CC-BY-SA-4.0"]
    deny_phrases: ["noai", "no tdm", "no machine learning"]

  default_gates:
    - snapshot_terms
    - resolve_license_spdx
    - restriction_phrase_scan
    - emit_training_manifest
    - emit_attribution_bundle
```

> You can keep any old v1-only keys (e.g., `near_duplicate_detection`) if you want, but they should be ignored by v2 scripts unless you explicitly wire them in. If you want exact behavior match, **disable/remove** v1-only behavior that doesn’t exist in math v2.

### 4.2 Add `companion_files.difficulties_map`

Math v2 points its difficulty worker to a difficulties YAML via `companion_files`:

```yaml
companion_files:
  license_map: "./license_map.yaml"
  field_schemas: "./field_schemas.yaml"
  denylist: "./denylist.yaml"
  difficulties_map: "./difficulties_logic.yaml"
```

---

## 5) Schema change: add routing fields (match math v2 queue schema)

Math v2’s `pipeline_driver.py` writes both:
- `routing` (nested dict)
- flattened `routing_subject`, `routing_domain`, … fields

### 5.1 Update `field_schemas.yaml`
In logic v2, copy **`queue_record_routing_v2.0.0`** from math v2’s `field_schemas.yaml` into logic’s `field_schemas.yaml`.

This enables consistent queue/evaluation record structure across domains.

### 5.2 Update `pipeline_driver.py` to support routing
Logic v1’s driver doesn’t define `resolve_routing_fields()`. Logic v2 should match math v2 exactly:
- Add `resolve_routing_fields(target)` function (same as math v2)
- Change default subject from `"math"` → `"logic"`

> Simplest: copy math v2’s `pipeline_driver.py` wholesale and change the subject default inside `resolve_routing_fields()` to `"logic"`.

---

## 6) Worker changes (port math v2 workers)

Logic v1 has `download_worker.py` and a `yellow` stage, but math v2 uses these v2 scripts.

### 6.1 Acquire stage: use `acquire_worker.py` (not `download_worker.py`)
- Copy math v2 `acquire_worker.py` to logic v2.
- Ensure defaults point to `/data/logic/*` via `targets_logic.yaml`.
- Run twice via stages:
  - `acquire_green` uses `green_download.jsonl`
  - `acquire_yellow` uses `yellow_pipeline.jsonl`

Expected outputs:
- `raw/green/{license_pool}/{target_id}/...`
- `raw/yellow/{license_pool}/{target_id}/...`

### 6.2 Screen YELLOW stage: `yellow_screen_worker.py`
- Copy math v2 `yellow_screen_worker.py`.
- Set screening candidates in `targets_logic.yaml.globals.screening.text_field_candidates` to include logic-friendly fields (examples above).
- It must write:
  - `screened_yellow/{license_pool}/shards/*.jsonl.gz`
  - `_ledger/yellow_passed.jsonl`
  - `_ledger/yellow_pitched.jsonl`
  - per-target done markers under `_manifests/{target_id}/...`

### 6.3 Merge stage: `merge_worker.py`
- Copy math v2 `merge_worker.py`.
- It merges:
  - GREEN raw records
  - screened YELLOW records
- Dedupe by `hash.content_sha256` (compute if missing), then write:
  - `combined/{license_pool}/shards/*.jsonl.gz`
  - `_ledger/combined_index.jsonl`

### 6.4 Difficulty stage: `difficulty_worker.py`
- Copy math v2 `difficulty_worker.py`.
- It assigns difficulty in this order (exact math v2 behavior):
  1. If record already has `difficulty.level` → keep
  2. Else if record has `routing` and `difficulties_logic.yaml` provides a default level for that (subject/domain/category) → use it
  3. Else fallback heuristic: **length-based** over `text`

It outputs:
- `final/{license_pool}/d01..d10/shards/*.jsonl.gz`
- `_ledger/final_index.jsonl`
- `_ledger/difficulty_summary.json`

---

## 7) Orchestration: replace `run_pipeline.sh` with the v2 stage runner

Logic v1’s `run_pipeline.sh` supports stages: `all, classify, download, yellow, catalog`.

Logic v2 must match math v2’s stage set:
- `all, classify, acquire_green, acquire_yellow, screen_yellow, merge, difficulty, catalog`

**Action:** Copy math v2’s `run_pipeline.sh` into logic v2 and replace the default `targets_math.yaml` text with `targets_logic.yaml`.

---

## 8) Difficulty mapping for Logic (what to implement)

Math v2 difficulty assignment depends on:
1. Targets/records having a `routing` block (`subject`, `domain`, `category`)
2. `difficulties_logic.yaml` defining a default level for that category

### 8.1 Logic difficulty rubric (1–10)
Use this as guidance when assigning category defaults:

1. Everyday/intro reasoning (simple arguments, basic fallacies)
2. Propositional fundamentals (connectives, basic equivalences, short truth tables)
3. Propositional techniques (CNF/DNF, multi-step rewrites, SAT-intuition)
4. FOL intro (quantifiers, translations, simple models/countermodels)
5. FOL proofs (natural deduction/tableaux, moderate proof length)
6. Automated reasoning foundations (Skolemization, unification, resolution basics)
7. SAT/SMT and modal/temporal basics (practical benchmarks + Kripke/LTL/CTL entry)
8. Computability & incompleteness (reductions, undecidability, Gödel basics) / harder ATP sets
9. Advanced proof theory/model theory (cut elimination/compactness depth) / tactic traces
10. Research-tier set theory/proof theory (forcing/large cardinals; highly technical corpora)

### 8.2 Domains + categories (recommended defaults)
These map cleanly to your current target list in `logic_pipeline_v1/targets_logic.yaml`:

| Domain | Category | Default level | Typical sources |
|---|---|---:|---|
| informal_reasoning | fallacies | 1 | Forall x / intro critical reasoning |
| propositional_logic | truth_tables | 2 | Textbook exercises |
| propositional_logic | equivalences_rewrites | 3 | rewrite drills |
| propositional_logic | normal_forms_cnf_dnf | 3 | CNF/DNF conversions |
| first_order_logic | translation_quantifiers | 4 | Open Logic / Forall x |
| first_order_logic | models_countermodels | 5 | countermodel exercises, FOlio-ish |
| first_order_logic | natural_deduction | 5 | ND derivations |
| automated_reasoning | skolem_resolution_unification | 6 | resolution/unification writeups |
| automated_reasoning | sat_benchmarks | 7 | SAT competition datasets |
| automated_reasoning | smt_benchmarks | 7 | SMT-LIB benchmark suites |
| proof_assistants | lean_formal_proofs | 8 | Lean mathlib4 |
| proof_assistants | metamath_formal_proofs | 8 | Metamath set.mm |
| automated_reasoning | tptp_atp | 8 | TPTP library |
| ai_reasoning_benchmarks | rule_taker | 6 | RuleTaker |
| ai_reasoning_benchmarks | proofwriter | 6 | ProofWriter |
| ai_reasoning_benchmarks | folio | 5 | FOLIO |
| philosophy_logic | sep_logic | 6 | SEP logic articles (license permitting) |

> If you prefer, you can bump proof-assistant tactic traces to 9 later; math v2 currently only uses default category levels and a length heuristic fallback.

---

## 9) Update your logic targets to include routing (so difficulty mapping works)

Math v2 expects `targets[*].routing` to exist for clean difficulty assignment.

### Suggested routing additions per existing logic targets
Add a `routing:` block to each target entry:

- `synthetic_logic_problems`:
```yaml
routing: { subject: logic, domain: propositional_logic, category: truth_tables, granularity: target }
```

- `lean_mathlib4`:
```yaml
routing: { subject: logic, domain: proof_assistants, category: lean_formal_proofs, granularity: target }
```

- `metamath_setmm`:
```yaml
routing: { subject: logic, domain: proof_assistants, category: metamath_formal_proofs, granularity: target }
```

- `open_logic_project` / `forallx_calgarystyle`:
```yaml
routing: { subject: logic, domain: first_order_logic, category: translation_quantifiers, granularity: target }
```

- `smtlib_benchmarks`:
```yaml
routing: { subject: logic, domain: automated_reasoning, category: smt_benchmarks, granularity: target }
```

- `sat_competition_benchmarks`:
```yaml
routing: { subject: logic, domain: automated_reasoning, category: sat_benchmarks, granularity: target }
```

- `rule_taker` / `proofwriter`:
```yaml
routing: { subject: logic, domain: ai_reasoning_benchmarks, category: rule_taker, granularity: target }
```

- `folio`:
```yaml
routing: { subject: logic, domain: ai_reasoning_benchmarks, category: folio, granularity: target }
```

- `tptp_problem_library`:
```yaml
routing: { subject: logic, domain: automated_reasoning, category: tptp_atp, granularity: target }
```

For “copyleft / quarantine” corpora like Wikipedia/StackExchange/CommonPile slices, you can still route them; license pool selection is independent.

---

## 10) Appendix: Draft `difficulties_logic.yaml` (save as companion file)

Save this as `logic_pipeline_v2/difficulties_logic.yaml` and point to it via `targets_logic.yaml -> companion_files.difficulties_map`.

```yaml
schema_version: "2.0"
updated_utc: "2025-12-22 00:00:00Z"

globals:
  destination_root_windows: "E:\\AI-Research\\datasets\\Natural\\logic"
  destination_root_wsl: "/data/logic"
  folder_layout: "final/{license_pool}/d{level:02d}/shards"
  sanitize_path_segments: true
  default_subject: "logic"
  default_domain: "informal_reasoning"
  default_category: "fallacies"
  default_level: 4

rubric:
  scale: "1-10"
  levels:
    1: { label: "Everyday logic", description: "Simple arguments, basic fallacies, short contexts.", signals: ["single-step", "informal"] }
    2: { label: "Propositional fundamentals", description: "Connectives, basic equivalences, small truth tables.", signals: ["truth table", "∨ ∧ → ¬"] }
    3: { label: "Propositional techniques", description: "CNF/DNF, multi-step rewrites, SAT intro intuition.", signals: ["CNF", "DNF", "normal form"] }
    4: { label: "FOL intro", description: "Quantifiers, translation, simple semantics and models.", signals: ["∀", "∃", "translation"] }
    5: { label: "FOL proofs", description: "Natural deduction/tableaux with moderate proof length.", signals: ["ND", "tableau", "derivation"] }
    6: { label: "Automated reasoning foundations", description: "Skolemization, unification, resolution basics.", signals: ["Skolem", "unification", "resolution"] }
    7: { label: "SAT/SMT + modal/temporal entry", description: "Benchmarks and basic modal/temporal logic.", signals: ["SMT-LIB", "SAT", "Kripke", "LTL"] }
    8: { label: "Computability + harder ATP/proofs", description: "Reductions/undecidability; harder ATP sets; formal proofs.", signals: ["undecidable", "reduction", "TPTP"] }
    9: { label: "Advanced proof/model theory", description: "Cut elimination/compactness depth; tactic traces.", signals: ["cut elimination", "compactness", "tactics"] }
    10: { label: "Research-tier logic/set theory", description: "Forcing/large cardinals; very technical corpora.", signals: ["forcing", "large cardinal"] }

subjects:
  logic:
    name: "Logic"
    domains:
      informal_reasoning:
        categories:
          fallacies: { level: { default: 1, min: 1, max: 2 }, notes: "Basic fallacies, argument ID." }
          validity_soundness: { level: { default: 2, min: 1, max: 3 }, notes: "Validity vs soundness, simple formalization." }

      propositional_logic:
        categories:
          truth_tables: { level: { default: 2, min: 1, max: 3 }, notes: "Truth-table evaluation/validity." }
          equivalences_rewrites: { level: { default: 3, min: 2, max: 4 }, notes: "Equivalence transformations." }
          normal_forms_cnf_dnf: { level: { default: 3, min: 3, max: 5 }, notes: "CNF/DNF conversions." }

      first_order_logic:
        categories:
          translation_quantifiers: { level: { default: 4, min: 3, max: 5 }, notes: "NL→FOL translation." }
          models_countermodels: { level: { default: 5, min: 4, max: 6 }, notes: "Model building and countermodels." }
          natural_deduction: { level: { default: 5, min: 4, max: 6 }, notes: "ND proofs, intro." }

      automated_reasoning:
        categories:
          skolem_resolution_unification: { level: { default: 6, min: 5, max: 7 }, notes: "Skolemization, resolution, unification." }
          sat_benchmarks: { level: { default: 7, min: 6, max: 8 }, notes: "SAT competition suites." }
          smt_benchmarks: { level: { default: 7, min: 6, max: 8 }, notes: "SMT-LIB benchmarks." }
          tptp_atp: { level: { default: 8, min: 7, max: 9 }, notes: "TPTP ATP problems." }

      proof_assistants:
        categories:
          lean_formal_proofs: { level: { default: 8, min: 7, max: 9 }, notes: "Lean mathlib4 theorem/proof corpora." }
          metamath_formal_proofs: { level: { default: 8, min: 7, max: 9 }, notes: "Metamath set.mm theorems/proofs." }

      ai_reasoning_benchmarks:
        categories:
          rule_taker: { level: { default: 6, min: 5, max: 7 }, notes: "Rule-based entailment datasets." }
          proofwriter: { level: { default: 6, min: 5, max: 7 }, notes: "ProofWriter-style reasoning." }
          folio: { level: { default: 5, min: 4, max: 6 }, notes: "FOLIO-style reasoning." }

      philosophy_logic:
        categories:
          sep_logic: { level: { default: 6, min: 5, max: 7 }, notes: "SEP logic-related articles (license permitting)." }

rule_sets:
  global:
    keyword_rules: []
  subjects:
    logic:
      keyword_rules: []

source_overrides:
  # Optional: hard-assign routing for targets that don't provide routing per-record
  targets:
    synthetic_logic_problems: { subject: logic, domain: propositional_logic, category: truth_tables }
    lean_mathlib4: { subject: logic, domain: proof_assistants, category: lean_formal_proofs }
    metamath_setmm: { subject: logic, domain: proof_assistants, category: metamath_formal_proofs }
    open_logic_project: { subject: logic, domain: first_order_logic, category: translation_quantifiers }
    forallx_calgarystyle: { subject: logic, domain: first_order_logic, category: translation_quantifiers }
    smtlib_benchmarks: { subject: logic, domain: automated_reasoning, category: smt_benchmarks }
    sat_competition_benchmarks: { subject: logic, domain: automated_reasoning, category: sat_benchmarks }
    rule_taker: { subject: logic, domain: ai_reasoning_benchmarks, category: rule_taker }
    proofwriter: { subject: logic, domain: ai_reasoning_benchmarks, category: proofwriter }
    folio: { subject: logic, domain: ai_reasoning_benchmarks, category: folio }
    tptp_problem_library: { subject: logic, domain: automated_reasoning, category: tptp_atp }
    sep_stanford_encyclopedia_of_philosophy: { subject: logic, domain: philosophy_logic, category: sep_logic }
```

> Note: math v2’s `difficulty_worker.py` currently **does not** consume `rule_sets` or `source_overrides`. It only uses `subjects → domains → categories → level.default` via the record’s `routing`. Keep `rule_sets`/`source_overrides` for forward compatibility.

---

## 11) Validation checklist (behavior parity)

After implementing logic v2, verify:

1. `./run_pipeline.sh --stage classify` writes:
   - `_queues/green_download.jsonl`
   - `_queues/yellow_pipeline.jsonl`
   - `_queues/red_rejected.jsonl`
   - per-target evaluations under `_queues/evaluations/…` (if enabled in the driver)
2. `acquire_green` and `acquire_yellow` write raw data to `raw/green/...` and `raw/yellow/...`
3. `screen_yellow` produces:
   - `screened_yellow/{pool}/shards/*.jsonl.gz`
   - `_ledger/yellow_passed.jsonl`, `_ledger/yellow_pitched.jsonl`
4. `merge` produces:
   - `combined/{pool}/shards/*.jsonl.gz`
   - `_ledger/combined_index.jsonl`
5. `difficulty` produces:
   - `final/{pool}/d01..d10/shards/*.jsonl.gz`
   - `_ledger/final_index.jsonl`
   - `_ledger/difficulty_summary.json`
6. `catalog` produces a v2 catalog under `_catalogs/` covering all stages.

If all six checks pass, logic v2 matches math v2’s stage behavior and output contract.
