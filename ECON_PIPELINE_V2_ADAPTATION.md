# econ_stats_decision_adaptation_pipeline_v2 — Adaptation Plan (match math_pipeline_v2 behavior)

**Objective:** upgrade `econ_stats_decision_adaptation_pipeline_v1` to **v2** so it behaves like `math_pipeline_v2`:
- same **stage flow** (classify → acquire → yellow_screen → merge → difficulty → catalog),
- same **directory layout** (`raw/`, `screened_yellow/`, `combined/`, `final/`, ledgers/manifests/queues),
- same **safety defaults** (dry-run by default, pitch-if-unclear, license evidence snapshots).

This doc assumes you will create a new repo folder:
- `econ_stats_decision_adaptation_pipeline_v2/`

and base it on the working contracts in:
- `math_pipeline_v2/`.


---

## 0) Quick delta summary (v1 → v2)

### v1 (econ) currently has
- `pipeline_driver.py` emitting queues + per-target license evidence manifests
- `download_worker.py` writing directly into `pools/{permissive|copyleft|quarantine}/{target_id}/...`
- `yellow_scrubber.py` (domain-specific text extraction helpers)
- `catalog_builder.py`

### v2 must add / change
1. **Replace download pool layout with v2 “raw buckets”**
   - `raw/{green|yellow}/{license_pool}/{target_id}/...`
   - driven by **`acquire_worker.py`** contract (copied from math v2)

2. **Add YELLOW canonicalization stage**
   - `yellow_screen_worker.py` reads raw YELLOW payloads, emits canonical JSONL records into `screened_yellow/`
   - strict **pitch-if-unclear** rule + ledger entries

3. **Add merge stage**
   - `merge_worker.py` combines canonical GREEN records and screened YELLOW records into `combined/`

4. **Add difficulty stage**
   - `difficulty_worker.py` performs final screening + assigns difficulty **d01..d10** into `final/`
   - driven by a new `difficulties_econ_stats_decision.yaml`

5. **Update catalog builder**
   - include new roots and ledgers in its inventory output


---

## 1) v2 folder layout (mirror math_pipeline_v2)

Adopt the same top-level v2 output roots (tune paths in your targets YAML):

```
<data_root>/
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
  _manifests/{target_id}/...
  _ledger/
    yellow_passed.jsonl
    yellow_pitched.jsonl
    combined_index.jsonl
    final_index.jsonl
  _pitches/
    yellow_pitched_samples.jsonl   (optional, but matches math v2 patterns)
    final_pitched.jsonl
  _logs/
  _catalogs/
```

**Important behavioral invariants (match math v2):**
- **Dry-run is default** for workers; require `--execute` to write/download.
- All “unclear / cannot normalize / license ambiguous / microdata-risk” is **pitched** with a reason code.
- Every stage writes **done markers** into `_manifests/{target_id}/...` so runs are resumable and idempotent.


---

## 2) Code adaptation (what to copy vs rewrite)

### 2.1 Create econ v2 repo by copying math v2 workers

Start by copying these files from `math_pipeline_v2/` into your new folder (then rename text/paths to econ):

- `acquire_worker.py`
- `yellow_screen_worker.py`
- `merge_worker.py`
- `difficulty_worker.py`
- `pipeline_driver.py` (you can start from econ v1 driver, but math v2 driver is a good reference)
- `catalog_builder.py`
- `review_queue.py`
- `yellow_scrubber.py` (keep econ’s version if it already has useful extractors; otherwise merge features carefully)
- `run_pipeline.sh`
- `requirements.txt`

Then bring over econ-specific policy/config from v1:
- `denylist.yaml` (update patterns; keep “anti-advocacy / partisan” filters)
- `license_map.yaml`
- `field_schemas.yaml` (replace chemistry remnants; see §3.3)

### 2.2 Replace `download_worker.py` with v2 `acquire_worker.py`
In v1, downloads go to `pools/{pool}/{target_id}/...`. In v2, acquisition must write to:

```
raw/{green|yellow}/{license_pool}/{target_id}/...
```

**How:**
- Remove `download_worker.py` from the v2 pipeline.
- Use `acquire_worker.py` (math v2) as the only acquisition worker.
- Ensure queue rows contain a `license_profile` (or compatible alias) and an acquisition `download.strategy`.

**Compatibility:** v2’s `resolve_license_pool()` already accepts `license_profile` or `license_pool`.

### 2.3 Extend `yellow_screen_worker.py` for econ/tabular sources (the key econ-specific work)
Math v2’s yellow screen expects raw inputs in JSONL-ish forms. Econ targets are often:
- CSV/TSV
- zipped bundles
- SDMX/JSON
- Parquet
- “API-first” endpoints

**Goal of yellow screen:** emit **canonical text records** as JSONL, with stable hashes and routing metadata.

Add adapters inside `yellow_screen_worker.py`:

**Adapters to implement (recommended order):**
1. **CSV/TSV → records**
   - Parse with `csv` or `pandas`.
   - Emit either:
     - *Row-level records* (only if rows are small and not microdata), or
     - *Chunked records* (N rows per chunk), or
     - *Schema + summary stats* (best default for large tables)
   - Always include:
     - column names
     - units (if known)
     - source table name / file name
     - date range coverage when present

2. **ZIP bundles**
   - Unpack to a temp directory
   - Run per-file adapter (CSV/TSV, JSON, TXT)
   - Keep “what was inside” in the record’s `source.artifacts[]`

3. **Parquet**
   - Optional `pyarrow`
   - Convert to same chunked representation as CSV

4. **SDMX/JSON**
   - Convert series metadata + observation windows into text blocks
   - Avoid emitting raw microdata-like rows if series are too granular

**Pitch rules (must match math v2 behavior):**
- Unknown encoding or corrupt file → pitch
- Possible microdata / PII risk (names, addresses, SSNs, individual-level records) → pitch
- License evidence missing/contradictory for a “YELLOW” record-level corpus → pitch

### 2.4 Add/keep `merge_worker.py`
`merge_worker.py` should:
- Read screened YELLOW shards from `screened_yellow/{pool}/shards/`
- Read canonical GREEN records (if you have GREEN canonicalization; see note below)
- Emit combined shards into `combined/{pool}/shards/`
- Dedupe by `hash.content_sha256`

**Note on GREEN:** econ GREEN acquisitions might be binary/tabular too. You have two options:
- **Option A (recommended):** canonicalize GREEN too (either by reusing yellow_screen logic for GREEN, or by adding a small `green_screen_worker.py`).
- **Option B (minimal):** only merge screened YELLOW; treat GREEN as “already canonical JSONL” targets.

If you want “same behavior as math v2” with minimal code churn, start with Option B and only mark GREEN targets that truly yield JSONL text records.

### 2.5 Add `difficulty_worker.py`
No econ-specific code changes are required if:
- your combined records include `routing.subject/domain/category` and optionally `difficulty.level`
- and you supply a **good** `difficulties_econ_stats_decision.yaml` (see §4)


---

## 3) Config adaptation

### 3.1 Replace v1 targets schema with v2 targets schema
Create a new file:

- `targets_econ_stats_decision_v2.yaml` (or overwrite `targets_econ_stats_decision.yaml` but change schema_version)

**Required v2 globals (shape must match math v2):**
```yaml
globals:
  raw_root: /data/econ/raw
  screened_yellow_root: /data/econ/screened_yellow
  combined_root: /data/econ/combined
  final_root: /data/econ/final

  ledger_root: /data/econ/_ledger
  pitches_root: /data/econ/_pitches
  manifests_root: /data/econ/_manifests
  queues_root: /data/econ/_queues
  catalogs_root: /data/econ/_catalogs
  logs_root: /data/econ/_logs

  sharding:
    max_records_per_shard: 25000
    compression: gzip
```

**Per-target v2 minimum fields (example):**
```yaml
targets:
  - id: bea_nipa_tables
    name: "BEA NIPA tables (public data)"
    enabled: true

    license_profile: permissive
    license_evidence:
      spdx_hint: CC0-1.0
      url: "https://.../terms"

    routing:
      subject: econ
      domain: macro
      category: national_accounts
      level: 4   # optional hint; difficulty worker can override

    download:
      strategy: http
      url: "https://..."

    yellow_screen:
      adapter: csv_chunked
      chunk_rows: 2000
```

### 3.2 Keep denylist + advocacy filters (econ-specific)
Econ/stats/decision corpora risk: **advocacy-as-fact**, “think-tank” narratives, and microdata/PII.

Keep your v1 denylist concepts, but ensure the pipeline uses them in:
- `pipeline_driver.py` gates (restriction phrases, noAI/noTDM)
- `yellow_screen_worker.py` (PII/microdata heuristics)
- `difficulty_worker.py` (final screening bounds: min/max chars, disallowed content flags)

### 3.3 Replace chemistry remnants in `field_schemas.yaml`
For v2 minimal viability, define only canonical record schema:

```yaml
canonical_record_v2:
  required:
    - record_id
    - text
    - source
    - routing
    - hash
  fields:
    record_id: {type: string}
    text: {type: string}
    source:
      type: object
      fields:
        target_id: {type: string}
        url: {type: string, nullable: true}
        artifact_path: {type: string, nullable: true}
        retrieved_at_utc: {type: string, nullable: true}
    routing:
      type: object
      fields:
        subject: {type: string}
        domain: {type: string}
        category: {type: string}
    hash:
      type: object
      fields:
        content_sha256: {type: string}
```

You can reintroduce tabular-structured schemas later.


---

## 4) Difficulty mapping (extra detailed, 1–10) — `difficulties_econ_stats_decision.yaml`

### 4.1 Philosophy (match math v2 intent)
Difficulty is meant to capture:
- **conceptual prerequisites** (econ theory + stats + optimization),
- **mathematical density** (calculus, linear algebra, probability, proofs),
- **method sophistication** (OLS vs IV vs GMM vs Bayesian; simple forecasting vs state-space),
- **implementation/engineering overhead** (code + data wrangling complexity),
- and (for decision/OR) the **complexity of the optimization/game model**.

It is **not** meant to reflect political controversy; advocacy is handled by deny/gating.

### 4.2 Rubric: levels 1–10 (anchor definitions)

**Level 1 — Everyday numeracy & basic finance**
- Reading simple charts, CPI basics, simple percentages, “what is inflation”.
- *Signals:* “percentage change”, “average”, “budget”, “simple interest”.

**Level 2 — Intro econ intuitions (no calculus)**
- Supply/demand, elasticity (qualitative), GDP components, unemployment concepts.
- *Signals:* “supply curve”, “demand curve”, “elasticity”, “opportunity cost”.

**Level 3 — Data literacy & descriptive statistics**
- Summary statistics, correlation vs causation warnings, index numbers, deflators.
- *Signals:* “mean/median”, “standard deviation”, “histogram”, “CPI deflator”.

**Level 4 — Intro probability & regression (applied)**
- Basic probability, confidence intervals, simple linear regression interpretation.
- *Signals:* “p-value”, “confidence interval”, “OLS”, “R-squared”.

**Level 5 — Intermediate micro/macro + multivariate regression**
- Utility maximization (basic), constrained choice, multivariate OLS, omitted variable bias conceptually.
- *Signals:* “Lagrangian” (light), “multicollinearity”, “heteroskedasticity”.

**Level 6 — Intro econometrics + causal basics**
- Panel intuition, fixed effects, DiD fundamentals, IV motivation, causal graphs basics.
- *Signals:* “difference-in-differences”, “instrumental variables”, “fixed effects”.

**Level 7 — Advanced causal + time series fundamentals**
- Event studies, synthetic control basics, time series (ARIMA, stationarity), MLE basics.
- *Signals:* “stationarity”, “ARIMA”, “Granger”, “synthetic control”.

**Level 8 — Advanced econometrics & optimization**
- GMM, M-estimation, robust inference, state-space/Kalman, convex optimization, dynamic programming basics.
- *Signals:* “GMM”, “Kalman filter”, “convex”, “Bellman equation”.

**Level 9 — Frontier methods & theory**
- Bayesian hierarchical models, structural estimation, mechanism design, general equilibrium modeling, stochastic control.
- *Signals:* “structural model”, “MCMC”, “mechanism design”, “equilibrium”.

**Level 10 — Research frontier (dense papers, proofs, new methods)**
- Novel estimators, asymptotic proofs, cutting-edge causal inference, high-dimensional stats theory.
- *Signals:* “asymptotic normality proof”, “minimax”, “oracle inequalities”.

### 4.3 Domain/category map (defaults + bounds)

Model the file after `math_pipeline_v2/difficulties_math.yaml`:

- `subjects.econ.domains.<domain>.categories.<category>.level.{default,min,max}`

Recommended econ domains:

1. `econ_core`
   - `micro_intro` (2–4)
   - `macro_intro` (2–4)
   - `micro_intermediate` (5–7)
   - `macro_intermediate` (5–7)
   - `public_econ_intro` (4–6)
   - `international_trade_intro` (4–6)

2. `data_and_measurement`
   - `official_statistics_overview` (3–5)
   - `index_numbers_deflators` (3–6)
   - `survey_sampling_intro` (4–6)
   - `national_accounts` (4–6)
   - `labor_statistics` (3–6)

3. `statistics`
   - `descriptive_stats` (3–5)
   - `probability_intro` (4–6)
   - `inference_intro` (4–6)
   - `bayesian_intro` (6–8)
   - `high_dimensional_stats` (8–10)

4. `econometrics`
   - `ols_basics` (4–6)
   - `panel_data` (6–8)
   - `iv_and_rdd` (6–9)
   - `gmm_mle` (8–10)
   - `structural_econometrics` (9–10)

5. `causal_inference`
   - `causal_graphs_intro` (6–8)
   - `did_event_studies` (6–9)
   - `synthetic_control` (7–9)
   - `causal_ml` (8–10)

6. `time_series_forecasting`
   - `arima_basics` (7–8)
   - `state_space_kalman` (8–10)
   - `volatility_models` (8–10)

7. `decision_science_or`
   - `optimization_intro` (6–8)
   - `convex_optimization` (8–10)
   - `integer_programming` (8–10)
   - `dynamic_programming` (8–10)
   - `stochastic_optimization` (9–10)

8. `game_theory_mechanism_design`
   - `game_theory_intro` (6–8)
   - `mechanism_design` (9–10)
   - `matching_markets` (8–10)

### 4.4 Keyword rules (high leverage; makes auto-sorting work)
Add `rule_sets.global.keyword_rules` entries similar to math.

Examples (illustrative; tune as you see distributions in `final_index.jsonl`):

- `match_any: ["supply and demand", "elasticity"]` → econ_core / micro_intro / level 2
- `match_any: ["GDP", "unemployment rate", "CPI"]` → data_and_measurement / official_statistics_overview / level 3
- `match_any: ["OLS", "R-squared", "heteroskedasticity"]` → econometrics / ols_basics / level 5
- `match_any: ["difference-in-differences", "event study"]` → causal_inference / did_event_studies / level 7
- `match_any: ["instrumental variable", "2SLS", "regression discontinuity"]` → econometrics / iv_and_rdd / level 8
- `match_any: ["ARIMA", "stationarity", "unit root"]` → time_series_forecasting / arima_basics / level 7
- `match_any: ["Kalman filter", "state space"]` → time_series_forecasting / state_space_kalman / level 9
- `match_any: ["convex optimization", "KKT conditions"]` → decision_science_or / convex_optimization / level 9
- `match_any: ["integer programming", "branch and bound"]` → decision_science_or / integer_programming / level 9
- `match_any: ["MCMC", "Bayesian hierarchical"]` → statistics / bayesian_intro / level 8
- `match_any: ["asymptotic normality", "oracle inequality"]` → statistics / high_dimensional_stats / level 10

**Priority + confidence:**
- Use `priority: 10` for reliable anchor terms (“difference-in-differences”, “Kalman filter”)
- Use lower confidence (0.4–0.6) for ambiguous terms (“equilibrium” can be many things)

### 4.5 Tabular-specific difficulty heuristics (recommended)
Pure tabular datasets often lack “keywords” in text. Add heuristics in `difficulty_worker.py` (or encode as metadata + rules):

If a record includes `source.table_profile` or similar, derive level from:
- number of columns,
- temporal frequency,
- presence of panel structure,
- presence of advanced transforms (seasonal adjustment, chain-weighted indexes),
- complexity of methodology notes.

Heuristic suggestions:
- Small official time series with clear labels → level 3–5
- Panel dataset with IDs/time and modeling guidance → level 6–8
- High-frequency financial series with volatility modeling context → level 8–10

### 4.6 Provide a concrete starter YAML (drop-in)
Create `difficulties_econ_stats_decision.yaml`:

```yaml
schema_version: "2.0"
updated_utc: "2025-12-22T00:00:00Z"

globals:
  destination_root_windows: E:/AI-Research/datasets/Natural/econ_stats_decision
  destination_root_wsl: /mnt/e/AI-Research/datasets/Natural/econ_stats_decision
  folder_layout: final/{license_pool}/d{level:02d}/{subject}/{domain}/{category}
  sanitize_path_segments: true

  default_subject: econ
  default_domain: misc
  default_category: misc
  default_level: 5

rubric:
  scale: {min: 1, max: 10}
  notes:
    - "Difficulty reflects prerequisites + math/method density + implementation overhead."

subjects:
  econ:
    name: "Economics / Statistics / Decision Science"
    domains:
      econ_core:
        name: "Econ core"
        categories:
          micro_intro: {level: {default: 2, min: 1, max: 4}, notes: "Supply/demand, basic elasticity."}
          macro_intro: {level: {default: 2, min: 1, max: 4}, notes: "GDP, inflation, unemployment basics."}
          micro_intermediate: {level: {default: 6, min: 5, max: 7}, notes: "Constrained optimization, welfare, IO basics."}
          macro_intermediate: {level: {default: 6, min: 5, max: 7}, notes: "IS/LM/AS-AD, growth, expectations."}

      data_and_measurement:
        name: "Data & measurement"
        categories:
          official_statistics_overview: {level: {default: 3, min: 2, max: 5}, notes: "How official indicators are defined."}
          national_accounts: {level: {default: 5, min: 4, max: 6}, notes: "NIPA, chain-weighting, deflators."}
          index_numbers_deflators: {level: {default: 4, min: 3, max: 6}, notes: "CPI/PPI, deflation, real vs nominal."}

      statistics:
        name: "Statistics"
        categories:
          descriptive_stats: {level: {default: 3, min: 2, max: 5}, notes: "Summaries, distributions, visualization."}
          probability_intro: {level: {default: 5, min: 4, max: 6}, notes: "Random variables, expectation."}
          bayesian_intro: {level: {default: 7, min: 6, max: 8}, notes: "Priors, posteriors, basic MCMC ideas."}
          high_dimensional_stats: {level: {default: 10, min: 9, max: 10}, notes: "Theory-heavy modern stats."}

      econometrics:
        name: "Econometrics"
        categories:
          ols_basics: {level: {default: 5, min: 4, max: 6}, notes: "OLS, inference, assumptions."}
          panel_data: {level: {default: 7, min: 6, max: 8}, notes: "FE/RE, clustering, panel inference."}
          iv_and_rdd: {level: {default: 8, min: 6, max: 9}, notes: "2SLS, RDD, identification."}
          gmm_mle: {level: {default: 9, min: 8, max: 10}, notes: "GMM, MLE, asymptotics."}
          structural_econometrics: {level: {default: 10, min: 9, max: 10}, notes: "Structural estimation."}

      causal_inference:
        name: "Causal inference"
        categories:
          causal_graphs_intro: {level: {default: 7, min: 6, max: 8}, notes: "DAGs, backdoor, SCM basics."}
          did_event_studies: {level: {default: 7, min: 6, max: 9}, notes: "DiD, event studies, identification threats."}
          synthetic_control: {level: {default: 8, min: 7, max: 9}, notes: "Synthetic control and variants."}
          causal_ml: {level: {default: 9, min: 8, max: 10}, notes: "Double ML, orthogonalization, high-dim causal."}

      time_series_forecasting:
        name: "Time series & forecasting"
        categories:
          arima_basics: {level: {default: 7, min: 6, max: 8}, notes: "Stationarity, ARIMA, diagnostics."}
          state_space_kalman: {level: {default: 9, min: 8, max: 10}, notes: "Kalman filter, state space models."}
          volatility_models: {level: {default: 9, min: 8, max: 10}, notes: "ARCH/GARCH, stochastic volatility."}

      decision_science_or:
        name: "Decision science / OR"
        categories:
          optimization_intro: {level: {default: 7, min: 6, max: 8}, notes: "LP basics, duality intuition."}
          convex_optimization: {level: {default: 9, min: 8, max: 10}, notes: "Convexity, KKT, proofs."}
          integer_programming: {level: {default: 9, min: 8, max: 10}, notes: "MILP, branch-and-bound."}
          dynamic_programming: {level: {default: 9, min: 8, max: 10}, notes: "Bellman equations, MDPs."}

      game_theory_mechanism_design:
        name: "Game theory & mechanism design"
        categories:
          game_theory_intro: {level: {default: 7, min: 6, max: 8}, notes: "Nash equilibrium basics."}
          matching_markets: {level: {default: 9, min: 8, max: 10}, notes: "Stable matching, market design."}
          mechanism_design: {level: {default: 10, min: 9, max: 10}, notes: "Incentive compatibility, revelation principle."}

rule_sets:
  global:
    keyword_rules:
      - match_any: ["supply and demand", "elasticity"]
        route: {subject: econ, domain: econ_core, category: micro_intro, level: 2}
        priority: 10
        confidence: 0.7
      - match_any: ["GDP", "unemployment rate", "CPI"]
        route: {subject: econ, domain: data_and_measurement, category: official_statistics_overview, level: 3}
        priority: 10
        confidence: 0.6
      - match_any: ["OLS", "R-squared", "heteroskedasticity"]
        route: {subject: econ, domain: econometrics, category: ols_basics, level: 5}
        priority: 10
        confidence: 0.6
      - match_any: ["difference-in-differences", "event study"]
        route: {subject: econ, domain: causal_inference, category: did_event_studies, level: 7}
        priority: 10
        confidence: 0.7
      - match_any: ["instrumental variables", "2SLS", "regression discontinuity"]
        route: {subject: econ, domain: econometrics, category: iv_and_rdd, level: 8}
        priority: 10
        confidence: 0.7
      - match_any: ["ARIMA", "unit root", "stationarity"]
        route: {subject: econ, domain: time_series_forecasting, category: arima_basics, level: 7}
        priority: 10
        confidence: 0.7
      - match_any: ["Kalman filter", "state space"]
        route: {subject: econ, domain: time_series_forecasting, category: state_space_kalman, level: 9}
        priority: 10
        confidence: 0.75
      - match_any: ["convex optimization", "KKT conditions"]
        route: {subject: econ, domain: decision_science_or, category: convex_optimization, level: 9}
        priority: 10
        confidence: 0.7
      - match_any: ["integer programming", "branch and bound"]
        route: {subject: econ, domain: decision_science_or, category: integer_programming, level: 9}
        priority: 10
        confidence: 0.7
      - match_any: ["MCMC", "Bayesian hierarchical"]
        route: {subject: econ, domain: statistics, category: bayesian_intro, level: 8}
        priority: 10
        confidence: 0.7
      - match_any: ["asymptotic normality", "oracle inequality", "minimax"]
        route: {subject: econ, domain: statistics, category: high_dimensional_stats, level: 10}
        priority: 10
        confidence: 0.8
  subjects:
    econ:
      keyword_rules: []

source_overrides:
  econ: {}
```

### 4.7 How the worker should use this mapping (expected behavior)
`difficulty_worker.py` assigns difficulty in this order:
1. If record already has `difficulty.level`, keep it (bounded to 1–10).
2. Else if record has `routing.subject/domain/category`, use the **domain/category defaults** + clamp to min/max.
3. Else apply **keyword rules** against record text and metadata fields.
4. Else fall back to heuristics (length bounds / mild defaults).

**Tuning loop:**
- After a run, inspect `_ledger/final_index.jsonl` distribution by level.
- If everything piles into 5–6, raise/lower category defaults and add more keyword rules for the long tail.


---

## 5) Update `run_pipeline.sh` to mirror math v2 stages

Make the runner expose the same stages (and keep DRY-RUN default):

- `classify`
- `acquire_green`
- `acquire_yellow`
- `screen_yellow`
- `merge`
- `difficulty`
- `catalog`

The script should pass:
- `--targets targets_econ_stats_decision_v2.yaml`
- `--difficulties difficulties_econ_stats_decision.yaml` (for the difficulty stage)


---

## 6) Acceptance checklist (prove parity with math v2)

After implementing v2, a full run should create:

1. `_queues/green_download.jsonl`, `_queues/yellow_pipeline.jsonl`, `_queues/red_rejected.jsonl`
2. `raw/green/...` and `raw/yellow/...` acquisition trees + `_manifests/{target_id}/acquire_done.json`
3. `screened_yellow/{pool}/shards/*.jsonl.gz` + `_ledger/yellow_passed.jsonl` + `_ledger/yellow_pitched.jsonl`
4. `combined/{pool}/shards/*.jsonl.gz` + `_ledger/combined_index.jsonl`
5. `final/{pool}/d01..d10/shards/*.jsonl.gz` + `_ledger/final_index.jsonl` + `_pitches/final_pitched.jsonl`
6. A catalog artifact in `_catalogs/` summarizing all of the above

If any stage doesn’t write ledgers/done markers in the same pattern, fix that first (the resumption + idempotence behavior depends on it).
