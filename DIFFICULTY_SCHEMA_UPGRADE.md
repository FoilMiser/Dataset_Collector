# Difficulty schema v2 (multi-domain) — pipeline update plan

Updated: 2025-12-21 04:17:08Z

This note reviews the current **math_pipeline_v1.1** difficulty routing and proposes a **more thorough, reusable difficulty schema** that can later support *other subjects* (physics, engineering, biology, etc.) while keeping the same 1–10 scale.

---

## What you have today (math_pipeline_v1.1)

### Current config: `difficulties_math.yaml`
Strengths:
- Clear **1–10** scale and simple `domain/category/level` routing.
- Supports **keyword-based fallback routing** (`rule_sets.keyword_rules`).
- Has a notion of “destination roots” (Windows + WSL), and a `folder_layout` template.

Gaps to address:
1. **Thin rubric**: `levels` are just a label + one-line description. There’s no domain-agnostic definition of what “level 6 vs 7” *means* (prereqs, abstraction, proof burden, etc.).
2. **Category coverage is uneven**: most categories cluster at 6–9; “level 1–4” coverage is minimal. This makes “difficulty” less useful as a curriculum axis.
3. **Not reusable outside math**: the file structure and keys are math-specific (`domains` are math-only; routing fields are `math_*`).
4. **Config/template is not wired up**:
   - `globals.folder_layout` exists, but `download_worker.py` currently hardcodes the output path.
   - `globals.destination_root_*` exists, but the actual pool roots come from `targets_math.yaml`.

### Current code path
- `targets_math.yaml` includes `math_routing: {domain, category, level, granularity}`
- `pipeline_driver.py` copies that into queue rows as:
  - `math_domain`, `math_category`, `difficulty_level`, `math_granularity`
- `download_worker.py` routes downloads using:
  1) explicit `math_*` fields from the queue row, else  
  2) `source_overrides`, else  
  3) `keyword_rules` match on `name` + `data_type`, else  
  4) defaults.

This is good for “route whole dataset by target id”, but it doesn’t yet generalize well.

---

## Goal for v2

Create a **single difficulty schema** that:
- Keeps the same **1–10** scale (so your curriculum and folder structure stay stable),
- Is **subject-agnostic** (math today; physics/engineering later),
- Supports **ranges and uncertainty** (a category can span 5–7 depending on artifact type),
- Allows **multi-stage routing** (dataset-level first, then record/chunk-level later),
- Provides a **standard rubric** that makes levels comparable across subjects.

---

## Proposed config design (v2)

### 1) Split the schema into two layers
**Layer A — global, subject-agnostic rubric**
- Defines each level (1–10) with:
  - prerequisites (typical prior topics)
  - cognitive operations (compute / model / prove / abstract)
  - formalism burden (symbol density, theorem-proof structure)
  - “artifact types” typical at that level (worksheets vs textbooks vs monographs)

**Layer B — subject taxonomy + mapping**
- For each `subject` (e.g., `math`, later `physics`, `engineering`):
  - `domains` and `categories`
  - category → `level_default` + `level_min/max`
  - optional `tags` (skills) and `aliases` (normalization)
  - subject-specific `rule_sets` (e.g., arXiv primary category mapping, MSC codes, grade-level keywords)

### 2) Add explicit routing fields (generic)
Replace `math_*` with a generic routing object:

```yaml
routing:
  subject: math
  domain: algebra
  category: linear_equations
  level: 4
  granularity: target   # target | record | chunk
  confidence: 0.9       # optional
  reason: "targets.yaml explicit"
```

Keep backwards compatibility for now (see migration plan).

### 3) Standardize “difficulty spans”
Many categories naturally span levels. Example:
- `quadratics_polynomials`: **(4–6)** depending on whether it’s “solve a quadratic” vs “Galois theory context”.

Store:

```yaml
level:
  default: 5
  min: 4
  max: 6
```

The pipeline can route by `default` initially, and later refine at record/chunk level.

### 4) Make rule sets reusable
Extend `keyword_rules` entries to include:
- `subject` (or allow subject inference)
- `priority`
- `confidence`

Example:

```yaml
- match_any: ["grade 3", "elementary"]
  route: {subject: math, domain: arithmetic, category: integers_basic, level: 1}
  confidence: 0.8
  priority: 10
```

Later, you can add other detectors:
- arXiv category → route
- MSC code → route
- textbook course tags (“AP Calculus”, “Real Analysis”, “Measure Theory”)
- repository structure (e.g., `lean/`, `coq/`, `isabelle/`)

---

## Pipeline changes required

### A) `pipeline_driver.py`
**Change**: Emit generic routing fields, while still emitting old math fields for compatibility.

1. From `targets_*.yaml`, read `routing` if present, else map `math_routing` → `routing` with `subject: math`.
2. Add these fields to each queue record:
   - `routing_subject`
   - `routing_domain`
   - `routing_category`
   - `routing_level` (keep `difficulty_level` for now, but plan to rename later)
   - `routing_granularity`
   - (optional) `routing_confidence`, `routing_reason`

**Why**: This lets you keep one pipeline that works for any subject later.

---

### B) `download_worker.py`
#### 1) Route resolution should become generic
Update `resolve_route()` to check in this order:

1. **Explicit routing fields in row**
   - Prefer: `routing_subject/domain/category/level`
   - Back-compat: `math_domain/math_category/difficulty_level` → treat as `subject=math`
2. `source_overrides` (by `target_id`)
3. Rule sets:
   - subject-aware keyword rules
   - (optional) arXiv / MSC / “course level” detectors
4. Default

Return a route object that always includes `subject`.

#### 2) Use `globals.folder_layout` instead of hardcoded paths
Currently the worker writes to:

```
{pool_root}/d{level}/{domain}/{category}/{target_id}
```

Instead:
- Read `globals.folder_layout` from the schema.
- Allow tokens like:
  - `{pool}`, `{subject}`, `{level:02d}`, `{domain}`, `{category}`, `{target_id}`

Recommended default:

```
pools/{pool}/{subject}/d{level:02d}/{domain}/{category}/{target_id}
```

This is the main change that makes the same schema portable to other subjects.

#### 3) Wire up `globals.destination_root_*` (optional but recommended)
Right now pools come from `targets_math.yaml` and default to `/data/math/pools/...`.

If you want the difficulty schema to “own” the output roots (especially on Windows/WSL), add logic:

- If `--pools-root` is not provided **and** `targets_yaml` doesn’t define pools, use:
  - `destination_root_wsl` when running under WSL
  - otherwise `destination_root_windows`

This makes “drop-in new subject” easier: you swap targets + schema, and output routes correctly.

---

### C) `targets_math.yaml` (and future `targets_<subject>.yaml`)
1. Add a generic `routing:` block next to `math_routing:` (or replace it after migration).
2. Include `subject: math`.

Example:

```yaml
routing:
  subject: math
  domain: calculus
  category: limits_derivatives
  level: 6
  granularity: target
```

Later, for physics:

```yaml
routing:
  subject: physics
  domain: mechanics
  category: newtonian_dynamics
  level: 5
  granularity: target
```

---

### D) `field_schemas.yaml`
Add standard routing fields to your “queue/evaluation record schema”, so everything downstream can rely on them:

- `routing_subject` (string, required)
- `routing_domain` (string, nullable)
- `routing_category` (string, nullable)
- `routing_level` (int 1–10)
- `routing_granularity` (enum: target|record|chunk)
- `routing_confidence` (float 0–1, optional)
- `routing_reason` (string, optional)

---

### E) Future (recommended) — record/chunk-level difficulty
Right now difficulty is **dataset-level**. To make the schema truly “thorough”:
- Add a *post-download* step (or integrate into build/chunk workers) that:
  1) parses a dataset into “records” or “problems”
  2) computes difficulty features (symbol density, proof markers, step count, etc.)
  3) refines `level` within the category’s `[min,max]` band
  4) routes individual records into `dXX/` folders.

This is where the rubric + level spans pay off.

---

## Migration plan (minimal disruption)

1. **Create new schema file** (e.g., `difficulties_v2.yaml`) with:
   - global rubric
   - `subjects.math` taxonomy
   - updated rule sets
2. Update `targets_math.yaml` companion pointer to use the new file:
   - `companion_files.difficulties_map: ./difficulties_v2.yaml`
3. Update `pipeline_driver.py` to emit both:
   - old `math_*` fields (temporary)
   - new `routing_*` fields (preferred)
4. Update `download_worker.py`:
   - prefer `routing_*`
   - fallback to old fields
   - implement `folder_layout` templating
5. After a couple runs, **remove `math_*` fields** from new targets and code paths.

---

## Suggested improvements to the math taxonomy (what to add)

To make levels 1–10 “feel real” for math, add categories that anchor each band:

- **1–2**: numeracy, operations, measurement, fractions/decimals, basic geometry
- **3–4**: ratio/percent, coordinate geometry, linear equations, basic proofs, contest-prep early
- **5–6**: algebra II, trig, intro stats/probability, precalc, intro proofs/discrete math
- **7**: calc I–II, linear algebra, ODE intro, abstract algebra intro
- **8**: real analysis intro, complex analysis intro, topology intro, advanced combinatorics
- **9**: measure theory, functional analysis, commutative algebra, algebraic topology, PDE
- **10**: category theory heavy, advanced monographs/research frontiers

Also consider a “proofness” axis in tags:
- `compute_only`, `applied_modeling`, `proof_intro`, `proof_advanced`

---

## Deliverables you should add to the repo

- `difficulties_v2.yaml` (new schema; multi-subject)
- `docs/DIFFICULTY_SCHEMA_UPGRADE.md` (this file)
- (optional) `routing_normalization.yaml` (aliases/synonyms)
- (optional) `difficulty_features.py` (for record/chunk scoring later)
