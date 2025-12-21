# Difficulty-aware folder routing for the Math Corpus Pipeline (WSL)

This guide shows how to integrate **math domain/category + difficulty levels (1–10)** so that, after license screening, the pipeline **routes outputs into difficulty folders automatically**.

Destination root you requested:
- Windows (human reference): `E:/AI-Research/datasets/Natural/math`
- WSL path (use this in config): `/mnt/e/AI-Research/datasets/Natural/math`

Companion routing config (provided in this deliverable):
- `difficulties_math.yaml`

---

## 1) Point the pipeline at your E: drive (WSL paths)

Edit `targets_math.yaml` to store everything under `/mnt/e/AI-Research/datasets/Natural/math`.

Recommended layout under your destination root:

```
/mnt/e/AI-Research/datasets/Natural/math/
  _staging/
  _manifests/
  _queues/
  _catalogs/
  _logs/
  pools/
    permissive/
    copyleft/
    quarantine/
```

Update these fields in `targets_math.yaml`:

```yaml
globals:
  storage_root: "/mnt/e/AI-Research/datasets/Natural/math"
  staging_root: "/mnt/e/AI-Research/datasets/Natural/math/_staging"
  manifests_root: "/mnt/e/AI-Research/datasets/Natural/math/_manifests"
  queues_root: "/mnt/e/AI-Research/datasets/Natural/math/_queues"
  catalogs_root: "/mnt/e/AI-Research/datasets/Natural/math/_catalogs"
  logs_root: "/mnt/e/AI-Research/datasets/Natural/math/_logs"

  pools:
    permissive: "/mnt/e/AI-Research/datasets/Natural/math/pools/permissive"
    copyleft: "/mnt/e/AI-Research/datasets/Natural/math/pools/copyleft"
    quarantine: "/mnt/e/AI-Research/datasets/Natural/math/pools/quarantine"
```

---

## 2) Register the difficulty taxonomy as a companion config

Add this to `targets_math.yaml`:

```yaml
companion_files:
  license_map: "./license_map.yaml"
  field_schemas: "./field_schemas.yaml"
  denylist: "./denylist.yaml"
  difficulties_map: "./difficulties_math.yaml"
```

---

## 3) Decide how routing will be assigned

There are two practical ways.

### Option A (recommended): explicit per-target routing in `targets_math.yaml`

Add a `math_routing` block to each target:

```yaml
- id: "openstax_calculus"
  name: "OpenStax Calculus"
  ...
  math_routing:
    domain: "calculus"
    category: "calc_single_variable_1"
    level: 6
    granularity: "target"  # target | record | chunk
```

Use `domain` and `category` IDs from `difficulties_math.yaml`:
- `domains.<domain>.categories.<category>.level`

### Option B: keep targets clean and use `source_overrides` in `difficulties_math.yaml`

```yaml
source_overrides:
  common-pile_wikipedia:
    domain: "math_reference"
    category: "encyclopedia"
    level: 6
```

---

## 4) Minimal code changes (automatic routing)

Your prototype already screens licenses into GREEN/YELLOW/RED queues, then downloads GREEN. To route into difficulty folders you only need:

1) **Emit routing fields into queue rows** (`pipeline_driver.py`)
2) **Use those fields to build the output directory** (`download_worker.py`)

### 4.1 Patch: `pipeline_driver.py`

**Why:** `download_worker.py` expects `row["output_pool"]`, but `pipeline_driver.py` currently doesn’t emit it (so downloads default to `quarantine`). While adding difficulty fields, emit `output_pool` too.

Add near the construction of `row = {...}`:

```python
# --- NEW: output pool + math routing ---
out_pool = (t.get("output", {}) or {}).get("pool")
if not out_pool:
    # fallback: keep copyleft isolated; otherwise permissive for GREEN, quarantine for non-green
    if profile == "copyleft":
        out_pool = "copyleft"
    elif eff_bucket == "GREEN":
        out_pool = "permissive"
    else:
        out_pool = "quarantine"

mr = t.get("math_routing", {}) or {}
row.update({
    "output_pool": out_pool,
    "math_domain": mr.get("domain"),
    "math_category": mr.get("category"),
    "difficulty_level": mr.get("level"),
})
```

(Optionally also copy these into the `evaluation` dict so they appear in `evaluation.json`.)

### 4.2 Patch: `download_worker.py`

Load `difficulties_math.yaml` and compute the route.

**Add CLI arg (optional):**

```python
ap.add_argument("--difficulty-yaml", default=None, help="Path to difficulties_math.yaml")
```

**Load config (once):**

```python
cfg_targets = load_yaml(Path(args.targets_yaml)) if args.targets_yaml else {}
comp = (cfg_targets.get("companion_files", {}) or {})

diff_path = args.difficulty_yaml or comp.get("difficulties_map")
diff_cfg = load_yaml(Path(diff_path)) if diff_path else {}
```

**Routing helper (precedence order):**
1. `row.difficulty_level` / `row.math_domain` / `row.math_category` (from targets)
2. `source_overrides[row.id]` (from difficulties yaml)
3. `keyword_rules` match against `row.name` + `row.data_type`
4. defaults from `globals.default_*`

```python
def resolve_route(row: dict, diff_cfg: dict) -> dict:
    g = (diff_cfg.get("globals", {}) or {})
    default = {
        "domain": g.get("default_domain", "misc"),
        "category": g.get("default_category", "misc"),
        "level": int(g.get("default_level", 5)),
    }

    # 1) explicit routing (from targets)
    domain = row.get("math_domain")
    category = row.get("math_category")
    level = row.get("difficulty_level")
    if domain and category and level:
        return {"domain": domain, "category": category, "level": int(level)}

    # 2) per-target override
    overrides = (diff_cfg.get("source_overrides", {}) or {})
    if row.get("id") in overrides:
        o = overrides[row["id"]]
        return {"domain": o.get("domain", default["domain"]),
                "category": o.get("category", default["category"]),
                "level": int(o.get("level", default["level"]))}

    # 3) keyword rules
    blob = f"{row.get('name','')} {' '.join(row.get('data_type',[]) or [])}".lower()
    for rule in ((diff_cfg.get("rule_sets", {}) or {}).get("keyword_rules", []) or []):
        if any(k.lower() in blob for k in (rule.get("match_any", []) or [])):
            r = rule.get("route", {}) or {}
            return {"domain": r.get("domain", default["domain"]),
                    "category": r.get("category", default["category"]),
                    "level": int(r.get("level", default["level"]))}

    return default
```

**Use the route when building the output directory:**

Replace the existing output-dir logic with:

```python
route = resolve_route(row, diff_cfg)
level = max(1, min(10, int(route["level"])))

base = {"permissive": ctx.pools.permissive,
        "copyleft": ctx.pools.copyleft}.get(pool, ctx.pools.quarantine)

out_dir = base / f"d{level:02d}" / safe_name(route["domain"]) / safe_name(route["category"]) / tid
ensure_dir(out_dir)
```

This yields deterministic folders like:

```
.../pools/permissive/d06/calculus/calc_single_variable_1/openstax_calculus/
.../pools/copyleft/d06/math_reference/encyclopedia/common-pile_wikipedia/
```

---

## 5) Run under WSL

```bash
cd /path/to/math_pipeline_v1/math_pipeline_v1
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Dry-run classification (writes queues + manifests)
./run_pipeline.sh --targets targets_math.yaml

# Execute downloads (now routed into d01..d10)
./run_pipeline.sh --targets targets_math.yaml --stage download --execute --workers 4
```

Quick verification:

```bash
ls /mnt/e/AI-Research/datasets/Natural/math/pools/permissive/d06 || true
ls /mnt/e/AI-Research/datasets/Natural/math/pools/copyleft/d06 || true
```

---

## 6) Practical defaults for mixed sources

If you’re routing at **target-level**, broad sources need a default “band” until you add record/chunk classifiers:

- Wikipedia / encyclopedia-like → `math_reference/encyclopedia`, level **6**
- arXiv math corpora → `math_reference/research_papers`, level **9** (or map by `math.<subcat>`)
- Lean/Coq/Isabelle libraries → `formal_systems/...`, level **8**

You can refine any of these later using `source_overrides`.

---

## 7) Checklist

- [ ] Copy `difficulties_math.yaml` next to `targets_math.yaml`.
- [ ] Update `targets_math.yaml` globals to `/mnt/e/...` paths.
- [ ] Add `companion_files.difficulties_map`.
- [ ] Add `math_routing` blocks (or use `source_overrides`).
- [ ] Patch `pipeline_driver.py` to emit `output_pool` + routing fields.
- [ ] Patch `download_worker.py` to build `.../dXX/<domain>/<category>/...` paths.
