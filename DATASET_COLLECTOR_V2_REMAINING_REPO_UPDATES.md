# Dataset_Collector v2 — Remaining Fixes for `Dataset_Collector-main (7).zip`

This is the **delta plan** for what still needs to be updated/fixed based on the current repo state.

**Current validator status:** `python tools/validate_repo.py --root .`  
➡️ **10 errors**, **6 warnings**  
Report written to: `validation_report.json`

---

## 0) What’s already in good shape ✅

- Repo-wide `license_map.yaml` schema is now consistent (profiles/gating/normalization present).
- Jupyter notebook is Windows-friendly and the bash runner is clearly marked as optional.
- Repo-level validator exists and is already catching real problems early.
- Environment file exists (`environment.yml`).

So the remaining work is mostly: **download config normalization**, **HF config support**, **Zenodo key alignment**, and **clearing placeholder enabled targets**.

---

## 1) Make `tools/validate_repo.py` pass (0 errors, 0 warnings)

### 1.1 Fix the 10 validator errors (missing download requirements)

#### A) `chem_pipeline_v2/targets_chem.yaml` — `qm7x` (Zenodo)
**Error:** `download.record_id/doi/url required for zenodo strategy`  
**Current:**
```yaml
download:
  strategy: zenodo
  record: "4288677"
```

✅ **Fix option (recommended):** rename `record` → `record_id`
```yaml
download:
  strategy: zenodo
  record_id: "4288677"
  integrity:
    verify_zenodo_md5: true
    compute_sha256: true
```

(You can also keep backwards compatibility by updating `handle_zenodo()` to accept `record` and map it to `record_id`—see section 2.3.)

---

#### B) `code_pipeline_v2/targets_code.yaml` — 3 enabled targets using `download.config` dict
**Errors:**
- `openai_humaneval`: missing `download.dataset_id`
- `evalplus_mbppplus`: missing `download.dataset_id`
- `python_peps`: missing `download.repo/repo_url/url`

**Current:**
```yaml
download:
  strategy: huggingface_datasets
  config: { dataset_id: "openai/openai_humaneval" }
```

✅ You have two viable fixes:

**Fix option 1 (preferred): implement download normalization in code (repo-wide)**
- Keep the YAML style.
- Update `tools/validate_repo.py` + all `*_pipeline_v2/acquire_worker.py` to merge `download.config` dict into the top-level `download` dict.
- This solves these errors **and** makes config handling consistent going forward.

**Fix option 2 (quick YAML-only): flatten these 3 targets**
```yaml
# openai_humaneval
download:
  strategy: huggingface_datasets
  dataset_id: "openai/openai_humaneval"

# evalplus_mbppplus
download:
  strategy: huggingface_datasets
  dataset_id: "evalplus/mbppplus"

# python_peps
download:
  strategy: git
  repo_url: "https://github.com/python/peps"
  shallow: true
```

If you do option 2, you should still do the HF config fix (section 2.2), because many other targets use `download.config: "pmc"` etc.

---

#### C) `materials_science_pipeline_v2/targets_materials.yaml` — placeholder enabled targets
**Errors (6):**
- `usgs_critical_minerals_aux` (http) urls missing
- `epa_egrid_aux` (http) urls missing
- `arxiv_materials_text` (http) urls missing
- `aflowlib` (http) urls missing
- `osm_geodata` (http) urls missing
- `zenodo_materials_collections` (zenodo) record_ids empty

✅ **Fix option (recommended for now): disable placeholders by default**
Set these to `enabled: false` until you fill real URLs/IDs:
```yaml
- id: usgs_critical_minerals_aux
  enabled: false
- id: epa_egrid_aux
  enabled: false
- id: arxiv_materials_text
  enabled: false
- id: aflowlib
  enabled: false
- id: osm_geodata
  enabled: false
- id: zenodo_materials_collections
  enabled: false
```

(You can re-enable them once you’ve confirmed stable download endpoints and licensing evidence.)

---

### 1.2 Fix the 6 validator warnings (unknown `license_profile`)

#### A) `cyber_pipeline_v2` — `license_profile: quarantine`
Targets:
- `sigma_rules`
- `epss_current_scores`

✅ Fix by adding a profile to `cyber_pipeline_v2/license_map.yaml`:
```yaml
profiles:
  quarantine:
    default_bucket: YELLOW
```

(Alternative: rename those targets to `license_profile: unknown` or `record_level`, but adding the profile is cleaner.)

---

#### B) `engineering_pipeline_v2` — `license_profile: public_domain`
Targets:
- `uspto_odp_bulk_api_catalog`
- `openalex_snapshot_docs`
- `wikidata_dump_cc0`
- `data_gov_policy_anchor`

✅ Fix by adding a profile to `engineering_pipeline_v2/license_map.yaml`:
```yaml
profiles:
  public_domain:
    default_bucket: GREEN
```

(Alternative: rename those targets to `license_profile: permissive`.)

---

## 2) Repo-wide functional fixes (these are real runtime bugs even if validation passes)

### 2.1 Implement **download config normalization** everywhere
Right now you have three patterns in `targets_*.yaml`:

1) top-level fields:
```yaml
download: { strategy: git, repo_url: "..." }
```
2) HF config name strings:
```yaml
download: { strategy: huggingface_datasets, dataset_id: "...", config: "pmc" }
```
3) nested config dicts:
```yaml
download: { strategy: huggingface_datasets, config: { dataset_id: "..." } }
```

✅ Update **all** of these components to normalize consistently:

- `tools/validate_repo.py`
- every `*_pipeline_v2/acquire_worker.py`
- every `*_pipeline_v2/merge_worker.py` (pool routing depends on license_profile; see 2.4)
- (optional) `*_pipeline_v2/pipeline_driver.py` if it tries to infer output.pool from profile (currently only 3d does)

**Implementation pattern (recommended):**

Add this helper to each pipeline’s `acquire_worker.py` (near the top):

```python
def normalize_download(download: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(download or {})
    cfg = d.get("config")

    # If config is a dict, merge into top-level (do not overwrite strategy)
    if isinstance(cfg, dict):
        merged = dict(cfg)
        merged.update({k: v for k, v in d.items() if k != "config"})
        d = merged

    # Zenodo backwards-compat: record/record_ids -> record_id
    if d.get("strategy") == "zenodo":
        if not d.get("record_id") and d.get("record"):
            d["record_id"] = d["record"]
        if not d.get("record_id") and isinstance(d.get("record_ids"), list) and d["record_ids"]:
            d["record_id"] = d["record_ids"][0]

    return d
```

Then, in **every handler**, start with:
```python
download = normalize_download(row.get("download", {}) or {})
```

Do the same normalization in `tools/validate_repo.py` before checking requirements.

---

### 2.2 Fix Hugging Face dataset **config name** support (string `download.config`)
Many targets (CommonPile subsets etc.) use:
```yaml
download:
  strategy: huggingface_datasets
  dataset_id: common-pile/common-pile
  config: pmc
```

But every `handle_hf_datasets()` currently does:
```python
ds = load_dataset(dataset_id, ...)
```
…and ignores the config name, so it will often download the **wrong subset**.

✅ Update `handle_hf_datasets()` in **all 18 pipelines**:

```python
download = normalize_download(row.get("download", {}) or {})
dataset_id = download.get("dataset_id")
cfg = download.get("config")
hf_name = cfg if isinstance(cfg, str) else None

load_kwargs = download.get("load_kwargs", {}) or {}
if hf_name and "name" not in load_kwargs:
    load_kwargs["name"] = hf_name

# then load_dataset(dataset_id, split=..., **load_kwargs)
```

This fixes **CommonPile subset acquisition** and keeps your YAML compact.

---

### 2.3 Fix Zenodo acquisition key mismatch (`record_id` vs `api/record_url`)
Your `handle_zenodo()` currently requires:
- `download.api` or `download.record_url`

But your validator and YAML now want:
- `download.record_id` / `download.doi` / `download.url`

✅ Make `handle_zenodo()` accept all of these:

- If `api` or `record_url` present → use it
- Else if `record_id` present → build:
  - `https://zenodo.org/api/records/{record_id}`
- Else if `doi` present → query:
  - `https://zenodo.org/api/records/?q=doi:{doi}`
- Else if `url` present:
  - if it already looks like `/api/records/` use it directly, otherwise treat as error (or require record_id)

This aligns runtime behavior with validator expectations and target YAML.

---

### 2.4 Fix license-profile → license-pool routing (currently too narrow)
Across **all** pipelines, `acquire_worker.py` and `merge_worker.py` treat license pools as:
- `permissive`, `copyleft`, `quarantine`

But the repo now uses license profiles like:
- `record_level`
- `unknown`
- `public_domain`
- `quarantine`
- `deny`

Right now, anything not exactly `{permissive,copyleft,quarantine}` gets forced into `quarantine`.

✅ Recommended mapping:

| license_profile | license_pool |
|---|---|
| permissive | permissive |
| public_domain | permissive |
| record_level | permissive |
| copyleft | copyleft |
| unknown | quarantine |
| quarantine | quarantine |
| deny | quarantine (bucket will be RED anyway) |

**Apply this in:**
- every `*_pipeline_v2/acquire_worker.py` → `resolve_license_pool()`
- every `*_pipeline_v2/merge_worker.py` → `route_pool()`

(If you prefer record_level to stay separate, add a 4th pool and update merge + catalog outputs accordingly. The above mapping is the “minimal change” version.)

---

### 2.5 Remove duplicated return statements in `load_license_map()` (all pipeline drivers)
All 18 `*_pipeline_v2/pipeline_driver.py` contain two `return LicenseMap(...)` blocks back-to-back.

✅ Remove the first one and keep the final one (or delete the final one), so the function has **exactly one** return.

This is not crashing anything today, but it is a drift landmine.

---

## 3) Notebook + README alignment

### 3.1 README still claims “Jupyter requires WSL/bash”
In `README.md` you still have:
- “### Jupyter (WSL / bash required)”

…but your notebook is now positioned as Windows-first, and the bash runner is optional.

✅ Update README to:

- Make `dataset_collector_run_all_pipelines.ipynb` the primary path
- Label WSL/bash as optional

Also ensure the `build_natural_corpus.py` example command is not truncated.

---

### 3.2 Add a validator cell to the notebook (recommended)
At the top of `dataset_collector_run_all_pipelines.ipynb`, add a cell:

```python
!python tools/validate_repo.py --root .
```

Then open `validation_report.json` if errors > 0.

This makes it very hard to accidentally run with broken enabled targets.

---

## 4) Verification steps (what “done” looks like)

1) Run:
```bash
python tools/validate_repo.py --root .
```
Expected:
```json
{"errors": 0, "warnings": 0, ...}
```

2) Dry-run a pipeline from its folder (example math):
```bash
python pipeline_driver.py --dest-root "E:\AI-Research\datasets\Natural" --execute false
```
Expected:
- GREEN targets route to `raw/green/*`
- YELLOW targets route to `raw/yellow/*`
- HF subsets pull the correct config (pmc/pes2o/etc.)

3) Run one full “happy path” pipeline with execute:
- classify
- acquire_green
- acquire_yellow
- yellow_screen
- merge
- catalog

4) Confirm the merged shards output lands under:
- `combined/permissive/shards/...`
- (and `combined/copyleft/...` if you keep that pool enabled)

---

## 5) Short checklist (copy/paste)

- [ ] Fix validator errors (qm7x zenodo, code targets config dicts, materials placeholders)
- [ ] Add missing license profiles (`quarantine`, `public_domain`)
- [ ] Implement `normalize_download()` in validator + all acquire workers
- [ ] Implement HF config-name support (`download.config: "pmc"`)
- [ ] Make zenodo handler accept `record_id` and build API URL
- [ ] Update license_profile → license_pool routing in acquire + merge workers
- [ ] Remove duplicate returns in pipeline_driver `load_license_map()`
- [ ] Update README to make Windows-first notebook the primary path
- [ ] Add validator cell to notebook

---
