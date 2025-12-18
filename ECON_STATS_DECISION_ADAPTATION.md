# Economics, Statistics, & Decision Science corpus: adapting the chem pipeline

This document explains how to adapt **chem_pipeline_v1** to collect **ethical, legally-cleared** datasets specialized for:

- economics (macro/micro, IO, labor, prices, development, energy econ)
- statistics (inference, causal, experimental design, forecasting)
- decision science / operations research (planning, optimization, cost curves, adoption, incentives)

Constraint: **avoid partisan/propaganda corpora** (the user called this “avoid right-wingism”). Practically, that means we bias toward **measurement-first sources** (gov/IGOs/peer-reviewed/open textbooks) and add an **advocacy/partisanship filter** rather than ingesting blogs/think-tanks.

> Not legal advice. This is an engineering plan for risk reduction.

---

## 1) What changes vs chemistry?

Chemistry corpora are mostly: papers + curated chemical DBs + patents + a few large crawls.  
Econ/stats/decision corpora are heavy on:

- **time series** (monthly/quarterly data; revisions; units & seasonal adjustment)
- **tabular panel data** (country-year / county-year / firm-year)
- **methods text** (stats textbooks; OR/optimization notes)
- **optimization instances** (MIP/LP graphs/VRP instances)
- **policy-like text** (regulations, agency guidance) that must be filtered to avoid ideology/advocacy.

That implies new **parsers**, new **download resolvers**, and new **content gates**.

---

## 2) “Green / Yellow / Red” selection rules for this domain

### Green (auto-ingest)
- **Public domain** U.S. federal data (BLS, BEA, EIA, Census tables where clearly PD).
- **CC-BY / CC0** IGO datasets with clear reuse statements (World Bank, many UN/FAO datasets).
- Open textbooks with **CC-BY / CC-BY-SA / CC0** where the license is explicit.

### Yellow (ingest only with review + stronger gates)
- “Record-level license” corpora where each record can differ (multi-source compilations like CommonPile components).
- Platforms with mixed content or PII risk (OpenML, some Dataverse microdata, scraped data portals).
- Anything with unclear upstream restrictions or “attribution chains” (OWID often includes derived datasets with upstream sources).

### Red (do not ingest)
- **Terms-of-service** that prohibit reuse for model training or redistribution.
- Paywalled/proprietary sources (IMF subscription datasets, many finance datasets, Bloomberg, etc.).
- High-PII microdata without a robust de-identification story.

The supplied targets file includes a red “do not ingest” example for **FRED** (kept as a reminder target).

---

## 3) Pipeline adaptations (concrete engineering tasks)

### A) Add tabular-first ingestion & normalization
Add a new build stage (or worker) that converts CSV/XLSX/TSV/JSON/SDMX into a tidy, model-friendly representation.

**New worker**: `tabular_worker.py` (suggested)
- detect dialect (delimiter, encoding)
- parse to Arrow/Parquet
- normalize to one of:
  - `time_series`: (series_id, time, value, unit, frequency, geo, seasonal_adj, notes, source)
  - `panel`: (entity_id, time, variable, value, unit, notes)
  - `cross_section`: (entity_id, variable, value, unit)
- generate **text renderings** (short: “data dictionary”, long: “series narrative”) so LLMs learn concepts + units.

Outputs:
- `tables.parquet` (authoritative numeric form)
- `table_dictionary.jsonl` (columns, units, codes, labels)
- `table_narratives.jsonl` (templated explanations; optional LLM paraphrase later)

### B) Add SDMX / CKAN / Socrata resolvers (high value for econ)
Many econ datasets are not “one file URL”:
- **SDMX**: OECD, Eurostat, IMF SDMX (some are restricted), UNData (varies)
- **CKAN**: many national/municipal portals
- **Socrata**: many U.S. city/state portals

Extend `download_worker.py` with new strategies:
- `sdmx`: constructs query URLs; handles paging; emits CSV/JSON.
- `ckan`: uses package_search → resources → download.
- `socrata`: uses SoQL; handles pagination; respects rate limits.

Until you implement these, keep those targets `enabled: false` (as in the new YAML).

### C) Add a PII / microdata safety layer
Econ can drift into microdata (surveys, firm-level datasets).  
Implement a conservative default:

- default allow: aggregated tables, public time series, open textbooks
- microdata requires: explicit license + explicit de-identification policy + field-level whitelist

Suggested gate: `pii_scan`
- detect direct identifiers (names, emails, phone, SSN-like, addresses)
- detect quasi-identifiers at scale (rare combos of age+zip+sex)
- for tabular: block columns matching risky patterns unless allowlisted

### D) Add an “advocacy / partisanship” filter (to avoid ideology-heavy corpora)
Do this as **content hygiene**, not a political classifier:
- keep **measurement & methods**
- drop **explicit partisan persuasion** and propaganda

Suggested gate: `advocacy_source_filter`
- source-based denylist: known partisan org sites, campaign pages, propaganda outlets
- content-based heuristics: “donate”, “take action”, “vote for”, “stop the steal”, etc.
- keep neutral references (laws, regs, statistical releases)

This is the practical way to satisfy “avoid right-wingism” without hard-coding ideology into the pipeline.

### E) Improve license evidence handling
Your current code expects `license_evidence_url` / `license_spdx_hint` fields (flat), while the older chem targets used nested structures.

**Recommendation**:
- standardize on the flat schema used in `targets_econ_stats_decision.yaml`
- update any older targets files similarly

Also patch: `pipeline_driver.py` should pass `output_pool` into queue rows so `download_worker.py` can route outputs (it currently defaults to quarantine).

---

## 4) How to use the provided targets file

1) Copy into the repo:
- `chem_pipeline_v1/targets_econ_stats_decision.yaml`

2) Run the pipeline driver:
```bash
python pipeline_driver.py --targets-yaml targets_econ_stats_decision.yaml
```

3) Run downloads (dry-run first):
```bash
python download_worker.py --queue /data/econ/_queues/download_queue.jsonl --targets-yaml targets_econ_stats_decision.yaml
python download_worker.py --queue /data/econ/_queues/download_queue.jsonl --targets-yaml targets_econ_stats_decision.yaml --execute --workers 4
```

4) Implement `tabular_normalize` / `zip_unpack` stages (these are referenced in YAML but not yet present as code).

---

## 5) Dataset candidate list (long-form)

### GREEN candidates (recommended)
**U.S. public domain (macro/labor/prices)**
- BLS time series flat files: CPI (CU), CES (CE), LAUS (LA), PPI (PC), JOLTS (JT), etc.
- BEA Regional zip datasets (CAINC*, CAGDP*, SQINC*, etc.)
- EIA Open Data bulk facility (manifest + selected ZIPs: TOTAL, PET, NG, ELEC; plus AEO releases)
- EPA open data (economics-adjacent environmental data; emissions inventories where relevant)

**International + development (clear license)**
- World Bank WDI bulk CSV
- World Bank PovcalNet outputs (if downloadable with explicit terms)
- OECD datasets (CC-BY 4.0; via SDMX once implemented)
- FAOSTAT bulk downloads (CC-BY 4.0; once bulk-selector implemented)

**Methods / decision science**
- NIST / SEMATECH e-Handbook of Statistical Methods (methods reference)
- OpenIntro materials (CC-BY-SA) – good for inference, regression, experimental design
- Public-domain U.S. government handbooks on cost estimating, decision analysis (where available)

### YELLOW candidates (good, but need review & stronger processing)
**Mixed-license / record-level corpora**
- CommonPile components relevant to econ/stats:
  - `pressbooks_filtered` (open textbooks)
  - `doab_filtered` (open access books)
  - `arxiv_abstracts_filtered` (research abstracts)
  - `wikimedia_filtered` (reference; generally safe but still record-level)
  - `regulations_filtered` (policy; requires ideology/advocacy filtering)
- Crossref/OpenAlex metadata snapshots (CC0, but large and may include abstracts with mixed rights—check)

**Portals/platforms with heterogeneous data**
- OpenML (dataset-by-dataset license + PII screening required)
- Dataverse collections (often mixed licensing; sometimes restricted microdata)

**Attribution-chain complexity**
- Our World in Data (OWID) datasets: often repackage upstream sources; must keep only OWID-produced or clearly open subsets.

### RED candidates (avoid)
- FRED (Terms restrict reuse; treat as a “do not ingest” example target)
- NBER working papers full text (copyright / restrictions)
- SSRN full text (restrictions)
- Paywalled/proprietary finance datasets (Bloomberg/Refinitiv/WRDS/Compustat)
- “Think tank / advocacy” corpora (partisan persuasion; not measurement-first)
- Courseware with NC licenses (CC-BY-NC / CC-BY-NC-SA) unless your use-case is strictly noncommercial

---

## 6) Decision-science-specific synthetic data you should generate (high leverage)

Once the real-data base is in place, synthetic tasks can give you **reasoning depth** without legal risk:

- cost curve fitting (learning curves, experience curves, Wright’s law)
- adoption / diffusion (Bass model; S-curves; policy incentive perturbations)
- causal inference exercises (DAG identification, diff-in-diff, IV, RDD)
- A/B test planning (power, sample size, sequential testing)
- optimization (LP/MIP toy instances + explanation; inventory control; routing; project selection)
- welfare / incentive design toy models (mechanism constraints, budget feasibility)

These can be generated deterministically and graded automatically (a big advantage).

---

## 7) Next steps checklist

1) **Implement** `zip_unpack` and `tabular_normalize` build stages.
2) Add `sdmx`, `ckan`, and `socrata` download strategies.
3) Add `pii_scan` for text + tabular (default block unless allowlisted).
4) Add `advocacy_source_filter` (denylist + heuristics).
5) Patch `pipeline_driver.py` to include `output_pool` in emitted queue rows.
6) Turn on disabled placeholders (OECD, FAOSTAT, OpenML) one-by-one with size caps + review.

