# Environmental / Earth Systems Corpus Pipeline (v1.0)

This package adapts the chemistry-focused prototype into an **ethically and legally screened pipeline for Environmental / Earth Systems science**. It preserves the same green / yellow / red governance model and queue-based execution, but swaps in Earth-specific targets, licensing policy, and worker notes derived from the adaptation brief.

## What's included
- **targets.yaml** — recreated from `targets_earth.yaml` with green / yellow / red rules for environmental and Earth observation sources.
- **license_map.yaml** — licensing + ethics policy tuned for public-domain government data, CC BY/CC0 content, Copernicus/ECMWF/GBIF mixed licenses, and hard blocks for NC/ND/IPCC/paywalled sources.
- **field_schemas.yaml** — Earth-specific schemas for dataset cards, STAC metadata summaries, and indicator tables.
- **ADAPTATION_PLAN_EARTH.md** — the full plan describing the domain shift, new workers (STAC/THREDDS/API tabular), metadata fields, and run-order guidance.
- Pipeline executables carried over from the chemistry package (`pipeline_driver.py`, `download_worker.py`, `yellow_scrubber.py`, `catalog_builder.py`, `review_queue.py`, `run_pipeline.sh`) so you can reuse the same review and catalog flow.

> Not legal advice. This tool helps you track licenses and restrictions; you are responsible for compliance.

## Quick start (dry-run first)
```bash
pip install -r requirements.txt
./run_pipeline.sh --targets targets.yaml
```

Run with execution enabled and limits:
```bash
./run_pipeline.sh --targets targets.yaml --execute --limit-targets 3 --limit-files 5
```

List pending YELLOW items for manual review:
```bash
./run_pipeline.sh --targets targets.yaml --stage review
```

## Earth-specific notes
- Licensing expectations and scrubbers come from `license_map.yaml`; see the "Licensing + ethics baseline" and "Content safety & ethics" sections in `ADAPTATION_PLAN_EARTH.md` for rationale.
- Targets emphasize dataset documentation, STAC/NetCDF metadata, and indicator tables over bulk raster downloads; see `targets.yaml` for worker hints and storage layout (text/structured/raw splits). Default storage roots in scripts now point to `/data/earth/...` rather than the old chemistry paths.
- Keep third-party media exclusions and sensitive geospatial handling enabled when processing biodiversity or location-rich feeds.
