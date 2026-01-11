# Gov portal add-ons (CKAN + Sitemap workers)

These add-ons help you ingest English government/open-data sources *without* turning your main
pipeline into a bespoke crawler.

## Files
- `ckan_worker.py`: resolves CKAN packages into a `../pipelines/targets/targets_nlp.yaml` fragment (or a download queue JSONL)
- `sitemap_worker.py`: expands a sitemap.xml into chunked targets (HTTP URL lists)
- `license_map_v0_3.yaml`: optional add-on (not included here) that adds SPDX IDs + normalization for UK/Canada gov licenses

## 1) License map update
If you have a `license_map_v0_3.yaml` add-on, merge its changes into `license_map.yaml`.

Added SPDX allowlist entries:
- OGL-UK-3.0 (UK Open Government Licence v3.0)
- OGL-Canada-2.0 (Canada Open Government Licence)
- OPL-UK-3.0 (UK Open Parliament Licence v3.0)

Also adds normalization rules so hints like `LicenseRef-OGL-UK-3.0` resolve to `OGL-UK-3.0`.

## 2) CKAN portals (Australia/Canada/NZ/etc.)
Example: Australia data.gov.au, only CC-BY 4.0 datasets, prefer PDFs and HTML.

Dry run:
  python ckan_worker.py \
    --api-base https://data.gov.au/data/api/3/action \
    --site-base https://data.gov.au \
    --portal-name "Australia data.gov.au" \
    --fq "license_id:cc-by-4.0" \
    --allow-license-ids cc-by-4.0 \
    --allow-formats PDF,HTML,TXT \
    --mode targets \
    --out /data/nlp/_targets/generated_au_ckan.yaml

Write output (execute):
  python ckan_worker.py ... --execute

Then merge the emitted `targets:` block into your main NLP targets file
(or point pipeline_driver at a combined targets file).

## 3) Sitemap expansion (GOV.UK)
GOV.UK has a global sitemap. Start narrow with include/exclude regex to avoid huge downloads.

Dry run:
  python sitemap_worker.py \
    --sitemap-url https://www.gov.uk/sitemap.xml \
    --id-prefix uk_govuk_policy_ogl \
    --name "GOV.UK policy and guidance pages" \
    --spdx-hint OGL-UK-3.0 \
    --include-regex "/government/publications/,/guidance/,/government/collections/" \
    --exclude-regex "/help/,/sign-in,\.jpg$,\.png$,\.zip$" \
    --max-urls 20000 \
    --chunk-size 2000 \
    --out /data/nlp/_targets/generated_uk_govuk_sitemap.yaml

Write output:
  python sitemap_worker.py ... --execute

## 4) Integration notes (important)
- These generated targets use `download.strategy: http` (supported by `acquire_worker.py`).
- The v2 pipeline driver already emits `output_pool` fields in queue rows, and acquire respects
  license profiles for raw routing. Double-check per-target `output.pool` when mixing pools.
