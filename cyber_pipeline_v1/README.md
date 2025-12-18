# Cybersecurity + Industrial Threat Modeling Corpus Pipeline (v1.0)

A safety-first **prototype** pipeline for building a defender-focused cybersecurity corpus from open datasets and open-access artifacts, adapted from the chemistry pipeline skeleton. It keeps the same audit-friendly structure (targets manifest → queues → downloads → catalog) while adding cyber-specific safety gates and normalization plans.

This repo helps you:
- maintain a single inventory (`targets.yaml`) of cybersecurity sources,
- snapshot license/terms evidence into per-target manifests,
- classify sources into **GREEN / YELLOW / RED** queues with conservative defaults,
- run **download** (GREEN) and **scrub/extract** (YELLOW) stages with cyber-aware filters,
- and build a global catalog / training manifests suitable for defensive LLM training.

> Not legal advice. This tool helps you *track* licenses and restrictions; you are responsible for compliance.

---

## Domain-specific safety additions
- **Secret + PII scanning** gates to redact credentials and personal data before emission.
- **Dual-use/exploit scanning** to quarantine offensive payloads or step-by-step intrusion instructions.
- Recommended **STIX/NVD/GitHub advisory normalizers** (stubs provided) to convert structured cyber data into consistent JSONL documents.
- Updated **license mapping** for MITRE ATT&CK/CWE/CAPEC and CVE Program Terms of Use (treated as YELLOW by default).
- Hardened denylist entries for exploit PoC corpora, malware dumps, and credential leak sources.

---

## Quick start

### Install
```bash
pip install -r requirements.txt
```

### Dry-run (recommended first)
Creates manifests + queues, but does not download or transform:
```bash
./run_pipeline.sh --targets targets.yaml
```

### Review pending YELLOW items
```bash
./run_pipeline.sh --targets targets.yaml --stage review
# or:
python3 review_queue.py --queue /data/cyber/_queues/yellow_pipeline.jsonl list
```

### Approve/reject a target (writes review_signoff.json)
```bash
python3 review_queue.py approve \
  --target attack_enterprise_stix \
  --reviewer "Your Name" \
  --reviewer-contact "you@example.com" \
  --reason "MITRE ToU acknowledged; will apply exploit filtering" \
  --constraints "Attribution + ToU compliance"
```

### Export reviewed items
```bash
python3 review_queue.py export --output /data/reviews.csv --format csv
python3 review_queue.py export --output /data/reviews.json --format json
```

---

## Notes on adapting from chem_pipeline_v1
- Core pipeline code is shared; new gates are enumerated in `targets.yaml` and should be enforced in `pipeline_driver.py`/`yellow_scrubber.py` as you extend the helpers.
- `field_schemas.yaml` now defines cyber normalization schemas (NVD CVE, GitHub advisories, STIX bundle summaries) instead of PubChem fields.
- `license_map.yaml` and `denylist.yaml` are tuned for cybersecurity/ICS sources.
- Additional worker stubs (`stix_worker.py`, `nvd_worker.py`, `advisory_worker.py`) provide starting points for structured-to-text transforms.
