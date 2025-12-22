# Code Corpus Pipeline (v2)

Version 2 aligns the code pipeline with the math_pipeline_v2 stage flow while layering in code-specific handling (secrets redaction/pitching, vendored stripping, lightweight language detection, and difficulty mapping tuned for code domains).

Stages:

1. Classify targets and snapshot license/ToU evidence (`pipeline_driver.py`).
2. Acquire GREEN and YELLOW targets into the v2 raw layout (`acquire_worker.py` + `code_worker.py` extraction).
3. Screen YELLOW data into canonical records with strict pitch rules (`yellow_screen_worker.py`).
4. Merge canonical GREEN + screened YELLOW into combined shards (`merge_worker.py`).
5. Apply a final light screen + difficulty assignment, writing difficulty shards (`difficulty_worker.py`).
6. Build catalogs and ledgers over the v2 layout (`catalog_builder.py`).

> Not legal advice. This tooling helps track licenses and restrictions; you remain responsible for compliance.

---

## Directory layout

Roots are defined in `targets_code.yaml -> globals`:

```
/data/code/
  raw/
    green/{license_pool}/{target_id}/...
    yellow/{license_pool}/{target_id}/...
  screened_yellow/{license_pool}/shards/*.jsonl.gz
  combined/{license_pool}/shards/*.jsonl.gz
  final/{license_pool}/d01..d10/shards/*.jsonl.gz
  _ledger/*.jsonl
  _pitches/*.jsonl
  _queues/*.jsonl
  _manifests/{target_id}/...
```

Sharding is controlled by `globals.sharding` (max records per shard, compression, naming `*_00000.jsonl.gz`).

---

## Stage overview

| Stage | Script | Notes |
| --- | --- | --- |
| Classify | `pipeline_driver.py` | Emits GREEN/YELLOW/RED queues with `routing` defaults tuned for code. |
| Acquire | `acquire_worker.py` | Downloads payloads into `raw/{green|yellow}/{license_pool}/{target_id}` and optionally runs `code_worker.py` to emit canonical shards. Supports HTTP/FTP/git/Zenodo/Dataverse, HuggingFace datasets, Figshare, and GitHub releases. |
| Screen YELLOW | `yellow_screen_worker.py` | Converts raw YELLOW payloads into canonical records, pitching anything that violates length/licensing/deny-phrase or secrets policies. Writes pass/pitch ledgers + done markers. |
| Merge | `merge_worker.py` | Combines canonical GREEN + screened YELLOW shards with deduplication and a combined ledger. |
| Difficulty | `difficulty_worker.py` | Final light screen + rule-based difficulty assignment using `difficulties_code.yaml`; writes difficulty shards and ledger. |
| Catalog | `catalog_builder.py` | Summarizes counts, bytes, language coverage, and ledgers across stages. |

`run_pipeline.sh` orchestrates these stages with `--stage classify|acquire_green|acquire_yellow|screen_yellow|merge|difficulty|catalog`.

---

## Quick start

```bash
pip install -r requirements.txt

# Dry-run classify only
./run_pipeline.sh --targets targets_code.yaml --stage classify

# Acquire GREEN and YELLOW (execute downloads + code extraction)
./run_pipeline.sh --targets targets_code.yaml --stage acquire_green --execute
./run_pipeline.sh --targets targets_code.yaml --stage acquire_yellow --execute

# Screen, merge, difficulty, catalog
./run_pipeline.sh --targets targets_code.yaml --stage screen_yellow --execute
./run_pipeline.sh --targets targets_code.yaml --stage merge --execute
./run_pipeline.sh --targets targets_code.yaml --stage difficulty --execute
./run_pipeline.sh --targets targets_code.yaml --stage catalog
```

### Notes

- YELLOW screening enforces "anything unclear is pitched"; see `targets_code.yaml -> globals.screening` and per-target `yellow_screen` overrides.
- Secrets found in YELLOW are pitched by default; GREEN records redact secrets in-place.
- Outputs are segregated by `license_profile` (`permissive`, `copyleft`, `quarantine`).
- Ledgers in `_ledger/` provide pass/pitch summaries and shard indexes for reproducibility.

## License

Pipeline code is provided as-is for research and development use.

**Data sources retain their own licenses** - this tool helps you track and respect them.
