# Troubleshooting

Common issues and fixes when running Dataset Collector pipelines.

## Pipeline exits without outputs

**Symptoms**
- Only logs are produced, no `combined/` output.

**Likely causes**
- The pipeline was run without `--execute` (dry-run mode).
- Stage order skipped required prerequisites.

**Fix**
- Re-run the stage with `--execute`.
- Follow the stage order documented in `docs/run_instructions.md`.

---

## Missing or empty queues/catalogs

**Symptoms**
- Queue folders are empty or catalogs do not populate.

**Likely causes**
- `DATASET_ROOT`/`DATASET_COLLECTOR_ROOT` points to a location without write access.
- The target list is empty or disabled in the `targets_*.yaml` file.

**Fix**
- Set `DATASET_ROOT` or update the pipeline config to a writable path.
- Confirm target entries are enabled and not commented out.

---

## GitHub API rate limiting

**Symptoms**
- Errors referencing GitHub API limits or HTTP 403 responses.

**Likely causes**
- Running unauthenticated GitHub API requests.

**Fix**
- Export `GITHUB_TOKEN` with a token that has read access.
- Re-run the acquisition stages.

---

## ChemSpider acquisition failures

**Symptoms**
- ChemSpider sources fail or are skipped.

**Likely causes**
- Missing `CHEMSPIDER_API_KEY` or incorrect key.

**Fix**
- Set `CHEMSPIDER_API_KEY` before running the chem pipeline.

---

## AWS S3 download errors

**Symptoms**
- `s3_sync` or requester-pays targets fail.

**Likely causes**
- AWS CLI not installed or not authenticated.
- Requester-pays buckets require explicit AWS credentials.

**Fix**
- Install AWS CLI v2 and configure credentials.
- Confirm requester-pays permissions before retrying.

---

## Torrent downloads fail

**Symptoms**
- Targets using `download.strategy: torrent` do not start.

**Likely causes**
- `aria2c` is not installed or unavailable on PATH.

**Fix**
- Install `aria2` and ensure `aria2c` is on PATH.

---

## Yellow screening does not run

**Symptoms**
- `screen_yellow` stage produces no output.

**Likely causes**
- No targets marked as YELLOW in targets YAML.

**Fix**
- Verify that the target entries include `safety_bucket: YELLOW` where expected.
- Ensure the target file is the same one passed to `--targets`.
