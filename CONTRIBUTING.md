# Contributing to Dataset Collector v2

Thanks for helping improve Dataset Collector v2. This repo builds datasets with
license-aware screening, so contributions must prioritize safe usage and
reproducibility.

## Quick contribution checklist

- Keep changes scoped and reproducible.
- Run `python tools/validate_repo.py --root .` before submitting.
- Update `updated_utc` for any YAML configs you edit.

## Adding a new target (required steps)

When you add a target to a pipeline YAML (for example `targets_math.yaml`):

1. **Use a stable `id`**
   - Lowercase, snake_case, no spaces.
   - Never reuse an `id` for a different dataset.

2. **Define a clear acquisition method**
   - Set `download.strategy` and the required config for the strategy
     (e.g. `url`, `dataset_id`, `repo`).

3. **Capture license evidence**
   - Fill in `license_evidence.url` with a permanent URL to the license.
   - For repos, capture `LICENSE` and `README` plus a commit hash.
   - For hosted datasets, record dataset revision/version and retrieval date.

4. **Review requirements**
   - If `review_required: true`, include `review_notes` explaining why.

5. **Update metadata**
   - Ensure `updated_utc` is a valid `YYYY-MM-DD` date and not future-dated.

## Safety reminders

- **GREEN** = allowed to merge automatically.
- **YELLOW** = requires manual review before merge.
- **RED** = do not collect or merge.

If anything is unclear, open an issue with the pipeline name and target ID.
