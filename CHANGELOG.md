# Changelog

All notable changes to Dataset Collector v2 will be documented in this file.

## [2.0.1] - 2025-12-30
### Added
- Repository governance files (`.gitignore`, `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`).
- `docs/PIPELINE_V2_REWORK_PLAN.md` for the documented stage flow.
- `tools/touch_updated_utc.py` to normalize `updated_utc` metadata.

### Changed
- Root `requirements.in` and `requirements.constraints.txt` now include shared pipeline dependencies.
- Normalized `updated_utc` values to `YYYY-MM-DD` and removed future dates.
- Validator warnings for future or non-date `updated_utc` values.
