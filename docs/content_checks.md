# Content Check Implementations

This document describes the configuration and output semantics for the lightweight
content check implementations shipped in `collector_core.checks.implementations`.
Each implementation exposes:

- `check_name`: the registry name for the check.
- `CheckResult`: a dataclass with `check`, `status`, and `details` fields.
- `check(record, config)`: the function that evaluates a record and returns a
  `CheckResult`.

## Common Output Semantics

All checks return a `CheckResult` with:

- `check`: the `check_name` for the module.
- `status`: one of `ok`, `warn`, `fail`, or `skip`.
- `details`: a dictionary with check-specific metadata.

## distribution_statement

**Purpose:** Validate that a record carries a distribution statement and that it
matches policy.

**Configuration:**

- `statement_field` (string, default `distribution_statement`): Record field to inspect.
- `allowed_statements` (list of strings): Approved statement values. When omitted
  the check returns `ok` for any present statement.

**Output details:**

- `statement`: The normalized statement value when present.
- `allowed_statements`: The configured list when validation occurs.
- `reason`: `missing_statement` or `allowed_statements_not_configured` when applicable.

**Status rules:**

- `warn` if the statement field is missing.
- `ok` if present and allowed (or no `allowed_statements` configured).
- `fail` if present but not in `allowed_statements`.

## language_detect

**Purpose:** Validate that a record language matches an approved list.

**Configuration:**

- `language_field` (string, default `language`): Record field containing language tags.
- `allowed_languages` (list of strings): Allowed language tags (case-insensitive).

**Output details:**

- `languages`: Normalized languages extracted from the record.
- `allowed_languages`: The configured allow list.
- `reason`: `missing_language` or `allowed_languages_not_configured` when applicable.

**Status rules:**

- `warn` if the language field is missing.
- `ok` if the language is allowed (or no allow list configured).
- `fail` if the language does not match the allow list.

## license_validate

**Purpose:** Validate that a record license matches an approved list.

**Configuration:**

- `license_field` (string, default `license`): Record field containing license values.
- `allowed_licenses` (list of strings): Approved license identifiers.

**Output details:**

- `licenses`: Normalized license identifiers extracted from the record.
- `allowed_licenses`: The configured allow list.
- `reason`: `missing_license` or `allowed_licenses_not_configured` when applicable.

**Status rules:**

- `warn` if the license field is missing.
- `ok` if any license is allowed (or no allow list configured).
- `fail` if no license matches the allow list.

## schema_validate

**Purpose:** Validate the presence of required fields in a record.

**Configuration:**

- `required_fields` (list of strings): Field names that must be present.

**Output details:**

- `required_fields`: The configured list.
- `missing_fields`: Field names that were missing or null.
- `reason`: `required_fields_not_configured` when applicable.

**Status rules:**

- `skip` if no `required_fields` are configured.
- `ok` if all required fields are present.
- `fail` if any required field is missing.

## toxicity_scan

**Purpose:** Scan text for configured toxicity terms.

**Configuration:**

- `text_field` (string, default `text`): Record field containing text to scan.
- `toxicity_terms` (list of strings): Terms to search for (case-insensitive).

**Output details:**

- `match_count`: Total number of term matches.
- `matches`: Per-term match counts.
- `reason`: `toxicity_terms_not_configured` or `missing_text` when applicable.

**Status rules:**

- `skip` if no `toxicity_terms` are configured.
- `warn` if the text field is missing.
- `warn` if any matches are found.
- `ok` if no matches are found.
