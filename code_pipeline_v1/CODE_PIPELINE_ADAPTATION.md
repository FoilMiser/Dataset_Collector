# Code dataset pipeline adaptation plan (from the chem corpus pipeline)

This document explains how to adapt the existing chemistry-focused dataset pipeline into a **code-specialized, ethics-first, legally cleared** corpus builder.

---

## What changes when the domain is “code”?

Compared to chemistry papers/records, code corpora add three big risks:

1) **Licensing is per-file / per-repo, and enforcement is granular.**  
Even “open source” code frequently carries obligations (attribution, NOTICE files, patent clauses), and large corpora contain *many* licenses.

2) **Security & privacy risks are higher.**  
Public repositories can contain secrets, API keys, private URLs, credentials, emails, or other sensitive data.

3) **Duplication is endemic.**  
Forks, vendored dependencies, generated files, and minified bundles explode redundancy.

---

## Minimum “code-specific” pipeline gates to add

Keep your existing gates (SPDX bucket inference, restriction phrase scan, denylist checks), and add:

### Gate A — Repo license & provenance snapshot (must-have)
For any repo-based source (git/hf), capture:
- repository URL + commit hash
- path
- detected license(s) (SPDX where possible)
- LICENSE / NOTICE text hash
- provenance metadata (source dataset record ids)

### Gate B — Secrets & credentials scanning (must-have)
Run a scanner over raw text and code:
- high-entropy token patterns
- common credential regexes (AWS keys, GitHub tokens, JWTs, etc.)
- private key blocks

Failing records go to quarantine with redaction reports.

### Gate C — Vendored/build output stripping (must-have)
Drop:
- `node_modules/`, `vendor/`, `third_party/`, `dist/`, `build/`
- minified files (`*.min.js`, etc.)
- large binaries and assets

### Gate D — Code-aware chunking (recommended)
Add a code chunker that:
- prefers **AST chunking** (tree-sitter / language parsers)
- falls back to file chunking
- keeps signature + docstring/comment context where relevant

### Gate E — Attribution bundle emission (recommended)
For any source with attribution obligations, emit:
- a machine-readable attribution index (repo/license/notice pointers)
- build manifests that can reconstruct the provenance slice used for training

---

## New worker(s) to add (paralleling `pmc_worker.py` in the chem pipeline)

You can keep your driver + download worker largely intact and add:

- `code_worker.py`
  - normalizes files
  - extracts code/text records
  - runs path/language filters
  - emits JSONL in your schema (see below)

- `secret_scanner.py`
  - record-level scan + redaction
  - produces `*_secrets_report.json`

- `code_chunker.py`
  - AST chunking + fallback chunking
  - emits `chunks.jsonl.gz`

- `yellow_scrubber_code.py`
  - moves YELLOW sources toward GREEN by:
    - joining per-record license metadata
    - removing noncompliant licenses
    - producing an “allowlist manifest” you can audit

---

## Field schema recommendations for code records

Add/extend a code record schema in `field_schemas.yaml` (or a new `field_schemas_code.yaml`):

**Required**
- `text` or `code` (payload)
- `language` (normalized)
- `source` (dataset id)
- `source_url`
- `record_id`
- `license_spdx` (or `license_raw`)
- `provenance` (repo/path/commit or dataset provenance)

**Optional / helpful**
- `docstring` / `comments`
- `imports` (parsed)
- `symbols` (function/class names)
- `is_generated` heuristics
- `secrets_redacted: bool`

---

## Dataset candidate list

Below is a pragmatic “GREEN / YELLOW / RED” triage list.  
(**GREEN** = programmatic ingest OK; **YELLOW** = ingest only with record-level license + extra review; **RED** = do not ingest.)

### GREEN (good to ingest automatically)

- **OpenAI HumanEval** (MIT) — small, clean evaluation set  
- **Python PEPs** (public domain / CC0) — language/spec + examples  
- **EvalPlus MBPP+** (Apache-2.0 tag on HF) — curated beginner problems  
- **Your internal synthetic code datasets** — best for clean training + automatic grading
- **Other tiny, clearly-licensed benchmarks (when confirmed):**
  - small MIT/Apache licensed eval sets hosted on HF with explicit license tags

### YELLOW (usable, but needs additional controls)

**Large code corpora / mixed licenses**
- **BigCode “The Stack”** — permissively licensed but still mixed obligations; keep provenance & attribution  
- **BigCode “The Stack v2”** — additional bulk download terms/process constraints  
- **Common Pile code sources** (e.g., GitHub Archive, StackV2 filtered slices) — generally license-filtered, but still treat as record-level with provenance  
- **CodeSearchNet** — mixed repo licenses; example-wise license join may be needed  
- **StarCoderData / similar GitHub-derived corpora** — verify exact filters, license handling, and removal mechanisms before ingest  
- **Open-source “packaging / specs / RFCs”** — only after confirming license on each corpus

**Benchmarks with special agreements / copyleft**
- **CodeXGLUE** — C-UDA (computational use agreement)  
- **DS-1000** — CC-BY-SA (share-alike) and derived from StackOverflow  
- **Stack Exchange dumps / StackOverflow-derived datasets** — CC-BY-SA plus evolving access/terms controversies; only ingest with a clear compliance story

### RED (avoid)

- **Scraping GitHub at scale yourself** (license/ToS and removal compliance risk)
- **LeetCode / HackerRank / Codewars / Codeforces solution dumps** (ToS + IP risk)
- **“Pastebin” / “gist” megadumps** (privacy + license risk)
- **Proprietary bug tracker exports** (privacy / contractual risk)
- **Random “instruction-tuned code” datasets without a clear provenance + license trail**
- **Anything with a “noai/no-training” clause or opt-out flags** unless you can enforce it robustly

---

## Practical implementation steps (suggested order)

1) **Stand up the new `targets_code.yaml`**
   - Start with small GREEN sources (HumanEval, PEPs, MBPP+).
2) **Implement `code_worker.py`**
   - language detection + file filters + JSONL output.
3) **Implement secrets scanning + vendored stripping**
4) **Implement record-level license join**
   - if the dataset doesn’t ship per-record licensing, treat it as YELLOW until it does.
5) **Only then turn on huge sources**
   - Common Pile code slices, The Stack, etc.
6) **Add removals + audit tooling**
   - keep a “removals ledger” and regenerate manifests reproducibly.

---

## Notes about very large HF datasets

Your current `huggingface_datasets` download handler uses `datasets.load_dataset(...)` directly. For multi-terabyte code sources, you’ll likely need:
- streaming mode
- shard-by-shard export
- incremental checkpointing

This is the main engineering gap to close before enabling The Stack / Stack v2 / large Common Pile slices.

