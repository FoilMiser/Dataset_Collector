# Regulation & Compliance adaptation plan (v0.1)

This document describes how to adapt the existing chemistry-focused ethical dataset collection pipeline
to specialize in **Regulation & Compliance** corpora for AI training.

## Why Reg/Compliance is different
Reg/Compliance sources are:
- **Versioned and time-bound** (effective dates, amendments, superseded sections)
- **Highly structured and cross-referenced** (citations and hierarchical sections)
- **Prone to embedded third-party material** (exhibits, reprints, incorporation by reference)
- **Sometimes high PII risk** (public comments, enforcement records, sanctions lists)

Therefore the adaptation focuses on:
1) strict license gating,
2) version-aware metadata,
3) section-preserving chunking,
4) third-party-content detection,
5) PII removal and policy controls.

---

## 1) New domain schema (metadata contract)
Add the following fields (JSONL recommended) to every document:

### Provenance & identity
- `doc_id` (stable hash)
- `source_id` (e.g., govinfo_cfr_bulk)
- `retrieved_at` (timestamp)
- `canonical_url`
- `jurisdiction` (US-FED / EU / UK / CA / AU / US-STATE-XX)
- `authority` (agency/court/parliament/etc.)
- `doc_type` (statute, regulation, guidance, enforcement, case_law, audit, docket, comment)

### Versioning & dates
- `published_date`
- `effective_date`
- `last_modified_date` (if known)
- `supersedes` / `superseded_by` (IDs or citations)
- `is_consolidated_version` (bool)
- `snapshot_date` (for “current view” sources like eCFR)

### Citation & hierarchy
- `citations`: list of normalized citations (USC, CFR, CELEX, SI numbers, docket IDs)
- `section_path`: e.g. ["Title 45", "Part 164", "Subpart E", "§ 164.502", "(a)", "(1)", "(ii)"]
- `heading_path`: normalized heading hierarchy

### Rights & policy
- `license_detected` + `license_confidence`
- `attribution_required` + `attribution_text` (store full required attribution bundle)
- `third_party_flags`: { `incorporation_by_reference`, `reprint`, `exhibit`, `copyright_notice`, ... }
- `pii_flags`: { `email`, `phone`, `address`, `signature`, ... }
- `policy_decision`: keep / drop / keep_redacted / quarantine_for_review

---

## 2) Recommended workers (connectors)
Implement these similarly to the chem pipeline’s download workers:

### A) govinfo_bulk_worker
- Input: dataset ID + bulkdata endpoint
- Output: parsed XML/PDF text + metadata
- Special: preserve section numbering; treat PDFs as last resort when XML exists.

### B) federalregister_api_worker
- Use the FederalRegister.gov API for incremental sync:
  - fetch documents by date range
  - store rich metadata: agencies, CFR references, docket IDs
- Then pull the authoritative text from govinfo where possible.

### C) regulationsgov_worker (YELLOW)
- Focus on agency-authored supporting materials first.
- Default policy: **exclude public comments** or ingest only after aggressive PII removal.
- Disable attachments by default; attachments often contain third-party copyrighted content.

### D) eurlex_worker / uk_legislation_worker / canada_open_gov_worker / australia_legislation_worker
- Implement per-source attribution and license detection
- Normalize identifiers (CELEX, SI, etc.)
- Preserve consolidated vs amended versions

### E) nist_worker / gao_worker / oig_worker
- Prefer HTML or accessible text
- Chunk by headings and control identifiers
- Maintain report numbers, release dates, and agencies

---

## 3) Chunking: section-preserving split strategy
Reg/Compliance training benefits from a “legal-structure aware” chunker:

### Goals
- Keep citations and definitions with their parent section
- Avoid splitting inside numbered lists (a)(1)(ii) / (A) / (i)
- Maintain a stable `chunk_id` derived from `doc_id + section_path + chunk_index`

### Suggested chunking rules
- Max tokens: configurable (e.g., 1,500–2,500 tokens)
- Split boundaries:
  1) top-level headings / sections
  2) paragraphs
  3) list items
- Add chunk metadata:
  - `chunk_section_path`
  - `chunk_heading_path`
  - `chunk_citations` (subset)
  - `chunk_is_definitions` (bool heuristic)

---

## 4) Yellow scrubbers (Reg/Compliance specific)

### A) Third-party content detector (critical)
Implement `third_party_guard.py`:
- Flag phrases:
  - “incorporation by reference”
  - “reprinted with permission”
  - “copyright”
  - “exhibit”, “appendix”, “annex”
  - “standard(s) by ISO/ASTM/NFPA/IEEE”
- Flag PDF attachments and images as likely risky unless the license is explicit.
- Default policy:
  - **drop flagged sections**
  - or **quarantine for review** with extracted context.

### B) PII scrubber
Implement `pii_scrubber.py` tuned for:
- emails, phones, addresses, signatures
- comment threads and docket submissions
- enforcement datasets with natural person names
Policy:
- remove / hash PII fields (configurable)
- keep organization names unless user policy says otherwise

### C) “Derivative risk” classifier
Implement a simple scoring:
- source category (gov vs public web)
- presence of copyright notices
- attachment ratio
- “reprint” / “IBR” flags
Then route to:
- keep
- keep_redacted
- quarantine
- drop

---

## 5) Quality filters & normalization
- Normalize citations (USC/CFR/CELEX) for retrieval tasks
- Deduplicate by:
  - canonical ID (FR Doc No, CFR cite, CELEX)
  - near-duplicate text hash (for consolidated versions)
- Maintain “authority graph”:
  - statute -> regulation -> guidance -> enforcement -> case law

---

## 6) Evaluation sets (recommended)
Build small, clean eval sets alongside the corpus:
- “What does §X require?” question-answering from CFR sections
- Definition extraction tasks (glossary/definitions sections)
- Citation resolution tasks (map “see 45 CFR 164.502” -> correct chunk)
- Compliance scenario classification:
  - HIPAA, AML, OSHA, EPA domain tags

Keep eval sets fully provenance-traceable and license-validated.

---

## 7) Suggested file additions
- `targets_regcomp_v0.1.yaml` (this new targets file)
- `workers/`:
  - `govinfo_bulk_worker.py`
  - `federalregister_api_worker.py`
  - `regulationsgov_worker.py`
  - `eurlex_worker.py`
  - `uk_legislation_worker.py`
  - `canada_open_gov_worker.py`
  - `australia_legislation_worker.py`
  - `nist_worker.py`, `gao_worker.py`, `oig_worker.py`
- `scrubbers/`:
  - `third_party_guard.py`
  - `pii_scrubber.py`
  - `derivative_risk_router.py`
- `normalizers/`:
  - `citation_normalizer.py`
  - `section_hierarchy_parser.py`

---

## 8) TODO (for v0.2)
1) Implement `third_party_guard.py` and integrate into yellow-scrub stage
2) Implement `citation_normalizer.py` (USC/CFR + CELEX at minimum)
3) Add `effective_date` and `superseded_by` tracking for CFR + EU consolidated docs
4) Add docket/comment policy switches:
   - include/exclude comments
   - include/exclude attachments
   - PII redaction level
5) Add per-source attribution bundle storage and validation
6) Add incremental sync for Federal Register and eCFR snapshots
7) Create a “golden set” of 100–500 documents for regression testing the pipeline
