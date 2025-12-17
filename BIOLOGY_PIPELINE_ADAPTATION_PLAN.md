# Biology corpus pipeline adaptation plan (v0.1 → v0.2)

This document describes how to adapt the existing chemistry-first pipeline into a **biology-first, ethics-first** pipeline for AI training.

---

## Goals

1. **Legal clarity first**
   - Only ingest content that is **public domain** or **open-licensed for commercial reuse** (e.g., CC0, CC BY, permissive OSS), *or* content that can be made safe via **record-level filtering**.
2. **Ethics clarity first**
   - Avoid **human-subject / patient-level** data by default.
   - Add automated detection and quarantine for **PII/PHI**.
3. **High-value biology coverage**
   - Mix structured biology knowledge (proteins, structures, ontologies, pathways, taxonomy) with open literature, OER textbooks, and public-domain government material.

---

## What stays the same from the chem pipeline

- **Targets-driven ingestion** (`targets.yaml`) + companion policy layer (`license_map.yaml`).
- **Evidence snapshotting** for every target: license pages / ToS pages stored in `/data/.../_manifests/<target_id>/`.
- **3-bucket workflow**
  - **GREEN**: ready to download and train
  - **YELLOW**: requires filtering / transformation / manual review
  - **RED**: rejected

---

## New biology-specific requirements

### 1) Human-subject hard line
Default policy:
- **RED**: clinical notes, patient-level datasets, controlled-access biobanks, any dataset governed by a DUA unless explicitly cleared.
- **YELLOW**: resources that *sometimes* include human-related fields (e.g., trial registries, some genomics metadata) until filters are proven.

### 2) PII/PHI scanning
Add a gate that runs after text extraction and before final acceptance:
- Email/phone/address patterns
- Names near dates-of-birth / MRN-like strings
- “This case report…” style narratives

Implementation note:
- The current code only scans **HTML/TXT/JSON** evidence blobs. Extend it to extract text from **PDF** evidence too (pdfminer / pypdf) so restriction scanning isn’t blind.

### 3) Biosecurity-aware exclusion (conservative)
A lightweight rule set:
- If a document is clearly oriented around **weaponization or misuse**, quarantine or exclude.
- Keep this conservative and auditable: do not “rewrite” risky content; just quarantine.

---

## New config artifacts

### A) `biology_targets.yaml`
- A biology-oriented version of your targets file.
- Uses `/data/bio/...` roots.
- Includes:
  - Structured “GREEN anchors” (PDB, UniProt Swiss-Prot, GO, Reactome, Wikidata)
  - Literature (PMC OA + CommonPile subsets)
  - Government (primarily via CommonPile USGPO and selected CDC sources)
  - Copyleft sources (Wikipedia) kept disabled by default
  - High-value but sensitive sources (GBIF/iNaturalist/human data) disabled until filters exist

### B) `bio_license_map.yaml`
- A biology-safe licensing policy layer:
  - Allows: CC0, CC BY, US public domain, OGL-UK-3.0 (open government)
  - Conditional: CC BY-SA, ODbL, copyleft OSS licenses
  - Denies: NC/ND variants and restrictive custom terms
- Adds extra restriction scan phrases for **controlled access** and **DUA** signals (conservative).

---

## Minimal code changes to run v0.1 safely

### 1) Rename / parameterize “chem” strings (cosmetic but helpful)
- `user_agent` already set per YAML; update the download worker header if you want.
- Update DRY RUN report title text to “BIOLOGY CORPUS PIPELINE”.

### 2) Add PDF-to-text for evidence scanning (important)
Current:
- `extract_text_for_scanning()` ignores PDFs.
Add:
- a small PDF extractor to convert `license_evidence.pdf` to text for restriction scanning.

### 3) Implement `pii_phi_scan` (YELLOW by default)
Where:
- after you have text chunks (PMC, CommonPile, etc.) or at least on metadata fields.
Behavior:
- hits -> quarantine or drop, depending on policy.

### 4) Implement record-level filtering for mixed-license biology sources
Needed for:
- GBIF / iNaturalist / mixed annotation corpora
Approach:
- Require a license field per record
- Keep only allowlist licenses (CC0/CC BY)
- Emit attribution metadata for retained records

---

## Roadmap

### v0.1 (this draft)
- Generate queues correctly from biology targets
- Download GREEN sources
- Quarantine / disable anything that is mixed-license or human-subject sensitive

### v0.2 (recommended next)
- Implement:
  - PDF evidence scanning
  - PII/PHI scan gate
  - Record-level license filter gate
  - Structured-only extraction for large reference DBs (drop free text)
- Add biology field schemas in `field_schemas.yaml`:
  - `uniprot_sprot_v1`
  - `pdb_mmcif_v1`
  - `go_obo_v1`
  - `reactome_v1`
  - `ncbi_gene_info_v1`

### v0.3+
- Add API-driven download strategies (GBIF downloads, iNaturalist exports, etc.)
- Add near-duplicate detection once the corpus grows
- Add split-aware partitioning to prevent leakage between train/eval from closely related records

---

## Safety checklist (operational)

Before you train:
- Confirm **all GREEN** sources have:
  - SPDX resolved to allowlist
  - No restriction-phrase hits
  - Evidence snapshots present
- Confirm **no human-subject** targets are enabled
- Confirm **copyleft** sources are isolated if enabled
- Confirm attribution bundle generation is turned on and stored with the run outputs

---

## Dataset candidate catalog (GREEN / YELLOW / RED)

This is a long, practical backlog you can pull from when expanding `biology_targets.yaml`.

### GREEN (generally safe when you keep evidence + attribution)

Structured biology / knowledge bases (usually CC0/CC-BY/permissive):
- wwPDB / RCSB PDB (structures)
- UniProtKB/Swiss-Prot (reviewed proteins)
- Gene Ontology (GO)
- Reactome pathways
- Wikidata dumps (CC0)
- OpenTree taxonomy (once github_release is implemented or a static artifact is used)
- Bgee (gene expression; verify license)
- STRING (network; verify license before enabling)
- UniRef clusters (verify license)

U.S. Government (generally public domain; still snapshot terms because portals sometimes include 3rd-party content):
- CDC public-domain materials and journals that explicitly permit reuse
- NCBI-hosted public-domain policy pages + bulk reference datasets *where provenance is clear*
- USGS biology/ecosystems publications (portal-level crawlers needed)
- NOAA fisheries/ecosystems datasets (dataset-by-dataset verification)

Open education / OER (often CC-BY, but require real exporters):
- OpenStax Biology / Anatomy & Physiology (CC-BY; exporter needed)
- Open textbook portals with explicit CC-BY/CC0 titles (record-level license capture)

CommonPile slices you’re likely to use as “already curated” sources (verify the exact dataset_id/config names):
- PMC OA
- peS2o
- Biodiversity Heritage Library
- USGPO (public domain)

### YELLOW (useful, but requires filtering, segregation, or extra review)

Mixed-license literature / web corpora:
- PMC OA (mixed; keep record-level)
- Wikipedia (CC BY-SA; keep in a copyleft pool if enabled)
- Open-access aggregators that mix CC-BY with custom terms

Mixed-provenance biology databases:
- NCBI Taxonomy / gene_info / gene2go (often public domain signals, but verify provenance and drop depositor free text)
- RefSeq / GenBank / SRA metadata (may include depositor text and human-related signals; treat conservatively)
- ENA/EBI exports (dataset-by-dataset licensing)
- Ensembl dumps (license verification required)

Biodiversity / observations (record-level licensing required):
- GBIF occurrence records (keep only CC0/CC-BY per record)
- iNaturalist observations (keep only CC0/CC-BY per record; location privacy concerns)
- eBird (license likely restrictive; often ends up RED)

Government portals outside the U.S. (often open-government licenses, but not public domain):
- UK (OGL-UK-3.0)
- Canada (Open Government Licence – Canada)
- EU Open Data portal (license varies; record-level)
- Australia (often CC-BY; record-level)

### RED (default exclude)

Human-subject / controlled-access / DUA-bound:
- EHR / clinical notes
- dbGaP controlled-access datasets
- EGA controlled-access datasets
- UK Biobank
- Any dataset requiring a DUA, IRB approval, or restricting redistribution/training

Restrictive licensing:
- CC-BY-NC*, CC-BY-ND*, custom “academic use only”, “no TDM”, “no ML training”, etc.

High-risk or questionable provenance:
- Scraped publisher paywalls / subscription journals
- “Free” mirrors of paywalled content

### More candidates to consider (expanded)

Additional **GREEN-ish** candidates (verify license page, then move to GREEN):
- BioModels (computational models; verify licensing per model)
- PDB chemical component dictionary (CCD) and ligands (often CC0/PD signals; verify)
- OpenAlex (paper metadata; license is open, but abstracts may be restricted—treat text as YELLOW)
- ORCID public data file (identifiers; verify terms)

Common **YELLOW** candidates (usually valuable, but frequently mixed provenance or tricky restrictions):
- OBO ontologies individually (Uberon, Cell Ontology, Disease Ontology, Sequence Ontology, ...): licenses vary; ingest per-ontology with explicit SPDX
- NCBI RefSeq and GenBank releases: drop depositor prose; avoid human-subject signals; keep structured fields
- PubMed abstracts: treat as YELLOW unless you can confirm reuse permissions for abstracts and enforce policy
- bioRxiv / medRxiv: terms and licensing vary per paper
- Protein family resources (InterPro, Pfam): verify license; some have redistribution constraints
- Pathway databases beyond Reactome (WikiPathways is often CC0/CC-BY; verify)

Common **RED** candidates (default reject unless you obtain explicit written permission / special agreement):
- KEGG (typically restrictive)
- OMIM
- UMLS / SNOMED CT
- Any “academic use only” dataset
- Any patient-level/clinical narrative corpus
