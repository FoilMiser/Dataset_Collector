# Adapting the chem pipeline for scientific knowledge graphs + literature navigation

This plan adapts the existing chemistry-focused dataset pipeline into a **KG + literature-navigation** pipeline whose training signals explicitly teach *how to find, cite, and ground facts* (via IDs, citations, and graph traversal), while staying conservative about licensing and privacy.

---

## What “KG + literature navigation” means in this pipeline

We want to produce **three families** of training artifacts:

1. **Open graph dumps** (triples/edges and minimal node attributes)
   - Example: DOI→DOI citation edges, Work→Concept edges, Work→Institution edges, Organization registries, etc.
2. **Graph query tasks** (deterministic supervision)
   - Example: “Given DOI X, list its referenced DOIs,” “Find a 2-hop path from concept A to concept B,” “Resolve this ROR ID to institution country.”
3. **Navigation episodes** (tool-use / retrieval behavior)
   - Example: “Given a claim + entity string, propose a search plan, retrieve candidate works, pick best evidence, output citations and a grounded answer.”

The key is to emphasize **grounded outputs** (IDs + evidence lists + provenance), not memorized prose.

---

## High-level pipeline changes (code + config)

### 1) New dataset family + paths
- Create a new family root: `/data/kg_nav`
- Keep the same structure as chem:
  - `_staging/`, `_manifests/`, `_queues/`, `_catalogs/`, `_logs/`
  - `pools/permissive`, `pools/copyleft`, `pools/quarantine`

### 2) Add download strategies required for KG sources
The current `download_worker.py` supports: `http`, `ftp`, `git`, `zenodo`, `dataverse`, `huggingface_datasets`.

For KG/literature sources you will want to **add**:
- **`s3_sync`**: `aws s3 sync` (public buckets, no-sign-request)  
  - Needed for: **OpenAlex** snapshots.
- **`aws_requester_pays`**: AWS requester-pays bucket fetch  
  - Needed for: **Crossref public data file** (bulk).
- **`torrent`**: (optional, policy-gated) Academic Torrents / magnet links  
  - Alternative for some Crossref bulk distributions.
- **`figshare`**: resolve Figshare article → download assets
  - Needed for: **OpenCitations** and **ORCID** (often hosted on Figshare).

Implementation pattern:
- Add a new handler in `STRATEGY_HANDLERS`.
- Resolve → download to staging → emit a manifest with checksums + file list.

### 3) Add KG-specific transforms (“build workers”)
Add one or more build workers similar to your `pmc_worker.py` concept, but for KGs:

**A. `kg_worker.py` (normalize + edge extraction)**
- Read dumps (OpenAlex JSONL, Wikidata JSON, OpenCitations CSV/NT).
- Emit:
  - `nodes.jsonl.gz` (minimal attributes)
  - `edges.jsonl.gz` (typed edges)
  - `provenance.jsonl.gz` (source file + row offsets + license/profile tags)

**B. `pii_scrub_worker.py` (ORCID + similar registries)**
- Default behavior: keep IDs and non-sensitive linkouts, drop:
  - email addresses
  - biographies / free-text fields
  - external URLs that may reveal personal pages (optional)
- Keep public-norm guidance: “no email outreach, no profiling.”

**C. `nav_episode_builder.py` (navigation episodes)**
- Consumes minimal graph slices (e.g., OpenAlex minimal + COCI).
- Produces supervised “episodes” with deterministic evidence.
- Output schema:
  - `prompt`: the user query / claim / task
  - `answer`: grounded response (short)
  - `evidence`: list of IDs (DOI/OpenAlex/Wikidata/QIDs/ROR/ORCID) + edge proofs
  - `metadata`: task_type, difficulty, hop_count, domains, etc.

### 4) Field schemas for computed-only extraction
Extend `field_schemas.yaml` with KG schemas (examples referenced in `targets_kg_nav.yaml`):
- `openalex_minimal_graph_v1.0.0`
- `crossref_minimal_graph_v1.0.0`
- optionally `datacite_minimal_graph_v1.0.0`

These schemas should **exclude**:
- abstracts
- full reference strings
- long free-text fields
…and prefer **IDs and structured fields**.

### 5) Licensing gates tuned for KG use
For this project, treat **CC0 dumps** as “GREEN” by default, and push anything that is:
- attribution-required,
- access-gated,
- mixed-license,
- or contains person data
into **YELLOW** with signoff + scrub.

---

## Dataset candidate list (Green / Yellow / Red)

Below is a practical, conservative split for *KG + literature navigation*.

### GREEN (auto-eligible if checksums + denylist pass)
- **OpenAlex snapshot (CC0)**: scholarly graph (works/authors/institutions/concepts/citations).
- **OpenCitations (CC0)**: open DOI→DOI citation edges (e.g., COCI).
- **ROR (CC0)**: organization registry (affiliations).
- **Wikidata (CC0)**: broad entity backbone; excellent for entity linking and cross-IDs.
- **Wikipedia link graph (if used only as link structure, not article text)**: treat carefully; text is CC BY-SA (copyleft), but link graph can be separated.

### YELLOW (requires signoff, extra transforms, or access steps)
- **Crossref bulk metadata**: bulk access has operational constraints (torrent / AWS requester-pays); must strip abstracts and other non-essential fields.
- **DataCite Public Data File (CC0)**: access requires requesting a download link; also contains links to external resources (don’t ingest linked content).
- **ORCID Public Data File (CC0)**: CC0 for the aggregated file, but includes person data; require PII scrub and strict use norms.
- **MeSH / controlled vocabularies**: generally open but terms/attribution norms apply; treat as copyleft-style obligations.
- **CommonPile scholarly slices**: mixed components and record-level licensing assumptions; require per-component screening and strict extraction.
- **OpenAIRE Graph** (often CC-BY + additional acceptable-use constraints): safe only after legal review; do not redistribute raw data.
- **S2ORC**: database-level licensing may be open, but underlying texts are complex; keep **metadata-only** if used at all.

### RED (reject by default for “legally cleared” training)
- Scopus, Web of Science, Dimensions, Lens.org (restricted/commercial).
- Semantic Scholar full corpus / proprietary distributions (license constraints).
- Any dataset that is “research-only,” “no commercial use,” “no redistribution,” or unclear provenance.

---

## What to store (to teach grounding) vs what to avoid

### Keep (preferred)
- Identifiers: DOI, OpenAlex IDs, ORCID iDs, ROR IDs, QIDs
- Typed edges: cites, authored_by, affiliated_with, has_concept, published_in
- Minimal numeric attributes: year, counts, type codes
- Controlled vocab terms (short strings) where license is clear

### Avoid (or keep only after explicit signoff)
- Abstracts, full text, long titles from uncertain sources
- Full reference strings that may embed copyrighted fragments
- Any private contact fields (emails, addresses)

---

## How the navigation episodes should work

Episode families (all deterministic/grounded):
1. **Citation tracing**: “Find 2-hop citation chain from DOI A to DOI B; output chain.”
2. **Entity resolution**: “Given org string → propose candidates → choose ROR ID; show evidence fields.”
3. **Concept grounding**: “Given concept term → map to OpenAlex concept ID + Wikidata QID (if link exists).”
4. **Evidence selection**: “Given claim about relationship X→Y → retrieve 3 candidate works → pick best evidence by graph heuristics.”
5. **Query-language tasks**: SPARQL/edge-list queries over Wikidata/OpenAlex-style structures.

Store every episode with:
- evidence objects with IDs and edge proofs,
- provenance (source dataset + version),
- grading hooks (exact-match on IDs / path).

---

## Practical first milestone

If you want a lean but powerful baseline:
- Download: **OpenAlex + OpenCitations COCI + ROR + Wikidata**
- Build: `openalex_minimal_graph`, `coci_edges`, `ror_nodes`, `wikidata_qid_links`
- Emit: first 1–5M navigation episodes (mixed difficulty), no free text.

---

## Operational notes

- KG snapshots are big (hundreds of GB). Use:
  - chunked gzip JSONL
  - sharding by ID prefix
  - streaming transforms (no full in-memory graphs)
- Keep “raw” sources in staging; emit only normalized minimal artifacts into pools.
- Maintain strict manifests for reproducibility (snapshot date, checksums, tool versions).

---

## Files produced in this draft

- `targets_kg_nav.yaml`: configuration describing the initial KG/lit-nav targets and derived builds.
- This plan (markdown): explains code additions and conservative licensing posture.
