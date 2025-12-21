# Adapting the Chemistry Dataset Pipeline to General Mathematics (v0.1 plan)

This document assumes the existing pipeline layout from `chem_pipeline_v1.zip`:
- `pipeline_driver.py` (classification + queue emission)
- `download_worker.py` (downloads GREEN targets)
- `yellow_scrubber.py` (transforms/reviews YELLOW targets)
- optional domain workers (e.g., `pmc_worker.py` in the chem variant)

The math adaptation keeps the same **ethics-first** structure (evidence → license mapping → GREEN/YELLOW/RED) but changes the “domain workers” and chunking rules.

---

## 1) What changes for mathematics

Chemistry was dominated by:
- structured databases (PubChem-like records),
- papers (PMC) with conventional prose, tables, and citations.

Mathematics needs **three** distinct content pipelines:

1) **Math narrative text**  
   Textbooks, lecture notes, encyclopedia-style articles.  
   Key difficulty: preserving equations/LaTeX, theorem-proof structure.

2) **Formal mathematics**  
   Lean/Coq/Isabelle/Agda/Metamath libraries.  
   Key difficulty: chunk by *definitions/lemmas/theorems* rather than “paragraphs”.

3) **Problem corpora**  
   Word problems and exercises.  
   Key difficulty: licensing (contest problems are often copyrighted) and answer-key formatting.

The chem pipeline already has the right “outer shell” (license evidence + denylist + queues). The math pipeline mostly needs **new ingestion workers** and **math-aware chunking**.

---

## 2) New target inventory file

Use the provided `targets_math_v0.1.yaml` (in this response) as the starting point.

Key conventions:
- **permissive pool**: Apache/MIT/BSD/CC-BY/CC0/PD-like sources.
- **copyleft pool**: CC-BY-SA and GPL/LGPL-style sources.
- **quarantine pool**: record-level licensing (mixed licenses inside one dataset) until filtered.

---

## 3) New/updated workers to add

### A) `hf_math_filter_worker.py` (new)
Purpose: take a downloaded Hugging Face dataset directory (`datasets.save_to_disk`) and emit **filtered JSONL**.

Why needed:
- Common Pile sources often include **mixed domains** (e.g., StackExchange includes many sites).
- Many datasets include **record-level license metadata**.

Core filters (implement as composable passes):
- `domain_filter_math_stackexchange`:
  - keep records where `metadata.url` contains:
    - `math.stackexchange.com`, `mathoverflow.net`, optionally `stats.stackexchange.com`.
- `domain_filter_arxiv_math`:
  - keep records where `metadata` indicates math categories (e.g., `math.*`).
- `domain_filter_math_textbooks`:
  - heuristic classifier: keep records mentioning math concepts + containing formulas/LaTeX.
- `record_level_filter`:
  - drop rows where license is NC/ND or missing.
  - route rows into pools based on row license (permissive vs copyleft).

Output schema (recommend):
- `record_id`
- `source_id` (target id)
- `source_url`
- `license_spdx`
- `license_url`
- `retrieved_at_utc`
- `text`
- `math_format` (`latex_inline`, `latex_block`, `unicode_math`, `plain`)
- `tags` (topic tags + difficulty heuristic)

### B) `formal_math_worker.py` (new)
Purpose: parse formal libraries and chunk by semantic unit.

Supported inputs (initial):
- Lean: `.lean`
- Coq: `.v`
- Isabelle: `.thy`
- Agda: `.agda`
- Metamath: `.mm`

Chunking rules (high-level):
- Chunk boundaries at:
  - definitions
  - theorem/lemma statements
  - proofs (optionally separate from statements)
- Emit:
  - `statement_text`
  - `proof_text` (optional, can be excluded for some training regimes)
  - `dependencies` (import lines, module path)

A pragmatic v0 implementation:
- regex/token-based chunker per language
- later upgrade to AST parsing where feasible.

### C) `pdf_math_worker.py` (new or extend existing PDF chunker)
Math PDFs often have:
- equation images,
- multi-column layouts,
- reference-heavy notation.

Minimal viable approach:
- extract text with a PDF text extractor
- preserve line breaks more aggressively than for chemistry
- keep math symbols (do **not** ASCII-fy)
- detect equation blocks using indentation + symbol density heuristics

Recommended extras:
- store original PDF page ranges per chunk for traceability
- store “equation_density” feature to help downstream filtering

---

## 4) License handling differences (math-specific)

### A) Share-alike (CC BY-SA)
Large, high-value math corpora are CC BY-SA (Wikipedia + StackExchange).  
That’s *not* automatically “bad,” but it means:
- keep attribution,
- understand downstream distribution obligations.

In the pipeline:
- route to **copyleft pool**
- always emit an attribution bundle.

### B) Record-level licensing is common in OER
OERCommons / LibreTexts / Pressbooks / DOAB: licensing varies by page/book.  
Treat as **YELLOW** until you:
- read per-record license metadata,
- drop disallowed licenses (NC/ND),
- route remaining records by license.

### C) “Open source repo license” vs “problem statement rights”
Some math datasets publish code under MIT/Apache but include *problem statements* that may be copyrighted (AMC/AIME/IMO, etc.).  
Treat those as **YELLOW** unless you can verify rights for the content itself.

---

## 5) Math-aware text normalization & dedupe

### Recommended normalization passes
- Unicode normalization (NFKC)
- preserve LaTeX tokens (do not strip backslashes)
- normalize whitespace but keep block math boundaries
- convert common Unicode math symbols to consistent forms (optional)

### Dedupe
Math duplicates can be disguised by:
- variable renaming
- formatting changes
- equivalent LaTeX spacing

Add “math-fingerprint” strategies:
- strip whitespace in LaTeX
- normalize variable names in short equations
- hash “theorem statement only” separately from full proof

---

## 6) Quality targets for “basic → advanced” math

A good curriculum mix:
- **basic**: grade-school arithmetic, fractions, geometry vocabulary (mostly synthetic + CC BY textbooks)
- **intermediate**: algebra/trig/precalc/calc narrative + exercises
- **advanced**: analysis, algebra, topology, category theory (formal libraries + arXiv CC subset)

---

## 7) Operational run steps (same as chem)

1) Classify + emit queues:
```bash
python pipeline_driver.py --targets targets_math_v0.1.yaml
```

2) Download GREEN targets:
```bash
python download_worker.py --targets targets_math_v0.1.yaml --queue /data/math/_queues/green_download.jsonl --execute
```

3) Process YELLOW targets:
- implement `hf_math_filter_worker.py`
- run it on quarantine pool outputs to:
  - filter disallowed licenses,
  - route records into permissive/copyleft pools,
  - emit final JSONL shards.

4) Build catalogs / training manifests (as in your pipeline roadmap).

---

## 8) Suggested new denylist entries (math)

Add terms you will almost certainly want to block at URL/publisher level:
- “All rights reserved”
- “NoAI”, “no training”, “no text and data mining”
- “NonCommercial”, “NoDerivatives”
- major paywalled publishers (Elsevier, Springer Nature, Wiley, etc.)
- MOOCs with restrictive ToS (Coursera, edX, etc.)

---

## 9) What to implement next (v0.2–v0.3)

1) `hf_math_filter_worker.py` (highest ROI; unlocks Common Pile math subsets)
2) `formal_math_worker.py` for Lean + Metamath (fastest win)
3) add `math_text_chunk_v1` schema to `field_schemas.yaml`
4) add “license router” step to split record-level sources into pools automatically
