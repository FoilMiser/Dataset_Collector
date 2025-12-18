# Adapting the Chemistry Corpus Pipeline to Cybersecurity + Industrial Threat Modeling

This document explains how to repurpose the existing *chem_pipeline_v1* package into an **ethical, legally-cleared cybersecurity dataset pipeline** focused on:

- **Defensive cybersecurity** (triage, remediation, secure architecture, governance, incident response)
- **Industrial/ICS threat modeling** (risk to OT/SCADA environments, safety impacts, resilience planning)
- **Threat modeling knowledge bases** (ATT&CK, D3FEND, MISP taxonomies/galaxies, etc.)
- **Vulnerability intelligence** (NVD feeds, KEV, Vulnrichment, advisories)

The accompanying dataset manifest is: **`targets_cyber.yaml`**.

---

## 1) Core Principles for “Ethical + Legally Cleared” Cyber Data

### Legal
1. Prefer **public domain** (US Government) and **permissive licenses** (CC0, CC‑BY, BSD/MIT/Apache).
2. Treat **Terms-of-Use-only** datasets (MITRE ATT&CK / CWE / CAPEC / CVE ToU) as **YELLOW** until you encode those ToU as a stable license mapping and confirm your intended usage.
3. Avoid scraping websites that prohibit automated download / redistribution.

### Safety / Dual-use
Cyber corpora uniquely risk “teaching the model to attack.” This pipeline should be *defender-centric*:
- **ALLOW**: vulnerability descriptions, mitigations, detection strategies, risk frameworks, secure coding guidance, incident response playbooks, postmortems.
- **QUARANTINE/REMOVE**: exploit payloads, weaponized proof-of-concepts, malware code, intrusion playbooks, credential dumps, “how to break in” step-by-step guidance.

---

## 2) What Changes from the Chemistry Pipeline?

The chem pipeline already has the right *skeleton*: manifests → download workers → basic license gating → (optional) scrubbers. For cyber, you’ll add **a few domain-specific processing and safety gates**.

### 2.1 New “content safety” gates (recommended additions)
The existing driver only *enforces* `restriction_phrase_scan` (license restrictions). Add enforcement for:

- **`secret_scan`**: detect and redact API keys, tokens, private keys, passwords.
- **`pii_scan`**: emails, phone numbers, SSNs, physical addresses; also decide what to do with IPs/domains (often present in CTI).
- **`dual_use_instruction_scan`**: route content that is meaningfully instructional for wrongdoing to quarantine.
- **`exploit_code_scan`**: quarantine exploit payload patterns / shellcode / weaponized PoCs / malware code.

Implementation tip: enforce these in **`pipeline_driver.py`** right after download and before emitting training JSONL, and/or in **`yellow_scrubber.py`** as a second pass.

### 2.2 Add cyber parsers / normalizers (new workers)
You’ll get best training signal if you normalize structured sources into consistent JSONL “documents.”

Recommended new workers:
- `stix_worker.py`: convert STIX bundles into:
  - (a) readable text summaries (entity → description → relationships), and
  - (b) a graph JSONL (edges/nodes) for downstream graph-aware training.
- `nvd_worker.py`: parse NVD CVE 2.0 JSON into normalized records:
  - `cve_id`, `published`, `last_modified`, `cvss`, `cwes`, `cpe_matches`, `description`, `references`, `mitigations` (if present).
- `advisory_worker.py`: normalize GitHub advisories (YAML) into:
  - package ecosystem, affected ranges, summary, severity, references.

Keep raw archives too for auditability.

### 2.3 Update `denylist.yaml` for cyber hazards
Add deny patterns for:
- secrets (AWS keys, GitHub tokens, private key blocks),
- credential artifacts (`password=`, `BEGIN PRIVATE KEY`, JWT-like strings),
- malware/exploit signatures (keep high precision; don’t nuke benign code),
- breach/dox content markers.

---

## 3) Targets Manifest

Use **`targets_cyber.yaml`** (provided) as the initial manifest.

### What’s already “GREEN”
- **NIST NVD feeds (CVE 2.0 + CPE + CPE Match)** — typically treated as US Government/public domain.
- **CISA KEV data (CC0)** and **CISA Vulnrichment (CC0)**.
- **MISP taxonomies/galaxies/objects** (generally CC0/BSD family; confirm in repo).
- **OASIS STIX/TAXII JSON schemas** (BSD‑3).
- **OpenSSF OSV schema** (Apache‑2.0).
- **GitHub Advisory Database** (CC‑BY‑4.0).

### What is “YELLOW”
- **MITRE ATT&CK / CTI / D3FEND**: excellent threat modeling data, but controlled by MITRE Terms of Use (non‑SPDX).
- **CVEProject/cvelistV5**: official CVE list; governed by CVE Program ToU.
- **Sigma rules**: custom license; plus rules can include attacker-ish intent. Safe with filtering and careful license mapping.
- **EPSS**: great risk signal but confirm redistribution permissions.
- **Mordor**: GPL‑3 telemetry dataset (copyleft).

### What is “RED / excluded”
- exploit PoC corpora, exploit frameworks, malware corpora, credential dumps, breach/leak collections.

---

## 4) Dataset Candidates (Long List)

The YAML includes “first tranche” sources. Here’s a longer candidate set to grow into.

### GREEN candidates (high confidence)
**US Government / CC0 / permissive**
- NIST NVD: CVE 2.0 feeds, CPE dictionary, CPE match (JSON 2.0).
- CISA KEV (CC0).
- CISA Vulnrichment (CC0) and related open CISA data repos.
- MISP Taxonomies / Galaxy / Objects (often CC0/BSD; verify).
- GitHub Advisory Database (CC‑BY‑4.0).
- OpenSSF OSV schema (Apache‑2.0).
- OASIS CTI JSON schemas (BSD‑3).

**Defensive community resources (license-check)**
- Zeek open docs/scripts repos
- OpenTelemetry security-related specs
- Security-focused public-domain audit reports (where applicable)

### YELLOW candidates (good, but needs license + dual-use screening)
- MITRE ATT&CK (Enterprise/Mobile/ICS) STIX
- MITRE D3FEND ontology
- MITRE CWE / CAPEC downloads (ToU)
- Official CVE list (ToU)
- FIRST EPSS (confirm redistribution terms)
- SigmaHQ rules (custom DRL license)
- OWASP materials (often CC BY-SA or similar; verify)
- Public incident postmortems and security blogs (copyright unless explicitly licensed)

### RED candidates (avoid for this pipeline)
- Exploit-DB, Metasploit exploit modules, offensive exploit PoC repos
- Malware binaries/source corpora (vx-underground, payload archives)
- Credential dumps / breach datasets / doxxing corpora
- “Pentest walkthrough” corpora that are instructional for compromise

---

## 5) Industrial / ICS Threat Modeling Additions

Industrial threat modeling should be **defender + safety** oriented.

Add (after license verification):
- ATT&CK for ICS (already covered via ATT&CK STIX targets).
- CISA ICS advisories and vendor bulletins (if bulk downloads are permitted).
- NIST ICS security guidance (SP 800-82 and related CSRC docs) — prefer official downloads, avoid scraping.
- Public-domain safety engineering incident reports that mention OT/ICS (pair with your existing safety/incident corpus work).

Avoid:
- proprietary standards like **IEC 62443** full texts (copyrighted).

---

## 6) CommonPile Integration (How to do it safely)

CommonPile is valuable as a broad base, but for cyber specialization you’ll want a **domain-relevance filter**:

1. Download a small slice of relevant components (USGPO, regulations, certain technical subsets).
2. Score documents with a lightweight keyword model:
   - positive terms: CVE, CWE, CAPEC, ATT&CK, patch, vuln, SBOM, SAST, SOC, EDR, SIEM, ICS, SCADA, PLC, Modbus, OPC UA, etc.
3. Keep only the top percentile (e.g., 5–20%) for “cyber relevance.”
4. Run the dual‑use / secret / PII gates before emitting training text.

In `targets_cyber.yaml`, CommonPile targets are included but **disabled** until you confirm the exact HF IDs/configs.

---

## 7) Concrete Code-Level To‑Do (Minimal Set)

### Must do to ship a safe v0
1. **Add secret scanning** in `yellow_scrubber.py` (fast regex-based pass).
2. **Add a dual-use quarantine gate** (high precision rules) in `yellow_scrubber.py`.
3. **Add STIX/NVD normalizers** (workers) so structured data becomes training-friendly text/jsonl.
4. **Update `license_map.yaml`**:
   - add normalization rules for MITRE ToU (ATT&CK/CWE/CAPEC) and CVE ToU to a LicenseRef (or keep UNKNOWN and force YELLOW signoff).
5. Turn on **manual signoff** for YELLOW sources (`require_yellow_signoff: true`) once you begin scaling.

### Nice-to-have
- Add `.tar.gz` extraction support (for NVD CPE archives) and parse those into JSONL.
- Add record-level provenance hashes and dataset-level summary cards.

---

## 8) Suggested Evaluation Tasks (Defender-centric)

To keep the model useful *without* training it to attack:
- “Given this CVE summary + environment, propose mitigations and detection signals.”
- “Map this incident narrative to likely ATT&CK techniques.”
- “Given a KEV entry, produce a patch priority + communications plan.”
- “Given an ICS asset inventory + constraints, propose segmentation and monitoring strategy.”
