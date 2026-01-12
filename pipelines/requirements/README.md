# Pipeline Requirements

This directory contains centralized domain requirements for all pipelines.
This is the canonical location for pipeline-specific dependencies as of v3.0.

## Issue 2.2 (v3.0)

Per-domain requirements are now centralized here instead of in individual
`*_pipeline_v2/requirements.txt` files. The legacy location still works
for backwards compatibility but new installations should use this location.

## Installation

Install the core package first, then add domain-specific requirements:

```bash
# Install base package
pip install -r requirements.constraints.txt
pip install -e .

# Install domain requirements (choose one or more)
pip install -r pipelines/requirements/math.txt
pip install -r pipelines/requirements/physics.txt
pip install -r pipelines/requirements/chem.txt
# etc.
```

## File Structure

- `base.txt` - Common optional dependencies (token counting, deduplication, PDF)
- `scientific.txt` - Scientific domain base (includes base.txt + sympy)
- `<domain>.txt` - Domain-specific requirements

## Domain Mapping

| Domain | Requirements File | Notes |
|--------|------------------|-------|
| 3d_modeling | 3d_modeling.txt | Mesh processing |
| agri_circular | agri_circular.txt | Agriculture |
| biology | biology.txt | Bioinformatics |
| chem | chem.txt | Chemistry |
| code | code.txt | Source code |
| cyber | cyber.txt | Cybersecurity |
| earth | earth.txt | Earth science |
| econ_stats_decision_adaptation | econ.txt | Economics/Stats |
| engineering | engineering.txt | Engineering |
| fixture | fixture.txt | Testing |
| kg_nav | kg_nav.txt | Knowledge graphs |
| logic | logic.txt | Logic/reasoning |
| materials_science | materials_science.txt | Materials science |
| math | math.txt | Mathematics |
| metrology | metrology.txt | Metrology |
| nlp | nlp.txt | NLP |
| physics | physics.txt | Physics |
| regcomp | regcomp.txt | Regulatory |
| safety_incident | safety_incident.txt | Safety incidents |

## Legacy Location

The `*_pipeline_v2/requirements.txt` files are deprecated and will be
removed in v4.0. They currently include the base constraints for
backwards compatibility.
