Architecture
============

Dataset Collector uses a shared pipeline contract with domain-specific implementations. The
``dc`` CLI is the preferred entrypoint; legacy shell scripts exist for compatibility only.

Pipeline flow
-------------

1. Targets YAML defines sources and policy metadata.
2. ``dc`` selects a stage (classify, acquire, yellow_screen, merge, catalog).
3. Stage-specific workers produce queues, manifests, and catalogs.
4. Outputs land under the dataset root using a consistent directory contract.

Core modules
------------

Key shared components in ``collector_core``:

* ``pipeline_spec`` and ``pipeline_specs_registry`` define pipeline configuration and registry
  data for each domain.
* ``pipeline_factory`` resolves pipeline drivers dynamically by domain.
* ``yellow_review_helpers`` centralizes queue loading, summary, and review plan helpers.
* ``pipeline_cli`` provides a unified CLI for catalog and review helpers.

Outputs and contracts
---------------------

Outputs from each stage follow the contracts in ``docs/output_contract.md`` and
``docs/pipeline_runtime_contract.md``. These contracts standardize queue naming, stage outputs,
and merge artifacts across pipelines.
