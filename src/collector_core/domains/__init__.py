"""
collector_core/domains - Domain-specific logic for pipelines.

Issue 2.3 (v3.0): This directory contains domain-specific implementations
that were migrated from the *_pipeline_v2/ directories.

Structure:
    domains/
        <domain>/
            __init__.py
            # Domain-specific modules

Most pipelines use the standard collector_core modules. Domain-specific
subdirectories are created only when custom logic is needed.
"""
