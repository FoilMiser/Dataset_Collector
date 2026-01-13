Schema Reference
================

Dataset Collector ships JSON Schema definitions under ``schemas/`` for validating targets,
licenses, denylist entries, and pipeline maps.

Available schemas
-----------------

* ``schemas/targets.schema.json``: validates targets YAML files.
* ``schemas/pipeline_map.schema.json``: validates pipeline configuration maps.
* ``schemas/denylist.schema.json``: validates denylist entries used by content checks.
* ``schemas/license_map.schema.json``: validates license profile mappings.
* ``schemas/field_schemas.schema.json``: validates metadata field definitions.

Validation workflow
-------------------

Use the preflight tooling or your preferred JSON Schema validator to check files before
running pipelines. For example:

.. code-block:: bash

   python -m tools.preflight --pipelines math_pipeline_v2
