Content Checks
==============

Dataset Collector includes content safety checks to enforce denylist and policy compliance.
These checks are configured through schema-backed YAML or JSON inputs.

Key inputs
----------

* ``schemas/denylist.schema.json``: defines denylist structures for URLs/domains.
* ``schemas/field_schemas.schema.json``: defines required content metadata fields.

Workflow
--------

1. Update the denylist or field schemas as needed.
2. Run the preflight validation to ensure target metadata complies.
3. Execute the pipeline stage that invokes content checks (classification and merge).

When adding new denylist entries, document the rationale in ``docs/denylist_rationale.md``
and keep the associated schema up to date.
