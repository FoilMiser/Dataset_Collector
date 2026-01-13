Production Deployment
=====================

This guide outlines the recommended production deployment workflow for Dataset Collector.

Profiles and environment variables
----------------------------------

* ``DC_PROFILE`` selects the configuration profile (for example, ``production``).
* ``DATASET_ROOT`` or ``DATASET_COLLECTOR_ROOT`` sets the base dataset output directory.

Configuring a production run
----------------------------

1. Set environment variables for dataset roots and profile selection.
2. Use targets files that are approved for production.
3. Ensure credentials for downstream storage (S3, etc.) are set via environment variables.

Example execution:

.. code-block:: bash

   export DC_PROFILE=production
   export DATASET_ROOT=/data/production

   dc pipeline biology -- --targets pipelines/targets/targets_biology.yaml --stage classify
   dc run --pipeline biology --stage acquire --dataset-root /data/production -- \
       --queue /data/production/_queues/green_pipeline.jsonl --bucket green --execute

Monitoring and troubleshooting
------------------------------

Review runtime logs under the dataset root and follow the guidance in
``docs/troubleshooting.md`` if queues or catalogs fail to materialize.
