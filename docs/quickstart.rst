Quickstart
==========

This quickstart introduces the core workflow for running Dataset Collector pipelines, including
installation, pipeline execution, and basic configuration.

Installation
------------

Install the base dependencies and the local package in editable mode:

.. code-block:: bash

   pip install -r requirements.constraints.txt
   pip install -e .

To enable optional extras, include the relevant extras bundle:

.. code-block:: bash

   # Full developer + observability + async stack
   pip install -e ".[all]"

Running a pipeline
------------------

The ``dc`` CLI orchestrates pipeline stages (classify, acquire, yellow_screen, merge, catalog).
Here is a typical workflow for a domain like ``math``:

.. code-block:: bash

   # List registered pipelines
   dc --list-pipelines

   # Classify targets into queues
   dc pipeline math -- --targets pipelines/targets/targets_math.yaml --stage classify

   # Execute acquisition for GREEN bucket
   dc run --pipeline math --stage acquire --dataset-root /data/math -- \
       --queue /data/math/_queues/green_pipeline.jsonl \
       --bucket green \
       --execute

   # Yellow screening
   dc run --pipeline math --stage yellow_screen --dataset-root /data/math -- \
       --queue /data/math/_queues/yellow_pipeline.jsonl \
       --targets pipelines/targets/targets_math.yaml \
       --execute

   # Merge and catalog
   dc run --pipeline math --stage merge --dataset-root /data/math -- \
       --targets pipelines/targets/targets_math.yaml \
       --execute
   dc catalog-builder --pipeline math -- \
       --targets pipelines/targets/targets_math.yaml \
       --output /data/math/_catalogs/catalog.json

Environment configuration
-------------------------

Default dataset roots can be set via ``DATASET_ROOT`` or ``DATASET_COLLECTOR_ROOT``. Profiles
(e.g. ``development``, ``production``) can be selected with ``DC_PROFILE``.
