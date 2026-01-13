Pipelines Guide
===============

Pipeline structure
------------------

Each domain pipeline lives in its own ``*_pipeline_v2`` directory and relies on shared logic in
``collector_core``. Pipeline metadata is registered in ``configs/pipelines.yaml`` and includes:

* ``pipeline_id`` and ``targets_path``
* routing defaults and routing keys
* optional knobs (custom workers, domain prefix, yellow screening module)

Targets and queues
------------------

Targets are defined in ``pipelines/targets/targets_<domain>.yaml``. Classification emits queue
files under ``_queues/`` (GREEN/YELLOW/RED). The queue format is standardized so stages can
consume it directly.

Running a pipeline
------------------

Use ``dc pipeline`` for classification and ``dc run`` for individual stages.

.. code-block:: bash

   dc pipeline physics -- --targets pipelines/targets/targets_physics.yaml --stage classify
   dc run --pipeline physics --stage acquire --dataset-root /data/physics -- \
       --queue /data/physics/_queues/green_pipeline.jsonl --bucket green --execute

When integrating a new domain, add a targets file, register it in
``configs/pipelines.yaml``, and ensure any custom workers are listed under the ``knobs``
section.
