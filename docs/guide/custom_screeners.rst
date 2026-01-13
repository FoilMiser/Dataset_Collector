Custom Screeners
================

Yellow screening allows domain-specific review and filtering logic for YELLOW bucket items.
To add a custom screener:

1. Implement a screener module in your pipeline directory or shared tooling.
2. Register the module name in ``configs/pipelines.yaml`` under ``knobs.yellow_screen_module``.
3. Ensure the module exposes the entrypoints expected by ``collector_core.yellow_screen``.

Example configuration snippet:

.. code-block:: yaml

   pipelines:
     chem:
       pipeline_id: chem_pipeline_v2
       knobs:
         yellow_screen_module: yellow_screen_chem

You can test your screener by running the yellow screening stage on a YELLOW queue:

.. code-block:: bash

   dc run --pipeline chem --stage yellow_screen --dataset-root /data/chem -- \
       --queue /data/chem/_queues/yellow_pipeline.jsonl \
       --targets pipelines/targets/targets_chem.yaml \
       --execute
