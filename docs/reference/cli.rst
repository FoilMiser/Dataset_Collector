CLI Reference
=============

Dataset Collector provides the ``dc`` command (and legacy aliases) for running pipeline stages
and helpers.

Primary commands
----------------

``dc --list-pipelines``
  Lists all registered pipeline domains.

``dc pipeline <domain> -- <args>``
  Runs the full pipeline driver for a domain (classification + queue creation).

``dc run --pipeline <domain> --stage <stage> -- <args>``
  Executes a single stage. Valid stages are ``acquire``, ``merge``, and ``yellow_screen``.

``dc review-queue --pipeline <domain> -- <args>``
  Runs the YELLOW review queue helper.

``dc catalog-builder --pipeline <domain> -- <args>``
  Generates catalog artifacts from merged data.

Legacy aliases
--------------

``dc-review`` and ``dc-catalog`` remain available as entrypoints, but ``dc`` is preferred.

Examples
--------

.. code-block:: bash

   dc --list-pipelines
   dc pipeline math -- --targets pipelines/targets/targets_math.yaml --stage classify
   dc run --pipeline math --stage merge --dataset-root /data/math -- \
       --targets pipelines/targets/targets_math.yaml --execute
