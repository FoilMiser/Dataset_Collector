Configuration Reference
=======================

Configuration is organized under ``configs/`` and includes pipeline maps, shared data, and
profile overrides.

Pipeline registry
-----------------

``configs/pipelines.yaml`` registers each pipeline domain with its targets path, routing keys,
and optional knobs (custom workers, yellow screen modules, and acquisition hooks).

Shared configuration
--------------------

The ``configs/common`` directory contains shared data inputs:

* ``denylist.yaml``
* ``field_schemas.yaml``
* ``license_map.yaml``
* ``resolvers.yaml``

Profiles
--------

Profiles under ``configs/profiles`` allow environment-specific overrides.
Select a profile with ``DC_PROFILE`` (``development`` and ``production`` are provided).
