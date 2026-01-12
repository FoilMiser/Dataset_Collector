from __future__ import annotations

"""Yellow screen entrypoint using the shared base orchestration."""

from collector_core.stability import stable_api
from collector_core.yellow.base import run_yellow_screen
from collector_core.yellow.domains import safety
from collector_core.yellow_screen_common import YellowRootDefaults


@stable_api
def main(*, defaults: YellowRootDefaults) -> None:
    run_yellow_screen(defaults=defaults, domain=safety)


__all__ = ["main"]
