"""Subprocess execution utilities.

This module provides the canonical implementation of shell command execution
utilities, used across acquisition strategies and tools.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from collector_core.stability import stable_api


@stable_api
def run_cmd(cmd: list[str], cwd: Path | None = None) -> str:
    """Run a shell command and return its stdout output.

    This function executes a command with subprocess.run, capturing stdout
    and stderr combined. It raises an exception if the command fails.

    Args:
        cmd: Command and arguments as a list of strings.
        cwd: Optional working directory for the command.

    Returns:
        The stdout output of the command as a string (decoded as UTF-8).

    Raises:
        subprocess.CalledProcessError: If the command exits with non-zero status.

    Example:
        >>> run_cmd(["git", "status"])
        'On branch main\\nnothing to commit, working tree clean\\n'

        >>> run_cmd(["ls", "-la"], cwd=Path("/tmp"))
        'total 0\\ndrwxrwxrwt  ...\\n'
    """
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return p.stdout.decode("utf-8", errors="ignore")
