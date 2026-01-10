"""
NLP pipeline helper for YELLOW bucket planning and manual review prep.

This is a thin wrapper around collector_core.yellow_review_helpers.
See that module for documentation on the review workflow.
"""

from __future__ import annotations

from collector_core.yellow_review_helpers import make_main


def main() -> None:
    make_main(
        domain_name="NLP",
        domain_prefix="nlp",
        targets_yaml_name="targets_nlp.yaml",
    )


if __name__ == "__main__":
    main()
