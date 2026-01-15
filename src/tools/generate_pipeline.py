#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class PipelineSpec:
    pipeline_id: str
    domain: str
    title: str
    class_name: str

    @property
    def dir_name(self) -> str:
        return self.pipeline_id

    @property
    def targets_filename(self) -> str:
        return f"targets_{self.domain}.yaml"


def normalize_domain(raw_domain: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", raw_domain).strip("_")
    return cleaned.lower()


def title_from_domain(domain: str) -> str:
    return domain.replace("_", " ").title()


def class_name_from_domain(domain: str) -> str:
    return "".join(part.capitalize() for part in domain.split("_"))


def build_spec(pipeline_id: str | None, domain: str | None, title: str | None) -> PipelineSpec:
    if not pipeline_id and not domain:
        raise ValueError("Provide --pipeline-id or --domain")

    resolved_domain = normalize_domain(domain or pipeline_id.replace("_pipeline_v2", ""))
    resolved_pipeline_id = pipeline_id or f"{resolved_domain}_pipeline_v2"
    resolved_title = title or title_from_domain(resolved_domain)
    class_name = class_name_from_domain(resolved_domain)

    return PipelineSpec(
        pipeline_id=resolved_pipeline_id,
        domain=resolved_domain,
        title=resolved_title,
        class_name=class_name,
    )


def render_readme(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f"""
        # {spec.title} Corpus Pipeline (v2)

        This pipeline follows the shared v2 stage flow described in
        `docs/PIPELINE_V2_REWORK_PLAN.md` and `docs/pipeline_runtime_contract.md`.

        ## Run all pipelines via JupyterLab

        The recommended workflow is the repository notebook
        `dataset_collector_run_all_pipelines.ipynb`, which runs every
        `*_pipeline_v2/` directory in sequence and prompts for required API keys.

        ## Standalone quick start (optional)

        ```bash
        pip install -r requirements.txt

        # Dry-run classify only
        python -m collector_core.dc_cli pipeline {spec.domain} -- --targets ../pipelines/targets/{spec.targets_filename}

        # Acquire GREEN and YELLOW
        python -m collector_core.dc_cli run --pipeline {spec.domain} --stage acquire -- \\
          --targets-yaml ../pipelines/targets/{spec.targets_filename} --queue /data/{spec.domain}/_queues/green_download.jsonl --bucket green --execute
        python -m collector_core.dc_cli run --pipeline {spec.domain} --stage acquire -- \\
          --targets-yaml ../pipelines/targets/{spec.targets_filename} --queue /data/{spec.domain}/_queues/yellow_pipeline.jsonl --bucket yellow --execute

        # Screen, merge, catalog
        python -m collector_core.dc_cli run --pipeline {spec.domain} --stage yellow_screen -- \\
          --targets ../pipelines/targets/{spec.targets_filename} --queue /data/{spec.domain}/_queues/yellow_pipeline.jsonl --execute
        python -m collector_core.dc_cli run --pipeline {spec.domain} --stage merge -- --targets ../pipelines/targets/{spec.targets_filename} --execute
        python -m collector_core.dc_cli catalog-builder --pipeline {spec.domain} -- \\
          --targets ../pipelines/targets/{spec.targets_filename} --output /data/{spec.domain}/_catalogs/catalog.json
        ```

        ## Directory layout

        Targets YAML defaults to `/data/...`; the orchestrator patches to your
        `--dest-root`. For standalone runs, pass `--dataset-root` or use
        `src/tools/patch_targets.py`.

        ## License

        Pipeline code is provided as-is for research and development use.
        """
        ).strip()
        + "\n"
    )


def render_pipeline_driver(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f"""
        #!/usr/bin/env python3
        from __future__ import annotations
        from pathlib import Path


        from collector_core.__version__ import __version__ as VERSION
        from collector_core.pipeline_driver_base import BasePipelineDriver, RoutingBlockSpec


        class {spec.class_name}PipelineDriver(BasePipelineDriver):
            DOMAIN = "{spec.domain}"
            PIPELINE_VERSION = VERSION
            TARGETS_LABEL = "{spec.targets_filename}"
            USER_AGENT = "{spec.domain}-corpus-pipeline"
            ROUTING_KEYS = ["{spec.domain}_routing"]
            DEFAULT_ROUTING = {{"subject": "{spec.domain}", "granularity": "target"}}
            ROUTING_BLOCKS = [
                RoutingBlockSpec(name="{spec.domain}_routing", sources=["{spec.domain}_routing"], mode="subset"),
            ]


        if __name__ == "__main__":
            {spec.class_name}PipelineDriver.main()
        """
        ).strip()
        + "\n"
    )


def render_acquire_worker(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f'''
        #!/usr/bin/env python3
        """
        acquire_worker.py (v2.0)

        Deprecated compatibility shim for `dc run --pipeline {spec.domain} --stage acquire`.
        Removal target: v3.0.
        """

        from __future__ import annotations
        import warnings
        from pathlib import Path


        from collector_core.acquire_strategies import (
            AcquireContext,
            Limits,
            RetryConfig,
            Roots,
            RunMode,
            _http_download_with_resume,
        )
        from collector_core.generic_workers import main_acquire

        __all__ = [
            "AcquireContext",
            "Limits",
            "RetryConfig",
            "Roots",
            "RunMode",
            "_http_download_with_resume",
            "main",
        ]


        def main() -> None:
            warnings.warn(
                "acquire_worker.py is deprecated; use `dc run --pipeline {spec.domain} --stage acquire` instead. "
                "Removal target: v3.0.",
                DeprecationWarning,
                stacklevel=2,
            )
            main_acquire("{spec.domain}", repo_root=Path(__file__).resolve().parents[1])


        if __name__ == "__main__":
            main()
        '''
        ).strip()
        + "\n"
    )


def render_yellow_screen_worker(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f'''
        #!/usr/bin/env python3
        """
        yellow_screen_worker.py (v2.0)

        Deprecated compatibility shim for `dc run --pipeline {spec.domain} --stage yellow_screen`.
        Removal target: v3.0.
        """

        from __future__ import annotations
        import warnings


        from collector_core.yellow_screen_dispatch import main_yellow_screen


        def main() -> None:
            warnings.warn(
                "yellow_screen_worker.py is deprecated; use `dc run --pipeline {spec.domain} --stage yellow_screen` instead. "
                "Removal target: v3.0.",
                DeprecationWarning,
                stacklevel=2,
            )
            main_yellow_screen("{spec.domain}")


        if __name__ == "__main__":
            main()
        '''
        ).strip()
        + "\n"
    )


def render_merge_worker(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f'''
        #!/usr/bin/env python3
        """
        merge_worker.py (v2.0)

        Deprecated compatibility shim for `dc run --pipeline {spec.domain} --stage merge`.
        Removal target: v3.0.
        """

        from __future__ import annotations
        import warnings


        from collector_core.generic_workers import main_merge
        DOMAIN = "{spec.domain}"


        def main() -> None:
            warnings.warn(
                "merge_worker.py is deprecated; use `dc run --pipeline {spec.domain} --stage merge` instead. "
                "Removal target: v3.0.",
                DeprecationWarning,
                stacklevel=2,
            )
            main_merge(DOMAIN)


        if __name__ == "__main__":
            main()
        '''
        ).strip()
        + "\n"
    )


def render_catalog_builder() -> str:
    return (
        textwrap.dedent(
            '''
        #!/usr/bin/env python3
        """
        catalog_builder.py (v2.0)

        Thin wrapper that delegates to the spec-driven generic catalog builder.
        """

        from __future__ import annotations
        from pathlib import Path


        from collector_core.generic_workers import main_catalog

        DOMAIN = Path(__file__).resolve().parent.name.removesuffix("_pipeline_v2")


        def main() -> None:
            main_catalog(DOMAIN)


        if __name__ == "__main__":
            main()
        '''
        ).strip()
        + "\n"
    )


def render_review_queue() -> str:
    return (
        textwrap.dedent(
            '''
        #!/usr/bin/env python3
        """
        review_queue.py (v2.0)

        Thin wrapper that delegates to the spec-driven review queue helper.
        """

        from __future__ import annotations
        from pathlib import Path


        from collector_core.generic_workers import main_review_queue

        DOMAIN = Path(__file__).resolve().parent.name.removesuffix("_pipeline_v2")


        def main() -> None:
            main_review_queue(DOMAIN)


        if __name__ == "__main__":
            main()
        '''
        ).strip()
        + "\n"
    )


def render_run_pipeline(spec: PipelineSpec) -> str:
    return (
        textwrap.dedent(
            f"""
        #!/usr/bin/env bash
        #
        # run_pipeline.sh (v2.0)
        #
        # Deprecated compatibility shim for the {spec.title} pipeline.
        # Use: dc run --pipeline {spec.domain} --stage <stage>
        # Removal target: v3.0.
        #
        set -euo pipefail

        YELLOW='\033[0;33m'
        RED='\033[0;31m'
        NC='\033[0m'

        TARGETS=""
        EXECUTE=""
        STAGE=""
        EXTRA_ARGS=()

        usage() {{
          cat << 'EOM'
        Deprecated pipeline wrapper (v2)

        Required:
          --targets FILE          Path to {spec.targets_filename}
          --stage STAGE           Stage to run: classify, acquire, yellow_screen, merge, catalog, review

        Options:
          --execute               Perform actions (default is dry-run/plan only)
          --                      Pass remaining args directly to the stage command
          -h, --help              Show this help

        Notes:
          - This shim no longer resolves queue paths automatically.
          - Provide stage arguments after -- (for example: --queue, --bucket, --targets-yaml).
        EOM
        }}

        while [[ $# -gt 0 ]]; do
          case "$1" in
            --targets) TARGETS="$2"; shift 2 ;;
            --stage) STAGE="$2"; shift 2 ;;
            --execute) EXECUTE="--execute"; shift ;;
            --) shift; EXTRA_ARGS+=("$@"); break ;;
            -h|--help) usage; exit 0 ;;
            *) EXTRA_ARGS+=("$1"); shift ;;
          esac
        done

        if [[ -z "$TARGETS" || -z "$STAGE" ]]; then
          echo -e "${{RED}}--targets and --stage are required${{NC}}"
          usage
          exit 1
        fi

        if [[ ! -f "$TARGETS" ]]; then
          echo -e "${{RED}}targets file not found: $TARGETS${{NC}}"
          exit 1
        fi

        echo -e "${{YELLOW}}[deprecated] run_pipeline.sh is deprecated; use 'dc run --pipeline {spec.domain} --stage <stage>' instead. Removal target: v3.0.${{NC}}" >&2

        SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
        REPO_ROOT="$(cd "${{SCRIPT_DIR}}/.." && pwd)"
        export PYTHONPATH="${{REPO_ROOT}}:${{PYTHONPATH:-}}"

        case "$STAGE" in
          classify)
            NO_FETCH=""
            if [[ -z "$EXECUTE" ]]; then
              NO_FETCH="--no-fetch"
            fi
            python -m collector_core.dc_cli pipeline {spec.domain} -- --targets "$TARGETS" $NO_FETCH "${{EXTRA_ARGS[@]}}"
            ;;
          acquire)
            python -m collector_core.dc_cli run --pipeline {spec.domain} --stage acquire -- --targets-yaml "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
            ;;
          yellow_screen)
            python -m collector_core.dc_cli run --pipeline {spec.domain} --stage yellow_screen -- --targets "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
            ;;
          merge)
            python -m collector_core.dc_cli run --pipeline {spec.domain} --stage merge -- --targets "$TARGETS" $EXECUTE "${{EXTRA_ARGS[@]}}"
            ;;
          catalog)
            python -m collector_core.dc_cli catalog-builder --pipeline {spec.domain} -- --targets "$TARGETS" "${{EXTRA_ARGS[@]}}"
            ;;
          review)
            python -m collector_core.dc_cli review-queue --pipeline {spec.domain} -- --targets "$TARGETS" "${{EXTRA_ARGS[@]}}"
            ;;
          *)
            echo -e "${{RED}}Unknown stage: $STAGE${{NC}}"
            usage
            exit 1
            ;;
        esac
        """
        ).strip()
        + "\n"
    )


def render_targets_yaml(spec: PipelineSpec) -> str:
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        textwrap.dedent(
            f"""
        schema_version: "0.9"
        updated_utc: "{updated}"

        companion_files:
          license_map:
            - "../../configs/common/license_map.yaml"
          field_schemas:
            - "../../configs/common/field_schemas.yaml"
          denylist:
            - "../../configs/common/denylist.yaml"

        globals:
          raw_root: /data/{spec.domain}/raw
          screened_yellow_root: /data/{spec.domain}/screened_yellow
          combined_root: /data/{spec.domain}/combined
          ledger_root: /data/{spec.domain}/_ledger
          pitches_root: /data/{spec.domain}/_pitches
          manifests_root: /data/{spec.domain}/_manifests
          queues_root: /data/{spec.domain}/_queues
          catalogs_root: /data/{spec.domain}/_catalogs
          logs_root: /data/{spec.domain}/_logs

          require_yellow_signoff: true

          sharding:
            max_records_per_shard: 50000
            compression: gzip

          screening:
            min_chars: 200
            max_chars: 12000
            text_field_candidates:
              - text
              - content
              - body
              - prompt
            record_license_field_candidates:
              - license
              - license_spdx
            require_record_license: false
            allow_spdx:
              - CC-BY-4.0
              - CC0-1.0
              - MIT
              - Apache-2.0
              - CC-BY-SA-4.0
            deny_phrases:
              - noai
              - "no tdm"
              - "no machine learning"

          default_license_gates:
            - snapshot_terms
            - restriction_phrase_scan
          default_content_checks: []

        queues:
          emit:
            - id: "green_download"
              path: "/data/{spec.domain}/_queues/green_download.jsonl"
              criteria:
                effective_bucket: "GREEN"
                enabled: true
            - id: "yellow_pipeline"
              path: "/data/{spec.domain}/_queues/yellow_pipeline.jsonl"
              criteria:
                effective_bucket: "YELLOW"
                enabled: true
            - id: "red_rejected"
              path: "/data/{spec.domain}/_queues/red_rejected.jsonl"
              criteria:
                effective_bucket: "RED"
                enabled: true

        gates_catalog:
          snapshot_terms: "Save Terms/ToS + license page HTML/PDF/TXT + retrieval timestamp."
          restriction_phrase_scan: "Scan terms/content for restriction phrases (no-LLM/no-TDM/etc.)."
          manual_legal_review: "Human review required before training."

        resolvers:
          github:
            mode: "release"
            api_base: "https://api.github.com"
            rate_limit:
              requests_per_hour: 60
              retry_on_403: true
              exponential_backoff: true
            endpoints:
              releases: "/repos/{{owner}}/{{repo}}/releases"
              latest: "/repos/{{owner}}/{{repo}}/releases/latest"
              assets: "/repos/{{owner}}/{{repo}}/releases/{{release_id}}/assets"
            note: "Use GITHUB_TOKEN env var for higher rate limits."
          huggingface_datasets:
            mode: "dataset_id"
            note: "Use datasets.load_dataset; capture dataset card + license field."
          git:
            mode: "repo"
            note: "Capture commit hash; store LICENSE + README as evidence."
          http:
            mode: "direct_url"
            note: "Direct file download with resume."
          ftp:
            mode: "base_url + globs"
            note: "List + filter by glob; store directory listing snapshot (planned)."
          zenodo:
            mode: "record_id_or_doi"
            api: "https://zenodo.org/api/records/{{record_id}}"
            outputs:
              - "files[*].links.self"
              - "files[*].checksum"
            verify_checksum: true
            checksum_algorithm: "md5"
          dataverse:
            mode: "persistent_id"
            default_instance: "https://dataverse.harvard.edu"

        targets:
          - id: "example_target"
            name: "Replace with a real dataset target"
            enabled: false
            priority: 10
            license_profile: "permissive"
            license_evidence:
              spdx_hint: "MIT"
              url: ""
            notes: "Update license metadata and enable when ready."
            data_type: ["{spec.domain}"]
            download:
              strategy: "none"
            routing:
              subject: {spec.domain}
              granularity: target
        """
        ).strip()
        + "\n"
    )


def render_requirements() -> str:
    return "# Add pipeline-specific Python dependencies here.\n"


def write_file(path: Path, content: str, force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def generate_pipeline(
    spec: PipelineSpec, repo_root: Path, force: bool, include_shims: bool
) -> Path:
    pipeline_dir = repo_root / spec.dir_name
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    write_file(pipeline_dir / "README.md", render_readme(spec), force)
    if include_shims:
        write_file(pipeline_dir / "pipeline_driver.py", render_pipeline_driver(spec), force)
        write_file(pipeline_dir / "acquire_worker.py", render_acquire_worker(spec), force)
        write_file(
            pipeline_dir / "yellow_screen_worker.py", render_yellow_screen_worker(spec), force
        )
        write_file(pipeline_dir / "merge_worker.py", render_merge_worker(spec), force)
        write_file(pipeline_dir / "catalog_builder.py", render_catalog_builder(), force)
        write_file(pipeline_dir / "review_queue.py", render_review_queue(), force)
        write_file(pipeline_dir / "legacy" / "run_pipeline.sh", render_run_pipeline(spec), force)
    targets_dir = repo_root / "pipelines" / "targets"
    targets_dir.mkdir(parents=True, exist_ok=True)
    write_file(targets_dir / spec.targets_filename, render_targets_yaml(spec), force)
    write_file(pipeline_dir / "requirements.txt", render_requirements(), force)

    return pipeline_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scaffold a new v2 pipeline configuration."
    )
    parser.add_argument("--pipeline-id", help="Pipeline directory name, e.g., math_pipeline_v2")
    parser.add_argument(
        "--domain", help="Domain slug (defaults to pipeline-id without _pipeline_v2)"
    )
    parser.add_argument("--title", help="Human-friendly pipeline title")
    parser.add_argument(
        "--dest",
        type=Path,
        default=ROOT_DIR,
        help="Repo root where the pipeline directory should be created (default: repo root)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite files if they already exist",
    )
    parser.add_argument(
        "--with-compat-shims",
        action="store_true",
        help="Include deprecated wrapper scripts (pipeline_driver, *_worker.py, legacy/run_pipeline.sh).",
    )
    args = parser.parse_args()

    try:
        spec = build_spec(args.pipeline_id, args.domain, args.title)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    pipeline_dir = generate_pipeline(spec, args.dest, args.force, args.with_compat_shims)
    print(f"Generated pipeline scaffold in {pipeline_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
