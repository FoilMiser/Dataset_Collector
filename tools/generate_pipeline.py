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
        python -m collector_core.generic_workers --domain {spec.domain} catalog -- \\
          --targets ../pipelines/targets/{spec.targets_filename} --output /data/{spec.domain}/_catalogs/catalog.json
        ```

        ## Directory layout

        Targets YAML defaults to `/data/...`; the orchestrator patches to your
        `--dest-root`. For standalone runs, pass `--dataset-root`, use
        `tools/patch_targets.py`, or run `run_pipeline.sh` (optional wrapper).

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

        Thin wrapper that delegates to the spec-driven generic acquire worker.
        """

        from __future__ import annotations
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

        Thin adapter for collector_core.yellow_screen_standard.
        """

        from __future__ import annotations
        from pathlib import Path


        from collector_core import yellow_screen_standard as core_yellow
        from collector_core.yellow_screen_common import default_yellow_roots

        DEFAULT_ROOTS = default_yellow_roots("{spec.domain}")


        def resolve_roots(cfg: dict):
            return core_yellow.resolve_roots(cfg, DEFAULT_ROOTS)


        def main() -> None:
            core_yellow.main(defaults=DEFAULT_ROOTS)


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

        Thin wrapper that delegates to the spec-driven generic merge worker.
        """

        from __future__ import annotations
        from pathlib import Path


        from collector_core import merge as core_merge
        from collector_core.generic_workers import main_merge
        from collector_core.pipeline_spec import get_pipeline_spec

        DOMAIN = "{spec.domain}"
        SPEC = get_pipeline_spec(DOMAIN)
        if SPEC is None:
            raise SystemExit(f"Unknown pipeline domain: {{DOMAIN}}")

        PIPELINE_ID = SPEC.pipeline_id
        DEFAULT_ROOTS = core_merge.default_merge_roots(SPEC.prefix)

        read_jsonl = core_merge.read_jsonl
        write_json = core_merge.write_json


        def resolve_roots(cfg: dict) -> core_merge.Roots:
            return core_merge.resolve_roots(cfg, DEFAULT_ROOTS)


        def merge_records(cfg: dict, roots: core_merge.Roots, execute: bool) -> dict:
            return core_merge.merge_records(cfg, roots, execute, pipeline_id=PIPELINE_ID)


        def main() -> None:
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
        # Wrapper script for the {spec.title} pipeline using the unified dc CLI.
        #
        set -euo pipefail

        RED='\033[0;31m'
        BLUE='\033[0;34m'
        NC='\033[0m'

        TARGETS=""
        EXECUTE=""
        STAGE="all"
        LIMIT_TARGETS=""
        LIMIT_FILES=""
        WORKERS="4"

        usage() {{
          cat << 'EOM'
        Pipeline wrapper (v2)

        Required:
          --targets FILE          Path to {spec.targets_filename}

        Options:
          --execute               Perform actions (default is dry-run/plan only)
          --stage STAGE           Stage to run: all, classify, acquire_green, acquire_yellow, \
                                  screen_yellow, merge, catalog, review
          --limit-targets N       Limit number of queue rows processed
          --limit-files N         Limit files per target during acquisition
          --workers N             Parallel workers for acquisition (default: 4)
          -h, --help              Show this help
        EOM
        }}

        while [[ $# -gt 0 ]]; do
          case "$1" in
            --targets) TARGETS="$2"; shift 2 ;;
            --stage) STAGE="$2"; shift 2 ;;
            --execute) EXECUTE="--execute"; shift ;;
            --limit-targets) LIMIT_TARGETS="$2"; shift 2 ;;
            --limit-files) LIMIT_FILES="$2"; shift 2 ;;
            --workers) WORKERS="$2"; shift 2 ;;
            -h|--help) usage; exit 0 ;;
            *) echo -e "${{RED}}Unknown option: $1${{NC}}"; usage; exit 1 ;;
          esac
        done

        if [[ -z "$TARGETS" ]]; then
          echo -e "${{RED}}--targets is required${{NC}}"
          usage
          exit 1
        fi

        if [[ ! -f "$TARGETS" ]]; then
          echo -e "${{RED}}targets file not found: $TARGETS${{NC}}"
          exit 1
        fi

        SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
        REPO_ROOT="$(cd "${{SCRIPT_DIR}}/.." && pwd)"
        export PYTHONPATH="${{REPO_ROOT}}:${{PYTHONPATH:-}}"
        QUEUES_ROOT=$(python - << PY
        from pathlib import Path
        from collector_core.config_validator import read_yaml
        from collector_core.pipeline_spec import get_pipeline_spec
        cfg = read_yaml(Path("${{TARGETS}}"), schema_name="targets") or {{}}
        spec = get_pipeline_spec("{spec.domain}")
        prefix = spec.prefix if spec else "{spec.domain}"
        print(cfg.get("globals", {{}}).get("queues_root", f"/data/{{prefix}}/_queues"))
        PY
        )
        CATALOGS_ROOT=$(python - << PY
        from pathlib import Path
        from collector_core.config_validator import read_yaml
        from collector_core.pipeline_spec import get_pipeline_spec
        cfg = read_yaml(Path("${{TARGETS}}"), schema_name="targets") or {{}}
        spec = get_pipeline_spec("{spec.domain}")
        prefix = spec.prefix if spec else "{spec.domain}"
        print(cfg.get("globals", {{}}).get("catalogs_root", f"/data/{{prefix}}/_catalogs"))
        PY
        )
        LIMIT_TARGETS_ARG=""; [[ -n "$LIMIT_TARGETS" ]] && LIMIT_TARGETS_ARG="--limit-targets $LIMIT_TARGETS"
        LIMIT_FILES_ARG=""; [[ -n "$LIMIT_FILES" ]] && LIMIT_FILES_ARG="--limit-files $LIMIT_FILES"

        run_classify() {{
          echo -e "${{BLUE}}== Stage: classify ==${{NC}}"
          local no_fetch=""
          if [[ -z "$EXECUTE" ]]; then
            no_fetch="--no-fetch"
          fi
          python -m collector_core.dc_cli pipeline {spec.domain} -- --targets "$TARGETS" $no_fetch
        }}

        run_review() {{
          local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
          echo -e "${{BLUE}}== Stage: review ==${{NC}}"
          python -m collector_core.generic_workers --domain {spec.domain} review-queue -- --queue "$queue_file" --targets "$TARGETS" --limit 50 || true
        }}

        run_acquire() {{
          local bucket="$1"
          local queue_file="$QUEUES_ROOT/${{bucket}}_download.jsonl"
          if [[ "$bucket" == "yellow" ]]; then
            queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
          fi
          if [[ ! -f "$queue_file" ]]; then
            echo -e "${{RED}}Queue not found: $queue_file${{NC}}"
            exit 1
          fi
          echo -e "${{BLUE}}== Stage: acquire_${{bucket}} ==${{NC}}"
          python -m collector_core.dc_cli run --pipeline {spec.domain} --stage acquire -- \
            --queue "$queue_file" \
            --targets-yaml "$TARGETS" \
            --bucket "$bucket" \
            --workers "$WORKERS" \
            $EXECUTE \
            $LIMIT_TARGETS_ARG \
            $LIMIT_FILES_ARG
        }}

        run_screen_yellow() {{
          local queue_file="$QUEUES_ROOT/yellow_pipeline.jsonl"
          if [[ ! -f "$queue_file" ]]; then
            echo -e "${{RED}}Queue not found: $queue_file${{NC}}"
            exit 1
          fi
          echo -e "${{BLUE}}== Stage: screen_yellow ==${{NC}}"
          python -m collector_core.dc_cli run --pipeline {spec.domain} --stage yellow_screen -- \
            --targets "$TARGETS" \
            --queue "$queue_file" \
            $EXECUTE
        }}

        run_merge() {{
          echo -e "${{BLUE}}== Stage: merge ==${{NC}}"
          python -m collector_core.dc_cli run --pipeline {spec.domain} --stage merge -- --targets "$TARGETS" $EXECUTE
        }}


        run_catalog() {{
          echo -e "${{BLUE}}== Stage: catalog ==${{NC}}"
          python -m collector_core.generic_workers --domain {spec.domain} catalog -- --targets "$TARGETS" --output "${{CATALOGS_ROOT}}/catalog.json"
        }}

        case "$STAGE" in
          all)
            run_classify
            run_acquire green
            run_acquire yellow
            run_screen_yellow
            run_merge
            run_catalog
            ;;
          classify) run_classify ;;
          acquire_green) run_acquire green ;;
          acquire_yellow) run_acquire yellow ;;
          screen_yellow) run_screen_yellow ;;
          merge) run_merge ;;
          catalog) run_catalog ;;
          review) run_review ;;
          *) echo -e "${{RED}}Unknown stage: $STAGE${{NC}}"; usage; exit 1 ;;
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
    path.write_text(content, encoding="utf-8")


def generate_pipeline(spec: PipelineSpec, repo_root: Path, force: bool) -> Path:
    pipeline_dir = repo_root / spec.dir_name
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    write_file(pipeline_dir / "README.md", render_readme(spec), force)
    write_file(pipeline_dir / "pipeline_driver.py", render_pipeline_driver(spec), force)
    write_file(pipeline_dir / "acquire_worker.py", render_acquire_worker(spec), force)
    write_file(pipeline_dir / "yellow_screen_worker.py", render_yellow_screen_worker(spec), force)
    write_file(pipeline_dir / "merge_worker.py", render_merge_worker(spec), force)
    write_file(pipeline_dir / "catalog_builder.py", render_catalog_builder(), force)
    write_file(pipeline_dir / "review_queue.py", render_review_queue(), force)
    write_file(pipeline_dir / "run_pipeline.sh", render_run_pipeline(spec), force)
    targets_dir = repo_root / "pipelines" / "targets"
    targets_dir.mkdir(parents=True, exist_ok=True)
    write_file(targets_dir / spec.targets_filename, render_targets_yaml(spec), force)
    write_file(pipeline_dir / "requirements.txt", render_requirements(), force)

    return pipeline_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Scaffold a new v2 pipeline wrapper.")
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
    args = parser.parse_args()

    try:
        spec = build_spec(args.pipeline_id, args.domain, args.title)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    pipeline_dir = generate_pipeline(spec, args.dest, args.force)
    print(f"Generated pipeline scaffold in {pipeline_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
