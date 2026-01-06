# Single-command run & clean-room reruns

This guide summarizes the minimal, single-command run, the expected output folders, and how
to do a clean-room rerun (what to delete vs. keep). It also calls out the Jupyter notebook
option for running the full suite.

## Single-command run (all pipelines)

### Windows (PowerShell)

```powershell
python tools\build_natural_corpus.py --dest-root "E:\AI-Research\datasets\Natural" --pipelines all --execute
```

### macOS/Linux (bash)

```bash
python tools/build_natural_corpus.py --dest-root "/data/Natural" --pipelines all --execute
```

This is the shortest “one-liner” to run every pipeline sequentially. It writes one
pipeline folder per domain under the destination root. For a notebook-driven run,
open `dataset_collector_run_all_pipelines.ipynb` and execute the cells in order.

## Minimal from-scratch sequence

Use this when you are starting on a fresh machine or want the most explicit sequence.

### Windows (PowerShell)

```powershell
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.constraints.txt
pip install -r math_pipeline_v2\requirements.txt
# repeat for other pipeline extras as needed

python tools\build_natural_corpus.py --dest-root "E:\AI-Research\datasets\Natural" --pipelines all --execute
```

### Notebook (JupyterLab)

1. Launch JupyterLab.
2. Open `dataset_collector_run_all_pipelines.ipynb`.
3. Run the cells in order. The notebook drives every pipeline sequentially and prompts for
   required API keys. It can also install per-pipeline requirements.

## Expected output folders

Each pipeline produces a dataset root under the destination folder. Example locations:

- Windows: `E:\AI-Research\datasets\Natural\math_pipeline_v2\`
- macOS/Linux: `/data/Natural/math_pipeline_v2/`

The canonical layout for each pipeline’s dataset root is documented in
`docs/output_contract.md`. At a high level you will see:

```
<dataset_root>/
  raw/
  screened_yellow/
  combined/
  _queues/
  _ledger/
  _pitches/
  _manifests/
  _catalogs/
  _logs/
```

### Logs, manifests, and ledgers

Within each pipeline dataset root:

- Logs: `<dataset_root>/_logs/`
- Manifests (and patched target YAMLs): `<dataset_root>/_manifests/`
- Ledgers: `<dataset_root>/_ledger/`

These locations are stable across pipelines and match the contract in
`docs/output_contract.md`.

## Clean-room rerun (what to delete vs. keep)

The goal of a clean-room rerun is to remove all prior outputs for a pipeline while keeping
your environment and configuration intact.

### Delete (pipeline outputs)

Delete the entire per-pipeline dataset root under your destination folder. Example for the
math pipeline:

- Windows: `E:\AI-Research\datasets\Natural\math_pipeline_v2\`
- macOS/Linux: `/data/Natural/math_pipeline_v2/`

This removes raw downloads, queues, manifests, ledgers, logs, catalogs, and merged outputs.
If you want a full clean-room rerun across all pipelines, delete each pipeline’s dataset
root folder under the destination root.

### Keep (environment + config)

Keep the following so you can rerun quickly:

- Your repo checkout (including `tools/build_natural_corpus.py` and
  `dataset_collector_run_all_pipelines.ipynb`).
- Virtual environment(s) and installed dependencies.
- Your local pipeline map or target YAMLs (for example, a copy of
  `tools/pipeline_map.sample.yaml` you customized).

After deleting the dataset roots, re-run the single command in the section above to start
from a clean slate.
