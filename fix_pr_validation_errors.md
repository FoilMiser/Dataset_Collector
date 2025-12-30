# Fix PR validation errors (dependency conflict) + harden dependency workflow

This repo’s GitHub Actions PR validation is failing in the **validate-lock** job because `requirements.lock` pins an **incompatible** set of versions:

- `requirements.lock` pins **`datasets==2.20.0`**
- `requirements.lock` also pins **`pyarrow==14.0.0`**
- but `datasets==2.20.0` depends on **`pyarrow>=15.0.0`**

Pip’s resolver correctly fails with `ResolutionImpossible`.

---

## 1) Reproduce the failure locally (matches CI)

From repo root:

```bash
python -m pip install -U pip
pip install -r requirements.lock
```

You should see an error like:

- `datasets 2.20.0 depends on pyarrow>=15.0.0`
- you requested `pyarrow==14.0.0`

---

## 2) Fast fix (unblock CI immediately)

### 2.1 Update `requirements.lock` to a compatible PyArrow

Edit `requirements.lock`:

```diff
 datasets==2.20.0
-pyarrow==14.0.0
+pyarrow==15.0.2
```

> Notes
> - Any `pyarrow==15.*` works as long as it’s `>=15.0.0`.
> - Use a specific patch (like `15.0.2`) to reduce surprise breakage over time.

### 2.2 Align `requirements.txt` so “minimal” install can’t drift below 15

Edit `requirements.txt`:

```diff
 datasets>=2.20.0
-pyarrow>=14.0.0
+pyarrow>=15.0.0
```

If you want the minimal install to mirror the lock more closely:

```diff
-pyarrow>=14.0.0
+pyarrow>=15.0.2
```

### 2.3 Update per-pipeline `requirements.txt` comments (consistency)

Many pipeline `requirements.txt` files include commented optional lines like:

```txt
# datasets>=2.20.0
# pyarrow>=14.0.0
```

Update them to:

```diff
 # datasets>=2.20.0
-# pyarrow>=14.0.0
+# pyarrow>=15.0.0
```

This is optional for CI, but prevents future copy/paste regressions.

---

## 3) Validate the fix (local + CI)

After editing the files:

```bash
python -m pip install -U pip
pip install -r requirements.lock
python -m pip install pytest ruff
ruff check .
pytest -q
python tools/validate_repo.py --root .
python tools/preflight.py --repo-root .
pip check
```

Expected outcome:
- no dependency-resolution failure
- tests + repo validators pass

---

## 4) Why this happened (root cause)

Your current “lock” file is **not a true lock** (it only pins a handful of top-level packages).
This creates two recurring failure modes:

1. **Direct conflicts** (like this one), where a pinned top-level dep contradicts another pinned dep.
2. **Transitive drift**, where unpinned dependencies (numpy, urllib3, etc.) float to new major versions and break CI unexpectedly.

---

## 5) Recommended hardening (pick ONE approach)

### Option A (recommended): Use pip-tools to generate a real lock

This gives you a stable, reproducible dependency set on CI and locally.

#### 5.1 Add pip-tools to dev requirements

Edit `requirements-dev.txt`:

```diff
 jupyterlab
 ipykernel
+pip-tools
```

#### 5.2 Introduce `requirements.in` as the “source of truth”

Create `requirements.in` (new file) and move your *top-level* deps there.
Example (start with your current `requirements.txt` contents):

```txt
pyyaml==6.0
requests==2.31.0
boto3==1.34.0
datasets==2.20.0
pyarrow==15.0.2
pypdf==4.0.0
pdfminer.six==20221105
beautifulsoup4==4.12.0
lxml==5.0.0
trafilatura==1.6.0
```

Then decide what `requirements.txt` should be:

- **Option A1**: Make `requirements.txt` the compiled lock, and rename your current `requirements.txt` to `requirements.in`
- **Option A2**: Keep `requirements.txt` as “minimal” and create `requirements.lock` via pip-tools

The most common pattern is:
- `requirements.in` = human-edited
- `requirements.txt` (or `.lock`) = generated

#### 5.3 Generate a real lock

From repo root:

```bash
python -m pip install -U pip pip-tools
pip-compile requirements.in -o requirements.lock
```

(Optional but nice):
- include hashes: `pip-compile --generate-hashes requirements.in -o requirements.lock`
- refresh: `pip-compile --upgrade requirements.in -o requirements.lock`

#### 5.4 Update CI to install the compiled lock

Your CI already does:

```yaml
pip install -r requirements.lock
```

So once `requirements.lock` becomes a true compiled lock, CI becomes stable.

#### 5.5 Add a CI check to prevent editing the lock by hand

Add a step in `validate-lock`:

```yaml
- name: Ensure lock is up to date
  run: |
    python -m pip install -U pip pip-tools
    pip-compile requirements.in -o requirements.lock.generated
    python -c "import sys; a=open('requirements.lock','rb').read(); b=open('requirements.lock.generated','rb').read(); sys.exit(0 if a==b else 1)"
```

Then CI fails if someone changes `requirements.in` without regenerating the lock.

---

### Option B: Use `constraints.txt` instead of a “lock”

This keeps `requirements.txt` minimal and applies strict pins as constraints.

#### 5.1 Rename the lock file

Rename:

- `requirements.lock` → `constraints.txt`

#### 5.2 Install using constraints

Update CI `validate-lock` install step:

```diff
- pip install -r requirements.lock
+ pip install -r requirements.txt -c constraints.txt
```

Locally you’d do:

```bash
pip install -r requirements.txt -c constraints.txt
```

This makes the relationship between “what we need” and “what we pin” explicit, and prevents future “lock vs minimal mismatch” confusion.

---

## 6) Recommended guardrails (small, high-value)

### 6.1 Add a tiny lock sanity check script (optional)

Create `tools/validate_deps.py` that checks for obvious contradictions like:

- if `datasets==2.20.0` is pinned, `pyarrow` must be `>=15.0.0`

Then call it in CI before install. This gives a clear error message before pip’s resolver output.

### 6.2 Pin major-version-risk transitive deps (if you keep the current “partial lock” style)

If you do **not** adopt pip-tools or constraints, consider at minimum pinning a few common “drift breakers”
in your lock (examples only; adjust as needed):

- `numpy` (major bumps can cause breakage)
- `urllib3` (major bumps can break older SDKs)
- `certifi` (rarely breaks, but sometimes interacts with system SSL)

But the better long-term solution is still Option A or B.

---

## 7) Final checklist for the PR

- [ ] `requirements.lock` updated to `pyarrow==15.0.2` (or any `>=15.0.0`)
- [ ] `requirements.txt` updated to `pyarrow>=15.0.0` (or `>=15.0.2`)
- [ ] pipeline `requirements.txt` optional comments updated to `pyarrow>=15.0.0`
- [ ] CI passes: validate-min + validate-lock on ubuntu + windows for py310 + py311
- [ ] `pip check` passes after installation
- [ ] Ruff + pytest + `tools/validate_repo.py` + `tools/preflight.py` pass

---

## Appendix: compatible version pairing

Given your current pin:

- `datasets==2.20.0` → require **`pyarrow>=15.0.0`**

So these are valid examples:

- ✅ `datasets==2.20.0` + `pyarrow==15.0.2`
- ✅ `datasets==2.20.0` + `pyarrow==16.1.0` (if you choose to move up later)
- ❌ `datasets==2.20.0` + `pyarrow==14.0.0` (current failure)
