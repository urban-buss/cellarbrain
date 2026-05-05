---
description: "Fully uninstall cellarbrain and its extras from the Python environment, clean up output files, and prepare for a fresh reinstall"
agent: "agent"
---
Fully remove the `cellarbrain` package and reset this workspace for a clean reinstall.

## 1. Uninstall the package

```
py -3 -m pip uninstall cellarbrain -y
```

If pip reports "No files were found to uninstall" (stale editable install), force-remove the metadata:

```
py -3 -m pip install cellarbrain --force-reinstall --no-deps
py -3 -m pip uninstall cellarbrain -y
```

## 2. Verify removal

```
py -3 -m cellarbrain --version
```

This should fail with `No module named cellarbrain`.

## 3. Clean up output directory

Remove the ETL output so the next install starts fresh:

```
Remove-Item -Recurse -Force output -ErrorAction SilentlyContinue
```

## 4. Confirm ready for reinstall

Run `py -3 -m pip show cellarbrain` — expect "not found". The workspace is now ready for a fresh install via the `install-cellarbrain` prompt.
