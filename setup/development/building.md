# Building Distribution Packages

How to build sdist and wheel packages for Cellarbrain.

## Prerequisites

```bash
pip install build
```

## Build

```bash
python -m build
```

Creates:

```
dist/
├── cellarbrain-X.Y.Z.tar.gz              # Source distribution
└── cellarbrain-X.Y.Z-py3-none-any.whl    # Wheel (binary)
```

## Verify

```bash
# Check wheel contents
unzip -l dist/cellarbrain-*.whl | head -30

# Test install in a fresh venv
python3 -m venv /tmp/test-install
source /tmp/test-install/bin/activate
pip install dist/cellarbrain-*.whl
cellarbrain --help
deactivate
rm -rf /tmp/test-install
```

## Build Metadata

Configured in `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[project.scripts]
cellarbrain = "cellarbrain.cli:main"
```

The `cellarbrain` CLI command calls `src/cellarbrain/cli.py:main()`.

## Next Steps

- [Release Process](../publishing/release-process.md) — Version, tag, publish
- [PyPI](../publishing/pypi.md) — Publish to PyPI
