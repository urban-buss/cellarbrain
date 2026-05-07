# Homebrew Publishing

Create and maintain a Homebrew tap for installing Cellarbrain on macOS.

> **Note:** A Homebrew formula for cellarbrain does not yet exist. This documents how to create and maintain one.

## Create a Homebrew Tap

```bash
# Create the tap repository on GitHub: urban-buss/homebrew-cellarbrain
git clone https://github.com/urban-buss/homebrew-cellarbrain.git
cd homebrew-cellarbrain
mkdir Formula
```

## Write the Formula

Create `Formula/cellarbrain.rb`:

```ruby
class Cellarbrain < Formula
  include Language::Python::Virtualenv

  desc "AI sommelier for your wine cellar — ETL, DuckDB queries, MCP server"
  homepage "https://github.com/urban-buss/cellarbrain"
  url "https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz"
  sha256 "REPLACE_WITH_ACTUAL_SHA256"
  license "MIT"

  depends_on "python@3.13"

  resource "pyarrow" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  # ... additional resources (use poet to generate)

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match "cellarbrain", shell_output("#{bin}/cellarbrain --help")
  end
end
```

## Generate Resource Stanzas

```bash
pip install homebrew-pypi-poet
poet cellarbrain
```

Outputs all `resource` blocks for cellarbrain's dependencies.

## Get the SHA256

```bash
curl -sL https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz | shasum -a 256
```

## Test Locally

```bash
brew install --build-from-source ./Formula/cellarbrain.rb
cellarbrain --help
brew uninstall cellarbrain
```

## Publish

```bash
cd homebrew-cellarbrain
git add Formula/cellarbrain.rb
git commit -m "cellarbrain 0.3.0"
git push origin main
```

Users install via:

```bash
brew tap urban-buss/cellarbrain
brew install cellarbrain
```

## Updating for New Releases

```bash
cd homebrew-cellarbrain

# 1. Update URL and version in Formula/cellarbrain.rb
# 2. Update sha256 hash:
curl -sL https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz | shasum -a 256
# 3. Re-run poet if dependencies changed

git add Formula/cellarbrain.rb
git commit -m "cellarbrain 0.3.1"
git push origin main
```

Users update: `brew update && brew upgrade cellarbrain`

## Optional Extras

The base formula installs core dependencies only. For extras:

```bash
CELLARBRAIN_VENV=$(brew --prefix cellarbrain)/libexec
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[sommelier]"
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[dashboard]"
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[ingest]"
```

## Next Steps

- [Release Process](release-process.md) — Version and tag workflow
- [PyPI](pypi.md) — PyPI publishing
