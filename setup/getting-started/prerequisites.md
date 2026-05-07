# Prerequisites

Required software for running Cellarbrain.

## Target Platform

| Spec | Value |
|------|-------|
| OS | macOS 15+ (Sequoia) or Windows 10+ or Linux |
| Python | 3.11+ (recommended: 3.13) |
| Shell | zsh (macOS) / PowerShell (Windows) / bash (Linux) |

## macOS (Homebrew)

```bash
# Package manager
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"

# Python, Git
brew install python@3.13 git

# VS Code (optional, for development)
brew install --cask visual-studio-code
```

## Windows

Install [Python 3.13](https://www.python.org/downloads/) and [Git](https://git-scm.com/download/win). Optionally install [VS Code](https://code.visualstudio.com/).

## VS Code Extensions (Development)

```bash
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension GitHub.copilot
code --install-extension GitHub.copilot-chat
```

| Extension | Purpose |
|-----------|---------|
| Python | Language support, debugging, test runner |
| Pylance | Type checking, IntelliSense, go-to-definition |
| GitHub Copilot | AI code completion |
| GitHub Copilot Chat | AI chat with MCP tool access |

## Verify

```bash
python3 --version   # Python 3.11+
git --version        # any recent version
```

## Next Steps

- [Installation](installation.md) — Install cellarbrain
- [Quick Start](quick-start.md) — Zero-to-working in 5 minutes
