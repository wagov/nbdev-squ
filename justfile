# List available commands
default:
    @just --list

# Install project in development mode
install:
    uv sync --dev
    npm install
    npm run build
    @echo "✅ Development environment ready!"

# Run tests
test:
    uv run pytest -s --tb=short

# Run tests with coverage  
test-cov:
    uv run pytest --cov=src/nbdev_squ --cov-report=term-missing --cov-report=html

# Run fast tests only (skip slow/integration tests)
test-fast:
    uv run pytest -m "not slow and not integration"

# Run integration tests (requires SQU_CONFIG environment variable)
test-integration:
    uv run pytest -m integration -v -s --tb=short

# Lint and format code
lint:
    uv run ruff check src/ tests/
    uv run ruff format src/ tests/

# Type check
typecheck:
    uv run mypy src/

# Run all quality checks
check: lint typecheck test

# Build package
build: 
    npm run build  # Build JS bundle first
    uv build

# Create release from current version (use 'just bump patch' first)  
release message="":
    #!/usr/bin/env bash
    just build
    VERSION=$(grep '__version__ = ' src/wagov_squ/__init__.py | cut -d'"' -f2)
    echo "Creating release for version v$VERSION"
    if [ "{{message}}" = "" ]; then
        COMMIT_MSG="Release v$VERSION"
    else
        COMMIT_MSG="{{message}}"
    fi
    git add -A && git commit -m "$COMMIT_MSG" && git tag "v$VERSION"
    echo "✅ Ready to push with: git push && git push --tags"
    echo "GitHub Actions will automatically publish to PyPI"

# Quick version bumps (patch/minor/major)
bump type:
    #!/usr/bin/env python3
    import re, sys
    with open('src/wagov_squ/__init__.py', 'r') as f: content = f.read()
    match = re.search(r'__version__ = "(\d+)\.(\d+)\.(\d+)"', content)
    if not match: sys.exit("Could not find version")
    major, minor, patch = map(int, match.groups())
    if "{{type}}" == "patch": new = f"{major}.{minor}.{patch+1}"
    elif "{{type}}" == "minor": new = f"{major}.{minor+1}.0"  
    elif "{{type}}" == "major": new = f"{major+1}.0.0"
    else: sys.exit("Use: just bump patch|minor|major")
    with open('src/wagov_squ/__init__.py', 'w') as f:
        f.write(re.sub(r'__version__ = "[^"]*"', f'__version__ = "{new}"', content))
    print(f"Bumped version to {new}")



# Analyze code complexity with scc
complexity:
    scc --by-file .


# Clean build artifacts
clean:
    rm -rf dist/ build/ *.egg-info/ .coverage htmlcov/
    find . -type d -name __pycache__ -delete
    find . -type f -name "*.pyc" -delete