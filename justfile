# List available commands
default:
    @just --list

# Install project in development mode
install:
    uv sync --extra dev
    npm install
    npm run build
    @echo "Development environment ready!"

# Run tests (fast, integration, or with coverage)
test type="fast":
    #!/usr/bin/env bash
    case "{{type}}" in
        fast) uv run pytest -m "not slow and not integration" ;;
        integration) uv run pytest -m integration -v -s --tb=short ;;
        cov) uv run pytest --cov=src/nbdev_squ --cov-report=term-missing --cov-report=html ;;
        *) uv run pytest -s --tb=short ;;
    esac

# Lint and format code
lint:
    uv run ruff check --fix --unsafe-fixes src/ tests/
    uv run ruff format src/ tests/

# Type check
typecheck:
    uv run mypy src/

# Run all quality checks  
check: lint typecheck
    just test

# Build package
build: 
    npm run build  # Build JS bundle first
    uv build

# Create release from current version (use 'just bump patch' first)  
release message="" push="true":
    #!/usr/bin/env bash
    just build
    VERSION=$(uv run python -c "import wagov_squ; print(wagov_squ.__version__)")
    MSG="{{message}}"
    [ -z "$MSG" ] && MSG="Release v$VERSION"
    git add -A && git commit -m "$MSG" && git tag -f "v$VERSION"
    [ "{{push}}" = "true" ] && git push && git push --tags -f || echo "Ready to push: git push && git push --tags"

# Quick version bumps (patch/minor/major)
bump type:
    #!/usr/bin/env bash
    NEW=$(uv run python -c "import wagov_squ, semver; print(getattr(semver.VersionInfo.parse(wagov_squ.__version__), 'bump_{{type}}')())")
    sed -i "s/__version__ = \".*\"/__version__ = \"$NEW\"/" src/wagov_squ/__init__.py
    echo "__version__ = \"$NEW\""



# Clean build artifacts and analyze complexity  
clean:
    rm -rf dist/ build/ *.egg-info/ .coverage htmlcov/ node_modules/
    find . -type d -name __pycache__ -delete
    find . -type f -name "*.pyc" -delete

complexity:
    scc --by-file .

# Test Jira export with configurable options
test-jira days="7" batch_size="100" dry_run="true" force_refresh="false" include_today="false":
    uv run az account set --subscription "DGov Sentinel"
    uv run python -c "from wagov_squ.legacy import export_jira_issues; export_jira_issues(days_to_export={{ days }}, batch_size={{ batch_size }}, dry_run={{ dry_run }}, force_refresh={{ force_refresh }}, include_today={{ include_today }})"