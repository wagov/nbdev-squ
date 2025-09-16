# Install project in development mode with uv
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

# Bump version and create release
release version:
    # Update version in pyproject.toml
    sed -i 's/version = "[^"]*"/version = "{{version}}"/' pyproject.toml
    # Update version in __init__.py  
    sed -i 's/__version__ = "[^"]*"/__version__ = "{{version}}"/' src/nbdev_squ/__init__.py
    uv build
    git add -A
    git commit -m "Release v{{version}}"
    git tag "v{{version}}"
    @echo "✅ Ready to publish with: uv publish"

# Publish to PyPI
publish:
    uv publish

# Analyze code complexity with scc
complexity:
    scc --by-file .

# Clean build artifacts
clean:
    rm -rf dist/ build/ *.egg-info/ .coverage htmlcov/
    find . -type d -name __pycache__ -delete
    find . -type f -name "*.pyc" -delete