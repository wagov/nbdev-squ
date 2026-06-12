# Contributing

## Commands
- `just install` - set up Python and JavaScript dependencies.
- `just test` - run the fast test suite.
- `just lint` - format and lint Python code.
- `just check` - run lint, type checks, and tests.
- `just build` - build the JavaScript bundle and Python package.

## Project notes
- Python package code lives in `src/wagov_squ/`; `src/nbdev_squ/` is compatibility-only.
- Configuration is loaded from Azure Key Vault with `SQU_CONFIG=keyvault/tenantid`.
- External integrations include Azure Sentinel, Runzero, Jira, AbuseIPDB, and Tenable.

## Standards
- Use Python 3.12+, ruff formatting, mypy, and pytest.
- Mark external-service tests with `@pytest.mark.integration`.
- Prefer simple library-backed code over custom abstractions.
- Validate essential business inputs near their boundary and let well-maintained libraries handle their own errors.
