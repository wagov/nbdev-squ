# Development Guide

## Quick Commands
- `just install` - Setup dev environment
- `just test` - Run all tests  
- `just test-fast` - Unit tests only
- `just test-integration` - Azure/Jira tests (needs SQU_CONFIG)
- `just lint` - Format and check code
- `just check` - Full quality check
- `just build` - Build package

## Project Structure
- **Package**: `src/nbdev_squ/` - Standard Python layout
- **Core modules**: api (SIEM queries), core (auth/caching), clients (external APIs)
- **Config**: Azure Key Vault via `SQU_CONFIG=keyvault/tenantid`
- **APIs**: Azure Sentinel, Runzero, Jira v3, AbuseIPDB, Tenable

## Development Standards
- Python 3.12+, ruff formatting, mypy type checking
- pytest with `@pytest.mark.integration` for external dependencies  
- Import order: stdlib, third-party, local
- Use library solutions over custom implementations
