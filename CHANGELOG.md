# Changelog

## [1.4.0] - 2025-09-16

### Added
- Ibis support for scalable data processing (`Fmt.ibis`)
- Modern uv-based package structure  
- Integration test suite for Azure and Jira scenarios
- Jira v3 API support (transparent upgrade from v2)

### Changed
- Migrated from nbdev to standard Python package
- Build system: uv, pytest, ruff, mypy with streamlined setup
- All API functions support pandas/ibis output formats
- Separated fast unit tests from integration tests
- Simplified Azure CLI extension handling

### Technical
- Zero breaking changes for existing users
- Dependencies: ibis-framework, duckdb, pip (for Azure CLI)
- Clean documentation and contributor-friendly setup
- Enhanced test coverage with comprehensive JQL testing

## [1.3.0] - Previous
Updated to include api clients and msticpy. Python 3.11 support.


