# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-09-13

### Fixed

- add redshift-connector dependency 

## [0.2.1] - 2026-09-13

### Fixed

- **S3 reads and writes now use the credentials the `aws` component resolved.**
  Polars was building a `boto3.Session` of its own, which cannot see anything
  supplied through `login()`, `.env` or the keychain, so it fell back to the
  default profile — a working `to_duckdb()` next to a `to_polars()` failing on an
  expired SSO token. pyarrow had the same problem: `to_arrow()` and `from_arrow()`
  built a filesystem from their own credential chain instead of the component's.
- **Credentials are refreshed as they rotate.** The pyarrow filesystem cached a
  frozen copy for the life of the session, and DuckDB's S3 secret was registered
  once at load time, so both stopped working an hour into an SSO or
  instance-role session. Both now follow the current credentials.
- **`from_polars()` and `polars_to_s3()` worked at all.** They called
  `pl.PartitionMaxSize`, which no longer exists in the Polars version this
  package requires; `pl.PartitionBy` replaces it with the same behaviour.

## [0.2.0] - 2026-09-13

### Added

- Pluggable components: every integration is registered through a spec and
  loaded on demand, including components contributed by other packages through
  the `arrows.components` entry point.
- `arrows` CLI: `components`, `secrets` and `doctor`.
- Layered secret resolution: env → `.env` → local files → OS keychain → AWS
  Secrets Manager.
- Packaging metadata for the first PyPI release (MIT licence, classifiers,
  project URLs, `py.typed`).

### Changed

- `load_credentials()` is deprecated in favour of `load('s3', 'redshift')`, and
  warns when called with no arguments.

[Unreleased]: https://github.com/George-Jiang/arrows/compare/v0.2.1...HEAD
[0.2.2]: https://github.com/George-Jiang/arrows/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/George-Jiang/arrows/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/George-Jiang/arrows/releases/tag/v0.2.0
