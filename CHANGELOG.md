# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/George-Jiang/arrows/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/George-Jiang/arrows/releases/tag/v0.2.0
