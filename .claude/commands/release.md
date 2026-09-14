---
description: Cut a new release — bump the version, write the changelog, open the release PR
---

Walk the release procedure in `docs/RELEASING.md`. Read it first; it is the
source of truth and may have moved on since this command was written.

Bump type from the user: $ARGUMENTS (patch, minor, or major — ask if absent).

1. Refuse to start unless `main` is checked out, clean, and up to date with
   `origin`. Releasing from a stale or dirty tree is how the wrong code ships.
2. Branch `release/<new version>`.
3. `uv version --bump <type>`, which updates `pyproject.toml` and `uv.lock`.
4. Write the `CHANGELOG.md` entry: turn `## [Unreleased]` into the new version
   with today's date, leave a fresh empty `[Unreleased]` above it, and fix the
   link definitions at the bottom of the file. Read `git log` since the previous
   tag to draft the entries — write them for someone deciding whether to
   upgrade, so breaking changes come first and say what to do instead.
5. `uv run ruff check . && uv run ruff format --check . && uv run pytest`.
6. `uv build` and smoke-test the wheel in an isolated environment, the way
   `docs/RELEASING.md` describes.
7. Commit, push, and open the PR.

Then stop, and tell the user what is left for them to do by hand: merge the PR,
pull `main`, tag it, push the tag, and approve the `pypi` environment. **Never
create or push the tag yourself** — pushing it is what triggers publication, and
that decision stays with a human.
