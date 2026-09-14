# Releasing arrows

Publishing to PyPI happens in GitHub Actions, triggered by a version tag, and
waits for a human to approve it. Nothing is ever uploaded from a laptop.

## The short version

```bash
# 1. On a branch: bump the version and write the changelog
git checkout main && git pull
git checkout -b release/0.3.0
uv version --bump minor              # patch | minor | major
# then edit CHANGELOG.md by hand — see "Writing the changelog" below
git commit -am "Release 0.3.0"
git push -u origin release/0.3.0
gh pr create

# 2. Merge it once CI is green, then tag the merge result on main
git checkout main && git pull
git tag -a v0.3.0 -m "Release 0.3.0"
git push origin v0.3.0

# 3. Approve the `pypi` environment in Actions
```

## Three rules

**Tag `main`, never a PR branch.** Merging creates a new commit with a different
hash, so a tag made on the branch points at code that is not in main's history.
The tag has to name the commit that was actually released.

**The version lives in `pyproject.toml` and nowhere else.** `arrows.__version__`
reads it back from the installed metadata. Use `uv version --bump`, which also
updates `uv.lock`, rather than editing the number by hand.

**Bump the version before tagging.** The release workflow refuses a tag that
disagrees with `uv version --short`, because a wrong version cannot be taken
back off PyPI.

## Writing the changelog

`CHANGELOG.md` collects entries under `## [Unreleased]` as pull requests land.
Releasing turns that heading into the version being released, and leaves a fresh
empty one behind:

```markdown
## [Unreleased]

## [0.3.0] - 2026-10-02

### Added

- ClickHouse component.
```

Then update the link definitions at the bottom of the file, so the version
numbers link to the diff on GitHub:

```markdown
[Unreleased]: https://github.com/George-Jiang/arrows/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/George-Jiang/arrows/compare/v0.2.0...v0.3.0
```

Write it for someone deciding whether to upgrade: what changed for them, not
which files were touched. Breaking changes go first and say what to do instead.

## Choosing the number

While the project is on `0.x`, a minor bump is allowed to break things.

- **patch** (`0.2.0` → `0.2.1`) — bug fixes only
- **minor** (`0.2.0` → `0.3.0`) — new components or features, and any breaking
  change for as long as the major version is 0
- **major** (`0.x` → `1.0.0`) — reserved for declaring the API stable

## What the pipeline does

`.github/workflows/release.yml` runs on `v*` tags. Its first job:

1. `uv lock --check`, ruff, and the test suite.
2. Refuses the tag if it disagrees with the project version.
3. `uv build`, then `twine check` on both artifacts.
4. Installs the built wheel into an isolated environment, imports `arrows` and
   runs the CLI — the same thing a user does after `pip install arrows`.

The second job uploads to PyPI through [trusted publishing][tp] (OIDC), so there
is no API token in the repository or in anyone's shell. It is gated on the
`pypi` GitHub environment, which requires a reviewer's approval before it runs.

[tp]: https://docs.pypi.org/trusted-publishers/

## After publishing

```bash
uv run --isolated --no-project --with arrows python -c "import arrows; print(arrows.__version__)"
```

Optionally give the tag release notes, which is separate from PyPI:

```bash
gh release create v0.3.0 --notes-from-tag
```

## When something goes wrong

**The workflow fails on the version check.** The tag and `pyproject.toml`
disagree. Nothing was published. Delete the tag, fix the version on main, tag
again:

```bash
git push origin :refs/tags/v0.3.0
git tag -d v0.3.0
```

**You tagged too early.** As long as you have not approved the `pypi`
environment, nothing has reached PyPI. Cancel the run in Actions and delete the
tag as above.

**The wrong version reached PyPI.** It cannot be replaced — a version number is
permanent, and re-uploading the same one is rejected. Fix the problem and
release the next patch version. If the release is actively harmful, yank it on
PyPI (`Manage` → `Yank`), which hides it from new installs while leaving
existing pins working.

**A dependency breaks on a new Python.** CI covers 3.11 through 3.14, but
`requires-python = ">=3.11"` has no upper bound, so newer interpreters are
already promised support. When a new Python is released, add it to the CI matrix
and to the classifiers in `pyproject.toml` together.

## One-time setup

Already done for this repository. Recorded here in case it has to be rebuilt, or
for a fork that wants its own releases.

**On PyPI** — with the project not yet existing, register a *pending* publisher
at Account settings → Publishing:

| Field | Value |
| --- | --- |
| Owner | `George-Jiang` |
| Repository | `arrows` |
| Workflow | `release.yml` |
| Environment | `pypi` |

It becomes a regular trusted publisher after the first successful upload.

**On GitHub** — Settings → Environments → New environment named `pypi`, with
**Required reviewers** set. This is what creates the approval gate. A workflow
that names an environment which does not exist gets one created automatically
*with no protection at all*, so the upload would proceed unreviewed.

## Rehearsing on TestPyPI

Rarely needed: `twine check` plus the isolated install in the pipeline already
cover metadata and packaging mistakes. For a change to packaging itself,
TestPyPI is a [separate site with its own account and its own API token][test]
(username `__token__`, password the token):

```bash
read -rs UV_PUBLISH_TOKEN && export UV_PUBLISH_TOKEN
uv publish --index testpypi

uv run --isolated --no-project \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  --with arrows python -c "import arrows; print(arrows.__version__)"
```

The second index is required: the dependencies do not exist on TestPyPI.

[test]: https://test.pypi.org/account/register/
