# arrows

Arrow-native ETL toolkit with pluggable components. `src/` layout, built and
published with uv.

## The invariant that matters most

**`import arrows` must stay cheap.** It imports no boto3, no psycopg2, no
pandas, no Google client. Component libraries are imported the first time a
component is *loaded*, not when the package is imported.

Two mechanisms keep that true, and both are easy to break by accident:

- `src/arrows/__init__.py` exposes submodules through a PEP 562 `__getattr__`,
  listed in `_LAZY_MODULES`. A plain `from . import s3` at the top of that file
  would defeat it.
- `src/arrows/components/*.py` register *metadata only*. The module holding the
  real implementation is named as a string in the spec and imported on load.

`tests/test_public_api.py:9` asserts that a bare `import arrows` leaves the heavy
modules out of `sys.modules`. If it fails, something grew an eager import.

## Decisions that look like bugs but are not

**No extras.** Every component's libraries are ordinary dependencies;
`pip install arrows` is the whole installation. `tests/test_packaging.py` fails
if anyone adds an `[project.optional-dependencies]` table. Install weight was
accepted knowingly in exchange for never having a half-installed state.

**`adbc-driver-postgresql==1.11.0` is pinned exactly.** Other releases have
broken Redshift. Loosening it needs testing against a real cluster first, not a
dependency cleanup pass.

**The version number lives only in `pyproject.toml`.** `__version__` reads it
back from the installed metadata — uv_build has no dynamic-version support, so
it cannot be done the other way round. Change it with `uv version --bump`, never
by hand, so `uv.lock` stays in step.

## Packaging facts worth knowing before editing

- Build backend is `uv_build`; `src/arrows` is picked up by convention.
- The sdist includes `tests/` and `examples/` via `source-include`. `docs/` is
  not in it.
- `README.md` is rendered on the PyPI project page, where **relative links are
  dead**. Repository links in it must be absolute GitHub URLs.
- `requires-python = ">=3.11"` has no upper bound, so newer interpreters are
  already promised support. When a new Python ships, add it to the `ci.yml`
  matrix *and* the classifiers in `pyproject.toml` together, or the package
  claims more than anyone tested.

## Conventions

- ruff, line length 120, single quotes. CI enforces both `ruff check .` and
  `ruff format --check .`, so run the formatter before pushing.
- Tests need no credentials. `uv run arrows doctor` is the one that talks to real
  services.

## Releasing

`docs/RELEASING.md` is the procedure, and `/release` walks through it. Two things
never to do: publish from a laptop (CI holds the only credential, via OIDC), and
tag anything other than `main`.
