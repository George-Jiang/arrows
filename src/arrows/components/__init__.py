"""Built-in components.

Only the :class:`ComponentSpec` metadata is imported here; the modules that
define the components — and their heavy third-party dependencies — are imported
the first time a component is actually loaded.

To add a component:

1. Copy ``sqlite.py``, implement ``setup``/``close``/``health_check``.
2. Declare ``requires`` (secret keys) and ``depends_on`` (other components).
3. Export ``COMPONENT`` and a ``SPEC``.
4. Register the ``SPEC`` below — or, from a separate distribution, advertise it
   through the ``arrows.components`` entry-point group; nothing in arrows needs
   to change.
"""

from __future__ import annotations

from ..core.component import ComponentSpec
from ..core.registry import register

__all__ = ['BUILTIN_SPECS']

BUILTIN_SPECS: tuple[ComponentSpec, ...] = (
    ComponentSpec(
        name='aws',
        module='arrows.components.aws',
        summary='boto3 session + DuckDB S3 credential chain (shared by s3 and redshift)',
    ),
    ComponentSpec(
        name='s3',
        module='arrows.components.s3',
        summary='S3 datasets in Parquet, queryable with DuckDB/Polars',
        depends_on=('aws',),
    ),
    ComponentSpec(
        name='redshift',
        module='arrows.components.redshift',
        summary='Amazon Redshift: query, UNLOAD to S3, COPY from S3',
        depends_on=('aws',),
    ),
    ComponentSpec(
        name='google',
        module='arrows.components.google',
        summary='Google OAuth credentials shared by gmail and google_sheets',
        aliases=('google_auth',),
    ),
    ComponentSpec(
        name='google_sheets',
        module='arrows.components.google_sheets',
        summary='Read and write Google Sheets as Arrow/Polars/DuckDB',
        depends_on=('google',),
        aliases=('sheets', 'googlesheets'),
    ),
    ComponentSpec(
        name='gmail',
        module='arrows.components.gmail',
        summary='Send email through the Gmail API',
        depends_on=('google',),
    ),
    ComponentSpec(
        name='sqlite',
        module='arrows.components.sqlite',
        summary='Local SQLite files, read and written through DuckDB (reference component)',
    ),
)

for _spec in BUILTIN_SPECS:
    register(_spec, replace=True)
