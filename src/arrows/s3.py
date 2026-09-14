"""S3 datasets: Parquet in, Arrow/Polars/DuckDB out.

Credentials and the filesystem come from the ``s3`` component; this module only
holds the data API.
"""

from __future__ import annotations

import uuid
from typing import Any

from .core.engine import get_duckdb, quote_literal
from .core.session import get as _get_component
from .core.session import login as _login
from .utils import _parse_self_sql

__all__ = [
    'S3Dataset',
    'arrow_to_s3',
    'create_dataset',
    'format_s3_path',
    'get_dataset',
    'login',
    'polars_to_s3',
    'set_default_bucket_name',
]


def _component() -> Any:
    return _get_component('s3')


def set_default_bucket_name(default_bucket_name: str) -> None:
    """Set the bucket used when a dataset is created without an explicit path."""
    from .core.session import default_session

    session = default_session()
    session.config.default_bucket = default_bucket_name
    if session.is_loaded('s3'):
        session.get('s3').default_bucket = default_bucket_name


def format_s3_path(s3_path: str) -> str:
    if not s3_path.endswith('/'):
        s3_path = s3_path + '/'
    if s3_path[:5].lower() != 's3://':
        s3_path = 's3://' + s3_path
    return s3_path


def create_dataset(s3_path: str | None = None, bucket: str | None = None) -> S3Dataset:
    return S3Dataset(s3_path=s3_path, bucket=bucket)


def get_dataset(s3_path: str) -> S3Dataset:
    return S3Dataset(s3_path=s3_path)


def arrow_to_s3(arrow, s3_path: str | None = None, bucket: str | None = None, engine: str = 'duckdb') -> S3Dataset:
    dataset = S3Dataset(s3_path=s3_path, bucket=bucket)
    dataset.from_arrow(arrow, engine=engine)
    return dataset


def polars_to_s3(df, s3_path: str | None = None, bucket: str | None = None) -> S3Dataset:
    dataset = S3Dataset(s3_path=s3_path, bucket=bucket)
    dataset.from_polars(df)
    return dataset


class S3Dataset:
    """A prefix in S3 holding one Parquet dataset."""

    def __init__(self, s3_path: str | None = None, bucket: str | None = None):
        component = _component()
        if s3_path is None:
            bucket = bucket or component.default_bucket
            if not bucket:
                raise ValueError(
                    'No S3 path given and no default bucket configured. '
                    'Pass bucket=..., call arrows.s3.set_default_bucket_name(...), '
                    'or set ARROWS_DEFAULT_BUCKET.'
                )
            s3_path = f's3://{bucket}/{uuid.uuid4()}/'
        self.s3_path = format_s3_path(s3_path)

    @property
    def s3(self):
        """The pyarrow S3 filesystem owned by the ``s3`` component."""
        return _component().filesystem

    def __repr__(self) -> str:
        return f'S3Dataset: {self.s3_path}'

    # -- read --------------------------------------------------------------
    def to_arrow(self, engine: str = 'pyarrow'):
        if engine == 'pyarrow':
            import pyarrow.parquet as pq

            return pq.read_table(self.s3_path)
        return self.to_duckdb().to_arrow_table()

    def to_duckdb(self):
        return get_duckdb().from_parquet(f'{self.s3_path}*.parquet')

    def to_polars(self, lazy: bool = False):
        import polars as pl

        return pl.scan_parquet(self.s3_path) if lazy else pl.read_parquet(self.s3_path)

    # -- write -------------------------------------------------------------
    def from_arrow(self, arrow, engine: str = 'pyarrow') -> None:
        self.clear_contents()
        if engine == 'pyarrow':
            import pyarrow.parquet as pq

            pq.write_to_dataset(arrow, self.s3_path)
        else:
            get_duckdb().execute(f"COPY arrow TO '{self.s3_path[:-1]}' (FORMAT parquet, FILE_SIZE_BYTES '1G')")

    def from_polars(self, df) -> None:
        import polars as pl

        self.clear_contents()
        partition_info = pl.PartitionMaxSize(base_path=self.s3_path, max_size=512_000)
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(partition_info)
        else:
            df.write_parquet(partition_info)

    def from_redshift(self, sql: str, **kwargs) -> None:
        from . import redshift

        self.clear_contents()
        redshift.unload(sql, s3_path=self, **kwargs)

    def to_redshift(self, table_name: str, mode: str = 'append', **kwargs) -> None:
        from . import redshift

        redshift.copy(table_name, self.s3_path, mode=mode, **kwargs)

    # -- query -------------------------------------------------------------
    def query(self, sql: str, **kwargs):
        """Run SQL against the dataset, where ``self`` refers to this dataset."""
        from jinja2 import Template

        rendered = Template(sql).render(**kwargs)
        source = quote_literal(f'{self.s3_path}*.parquet')
        body = _parse_self_sql(rendered, 'self', 'temp_s3_dataset_table')
        # `source` is a quoted literal; `body` is the caller's own SQL.
        return get_duckdb().sql(f'WITH temp_s3_dataset_table AS (SELECT * FROM {source})\n{body}')  # noqa: S608

    def sql(self, sql: str, **kwargs):
        """Run SQL against a lazily scanned Polars frame of this dataset."""
        from jinja2 import Template

        df = self.to_polars(lazy=True)  # noqa: F841 - referenced by DuckDB replacement scan
        rendered = _parse_self_sql(Template(sql).render(**kwargs), 'self', 'df')
        return get_duckdb().sql(rendered)

    # -- lifecycle ---------------------------------------------------------
    def delete(self) -> None:
        from pyarrow.fs import FileType

        path = self.s3_path[5:]
        file_type = self.s3.get_file_info(path).type
        if file_type == FileType.Directory:
            self.s3.delete_dir(path)
        elif file_type == FileType.File:
            self.s3.delete_file(path)
            if self.s3.get_file_info(path).type == FileType.Directory:
                self.s3.delete_dir(path)

    def clear_contents(self) -> None:
        from pyarrow.fs import FileType

        path = self.s3_path[5:]
        if self.s3.get_file_info(path).type == FileType.Directory:
            self.s3.delete_dir_contents(path)


def login(
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    region: str | None = None,
    bucket: str | None = None,
    profile: str | None = None,
    *,
    save: bool = False,
    prompt: bool | None = None,
    **kwargs,
):
    """Supply AWS credentials and load the s3 component.

    Prefer ``profile=`` (or passing nothing at all, so boto3 uses the instance
    role) over static keys: those rotate on their own, and DuckDB is then given
    the credential chain rather than the key material.
    """
    return _login(
        's3',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region=region,
        bucket=bucket,
        profile=profile,
        save=save,
        prompt=prompt,
        **kwargs,
    )
