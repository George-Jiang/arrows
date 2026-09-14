"""Amazon Redshift: query, UNLOAD to S3, COPY from S3.

Connection settings and credentials come from the ``redshift`` component; this
module only holds the data API.
"""

from __future__ import annotations

from typing import Any

from .core.session import get as _get_component
from .core.session import login as _login
from .template_renderer import render_template

__all__ = [
    'arrow_to_redshift',
    'copy',
    'execute_sql',
    'execute_sql_file',
    'fetch_arrow',
    'fetch_dataframe',
    'get_boto3_session',
    'get_connection',
    'login',
    'unload',
]


def _component() -> Any:
    return _get_component('redshift')


def _render(sql: str, **kwargs) -> str:
    from jinja2 import Template

    return Template(sql).render(**kwargs) if kwargs else sql


def get_connection():
    """Open a new psycopg2 connection to Redshift."""
    return _component().connect()


def get_boto3_session():
    """The boto3 session owned by the ``aws`` component."""
    return _get_component('aws').boto3_session


def unload(sql: str, s3_path=None, bucket: str | None = None, **kwargs):
    """UNLOAD a query to Parquet files in S3 and return the dataset."""
    from . import s3

    dataset = s3_path if isinstance(s3_path, s3.S3Dataset) else s3.S3Dataset(s3_path=s3_path, bucket=bucket)
    import awswrangler as wr

    dataset.clear_contents()
    connection = get_connection()
    try:
        wr.redshift.unload_to_files(
            sql=_render(sql, **kwargs),
            path=dataset.s3_path,
            con=connection,
            boto3_session=get_boto3_session(),
        )
    finally:
        connection.close()
    return dataset


def fetch_arrow(sql: str, engine: str = 's3', bucket: str | None = None, **kwargs):
    """Run a query and return an Arrow table.

    ``engine='s3'`` goes through UNLOAD (best for large results);
    ``engine='adbc'`` streams directly over the wire (best for small ones).
    """
    if engine == 'adbc':
        import adbc_driver_postgresql.dbapi as postgresql

        connection = postgresql.connect(_component().dsn())
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_render(sql, **kwargs))
                return cursor.fetch_arrow_table()
            finally:
                cursor.close()
        finally:
            connection.close()

    dataset = unload(sql, bucket=bucket, **kwargs)
    try:
        return dataset.to_arrow()
    finally:
        dataset.delete()


def fetch_dataframe(sql: str, engine: str = 'adbc', dtype_backend: str = 'numpy', **kwargs):
    """Run a query and return a pandas DataFrame."""
    arrow = fetch_arrow(sql, engine=engine, **kwargs)
    if dtype_backend == 'pyarrow':
        import pandas as pd

        return arrow.to_pandas(types_mapper=pd.ArrowDtype)
    return arrow.to_pandas()


def copy(table_name: str, s3_path, mode: str = 'append', **kwargs) -> None:
    """COPY a Parquet dataset in S3 into ``schema.table``."""
    import awswrangler as wr

    from . import s3

    schema, table = table_name.split('.')
    dataset = s3_path if isinstance(s3_path, s3.S3Dataset) else s3.S3Dataset(s3_path=s3_path)
    connection = get_connection()
    try:
        wr.redshift.copy_from_files(
            path=dataset.s3_path,
            con=connection,
            table=table,
            schema=schema,
            boto3_session=get_boto3_session(),
            mode=mode,
            **kwargs,
        )
    finally:
        connection.close()


def arrow_to_redshift(arrow, table_name: str, mode: str = 'append', bucket: str | None = None, **kwargs) -> None:
    """Stage an Arrow table in S3 and COPY it into Redshift."""
    from . import s3

    dataset = s3.S3Dataset(bucket=bucket)
    try:
        dataset.from_arrow(arrow)
        dataset.to_redshift(table_name, mode=mode, **kwargs)
    finally:
        dataset.delete()


def execute_sql(sql: str, **kwargs) -> None:
    """Execute a statement in a transaction, rolling back on failure."""
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(_render(sql, **kwargs))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def execute_sql_file(sql_script_path, sql_script_folder=None, **kwargs) -> None:
    """Render a ``.sql`` Jinja template from disk and execute it."""
    from pathlib import Path

    if sql_script_folder is not None:
        sql_script_path = Path(sql_script_folder) / sql_script_path
    execute_sql(render_template(sql_script_path, **kwargs))


def login(
    host: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
    port: int | str | None = None,
    cluster_identifier: str | None = None,
    *,
    save: bool = False,
    prompt: bool | None = None,
    **kwargs,
):
    """Supply Redshift credentials and load the component.

    Pass what you have; anything omitted falls back to the configured secret
    providers, and in an interactive session you are prompted for the rest::

        arrows.redshift.login(host='...', user='analyst', password='...')
        arrows.redshift.login()                     # prompt for everything missing

    Values given here outrank the environment. ``save=True`` remembers them in
    the OS keychain. Pass ``cluster_identifier`` instead of ``password`` to mint
    short-lived IAM credentials per run.
    """
    return _login(
        'redshift',
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        cluster_identifier=cluster_identifier,
        save=save,
        prompt=prompt,
        **kwargs,
    )
