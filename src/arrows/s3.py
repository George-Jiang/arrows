from pyarrow.fs import S3FileSystem, FileType
import os
import duckdb
import polars as pl
from . import redshift
import uuid
import pyarrow.parquet as pq
import polars as pl
from jinja2 import Template
from .utils import _parse_self_sql
from .auth import load_aws_credentials

def set_default_bucket_name(default_bucket_name):
    os.environ.update({'DEFAULT_BUCKET_NAME': default_bucket_name})

def format_s3_path(s3_path:str):
    if not s3_path.endswith('/'):
        s3_path = s3_path + '/'
    if s3_path[:5].lower() != 's3://':
        s3_path = 's3://' + s3_path
    return s3_path


def create_dataset(s3_path=None, bucket=None):
    dataset = S3Dataset(s3_path=s3_path, bucket=bucket)
    return dataset


def get_dataset(s3_path):
    dataset = S3Dataset(s3_path=s3_path)
    return dataset


def arrow_to_s3(arrow, s3_path=None, bucket=None, engine='duckdb'):
    dataset = S3Dataset(s3_path=s3_path, bucket=bucket)
    dataset.from_arrow(arrow, engine=engine)
    return dataset

def polars_to_s3(df, s3_path=None, bucket=None):
    dataset = S3Dataset(s3_path=s3_path, bucket=bucket)
    dataset.from_polars(df)
    return dataset


class S3Dataset():
    def __init__(self, s3_path=None, bucket=None):
        self.s3 = S3FileSystem()
        bucket = bucket if bucket else os.getenv('DEFAULT_BUCKET_NAME')
        s3_path = s3_path if s3_path is not None else f's3://{bucket}/{uuid.uuid4()}/'
        self.s3_path = format_s3_path(s3_path)
        
    def __repr__(self):
        return f'S3Dataset: {self.s3_path}'
    
    def to_redshift(self, table_name, mode='append', **kwargs):
        redshift.copy(table_name, self.s3_path, mode=mode, **kwargs)
    
    def to_arrow(self, engine='pyarrow'):
        #fetch_arrow
        if engine == 'pyarrow':
            arrow = pq.read_table(self.s3_path)
        else:
            arrow = self.to_duckdb().to_arrow_table()
        return arrow
    
    def to_duckdb(self):
        # fetch duckdb connection instance using duckdb
        duckdb_relation = duckdb.from_parquet(f'{self.s3_path}*.parquet')
        return duckdb_relation
    
    def to_polars(self, lazy=False):
        # fetch polars dataframe using polars
        if lazy == False:
            df = pl.read_parquet(self.s3_path)
        else:
            df = pl.scan_parquet(self.s3_path)
        return df
    
    def from_arrow(self, arrow, engine='pyarrow'):
        self.clear_contents()   
        
        try:
            if engine == 'pyarrow':
                pq.write_to_dataset(arrow, self.s3_path)
            else:
                duckdb.execute(f'''
                                COPY arrow TO
                                '{self.s3_path[:-1]}'
                                (
                                    FORMAT parquet,
                                    FILE_SIZE_BYTES '1G'
                                )
                                ''')
        except Exception as e:
            print(f'{e}')
            raise e
        
    def from_polars(self, df:pl.DataFrame|pl.LazyFrame):
        self.clear_contents()  
        
        partition_info = pl.PartitionMaxSize(base_path=self.s3_path, max_size=512_000)
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(partition_info)
        else:
            df.write_parquet(partition_info)
        
    def from_redshift(self, sql, **kwargs):
        self.clear_contents()
        redshift.unload(sql, s3_path=self, **kwargs)
        
    def query(self, sql, **kwargs):
        sql = Template(sql).render(**kwargs)
        template = Template('''
        with temp_s3_dataset_table as (select * from '{{ s3_path + '*.parquet'}}')
        {{ _parse_self_sql(sql, 'self', 'temp_s3_dataset_table') }}
        ''')
        sql = template.render(sql = sql , s3_path = self.s3_path, _parse_self_sql = _parse_self_sql)
        return duckdb.sql(sql)
    
    def sql(self, sql, **kwargs):
        df = self.to_polars(lazy=True)
        sql = Template(sql).render(**kwargs)
        sql = _parse_self_sql(sql, 'self', 'df')
        return duckdb.sql(sql)
    
    def delete(self):
        try:
            path = self.s3_path[5:]
            file_type = self.s3.get_file_info(path).type
            if file_type == FileType.Directory:
                self.s3.delete_dir(path)
            elif file_type == FileType.File:
                self.s3.delete_file(path)
                if self.s3.get_file_info(path).type == FileType.Directory:
                    self.s3.delete_dir(path)
        except Exception as e:
            raise e
        
    def clear_contents(self):
        try:
            path = self.s3_path[5:]
            file_type = self.s3.get_file_info(path).type
            if file_type == FileType.Directory:
                self.s3.delete_dir_contents(path)
        except Exception as e:
            raise e



