import os
import adbc_driver_postgresql.dbapi as postgresql
import uuid
import boto3
import awswrangler as wr
import duckdb
import pandas as pd
import psycopg2
from jinja2 import Template
from .template_renderer import TemplateRenderer, render_template
from . import s3
from .auth import load_redshift_credentials


def get_connection():
    conn = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), 
                            database=os.getenv('REDSHIFT_DATABASE'),
                            user=os.getenv('REDSHIFT_USER'),
                            password=os.getenv('REDSHIFT_PASSWORD'),
                            port=os.getenv('REDSHIFT_PORT'))
    return conn


def get_boto3_session():
    boto3_session = boto3.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                  aws_session_token=os.getenv('AWS_SESSION_TOKEN')
                                  )
    return boto3_session


def unload(sql, s3_path=None, bucket=None, **kwargs):
    if isinstance(s3_path, s3.S3Dataset):
        dataset = s3_path
    else:
        dataset = s3.S3Dataset(s3_path=s3_path, bucket=bucket)
    try:
        dataset.clear_contents()
        conn = get_connection()
        
        boto3_session = get_boto3_session()

        wr.redshift.unload_to_files(
                                    sql=Template(sql).render(**kwargs),
                                    path=dataset.s3_path,
                                    con=conn,
                                    boto3_session=boto3_session
                                    )
        
        conn.close()
        
    except Exception as e:
            print(f'{e}')
            raise e
    return dataset


def fetch_arrow(sql, engine = 's3', bucket=None, **kwargs):
    if engine == 'adbc':
        host=os.getenv('REDSHIFT_HOST')
        database=os.getenv('REDSHIFT_DATABASE')
        user=os.getenv('REDSHIFT_USER')
        password=os.getenv('REDSHIFT_PASSWORD')
        port=os.getenv('REDSHIFT_PORT')
        
        try:
            conn = postgresql.connect(f"postgresql://{user}:{password}@{host}:{port}/{database}")
            cursor = conn.cursor()
            cursor.execute(Template(sql).render(**kwargs))
            arrow = cursor.fetch_arrow_table()
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f'{e}')
            raise e
        
        return arrow

    else:
        try:
            s3_dataset = unload(sql.format(**kwargs), bucket=bucket)
            arrow = s3_dataset.to_arrow()
        except Exception as e:
            raise e
        finally:
            s3_dataset.delete()
                
        return arrow
    
def fetch_dataframe(sql, engine='adbc', dtype_backend='numpy', **kwargs):
    arrow = fetch_arrow(sql, engine=engine, **kwargs)
    if dtype_backend == 'pyarrow':
        df = arrow.to_pandas(types_mapper=pd.ArrowDtype)
    else:
        df = arrow.to_pandas()    
    return df


def copy(table_name, s3_path, mode='append', **kwargs):
    boto3_session = get_boto3_session()
    conn = get_connection()
    schema, table = table_name.split('.')
    dataset = s3.S3Dataset(s3_path=s3_path)
    wr.redshift.copy_from_files(
                                path=dataset.s3_path,
                                con=conn,
                                table=table,
                                schema=schema,
                                boto3_session=boto3_session,
                                mode=mode,
                                **kwargs
                                )
    conn.close()
    print(f'Success: Data transfered to Redshift.')


def arrow_to_redshift(arrow, table_name, mode = 'append', bucket=None, **kwargs):
    dataset = s3.S3Dataset(bucket=bucket)
    
    try:
        dataset.from_arrow(arrow)
        dataset.to_redshift(table_name, mode=mode, **kwargs)

    except Exception as e:
        raise e
    finally:
        dataset.delete()
    
        

def execute_sql(sql, **kwargs):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        print(f'Running SQL...')
        print(sql)
        cursor.execute(Template(sql).render(**kwargs))
        conn.commit()
        print('SUCCESS: SQL executed.')
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
        
        
def execute_sql_file(sql_script_path, sql_script_folder=None, **kwargs):
    if sql_script_folder is not None:
        sql_script_path = sql_script_folder / sql_script_path
    sql = render_template(sql_script_path, **kwargs)
    execute_sql(sql)


