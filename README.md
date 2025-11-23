# arrows

arrows is an ETL toolkit built around Apache Arrow for efficiently transferring and transforming data between different data sources.

## Features

- 🔄 **Data Format Conversion**: Seamless conversion between Apache Arrow, Pandas, Polars, and other data formats
- 📊 **Google Sheets Integration**: Easily read from and write to Google Sheets with SQL query support
- 🗄️ **Amazon Redshift Integration**: Efficient Redshift data querying, importing, and exporting
- ☁️ **AWS S3 Integration**: Convenient S3 data storage and reading with Parquet format support
- 📧 **Gmail Integration**: Send emails programmatically with template support
- 🔐 **Automatic Authentication Management**: Automatically loads and configures credentials for AWS, Redshift, and Google
- 📝 **SQL Template Rendering**: Dynamically generate SQL queries using Jinja2 template engine

## Installation

### Dependencies

- `duckdb` - High-performance analytical database
- `pyarrow` - Apache Arrow Python implementation
- `pandas` - Data analysis library
- `polars` - Fast dataframe library
- `google-api-python-client` - Google API client
- `google-auth` - Google authentication library
- `awswrangler` - AWS data tools
- `boto3` - AWS SDK
- `adbc-driver-postgresql` - PostgreSQL/Redshift ADBC driver
- `psycopg2` - PostgreSQL adapter
- `jinja2` - Template engine

## Configuration

The package get all the credentials from ENV variables. You can set the ENV variables yourself, or use the `auth` module to read from files.


Before using, you need to configure the following authentication information in the `~/.credentials/` directory.

### 1. AWS Credentials


File: `~/.credentials/aws_credentials.txt`

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SESSION_TOKEN=your_session_token
```

### 2. Redshift Credentials

File: `~/.credentials/redshift_credentials.json`

```json
{
  "REDSHIFT_HOST": "your-redshift-cluster.amazonaws.com",
  "REDSHIFT_DATABASE": "your_database",
  "REDSHIFT_USER": "your_username",
  "REDSHIFT_PASSWORD": "your_password",
  "REDSHIFT_PORT": "5439"
}
```

### 3. Google Credentials

File: `~/.credentials/google_token.json`

Configure Google OAuth token (JSON format) generated from authorized user info.

## Usage

### Google Sheets

#### Reading from Google Sheets

```python
from arrows import google_sheets

# Read data from Google Sheet as Arrow format
arrow = google_sheets.fetch_arrow(
    spreadsheet_id='your_spreadsheet_id',
    sheet_name='Sheet1',
    sheet_range='A1:D100',  # Optional
    all_varchar=False       # Optional: Treat all columns as VARCHAR
)

# Convert to Pandas DataFrame
df = arrow.to_pandas()

# Use SQL query
arrow = google_sheets.fetch_arrow(
    spreadsheet_id='your_spreadsheet_id',
    sheet_name='Sheet1',
    sql='''
        SELECT
            *
        FROM 
            self
        WHERE column1 > 100
        '''
)
```

#### Writing to Google Sheets

```python
from arrows import google_sheets

# Write Arrow data to Google Sheet
sheet = google_sheets.arrow_to_googlesheet(
    arrow=arrow,
    spreadsheet_id='your_spreadsheet_id',
    sheet_name='Sheet1'
)
```

#### Managing Spreadsheets and Sheets

```python
from arrows import google_sheets

# Create a new Spreadsheet
spreadsheet = google_sheets.create_spreadsheet(
    spreadsheet_name='My Spreadsheet',
    parent_id='parent_id' # Optional
)

# Get a Spreadsheet
spreadsheet = google_sheets.get_spreadsheet('spreadsheet_id')

# Create a new Sheet
sheet = spreadsheet.create_sheet('New Sheet')

# Get a Sheet
sheet = spreadsheet.get_sheet('Sheet1')

# Share Spreadsheet
spreadsheet.share(email='user@example.com', role='writer')

# Delete a Sheet
spreadsheet.delete_sheet('Sheet1')
```

### Amazon Redshift

#### Querying Data

```python
from arrows import redshift

# Query using S3 Unload engine (default, suitable for large data)
arrow = redshift.fetch_arrow(
    sql='SELECT * FROM my_table',
    engine='s3'
)

# Query using ADBC engine (fast for smaller datasets)
arrow = redshift.fetch_arrow(
    sql='SELECT * FROM my_table WHERE date > %(date)s',
    engine='adbc',
    date='2024-01-01'
)

# Get Pandas DataFrame (defaults to ADBC engine)
df = redshift.fetch_dataframe(
    sql='SELECT * FROM my_table',
    engine='adbc',
    dtype_backend='numpy'  # or 'pyarrow'
)
```

#### Importing Data to Redshift

```python
from arrows import redshift

# Import Arrow data to Redshift
redshift.arrow_to_redshift(
    arrow=arrow,
    table_name='schema.table_name',
    mode='append'  # or 'overwrite'
)

# Copy from S3 to Redshift
redshift.copy(
    table_name='schema.table_name',
    s3_path='s3://bucket/path/',
    mode='append'
)
```

#### Exporting Data to S3

```python
from arrows import redshift

# Export Redshift query results to S3
dataset = redshift.unload(
    sql='SELECT * FROM my_table',
    s3_path='s3://bucket/path/'
)
```

#### Executing SQL

```python
from arrows import redshift

# Execute SQL statement
redshift.execute_sql(
    sql='CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR)'
)

# Execute SQL file (supports Jinja2 templates)
redshift.execute_sql_file(
    sql_script_path='scripts/create_table.sql',
    table_name='my_table'
)
```

### AWS S3

#### Storing and Reading Data

```python
from arrows import s3

# Store Arrow data to S3
dataset = s3.arrow_to_s3(
    arrow=arrow,
    s3_path='s3://bucket/path/',
    bucket='my-bucket',  # Optional
    engine='duckdb'      # or 'pyarrow'
)

# Store Polars DataFrame to S3
dataset = s3.polars_to_s3(
    df=df,
    s3_path='s3://bucket/path/'
)

# Read data from S3
dataset = s3.get_dataset('s3://bucket/path/')
arrow = dataset.to_arrow()

# Convert to Pandas
df = dataset.to_duckdb().df()

# Convert to Polars
df = dataset.to_polars(lazy=False)
```

#### S3Dataset Operations

```python
from arrows import s3


# Get dataset
dataset = s3.get_dataset('s3://bucket/path/')

dataset = s3.S3Dataset(s3_path='s3://bucket/path/')


# Create dataset
dataset = s3.create_dataset(s3_path='s3://bucket/path/')

# Write from Arrow
dataset.from_arrow(arrow)

# Write from Polars
dataset.from_polars(df)

# Import from Redshift
dataset.from_redshift(sql='SELECT * FROM my_table')

# Export to Redshift
dataset.to_redshift('schema.table_name', mode='append')

# Query S3 data with SQL
result = dataset.query("SELECT * FROM self WHERE id > 100")

# Delete dataset
dataset.delete()

# Clear dataset contents
dataset.clear_contents()
```

### Gmail

#### Sending Emails

```python
from arrows import gmail

# Simple email
gmail.send_email(
    to=['user@example.com'],
    subject='Report',
    content='<h1>Hello</h1>',
    cc=['manager@example.com']
)

# Advanced usage with Email class
email = gmail.Email(
    subject='Monthly Report',
    to=['user@example.com'],
    sender='Data Team'
)

# Set content from template
email.from_template('path/to/template.html', variable='value')

# Send email
email.send()
```

### SQL Template Rendering

```python
from arrows.template_renderer import render_template

# Render SQL template
sql = render_template(
    'path/to/template.sql',
    table_name='my_table',
    date='2024-01-01'
)
```

## Core API

### Google Sheets

- `fetch_arrow()` - Read data from Google Sheet as Arrow format
- `arrow_to_googlesheet()` - Write Arrow data to Google Sheet
- `get_sheet()` - Get Sheet object
- `get_spreadsheet()` - Get Spreadsheet object
- `create_spreadsheet()` - Create new Spreadsheet

### Redshift

- `fetch_arrow()` - Query data from Redshift as Arrow format
- `fetch_dataframe()` - Query data from Redshift as DataFrame
- `arrow_to_redshift()` - Import Arrow data to Redshift
- `unload()` - Export Redshift query results to S3
- `copy()` - Copy data from S3 to Redshift
- `execute_sql()` - Execute SQL on Redshift
- `execute_sql_file()` - Execute SQL file

### S3

- `arrow_to_s3()` - Store Arrow data to S3
- `polars_to_s3()` - Store Polars DataFrame to S3
- `get_dataset()` - Get S3 dataset
- `create_dataset()` - Create new S3 dataset

### Gmail

- `send_email()` - Send an email
- `Email` - Class for constructing and sending emails

## Data Format Support

All modules support conversion between the following data formats:

- **Apache Arrow** - Core data format for efficient data transfer
- **Pandas DataFrame** - Via `arrow.to_pandas()` or `df()` method
- **Polars DataFrame** - Via `arrow.to_polars()` or `pl()` method
- **DuckDB Relation** - Via `to_duckdb()` method with SQL query support

## Notes

1. **Credentials**: Ensure all credential files are properly configured in the `~/.credentials/` directory
2. **Google Sheets Permissions**: Appropriate OAuth credentials need to be configured for Google Sheets API
3. **S3 Permissions**: Ensure AWS credentials have appropriate access permissions for S3 and Redshift
4. **Data Formats**: All data operations are based on Apache Arrow format, ensure data type compatibility

