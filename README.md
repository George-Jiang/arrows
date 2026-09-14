# arrows

arrows is an **universal data wrangling tool** built around Apache Arrow, DuckDB, Polars, S3 for efficiently transferring and transforming data between different data sources.

Every integration is a **component**: an independently installable, independently
loadable unit that owns its own dependencies and its own credentials. A job that
only sends email never imports boto3, and never needs a Redshift password to exist.

## Features

- 🧩 **Pluggable components**: load only what a job needs; add new ones without touching the core
- 🔄 **Data format conversion**: Apache Arrow, Pandas, Polars, DuckDB
- 📊 **Google Sheets**: read and write, with SQL query support
- 🗄️ **Amazon Redshift**: query, UNLOAD to S3, COPY from S3
- ☁️ **AWS S3**: Parquet datasets, queryable in place
- 📧 **Gmail**: send templated email
- 🔐 **Layered secret resolution**: env → .env → local files → OS keychain → AWS Secrets Manager
- 📝 **SQL template rendering** with Jinja2

## Installation

```bash
pip install arrows        # every component
uv add arrows             # with uv
```

That is the whole installation. There are no extras to choose between: every
component's libraries are ordinary dependencies.

Installing everything does **not** mean loading everything. `import arrows` pulls
in none of it — boto3, psycopg2, the Google client and Polars are imported the
first time a component that needs them is loaded, so start-up stays in the tens
of milliseconds whatever a job actually uses.

## Components

```bash
arrows components          # what exists, what it needs, what is loaded
arrows secrets redshift    # which secrets resolve, and from where (values redacted)
arrows doctor s3 redshift  # load them and run a real health check
arrows login redshift      # type the missing secrets, then verify
```

| Component | Depends on | Third-party libraries | Purpose |
|---|---|---|---|
| `aws` | – | boto3 | boto3 session + DuckDB S3 credentials |
| `s3` | `aws` | pyarrow | Parquet datasets in S3 |
| `redshift` | `aws` | psycopg2, awswrangler, ADBC | Query, UNLOAD, COPY |
| `google` | – | google-auth | Shared Google OAuth credentials |
| `google_sheets` | `google` | google-api-python-client | Read/write Sheets |
| `gmail` | `google` | google-api-python-client | Send email |
| `sqlite` | – | duckdb | Local SQLite files (reference component) |

### Loading

Nothing is loaded on import. Ask for what the job needs:

```python
import arrows

arrows.load('s3', 'redshift')  # authenticates both, fails fast if a secret is missing
arrows.load('gmail')  # pulls in 'google' automatically
```

Loading is how a job states its requirements up front, so a missing credential
surfaces at start-up instead of an hour into a run. In production, turn off
implicit loading so that is enforced:

```python
arrows.configure(autoload=False)  # or ARROWS_AUTOLOAD=0
```

With `autoload` left on (the default), using a data API loads its component on
first call, which is what you want in a notebook.

Other entry points:

```python
arrows.list_components()  # names, dependencies, load state
arrows.health()  # per-component health checks
arrows.unload('redshift')  # close connections, drop registered secrets
arrows.close()  # release everything
```

`ARROWS_COMPONENTS=s3,redshift` supplies the default set, so `arrows.load()` with
no arguments loads it — useful for containers and scheduled jobs. Creating the
session itself never touches a credential, so `arrows.configure(secrets=...)` can
install a different secret source first.

### Isolated sessions

A `Session` owns its components and its secret store, so one process can talk to
two environments at once:

```python
from arrows.core import Session, SecretStore
from arrows.core.secrets import AwsSecretsManagerProvider

prod = Session(secrets=SecretStore([AwsSecretsManagerProvider('prod/arrows')]))
with prod.activate():  # module-level APIs now use this session
    prod.load('redshift')
    arrow = arrows.redshift.fetch_arrow('select 1')
```

### Adding a component

Copy [`src/arrows/components/sqlite.py`](src/arrows/components/sqlite.py) — the
reference implementation — and declare what you need:

```python
from arrows.core.component import Component, HealthStatus


class ClickhouseComponent(Component):
    name = 'clickhouse'
    requires = ('CLICKHOUSE_URL', 'CLICKHOUSE_PASSWORD')  # resolved before setup runs
    optional = ('CLICKHOUSE_DATABASE',)
    depends_on = ()  # other components

    def setup(self, secrets):
        # A hint is only needed for a component distributed separately from
        # arrows; a built-in leaves it out, since its libraries always ship.
        driver = self.import_module('clickhouse_connect', hint='pip install arrows-clickhouse')
        self.client = driver.get_client(
            host=self.require_secret('CLICKHOUSE_URL'),
            password=self.require_secret('CLICKHOUSE_PASSWORD'),
        )

    def health_check(self):
        return HealthStatus(self.name, bool(self.client.ping()))

    def close(self):
        self.client.close()


COMPONENT = ClickhouseComponent
```

Register it in [`src/arrows/components/__init__.py`](src/arrows/components/__init__.py)
for a built-in, or ship it from **your own package** with no change to arrows at all:

```toml
[project.entry-points."arrows.components"]
clickhouse = "arrows_clickhouse:SPEC"
```

```python
SPEC = ComponentSpec(
    name='clickhouse',
    module='arrows_clickhouse.component',  # imported only when the component is loaded
    summary='ClickHouse over the native protocol',
    install_hint='pip install arrows-clickhouse',
)
```

The component's data API is an ordinary module; it reaches its component with
`arrows.core.session.get('clickhouse')`.

## Secrets

Components never read files or environment variables directly. They ask a
`SecretStore` for a **key**; where that key comes from is a deployment decision.
The default chain, first hit wins:

| Order | Provider | Intended for |
|---|---|---|
| 1 | `EnvProvider` | Containers, CI, 12-factor deployments |
| 2 | `DotEnvProvider('.env')` | Local development |
| 3 | `SecretFileProvider(~/.credentials)` | One file per secret; also Docker/K8s `/run/secrets` |
| 4 | `JsonFileProvider(...)` | The legacy `~/.credentials/*.json` layout |
| 5 | `KeyringProvider` | macOS Keychain / libsecret / Windows Credential Manager |
| 6 | `PromptProvider` | Notebooks and one-off scripts, opt-in — see below |

`AwsSecretsManagerProvider` is available for production, and is the recommended
source once more than one machine is involved:

```python
import arrows
from arrows.core import SecretStore
from arrows.core.secrets import AwsSecretsManagerProvider, EnvProvider

arrows.configure(secrets=SecretStore([EnvProvider(), AwsSecretsManagerProvider('prod/arrows')]))
arrows.load('redshift')
```

Guarantees worth knowing:

- **Values are not copied into `os.environ`.** The environment is inherited by
  every subprocess and appears in crash dumps. Use `store.export([...])` only for
  a library that insists on reading the environment itself.
- **Values cannot leak into a log by accident.** `Secret.__str__` and
  `__repr__` render `***`; the plaintext requires an explicit `.reveal()`.
- **Credential files are permission-checked.** A file readable by group or others
  triggers a warning telling you to `chmod 600`.
- **No secret is ever written to disk by arrows.**

### Notebooks: supplying secrets by hand

Two ways, mixable. **In code**, with named arguments per component:

```python
import arrows

arrows.redshift.login(host='my-cluster...amazonaws.com', database='dev', user='analyst', password='...')

arrows.s3.login(profile='analytics', bucket='my-staging-bucket')
arrows.google_sheets.login(token_json=open('~/token.json').read())
```

**By prompt**, for anything not supplied — masked, and only for what is actually
missing:

```python
arrows.login('redshift')
# redshift: enter 2 secret(s), blank to skip
# REDSHIFT_HOST: ········
# REDSHIFT_PASSWORD: ········

arrows.redshift.login(user='analyst')  # asks only for host, database, password
```

The precedence rule differs between the two on purpose:

| Source | Position in the chain | Why |
|---|---|---|
| Passed in code | **first** — outranks everything | You wrote it down; it is an instruction, not a fallback |
| Typed at a prompt | **last** — fills gaps only | A value typed in a notebook must not shadow real configuration |

Calling `login()` again with different credentials rebuilds the affected
components (and anything depending on them), so rotating a password mid-session
takes effect immediately.

Keyword names come from each component's `login_args`; a raw `UPPER_CASE` secret
key always works too, which is the escape hatch for keys a component has not
named:

```python
arrows.login('redshift', REDSHIFT_IAM_DURATION_SECONDS='900')
```

To stop retyping on every kernel restart, save to the OS keychain — needs
`arrows[keyring]`:

```python
arrows.redshift.login(password='...', save=True)  # found by KeyringProvider next time
arrows.login('redshift', save=True)  # same, for prompted answers
```

Same thing from a terminal: `arrows login redshift --save`.

Prompting is inert outside a terminal or an IPython kernel, and
`ARROWS_NON_INTERACTIVE=1` disables it everywhere — a scheduled job raises a
missing-secret error naming the key, instead of hanging forever on a prompt
nobody will answer. `prompt=False` forces that behaviour, `prompt=True` requires
a human. So the same call works in both places:

```python
arrows.redshift.login(host=..., user=..., password=...)  # script: no prompt, nothing missing
arrows.redshift.login()  # notebook: asks for all of it
```

A key ending in `_JSON` or `_FILE` accepts a path at the prompt, which is the
sane way to supply a Google token:

```python
arrows.login('google')
# GOOGLE_TOKEN_JSON: ~/Downloads/token.json     ← read from the file
```

### Keys by component

| Component | Required | Optional |
|---|---|---|
| `aws` | – (falls back to the standard AWS chain) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION` |
| `s3` | – | `ARROWS_DEFAULT_BUCKET` |
| `redshift` | `REDSHIFT_HOST`, `REDSHIFT_DATABASE`, `REDSHIFT_USER` | `REDSHIFT_PASSWORD` **or** `REDSHIFT_CLUSTER_IDENTIFIER`, `REDSHIFT_PORT` |
| `google` | `GOOGLE_TOKEN_JSON` | `GOOGLE_SCOPES` |
| `sqlite` | – | `SQLITE_DATABASE` |

Two credentials deserve a note:

- **AWS**: prefer *no* credentials in arrows at all. With nothing configured,
  boto3 resolves its own chain — `AWS_PROFILE`, SSO, EC2/ECS/EKS instance roles —
  all of which rotate automatically. Explicit keys are supported for laptops but
  are the weakest option, and they are the only case where key material has to be
  handed to DuckDB as a literal secret.
- **Redshift**: set `REDSHIFT_CLUSTER_IDENTIFIER` and omit `REDSHIFT_PASSWORD`.
  arrows then mints a short-lived password through `redshift:GetClusterCredentials`
  using the AWS identity already loaded, so no database password exists on disk.

### Migrating from `load_credentials()`

`arrows.load_credentials()` and the `arrows.auth.load_*` functions still work and
now delegate to the component loader, with a `DeprecationWarning`:

| Before | Now |
|---|---|
| `arrows.load_credentials()` | `arrows.load('s3', 'redshift', 'google')` |
| `auth.load_aws_credentials()` | `arrows.load('s3')` |
| `auth.load_redshift_credentials()` | `arrows.load('redshift')` |
| `auth.load_google_credentials()` | `arrows.load('google')` |

The legacy `~/.credentials/` files are still read, so no file needs to move.

## Usage

### Amazon Redshift

#### Querying Data

```python
import arrows
from arrows import redshift

arrows.load('redshift')

# Query using S3 Unload engine (default, suitable for large data)
arrow = redshift.fetch_arrow(sql='SELECT * FROM my_table', engine='s3')

# Query using ADBC engine (fast for smaller datasets)
arrow = redshift.fetch_arrow(sql='SELECT * FROM my_table WHERE date > %(date)s', engine='adbc', date='2024-01-01')

# Get Pandas DataFrame (defaults to ADBC engine)
df = redshift.fetch_dataframe(
    sql='SELECT * FROM my_table',
    engine='adbc',
    dtype_backend='numpy',  # or 'pyarrow'
)
```

#### Importing Data to Redshift

```python
from arrows import redshift

# Import Arrow data to Redshift
redshift.arrow_to_redshift(
    arrow=arrow,
    table_name='schema.table_name',
    mode='append',  # or 'overwrite'
)

# Copy from S3 to Redshift
redshift.copy(table_name='schema.table_name', s3_path='s3://bucket/path/', mode='append')
```

#### Exporting Data to S3

```python
from arrows import redshift

# Export Redshift query results to S3
dataset = redshift.unload(sql='SELECT * FROM my_table', s3_path='s3://bucket/path/')
```

#### Executing SQL

```python
from arrows import redshift

# Execute SQL statement
redshift.execute_sql(sql='CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR)')

# Execute SQL file (supports Jinja2 templates)
redshift.execute_sql_file(sql_script_path='scripts/create_table.sql', table_name='my_table')
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
    engine='duckdb',  # or 'pyarrow'
)

# Store Polars DataFrame to S3
dataset = s3.polars_to_s3(df=df, s3_path='s3://bucket/path/')

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
result = dataset.query('SELECT * FROM self WHERE id > 100')

# Delete dataset
dataset.delete()

# Clear dataset contents
dataset.clear_contents()
```

### Google Sheets

#### Reading from Google Sheets

```python
from arrows import google_sheets

# Read data from Google Sheet as Arrow format
arrow = google_sheets.fetch_arrow(
    spreadsheet_id='your_spreadsheet_id',
    sheet_name='Sheet1',
    sheet_range='A1:D100',  # Optional
    all_varchar=False,  # Optional: Treat all columns as VARCHAR
)

spreadsheet = google_sheets.get_spreadsheet(spreadsheet_id)
sheet = spreadsheet.get_sheet(sheet_name)
# or
sheet = google_sheets.get_sheet(spreadsheet_id, sheet_name)

arrow = sheet.to_arrow(self, sheet_range=None, all_varchar=False, sql=None)
# or
df = sheet.to_polars(self, sheet_range=None, all_varchar=False, sql=None)
# or
df = sheet.to_pandas(self, sheet_range=None, all_varchar=False, sql=None)
# or
duckdb_relation = sheet.to_duckdb(self, sheet_range=None, all_varchar=False, sql=None)


# Use SQL query
arrow = google_sheets.fetch_arrow(
    spreadsheet_id='your_spreadsheet_id',
    sheet_name='Sheet1',
    sql="""
        SELECT
            *
        FROM 
            self
        WHERE column1 > 100
        """,
)
```

#### Writing to Google Sheets

```python
from arrows import google_sheets

# Write Arrow data to Google Sheet
sheet = google_sheets.arrow_to_googlesheet(arrow=arrow, spreadsheet_id='your_spreadsheet_id', sheet_name='Sheet1')
```

#### Managing Spreadsheets and Sheets

```python
from arrows import google_sheets

# Create a new Spreadsheet
spreadsheet = google_sheets.create_spreadsheet(
    spreadsheet_name='My Spreadsheet',
    parent_id='parent_id',  # Optional
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

### Gmail

#### Sending Emails

```python
from arrows import gmail

# Simple email
gmail.send_email(to=['user@example.com'], subject='Report', content='<h1>Hello</h1>', cc=['manager@example.com'])

# Advanced usage with Email class
email = gmail.Email(subject='Monthly Report', to=['user@example.com'], sender='Data Team')

# Set content from template
email.from_template('path/to/template.html', variable='value')

# Send email
email.send()
```

### SQL Template Rendering

```python
from arrows.template_renderer import render_template

# Render SQL template
sql = render_template('path/to/template.sql', table_name='my_table', date='2024-01-01')
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


## Development

```bash
uv sync                      # install with every component
uv run pytest                # unit tests, no credentials needed
uv run ruff check .          # lint
uv run ruff format .         # format; CI checks this
uv run arrows doctor         # check real connectivity for what is configured
```

Publishing a new version is
[docs/RELEASING.md](https://github.com/George-Jiang/arrows/blob/main/docs/RELEASING.md).

## Notes

1. **Load what you need**: `arrows.load(...)` fails fast on a missing credential; `arrows doctor` checks a real round-trip
2. **Google Sheets permissions**: the OAuth token needs the `spreadsheets` and `drive` scopes; Gmail needs `gmail.send`
3. **S3 permissions**: Redshift UNLOAD/COPY needs the cluster's IAM role to reach the same bucket your session does
4. **Data formats**: everything is Apache Arrow in the middle; check type compatibility at the edges
5. **Breaking change in 0.2**: `redshift.fetch_arrow(..., engine='s3')` now renders its SQL with Jinja2 (`{{ name }}`)
   like every other entry point, instead of `str.format` (`{name}`)

