# Deploying arrows on AWS

[`bootstrap_aws.py`](bootstrap_aws.py) is the startup path for a production job.
It assumes the container or instance has an IAM role, and that no credential
files exist on the box.

```bash
python -m examples.bootstrap_aws --check s3 redshift   # verify a deployment
```

## What goes where

Non-secret configuration belongs in the task definition as environment
variables. Only the Google token is a real secret, because there is no IAM
equivalent for it.

| Key | Source | Notes |
|---|---|---|
| `AWS_REGION` | task env | |
| `ARROWS_DEFAULT_BUCKET` | task env | staging bucket for UNLOAD/COPY |
| `REDSHIFT_HOST` / `REDSHIFT_DATABASE` / `REDSHIFT_USER` / `REDSHIFT_PORT` | task env | not secrets |
| `REDSHIFT_CLUSTER_IDENTIFIER` | task env | **set this and omit `REDSHIFT_PASSWORD`** |
| `ARROWS_SECRET_ID` | task env | id of the Secrets Manager secret below |
| `GOOGLE_TOKEN_JSON` | Secrets Manager | only if the job touches Sheets or Gmail |

Deliberately absent: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `REDSHIFT_PASSWORD`. If any of them is set, arrows uses it
and every automatic-rotation property below is lost.

The Secrets Manager secret is a flat JSON document:

```json
{ "GOOGLE_TOKEN_JSON": "{\"refresh_token\": \"...\", \"client_id\": \"...\", \"client_secret\": \"...\"}" }
```

## IAM policy for the task role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "StageDataInS3",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::my-staging-bucket", "arn:aws:s3:::my-staging-bucket/*"]
    },
    {
      "Sid": "MintRedshiftCredentialsPerRun",
      "Effect": "Allow",
      "Action": "redshift:GetClusterCredentials",
      "Resource": [
        "arn:aws:redshift:REGION:ACCOUNT:dbuser:my-cluster/analyst",
        "arn:aws:redshift:REGION:ACCOUNT:dbname:my-cluster/dev"
      ]
    },
    {
      "Sid": "ReadNonAwsSecrets",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT:secret:prod/arrows-*"
    }
  ]
}
```

The Redshift **cluster** also needs its own role, attached to the cluster and
granting `s3:GetObject`/`s3:PutObject` on the same bucket — UNLOAD and COPY are
executed by the cluster, not by your process. Grant it with `ASSUMEROLE`, or
reference it explicitly:

```python
arrows.redshift.copy('analytics.events', dataset, iam_role='arn:aws:iam::ACCOUNT:role/RedshiftCopyRole')
```

## Notes and limits

- **Redshift Serverless** has no cluster identifier; `GetClusterCredentials` does
  not apply. Use a Secrets Manager secret holding `REDSHIFT_PASSWORD`, or extend
  `RedshiftComponent._iam_password()` to call
  `redshift-serverless:GetCredentials`.
- **Lambda** works, but `psycopg2-binary` and `pyarrow` need a container image or
  a layer; prefer `engine='adbc'` for small results and skip the S3 round trip.
- **Google token rotation** is the one thing IAM cannot do for you. Refresh
  tokens do not expire on a schedule but are revoked when a password changes or
  the OAuth app is reconfigured — `arrows doctor google` in a canary job catches
  that before a report silently stops arriving.
