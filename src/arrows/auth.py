import os
from pathlib import Path
import json
import duckdb
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


def load_aws_credentials():
    with open(Path.home()/'.credentials/aws_credentials.txt') as file:
        content = file.read()

    aws_credentials = [e.split('=',1)[-1] for e in content.split()[1:]]

    aws_credentials = {
        'AWS_ACCESS_KEY_ID': aws_credentials[0],
        'AWS_SECRET_ACCESS_KEY': aws_credentials[1],
        'AWS_SESSION_TOKEN': aws_credentials[2]
    }

    os.environ.update(aws_credentials)
    
    duckdb.execute('''
                    CREATE OR REPLACE SECRET secret (
                        TYPE s3,
                        PROVIDER credential_chain
                    );
                    ''')
    
    
def load_redshift_credentials():
    with open(Path.home()/'.credentials/redshift_credentials.json') as file:
        redshift_credentials = json.load(file)
    os.environ.update(redshift_credentials)


def load_google_credentials():
    with open(Path.home()/'.credentials/google_token.json') as file:
        GOOGLE_TOKEN_JSON = json.load(file)
        
    creds = Credentials.from_authorized_user_info(GOOGLE_TOKEN_JSON)
    creds.refresh(Request())
    google_credentials = {
        'GOOGLE_TOKEN_JSON': creds.to_json()
    }
    os.environ.update(google_credentials)
    
    duckdb.execute(f'''
                    INSTALL gsheets FROM community;
                    LOAD gsheets;

                    CREATE OR REPLACE SECRET (TYPE gsheet, 
                                   provider access_token, 
                                   token '{creds.token}');
                    ''')


def _get_google_credentials():
    creds = Credentials.from_authorized_user_info(json.loads(os.getenv('GOOGLE_TOKEN_JSON')))
    return creds
    
    
    




    
