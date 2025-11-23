import duckdb
from googleapiclient.discovery import build
from jinja2 import Template

import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from .auth import _get_google_credentials
from .utils import _parse_self_sql


def get_sheet(spreadsheet_id, sheet_name):
    sheet = Sheet(spreadsheet_id, sheet_name)
    return sheet


def get_spreadsheet(spreadsheet_id):
    spreadsheet = SpreadSheet(spreadsheet_id=spreadsheet_id)
    return spreadsheet


def create_spreadsheet(spreadsheet_name=None, parent_folder_id=None):
    creds = _get_google_credentials()
    drive_service = build('drive', 'v3', credentials=creds)

    spreadsheet_name = spreadsheet_name if spreadsheet_name is not None else 'Untitled'
    file_metadata = {
                        'name': spreadsheet_name,
                        'mimeType': 'application/vnd.google-apps.spreadsheet',
                        'parents': [parent_folder_id]
                    }

    file = drive_service.files().create(body=file_metadata, fields='id').execute()
    googlesheet_id = file.get('id')
    
    spreadsheet = SpreadSheet(spreadsheet_id=googlesheet_id)
    return spreadsheet


def fetch_arrow(spreadsheet_id, sheet_name, sheet_range=None, all_varchar=False, sql=None):
    sheet = Sheet(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    arrow = sheet.to_arrow(sheet_range=sheet_range, all_varchar=all_varchar, sql=sql)
    return arrow

def arrow_to_googlesheet(arrow, spreadsheet_id=None, sheet_name=None, spreadsheet_name=None, parent_folder_id=None, sheet=None, sheet_range=None, overwrite_sheet=True, overwrite_range=False):
    if not arrow:
        raise ValueError
    if sheet:
        sheet = sheet
    elif spreadsheet_id and sheet_name:
        sheet = get_sheet(spreadsheet_id, sheet_name)
    elif spreadsheet_id:
        spreadsheet = get_spreadsheet(spreadsheet_id)
        sheet = spreadsheet.create_sheet()
    else:
        spreadsheet = create_spreadsheet(spreadsheet_name=spreadsheet_name, parent_folder_id=parent_folder_id)
        sheet = spreadsheet.sheets[0]
        if sheet_name:
            sheet.rename(sheet_name=sheet_name)
    sheet.from_arrow(arrow=arrow, sheet_range=sheet_range, overwrite_sheet=overwrite_sheet, overwrite_range=overwrite_range)
    return sheet


class SpreadSheet():
    def __init__(self, spreadsheet_id):
        if isinstance(spreadsheet_id, SpreadSheet):
            self.spreadsheet_id = spreadsheet_id.spreadsheet_id
        else:
            self.spreadsheet_id = spreadsheet_id
        
    def __contains__(self, sheet):
        if_same_spreadsheet =  sheet.spreadsheet_id == self.spreadsheet_id
        if_sheet_name_in = sheet.sheet_name in self.sheets_names
        if_contains = if_same_spreadsheet and if_sheet_name_in
        return if_contains
    
    def share(self, email, role='reader', type='user', send_notification=True):
        creds = _get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        email = email if isinstance(email, list) else [email]
        for each_email in email:
            permission = {
                        'type': type,          # could be 'user', 'group', 'domain', 'anyone'
                        'role': role,        # 'reader', 'commenter', 'writer', 'owner'
                        'emailAddress': each_email
                        }
            drive_service.permissions().create(fileId=self.spreadsheet_id, body=permission, sendNotificationEmail=send_notification).execute()
        
    @property
    def url(self):
        return f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}'
    
    @property
    def sheets_names(self):
        return [e.sheet_name for e in self.sheets]
    
    @property
    def sheets_ids(self):
        return [e.sheet_id for e in self.sheets]
    
    @property
    def sheets(self):
        #Return a list of Sheet Objects
        creds = _get_google_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
        google_spreadsheet_object = _sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = google_spreadsheet_object.get('sheets', [])
        sheets = [Sheet(spreadsheet_id=self, sheet_name=each['properties']['title'], sheet_id=each['properties']['sheetId']) for each in sheets]
        return sheets
    
    def exists(self):
        creds = _get_google_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
        try:
            _sheets_service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            return True
        except HttpError as e:
            if e.resp.status == 404:
                return False
            elif e.resp.status == 403:
                raise ValueError("Spreadsheet Exists. BUT Access denied.")
        
    def rename(self,spreadsheet_name):
        creds = _get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        drive_service.files().update(fileId=self.spreadsheet_id,
                                     body={"name": spreadsheet_name},
                                     fields='id, name'
                                     ).execute()
    
    def delete(self):
        creds = _get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        drive_service.files().delete(fileId=self.spreadsheet_id).execute()
    
    def _generate_sheet_name(self):
        sheets_names = [e.lower() for e in self.sheets_names]
        n = len(sheets_names) + 1
        new_name = f"Sheet{n}"
        while new_name.lower() in sheets_names:
            n += 1
            new_name = f"Sheet{n}"
        return new_name
        
    def create_sheet(self, sheet_name=None):
        if sheet_name is None:
            sheet_name = self._generate_sheet_name()
        
        creds = _get_google_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
        requests = [
                        {
                            "addSheet": {
                                "properties": {
                                    "title": sheet_name,  # The name of your new sheet
                                    "gridProperties": {
                                        "rowCount": 100,
                                        "columnCount": 20
                                    }
                                }
                            }
                        }
                    ]
        body = {"requests": requests}
        response = _sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()
        new_sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
        sheet = Sheet(spreadsheet_id=self, sheet_name=sheet_name, sheet_id=new_sheet_id)
        return sheet
    
    def get_sheet(self, sheet_name):
        sheets = [e for e in self.sheets if e.sheet_name == sheet_name]
        if len(sheets) != 1:
            raise ValueError(f'sheet "{sheet_name}" NOT FOUND')
        else:
            sheet = sheets[0]
        return sheet
    
    def rename_sheet(self, old_name, new_name):
        sheet = self.get_sheet(old_name)
        sheet.rename(new_name)
        
    def delete_sheet(self, sheet_name):
        sheet = self.get_sheet(sheet_name=sheet_name)
        sheet.delete()
        


class Sheet():
    def __init__(self, spreadsheet_id, sheet_name, sheet_id=None):
        if isinstance(spreadsheet_id, SpreadSheet):
            self.spreadsheet = spreadsheet_id
            self.spreadsheet_id = spreadsheet_id.spreadsheet_id
        else:
            self.spreadsheet = SpreadSheet(spreadsheet_id=spreadsheet_id)
            self.spreadsheet_id = spreadsheet_id
        
        self.sheet_name = sheet_name
        self.sheet_id = sheet_id
        
    def __eq__(self, sheet):
        check = self.spreadsheet_id == sheet.spreadsheet_id and self.sheet_name == sheet.sheet_name
        return check
        
    @property
    def url(self):
        if self.sheet_id is None:
            self.get_sheet_id()
        return f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit?gid={self.sheet_id}#gid={self.sheet_id}'
    
    def exists(self):
        if_exist = self.sheet_name in self.spreadsheet.sheets_names
        return if_exist
    
    def create(self):
        if not self.exists():
            new_sheet = self.spreadsheet.create_sheet(self.sheet_name)
            self.sheet_id = new_sheet.sheet_id
            #print(f'New Sheet "{self.sheet_name}" CREATED.')
        else:
            print(f'Sheet "{self.sheet_name}" Already EXISTS. WILL NOT CREATE NEW SHEET.')
        
    def get_sheet_id(self):
        sheet = self.spreadsheet.get_sheet(sheet_name=self.sheet_name)
        self.sheet_id = sheet.sheet_id
        return self.sheet_id
      
    def to_duckdb(self, sheet_range=None, all_varchar=False, sql=None):
        sheet_range = f'!{sheet_range}' if sheet_range else ''
        sheet_expression = f'''
            read_gsheet('{self.spreadsheet_id}', sheet='{self.sheet_name}{sheet_range}'{', all_varchar = true' if all_varchar else ''})
        '''
        if sql:
            sql = Template(sql).render(google_sheet = sheet_expression)
            template = Template('''
                                with temp_google_sheet_table as (select * from {{ sheet_expression }})
                                {{ _parse_self_sql(sql, 'self', 'temp_google_sheet_table') }}
                                ''')
            sql = template.render(sql = sql, sheet_expression = sheet_expression, _parse_self_sql = _parse_self_sql)
        else:
            sql = f'''
                SELECT * FROM {sheet_expression}
            '''
        duckdb_relation = duckdb.sql(sql)
        return duckdb_relation
    
    def to_arrow(self, sheet_range=None, all_varchar=False, sql=None):
        arrow = self.to_duckdb(sheet_range=sheet_range, all_varchar=all_varchar, sql=sql).to_arrow_table()
        return arrow
    
    def to_polars(self, sheet_range=None, all_varchar=False, sql=None):
        df = self.to_duckdb(sheet_range=sheet_range, all_varchar=all_varchar, sql=sql).pl()
        return df
    
    def to_pandas(self, sheet_range=None, all_varchar=False, sql=None):
        df = self.to_duckdb(sheet_range=sheet_range, all_varchar=all_varchar, sql=sql).df()
        return df
    

    def from_arrow(self, arrow, sheet_range=None, overwrite_sheet=True, overwrite_range=False):
        if not self.exists():
            self.create()
        
        range_str = (", range '" + sheet_range + "'") if sheet_range else ''
        overwrite_range_str = ', overwrite_range True' if overwrite_range else ''
        overwrite_sheet_str = ', overwrite_sheet False' if overwrite_sheet == False else ''
        duckdb.execute(f'''
                    COPY arrow
                    TO '{self.spreadsheet_id}' 
                    (format gsheet, sheet '{self.sheet_name}' {range_str} {overwrite_range_str} {overwrite_sheet_str});
                    ''')
        print(f'Success: Data transfered to Google Sheet.')
    
    def rename(self, sheet_name):
        if self.sheet_id is None:
            self.get_sheet_id()
        
        creds = _get_google_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
        requests = [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": self.sheet_id,
                                    "title": sheet_name
                                },
                                "fields": "title"
                            }
                        }
                    ]
        body = {"requests": requests}
        _sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()
        self.sheet_name = sheet_name
    
    def delete(self):
        if not self.exists():
            print(f'sheet "{self.sheet_name}" does not exist. Will DO NOTHING.')
            return
        if self.sheet_id is None:
            self.get_sheet_id()
        
        creds = _get_google_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
        requests = [
                        {
                            "deleteSheet": {
                                "sheetId": self.sheet_id
                            }
                        }
                    ]
        body = {"requests": requests}
        _sheets_service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()
    
    