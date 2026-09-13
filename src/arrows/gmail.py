from googleapiclient.discovery import build
from email.mime.text import MIMEText
import base64
import json
import os

from .auth import _get_google_credentials, load_google_credentials
from .template_renderer import render_template



def send_email(to, subject=None, content='', cc=[]):
    email = Email(subject=subject, to=to, content=content, cc=cc)
    email.send()


class Email():
    @classmethod
    def from_dict(cls, email_info):
        pass
    
    def __init__(self, subject=None, to=[], content='', cc=[], sender = ''):
        self.subject = subject
        self.to = to
        self.cc = cc
        self.sender = sender
        self.content = content
        
    def from_template(self, template_path, **kwargs):
        content = render_template(template_path, **kwargs)
        self.content = content
        pass
    
    def set_subject(self, subject):
        self.subject = subject
        return self
            
    def set_to(self, to):
        self.to = to
        return self
    
    def set_sender(self, sender):
        self.sender = sender
        return self
        
    def set_cc(self, cc):
        self.cc = cc
        return self
    
    def set_content(self, content):
        self.content = content
        return self
        
    def send(self):
        gmail_service = build("gmail", "v1", credentials=_get_google_credentials())
        profile = gmail_service.users().getProfile(userId="me").execute()
        email_address = profile.get("emailAddress")
        
        msg = MIMEText(self.content, 'html')
        msg['subject'] = self.subject
        msg['from'] = f'{self.sender} <{email_address}>'
        msg['to'] = ''.join(self.to)
        msg['cc'] = ''.join(self.cc)
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        
        (
            gmail_service
            .users()
            .messages()
            .send(userId = 'me', body = {'raw': raw_message})
            .execute()
        )
        
        