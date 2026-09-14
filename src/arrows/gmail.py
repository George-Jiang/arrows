"""Send email through the Gmail API.

Credentials come from the ``gmail`` component; this module only holds the API.
"""

from __future__ import annotations

import base64
from email.mime.text import MIMEText
from typing import Any

from .core.session import get as _get_component
from .core.session import login as _login
from .template_renderer import render_template

__all__ = ['Email', 'send_email']


def _component() -> Any:
    return _get_component('gmail')


def send_email(to, subject: str | None = None, content: str = '', cc=None) -> None:
    Email(subject=subject, to=to, content=content, cc=cc or []).send()


class Email:
    """A single HTML message, built fluently and sent through Gmail."""

    @classmethod
    def from_dict(cls, email_info: dict) -> Email:
        return cls(
            subject=email_info.get('subject'),
            to=email_info.get('to', []),
            content=email_info.get('content', ''),
            cc=email_info.get('cc', []),
            sender=email_info.get('sender', ''),
        )

    def __init__(self, subject=None, to=None, content='', cc=None, sender=''):
        self.subject = subject
        self.to = _as_list(to)
        self.cc = _as_list(cc)
        self.sender = sender
        self.content = content

    def from_template(self, template_path, **kwargs) -> Email:
        self.content = render_template(template_path, **kwargs)
        return self

    def set_subject(self, subject) -> Email:
        self.subject = subject
        return self

    def set_to(self, to) -> Email:
        self.to = _as_list(to)
        return self

    def set_sender(self, sender) -> Email:
        self.sender = sender
        return self

    def set_cc(self, cc) -> Email:
        self.cc = _as_list(cc)
        return self

    def set_content(self, content) -> Email:
        self.content = content
        return self

    def send(self) -> dict:
        component = _component()
        message = MIMEText(self.content, 'html')
        message['subject'] = self.subject
        message['from'] = f'{self.sender} <{component.address()}>'.strip()
        message['to'] = ', '.join(self.to)
        if self.cc:
            message['cc'] = ', '.join(self.cc)
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        return component.service.users().messages().send(userId='me', body={'raw': raw_message}).execute()

    def __repr__(self) -> str:
        return f'Email(subject={self.subject!r}, to={self.to!r})'


def _as_list(value) -> list[str]:
    """Accept a single address or a list; ``'a@b'`` must not become a char list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def login(token_json: str | None = None, scopes: str | None = None, *, save: bool = False,
          prompt: bool | None = None, **kwargs):
    """Supply the Google OAuth token and load the gmail component."""
    return _login('gmail', token_json=token_json, scopes=scopes, save=save, prompt=prompt, **kwargs)
