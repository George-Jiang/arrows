"""Backwards-compatible credential loading.

Superseded by :mod:`arrows.core.secrets` and :func:`arrows.load`. These
functions remain so existing scripts keep running; each one loads the matching
component and warns once.
"""

from __future__ import annotations

import warnings
from typing import Any

from .core.session import default_session

__all__ = ['load_aws_credentials', 'load_google_credentials', 'load_redshift_credentials']


def _deprecated(old: str, new: str) -> None:
    warnings.warn(f'arrows.auth.{old}() is deprecated; use {new} instead.', DeprecationWarning, stacklevel=3)


def load_aws_credentials() -> Any:
    _deprecated('load_aws_credentials', "arrows.load('s3')")
    return default_session().get('s3')


def load_redshift_credentials() -> Any:
    _deprecated('load_redshift_credentials', "arrows.load('redshift')")
    return default_session().get('redshift')


def load_google_credentials() -> Any:
    _deprecated('load_google_credentials', "arrows.load('google')")
    return default_session().get('google')


def _get_google_credentials() -> Any:
    """Legacy accessor for the raw google-auth credentials object."""
    return default_session().get('google').credentials
