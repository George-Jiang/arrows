"""Scope resolution for the google component, no credentials needed."""

from arrows.components.google import DEFAULT_SCOPES, _resolve_scopes

TOKEN_SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def test_configured_scopes_win_over_the_token_document():
    assert _resolve_scopes('a,b', {'scopes': TOKEN_SCOPES}) == ['a', 'b']


def test_configured_scopes_are_trimmed():
    assert _resolve_scopes(' a , b ,', {}) == ['a', 'b']


def test_token_scopes_beat_the_defaults():
    assert _resolve_scopes(None, {'scopes': TOKEN_SCOPES}) == TOKEN_SCOPES


def test_space_separated_token_scopes():
    assert _resolve_scopes('', {'scopes': 'a b'}) == ['a', 'b']


def test_defaults_are_the_last_resort():
    assert _resolve_scopes(None, {}) == list(DEFAULT_SCOPES)
    assert _resolve_scopes(' , ', {'scopes': []}) == list(DEFAULT_SCOPES)
