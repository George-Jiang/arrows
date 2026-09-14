"""Gmail sending, on top of the shared google component."""

from __future__ import annotations

from typing import Any

from ..core.component import Component, HealthStatus
from ..core.secrets import SecretStore


class GmailComponent(Component):
    name = 'gmail'
    depends_on = ('google',)

    def setup(self, secrets: SecretStore) -> None:
        self.import_module('googleapiclient.discovery')

    @property
    def service(self) -> Any:
        return self.session.get('google').service('gmail', 'v1')

    def address(self) -> str:
        """The authenticated mailbox address."""
        return self.service.users().getProfile(userId='me').execute().get('emailAddress', '')

    def health_check(self) -> HealthStatus:
        try:
            return HealthStatus(self.name, True, f'mailbox={self.address()}')
        except Exception as exc:
            return HealthStatus(self.name, False, f'{type(exc).__name__}: {exc}')


COMPONENT = GmailComponent
