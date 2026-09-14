"""Production bootstrap for arrows on AWS (EC2 / ECS / EKS / Batch / MWAA).

The shape of a correct production setup, in one place:

* **No AWS keys anywhere.** The instance, task or IRSA role *is* the identity.
  boto3 resolves and refreshes it; arrows stays out of the way, and DuckDB is
  pointed at the same chain instead of being handed key material.
* **No database password on disk.** Redshift credentials are minted per run
  through ``redshift:GetClusterCredentials`` against that same role.
* **Only genuinely external secrets go in Secrets Manager.** Here that is the
  Google OAuth token — there is no IAM equivalent for it.
* **Fail at start-up.** ``autoload=False`` plus an explicit load and health
  check means a missing permission surfaces in the first second of the job, not
  forty minutes in.

Run it directly to verify a deployment::

    python -m examples.bootstrap_aws --check

Wire it into a job::

    from examples.bootstrap_aws import bootstrap

    with bootstrap('s3', 'redshift') as session:
        arrow = arrows.redshift.fetch_arrow('select count(*) from events')
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from contextlib import contextmanager

import arrows
from arrows.core import SecretStore, Session
from arrows.core.secrets import AwsSecretsManagerProvider, EnvProvider, SecretFileProvider

log = logging.getLogger('arrows.bootstrap')

#: Secrets Manager secret holding a flat JSON document of the keys that cannot
#: come from IAM. Set per environment; the task role needs GetSecretValue on it.
SECRET_ID = os.environ.get('ARROWS_SECRET_ID', '')

#: Where EKS mounts a projected secret volume, if you use one. Harmless if absent.
MOUNTED_SECRETS_DIR = '/run/secrets'


def build_secret_store(secret_id: str = SECRET_ID, region: str | None = None) -> SecretStore:
    """The production chain: task config -> mounted volume -> Secrets Manager.

    Deliberately *not* included: ``DotEnvProvider`` and ``~/.credentials``. A
    production container should have no credential files of its own, and a stray
    ``.env`` left in an image should never be able to shadow the real source.
    """
    providers = [
        # Non-secret configuration from the task definition: hosts, database
        # names, bucket names, AWS_REGION.
        EnvProvider(),
        # Optional: secrets projected as files by the EKS Secrets Store CSI
        # driver or ECS. Safer than environment variables, which every child
        # process inherits.
        SecretFileProvider(MOUNTED_SECRETS_DIR),
    ]
    if secret_id:
        providers.append(AwsSecretsManagerProvider(secret_id, region_name=region))
    else:
        log.warning('ARROWS_SECRET_ID is not set; only env and %s will be searched.', MOUNTED_SECRETS_DIR)
    return SecretStore(providers)


def bootstrap(*components: str, secret_id: str = SECRET_ID, strict_health: bool = True) -> Session:
    """Install the production secret store and load exactly ``components``.

    Returns a session that the module-level APIs (``arrows.s3``,
    ``arrows.redshift``, ...) will use. Raises before returning if any component
    cannot authenticate, so the process exits while the failure is still cheap.
    """
    store = build_secret_store(secret_id, region=os.environ.get('AWS_REGION'))

    # configure() must come before any load: creating the session reads nothing,
    # so this is the moment where the credential source is decided.
    arrows.configure(secrets=store, autoload=False, reset=True)
    log.info('secret providers: %s', ', '.join(p.name for p in store.providers))

    session = arrows.load(*components)
    log.info('loaded components: %s', ', '.join(session.loaded))

    failures = [status for status in session.health() if not status.ok]
    for status in session.health():
        log.info('health %-14s %s  %s', status.name, 'ok' if status.ok else 'FAIL', status.detail)
    if failures and strict_health:
        session.close()
        raise RuntimeError('unhealthy components: ' + ', '.join(f'{s.name} ({s.detail})' for s in failures))
    return session


@contextmanager
def session_scope(*components: str, **kwargs):
    """``bootstrap`` with guaranteed teardown.

    ``close()`` drops the DuckDB secrets arrows registered and closes pooled
    connections, so a long-lived worker does not leave credentials sitting in
    the DuckDB catalog after a job finishes.
    """
    session = bootstrap(*components, **kwargs)
    try:
        yield session
    finally:
        session.close()


def _configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
        stream=sys.stdout,
    )
    # boto3 logs request signing at DEBUG; keep it off in production logs.
    logging.getLogger('botocore').setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Bootstrap arrows for a production AWS environment.')
    parser.add_argument('components', nargs='*', default=['s3', 'redshift'], help='components to load')
    parser.add_argument('--secret-id', default=SECRET_ID, help='Secrets Manager secret id')
    parser.add_argument('--check', action='store_true', help='load, health-check, and exit')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)
    try:
        with session_scope(*args.components, secret_id=args.secret_id) as session:
            if args.check:
                log.info('startup check passed for %s', ', '.join(session.loaded))
                return 0
            run(session)
    except Exception as exc:
        # Never log the exception's arguments blindly at production level: arrows
        # redacts its own secrets, but a driver's error may quote a DSN.
        log.error('bootstrap failed: %s: %s', type(exc).__name__, exc)
        return 1
    return 0


def run(session: Session) -> None:
    """Replace with the actual job."""
    arrow = arrows.redshift.fetch_arrow('select current_date as today', engine='adbc')
    log.info('redshift says: %s', arrow.to_pydict())


if __name__ == '__main__':
    sys.exit(main())
