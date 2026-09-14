"""``arrows`` command line: see what exists, and whether it works.

    arrows components            list every registered component
    arrows doctor s3 redshift    load them and run their health checks
    arrows secrets redshift      show which required secrets resolve (redacted)
    arrows login redshift --save type the missing ones, store them in the keychain
"""

from __future__ import annotations

import argparse
import sys

from . import list_components
from .core import registry
from .core.session import default_session


def _cmd_components(args: argparse.Namespace) -> int:
    rows = list_components()
    width = max((len(row['name']) for row in rows), default=4)
    for row in rows:
        marker = '*' if row['loaded'] else ' '
        depends = f" (needs {', '.join(row['depends_on'])})" if row['depends_on'] else ''
        hint = f"  [{row['install_hint']}]" if row['install_hint'] else ''
        print(f"{marker} {row['name']:<{width}}  {row['summary']}{depends}{hint}")
    print('\n* = loaded in this session')
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    session = default_session()
    names = args.components or registry.names()
    exit_code = 0
    for name in names:
        try:
            session.load(name)
            status = session.get(name).health_check()
            print(f"{'ok  ' if status.ok else 'FAIL'}  {name:<14} {status.detail}")
            exit_code |= 0 if status.ok else 1
        except Exception as exc:
            print(f'FAIL  {name:<14} {type(exc).__name__}: {exc}')
            exit_code = 1
    return exit_code


def _cmd_secrets(args: argparse.Namespace) -> int:
    session = default_session()
    print(f'providers: {", ".join(p.name for p in session.secrets.providers)}\n')
    exit_code = 0
    for name in args.components or registry.names():
        spec = registry.get_spec(name)
        component = spec.build()
        print(f'{spec.name}:')
        for key in component.requires:
            found = session.secrets.get(key)
            print(f"  {'ok  ' if found else 'MISS'}  {key:<32} {found.source if found else '-'}")
            exit_code |= 0 if found else 1
        for key in component.optional:
            found = session.secrets.get(key)
            print(f"  {'ok  ' if found else '--  '}  {key:<32} {(found.source if found else 'optional')}")
        if not component.requires and not component.optional:
            print('  (no secrets required)')
    return exit_code


def _cmd_login(args: argparse.Namespace) -> int:
    session = default_session()
    try:
        session.login(*args.components, save=args.save, include_optional=args.all)
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    return _cmd_doctor(args)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='arrows', description=__doc__.splitlines()[0])
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('components', help='list registered components').set_defaults(func=_cmd_components)

    doctor = subparsers.add_parser('doctor', help='load components and run health checks')
    doctor.add_argument('components', nargs='*', help='component names (default: all)')
    doctor.set_defaults(func=_cmd_doctor)

    secrets = subparsers.add_parser('secrets', help='report which secrets resolve, without revealing them')
    secrets.add_argument('components', nargs='*', help='component names (default: all)')
    secrets.set_defaults(func=_cmd_secrets)

    login = subparsers.add_parser('login', help='prompt for missing secrets, then load and check')
    login.add_argument('components', nargs='+', help='component names')
    login.add_argument('--save', action='store_true', help='remember the answers in the OS keychain')
    login.add_argument('--all', action='store_true', help='also ask for optional keys')
    login.set_defaults(func=_cmd_login)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
