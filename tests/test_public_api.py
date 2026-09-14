import subprocess
import sys

import arrows

HEAVY = ('boto3', 'botocore', 'psycopg2', 'awswrangler', 'googleapiclient', 'polars', 'pandas')


def test_importing_arrows_pulls_in_no_component_dependencies():
    code = f'import arrows, sys; print([m for m in {HEAVY!r} if m in sys.modules])'
    output = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, check=True)
    assert output.stdout.strip() == '[]'


def test_submodules_are_reachable_lazily():
    assert arrows.template_renderer.render_template is not None
    assert 'template_renderer' in dir(arrows)


def test_unknown_attribute_still_raises_attribute_error():
    try:
        arrows.not_a_module
    except AttributeError as exc:
        assert 'not_a_module' in str(exc)
    else:
        raise AssertionError('expected AttributeError')


def test_list_components_describes_the_builtins():
    names = {component['name'] for component in arrows.list_components()}
    assert {'aws', 's3', 'redshift', 'google', 'google_sheets', 'gmail', 'sqlite'} <= names


def test_cli_lists_components():
    output = subprocess.run([sys.executable, '-m', 'arrows', 'components'], capture_output=True, text=True, check=True)
    assert 'redshift' in output.stdout
