"""Keywords cannot reach bb's positional placeholders: such code is refused when added."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

import bb

BB = Path(__file__).parent.parent / 'bb.py'
HASH = 'a' * 64


def refused(source):
    with pytest.raises(ValueError) as caught:
        bb.code_prepare_source(source, 'eng')
    return str(caught.value)


@pytest.mark.parametrize('source, shown', [
    (f'from bb.pool import object_{HASH} as parse\n\ndef f(text):\n    return parse(text, height=3)\n',
     'line 4: parse(text, height=3): keyword arguments (height=) to parse'),
    ('def f(n, step=1):\n    return f(n - 1, step=2) if n else 0\n', 'keyword arguments (step=) to f'),
    ('def f(text):\n    def inner(value, height=1):\n        return value * height\n    return inner(text, height=3)\n',
     'line 4: inner(text, height=3)'),
    ('def f(text):\n    scale = lambda value, k=1: value * k\n    return scale(text, k=2)\n', 'keyword arguments (k=) to scale'),
    ('def f(text):\n    def inner(value, k):\n        return value\n    return inner(text, **{"k": 1})\n', '(**) to inner'),
    ('def f(text, *, height=1):\n    return text\n', 'def f: keyword-only height'),
    ('def f(text, **props):\n    return text\n', 'def f: **props'),
    ('def f(text):\n    def inner(**options):\n        return options\n    return inner()\n', 'def inner: **options'),
])
def test_keywords_that_bb_would_break_are_refused_with_their_line(source, shown):
    message = refused(source)
    assert shown in message and 'positional arguments only' in message


def test_keywords_to_everything_else_and_defaults_and_star_args_are_fine():
    source = ('import json\n\ndef f(items, reverse=False, *rest):\n'
              '    """Sorted items, as JSON."""\n'
              '    def key(item, default=0):\n        return item.get("n", default)\n'
              '    return json.dumps(sorted(items, key=key, reverse=reverse), indent=2)\n')
    prepared = bb.code_prepare_source(source, 'eng')
    assert prepared['hash']


def test_positional_calls_to_an_inner_function_run(tmp_path):
    env = {**os.environ, 'BB_DIRECTORY': str(tmp_path / 'bb'), 'BB_STATE_DIRECTORY': str(tmp_path / 'state')}
    keyword = tmp_path / 'outer_eng.py'
    keyword.write_text('def outer(text):\n    def inner(value, height=1):\n        return value * height\n'
                       '    return inner(text, height=3)\n', encoding='utf-8')
    result = subprocess.run([sys.executable, str(BB), 'add', f'{keyword}@eng'], capture_output=True, text=True, env=env)
    assert result.returncode == 1 and 'line 4: inner(text, height=3)' in result.stderr

    keyword.write_text('def outer(text):\n    def inner(value, height=1):\n        return value * height\n'
                       '    return inner(text, 3)\n', encoding='utf-8')
    result = subprocess.run([sys.executable, str(BB), 'add', f'{keyword}@eng'], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    func_hash = result.stdout.split('Hash:')[1].split()[0]
    run = subprocess.run([sys.executable, '-c', f'import bb; print(bb.code_load_normalized("{func_hash}")("ab"))'],
                         capture_output=True, text=True, env=env, cwd=str(BB.parent))
    assert run.stdout.strip() == 'ababab', run.stderr


def test_a_dotted_import_keeps_its_top_level_name_and_runs(tmp_path):
    env = {**os.environ, 'BB_DIRECTORY': str(tmp_path / 'bb'), 'BB_STATE_DIRECTORY': str(tmp_path / 'state')}
    source = tmp_path / 'quote_eng.py'
    source.write_text('import urllib.parse\n\n\ndef quote(text):\n    """Quote text for a URL."""\n'
                      '    return urllib.parse.quote(text)\n', encoding='utf-8')
    prepared = bb.code_prepare_source(source.read_text(), 'eng')
    assert 'urllib.parse.quote' in prepared['normalized_code']
    result = subprocess.run([sys.executable, str(BB), 'add', f'{source}@eng'], capture_output=True, text=True, env=env)
    func_hash = result.stdout.split('Hash:')[1].split()[0]
    run = subprocess.run([sys.executable, '-c', f'import bb; print(bb.code_load_normalized("{func_hash}")("a b"))'],
                         capture_output=True, text=True, env=env, cwd=str(BB.parent))
    assert run.stdout.strip() == 'a%20b', run.stderr


def test_shown_code_keeps_multi_line_strings_readable_and_its_hash(tmp_path):
    source = ('def css():\n    """The stylesheet."""\n    return """body {\n  color: navy;\n}\na::after { content: "\\\\2192"; }\n"""\n')
    prepared = bb.code_prepare_source(source, 'eng')
    shown = bb.code_denormalize(prepared['normalized_code'], prepared['name_mapping'], prepared['alias_mapping'])
    assert 'body {\n  color: navy;\n}' in shown and '\\n' not in shown.split('return', 1)[1].split('\n')[0]
    assert bb.code_prepare_source(shown, 'eng')['hash'] == prepared['hash']   # shown code is the same function


def test_strings_that_cannot_be_triple_quoted_fall_back(tmp_path):
    for value in ['ends with a quote\n"', 'has """ and \'\'\' both\n', 'tab\tand\rreturn\n', 'nul\x00\n']:
        source = f'def f():\n    return {value!r}\n'
        prepared = bb.code_prepare_source(source, 'eng')
        shown = bb.code_denormalize(prepared['normalized_code'], prepared['name_mapping'], prepared['alias_mapping'])
        namespace = {}
        exec(shown, namespace)
        assert namespace['f']() == value
        assert bb.code_prepare_source(shown, 'eng')['hash'] == prepared['hash']
