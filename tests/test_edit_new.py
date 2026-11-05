"""bb edit can create a function; edits chain parents, so a name keeps one current version."""
import os
import subprocess
import sys
from pathlib import Path

BB = Path(__file__).parent.parent / 'bb.py'


def bb(env, *args):
    return subprocess.run([sys.executable, str(BB), *args], capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL)


def editor_env(tmp_path, command):
    return {**os.environ, 'BB_DIRECTORY': str(tmp_path / 'bb'), 'BB_STATE_DIRECTORY': str(tmp_path / 'state'),
            'EDITOR': command}


def test_a_new_function_then_a_change_keep_one_current_version(tmp_path):
    draft = tmp_path / 'draft.md'
    # Read: an editor that copies the buffer out and changes nothing
    read = editor_env(tmp_path, f'sh -c \'cp "$0" {draft}\'')
    result = bb(read, 'edit', 'slugify@eng')
    assert 'No changes' in result.stdout and 'def slugify():' in draft.read_text()

    draft.write_text(draft.read_text().replace(
        'def slugify():\n    """Say what slugify does."""\n    raise NotImplementedError',
        'def slugify(text):\n    """Lower-case words joined by dashes."""\n    return "-".join(text.lower().split())'))
    write = editor_env(tmp_path, f'cp {draft}')
    first = bb(write, 'edit', 'slugify@eng', '--comment', 'first version')
    assert first.returncode == 0, first.stderr
    first_hash = first.stdout.split('Hash:')[1].split()[0]
    assert 'Parent:' not in first.stdout

    bb(read, 'edit', 'slugify@eng')
    draft.write_text(draft.read_text().replace("'-'.join", "'_'.join"))
    second = bb(write, 'edit', 'slugify@eng', '--comment', 'underscores')
    assert second.returncode == 0, second.stderr
    assert f'Parent: {first_hash}' in second.stdout

    latest = bb(read, 'latest', 'slugify', 'eng')
    assert "Current versions of 'slugify@eng': 1" in latest.stdout  # no ambiguity


def test_a_new_function_needs_its_language_and_the_template_alone_saves_nothing(tmp_path):
    env = editor_env(tmp_path, 'true')
    assert 'give its language' in bb(env, 'edit', 'slugify').stderr
    result = bb(env, 'edit', 'slugify@eng')
    assert 'no function was created' in result.stdout
    assert 'No function found' in bb(env, 'show', 'slugify@eng').stderr
