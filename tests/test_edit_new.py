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
    assert 'Nothing saved' in result.stdout and 'def slugify():' in draft.read_text()

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
    assert 'Nothing saved' in result.stdout and 'no function was created' in result.stdout
    assert 'No function found' in bb(env, 'show', 'slugify@eng').stderr


def test_saving_runs_nothing_without_checks_and_checks_need_reviewed_imports(tmp_path):
    draft = tmp_path / 'draft.md'
    read = editor_env(tmp_path, f'sh -c \'cp "$0" {draft}\'')
    write = editor_env(tmp_path, f'cp {draft}')
    bb(read, 'edit', 'double@eng')
    draft.write_text(draft.read_text().replace(
        'def double():\n    """Say what double does."""\n    raise NotImplementedError',
        'def double(n):\n    """Twice n."""\n    return 2 * n'))
    helper = bb(write, 'edit', 'double@eng', '-m', 'new').stdout.split('Hash:')[1].split()[0]

    # No checks: saved without running it, though double was never reviewed
    bb(read, 'edit', 'quadruple@eng')
    draft.write_text(draft.read_text().replace(
        'def quadruple():\n    """Say what quadruple does."""\n    raise NotImplementedError',
        f'import bb\ndouble = bb.ref("{helper}")\n\n\ndef quadruple(n):\n    """Four times n."""\n'
        '    return double(double(n))'))
    saved = bb(write, 'edit', 'quadruple@eng', '-m', 'new')
    assert saved.returncode == 0 and 'Hash:' in saved.stdout and 'reviewed' not in saved.stderr

    # A check runs the code. double was saved here, by you: it may run
    bb(read, 'edit', 'quadruple@eng')
    with_check = draft.read_text().rstrip('\n') + ('\n\n### check_quadruple\n\n```python def\n'
                                                   'def check_quadruple(quadruple):\n    """Four times one."""\n'
                                                   '    return quadruple(1) == 4\n```\n')
    draft.write_text(with_check)
    ran = bb(write, 'edit', 'quadruple@eng', '-m', 'with a check')
    assert ran.returncode == 0 and 'PASS check_quadruple' in ran.stdout, ran.stderr

    # Someone else's double (here: another user's private state) is not reviewed: refused, or the flag
    other = {**write, 'BB_STATE_DIRECTORY': str(tmp_path / 'other-state')}
    draft.write_text(with_check.replace('quadruple(1) == 4', 'quadruple(2) == 8'))
    refused = bb(other, 'edit', 'quadruple@eng', '-m', 'their check')
    assert refused.returncode == 1 and 'not reviewed' in refused.stderr
    allowed = bb(other, 'edit', 'quadruple@eng', '-m', 'their check', '--dangerously-unreviewed')
    assert allowed.returncode == 0 and 'nobody reviewed' in allowed.stderr and 'Parent:' in allowed.stdout


def test_latest_takes_name_at_lang_like_every_other_command(tmp_path):
    draft = tmp_path / 'draft.md'
    read = editor_env(tmp_path, f'sh -c \'cp "$0" {draft}\'')
    bb(read, 'edit', 'slugify@eng')
    draft.write_text(draft.read_text().replace('raise NotImplementedError', 'return 1'))
    assert bb(editor_env(tmp_path, f'cp {draft}'), 'edit', 'slugify@eng', '-m', 'new').returncode == 0
    assert bb(read, 'latest', 'slugify@eng').stdout == bb(read, 'latest', 'slugify', 'eng').stdout != ''


def test_search_alone_lists_functions_log_names_them_latest_hash_prints_hashes(tmp_path):
    draft = tmp_path / 'draft.md'
    read = editor_env(tmp_path, f'sh -c \'cp "$0" {draft}\'')
    bb(read, 'edit', 'slugify@eng')
    draft.write_text(draft.read_text().replace('raise NotImplementedError', 'return 1'))
    saved = bb(editor_env(tmp_path, f'cp {draft}'), 'edit', 'slugify@eng', '-m', 'new')
    func_hash = saved.stdout.split('Hash:')[1].split()[0]

    listed = bb(read, 'search')
    assert listed.returncode == 0 and 'Functions (1 current versions)' in listed.stdout
    assert 'Name: slugify (eng)' in listed.stdout
    assert 'Names: slugify@eng' in bb(read, 'log').stdout
    assert bb(read, 'latest', 'slugify@eng', '--hash').stdout == func_hash + '\n'
