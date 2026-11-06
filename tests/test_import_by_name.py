"""Pool imports by name resolve by lineage to one current version, at save time."""
import os
import subprocess
import sys
from pathlib import Path

BB = Path(__file__).parent.parent / 'bb.py'


class Pool:
    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.draft = tmp_path / 'draft.md'
        self.env = {**os.environ, 'BB_DIRECTORY': str(tmp_path / 'bb'), 'BB_STATE_DIRECTORY': str(tmp_path / 'state')}

    def bb(self, *args, editor=None):
        env = {**self.env, 'EDITOR': editor} if editor else self.env
        return subprocess.run([sys.executable, str(BB), *args], capture_output=True, text=True, env=env,
                              stdin=subprocess.DEVNULL)

    def edit(self, name, code, *args):
        """bb edit name@eng with the implementation replaced by code (a new name or a change)."""
        self.bb('edit', f'{name}@eng', editor=f'sh -c \'cp "$0" {self.draft}\'')
        text = self.draft.read_text()
        start = text.index('```python def\n') + len('```python def\n')
        end = text.index('```', start)
        self.draft.write_text(text[:start] + code + text[end:])
        return self.bb('edit', f'{name}@eng', '-m', 'change', *args, editor=f'cp {self.draft}')

    def add(self, name, code):
        source = self.tmp / f'{name}_eng.py'
        source.write_text(code)
        return self.bb('add', f'{source}@eng')

    def run(self, func_hash, *args):
        code = f'import bb; print(bb.code_load_normalized("{func_hash}")(*{args!r}))'
        return subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=self.env,
                              cwd=str(BB.parent)).stdout.strip()


def hash_of(result):
    return result.stdout.split('Hash:')[1].split()[0]


def test_a_name_resolves_to_its_one_current_version_and_follows_edits(tmp_path):
    pool = Pool(tmp_path)
    first = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n'))

    saved = pool.edit('quadruple', 'import bb\ndouble = bb.latest("double")\n\n\ndef quadruple(n):\n    """Four times n."""\n'
                      '    return double(double(n))\n')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: double -> {first}' in saved.stdout
    four_times = hash_of(saved)
    assert pool.run(four_times, 3) == '12'
    stored = next((tmp_path / 'bb' / 'pool').glob(f'{four_times[:2]}/{four_times[2:]}/object.json')).read_text()
    assert f"bb.ref('{first}')" in stored  # stored by hash
    assert "double = bb.latest('double')" in pool.bb('show', 'quadruple@eng').stdout  # shown by name

    # A new version of double: the name now resolves to it; quadruple keeps the version it was saved with
    second = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return n + n + 0\n'))
    again = pool.edit('octuple', 'import bb\ntwice = bb.latest("double")\n\n\ndef octuple(n):\n    """Eight times n."""\n'
                      '    return twice(twice(twice(n)))\n')
    assert f'Import: double -> {second}' in again.stdout and pool.run(hash_of(again), 1) == '8'
    # double now names another version: quadruple shows the one it uses, pinned
    assert f"double = bb.ref('{first}')" in pool.bb('show', 'quadruple@eng').stdout


def test_two_unrelated_functions_with_one_name_are_refused_with_both_listed(tmp_path):
    pool = Pool(tmp_path)
    one = hash_of(pool.add('helper', 'def helper(n):\n    """Plus one."""\n    return n + 1\n'))
    two = hash_of(pool.add('helper', 'def helper(n):\n    """Times ten."""\n    return n * 10\n'))
    refused = pool.edit('user', 'import bb\nhelper = bb.latest("helper")\n\n\ndef user(n):\n    """Use helper."""\n'
                        '    return helper(n)\n')
    assert refused.returncode == 1
    assert '2 current versions' in refused.stderr and one in refused.stderr and two in refused.stderr
    chosen = pool.edit('user', f'import bb\nhelper = bb.ref("{two}")\n\n\ndef user(n):\n    """Use helper."""\n'
                       '    return helper(n)\n')
    assert chosen.returncode == 0 and pool.run(hash_of(chosen), 4) == '40'


def test_an_unknown_name_is_refused_by_add_and_edit(tmp_path):
    pool = Pool(tmp_path)
    added = pool.add('user', 'import bb\nnowhere = bb.latest("nowhere")\n\n\ndef user(n):\n    """Use nowhere."""\n    return nowhere(n)\n')
    assert added.returncode == 1 and "no function called 'nowhere' in eng" in added.stderr
    edited = pool.edit('user', 'import bb\nnowhere = bb.latest("nowhere")\n\n\ndef user(n):\n    """Use nowhere."""\n'
                       '    return nowhere(n)\n')
    assert edited.returncode == 1 and "no function called 'nowhere'" in edited.stderr
