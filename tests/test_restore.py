"""Going back to an older version: a restore record makes it current again."""
from tests.test_edit_file import set_code
from tests.test_import_by_name import Pool, hash_of

QUX = 'def qux(n):\n    """Q."""\n    return n + {}\n'
APP = 'import bb\nqux = bb.latest("qux")\n\n\ndef app(n):\n    """App."""\n    return qux(n)\n'


def current(pool, name):
    out = pool.bb('latest', f'{name}@eng').stdout
    assert f"Current versions of '{name}@eng': 1" in out, out
    return out.split('Hash:')[1].split()[0]


def test_editing_back_to_old_code_restores_it(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    v1 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1'))
    set_code(draft, QUX.format(2))
    pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2')
    set_code(draft, QUX.format(1))
    back = pool.bb('edit', 'qux@eng', str(draft), '-m', 'back')
    assert back.returncode == 0, back.stderr
    assert f'Restored: {v1}' in back.stdout
    assert current(pool, 'qux') == v1
    # Editing goes on from the restore: one current version, the new one
    set_code(draft, QUX.format(3))
    v3 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v3'))
    assert current(pool, 'qux') == v3
    assert 'No matches found' in pool.bb('search', '--diverged').stdout


def test_edit_back_with_refactor_restores_the_app_too(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1')
    app1 = hash_of(pool.edit('app', APP))
    set_code(draft, QUX.format(2))
    assert pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2', '--refactor', 'app@eng').returncode == 0
    assert current(pool, 'app') != app1
    set_code(draft, QUX.format(1))
    back = pool.bb('edit', 'qux@eng', str(draft), '-m', 'back', '--refactor', 'app@eng')
    assert back.returncode == 0, back.stderr
    assert f'restored: {app1}' in back.stdout
    assert current(pool, 'app') == app1
    assert pool.run(app1, 1) == '2'


def test_refactoring_to_an_older_dependency_restores_the_caller(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    v1 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1'))
    app1 = hash_of(pool.edit('app', APP))
    set_code(draft, QUX.format(2))
    v2 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2', '--refactor', 'app@eng'))
    app2 = current(pool, 'app')
    back = pool.bb('refactor', app2, v2, v1, '-m', 'use v1 again')
    assert f'restored: {app1}' in back.stdout
    assert current(pool, 'app') == app1
    assert current(pool, 'qux') == v2  # the name qux itself still picks v2
    assert f"qux = bb.ref('{v1}')" in pool.bb('show', 'app@eng').stdout  # so app shows v1 pinned


def test_editing_into_another_functions_code_is_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice."""\n    return 2 * n\n')
    pool.edit('triple', 'def triple(n):\n    """Thrice."""\n    return 3 * n\n')
    same = pool.edit('triple', 'def triple(n):\n    """Twice."""\n    return 2 * n\n')
    assert same.returncode == 1 and 'another function' in same.stderr


def test_a_restore_is_in_the_log_and_not_a_language(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    v1 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1'))
    set_code(draft, QUX.format(2))
    pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2')
    set_code(draft, QUX.format(1))
    pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2 broke the totals')
    log = pool.bb('log').stdout
    assert '1 restores' in log and f'Restored: {v1}' in log and 'v2 broke the totals' in log
    assert 'Languages: eng\n' in log
    assert list((tmp_path / 'bb' / 'restore').glob('*.json'))
