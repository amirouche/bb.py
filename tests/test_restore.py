"""Going back to an older version: a restore step makes it current again."""
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
    assert f"out of date: uses {v1[:12]}..." in pool.bb('show', 'app@eng').stdout  # app's debt, shown


def test_editing_into_another_names_code_shares_it(tmp_path):
    pool = Pool(tmp_path)
    double = hash_of(pool.edit('double', 'def double(n):\n    """Twice."""\n    return 2 * n\n'))
    pool.edit('triple', 'def triple(n):\n    """Thrice."""\n    return 3 * n\n')
    same = pool.edit('triple', 'def triple(n):\n    """Twice."""\n    return 2 * n\n')
    assert same.returncode == 0, same.stderr
    # Both names are now on one code, each by its own lineage
    assert current(pool, 'triple') == current(pool, 'double') == double


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
    assert 'lineage steps' in log and f'Restore: qux@eng {v1}' in log and 'v2 broke the totals' in log
    assert 'Languages: eng\n' in log
    assert not (tmp_path / 'bb' / 'restore').exists()  # restores are lineage steps now


def test_names_show_debt_and_editing_pays_it_while_pins_stay(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    v1 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1'))
    pool.edit('app', APP)
    pinned = 'import bb\nqux = bb.ref("{}")\n\n\ndef keep(n):\n    """Keep."""\n    return qux(n) + 0\n'.format(v1)
    pool.edit('keep', pinned)
    set_code(draft, QUX.format(2))
    v2 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2'))
    # app is behind: listed as debt; keep pinned v1 on purpose: not listed
    outdated = pool.bb('search', '--outdated').stdout
    assert 'app' in outdated and 'keep' not in outdated.split('Search Results')[1].split('\n', 2)[2]
    assert f"qux = bb.ref('{v1}')" in pool.bb('show', 'keep@eng').stdout
    # Editing app (no change of its own) brings it up to date
    app_draft = tmp_path / 'app.md'
    pool.bb('edit', 'app@eng', str(app_draft))
    saved = pool.bb('edit', 'app@eng', str(app_draft), '-m', 'catch up')
    assert f'Import: qux -> {v2} (was {v1})' in saved.stdout
    assert 'No matches found' in pool.bb('search', '--outdated').stdout


def test_log_of_one_name_shows_its_steps_only(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('alpha', 'def alpha():\n    """A."""\n    return 1\n')
    pool.edit('alpha', 'def alpha():\n    """A again."""\n    return 2\n')
    pool.edit('beta', 'def beta():\n    """B."""\n    return 3\n')
    log = pool.bb('log', 'alpha@eng').stdout
    assert log.startswith('Log of alpha@eng (2 functions, 2 lineage steps')
    assert 'beta' not in log
    assert pool.bb('log', 'nobody@eng').returncode == 1
