"""bb edit --refactor ROOT: save, then update ROOT to the new version."""
from tests.test_import_by_name import Pool, hash_of


def chain(pool):
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    pool.edit('quadruple', 'import bb\ndouble = bb.latest("double")\n\n\ndef quadruple(n):\n    """Four times n."""\n'
              '    return double(double(n))\n')
    return hash_of(pool.edit('app', 'import bb\nquadruple = bb.latest("quadruple")\n\n\ndef app(n):\n    """The app."""\n'
                             '    return quadruple(n) + 1\n'))


def test_refactor_updates_everything_between_the_root_and_the_edited_function(tmp_path):
    pool = Pool(tmp_path)
    old_app = chain(pool)
    assert pool.run(old_app, 1) == '5'
    changed = pool.edit('double', 'def double(n):\n    """Twice n, plus ten."""\n    return 2 * n + 10\n',
                        '--refactor', 'app@eng')
    assert changed.returncode == 0, changed.stderr
    assert 'Updating app@eng to it:' in changed.stdout
    latest = pool.bb('latest', 'app@eng')
    assert "Current versions of 'app@eng': 1" in latest.stdout and old_app not in latest.stdout.split('Hash:')[1].split()[0]
    new_app = latest.stdout.split('Hash:')[1].split()[0]
    # double(1) = 12, double(12) = 34, + 1
    assert pool.run(new_app, 1) == '35'
    assert pool.run(old_app, 1) == '5'  # the old app keeps its versions


def test_refactor_mistakes_are_refused_before_the_editor_opens(tmp_path):
    pool = Pool(tmp_path)
    chain(pool)
    new = pool.bb('edit', 'fresh@eng', '--refactor', 'app@eng', editor='false')
    assert new.returncode == 1 and 'needs an existing function' in new.stderr
    unrelated = pool.bb('edit', 'app@eng', '--refactor', 'double@eng', editor='false')
    assert unrelated.returncode == 1 and 'does not use app@eng' in unrelated.stderr
    alone = pool.bb('edit', 'double@eng', '--at', 'quadruple@eng', editor='false')
    assert alone.returncode == 1 and '--at goes with --refactor' in alone.stderr
