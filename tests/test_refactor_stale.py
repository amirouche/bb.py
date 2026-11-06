"""bb edit --refactor over a function edited since the root last saw it."""
from tests.test_edit_file import set_code
from tests.test_import_by_name import Pool, hash_of


def current(pool, name):
    out = pool.bb('latest', f'{name}@eng').stdout
    assert f"Current versions of '{name}@eng': 1" in out, out
    return out.split('Hash:')[1].split()[0]


def test_a_refactor_composes_with_an_edit_the_root_does_not_use_yet(tmp_path):
    pool = Pool(tmp_path)
    leaf, mid = tmp_path / 'leaf.md', tmp_path / 'mid.md'
    pool.bb('edit', 'leaf@eng', str(leaf))
    set_code(leaf, 'def leaf(n):\n    """Leaf."""\n    return n + 1\n')
    pool.bb('edit', 'leaf@eng', str(leaf), '-m', 'leaf')
    pool.bb('edit', 'mid@eng', str(mid))
    set_code(mid, 'import bb\nleaf = bb.latest("leaf")\n\n\ndef mid(n):\n    """Mid."""\n    return leaf(n) * 2\n')
    pool.bb('edit', 'mid@eng', str(mid), '-m', 'mid')
    pool.edit('main', 'import bb\nmid = bb.latest("mid")\n\n\ndef main(n):\n    """Main."""\n    return mid(n)\n')
    # mid changes, saved without --refactor: main still uses the first mid
    set_code(mid, 'import bb\nleaf = bb.latest("leaf")\n\n\ndef mid(n):\n    """Mid."""\n    return leaf(n) * 3\n')
    mid2 = hash_of(pool.bb('edit', 'mid@eng', str(mid), '-m', 'times three'))
    # then leaf changes, with --refactor main: mid must not fork
    set_code(leaf, 'def leaf(n):\n    """Leaf."""\n    return n + 10\n')
    saved = pool.bb('edit', 'leaf@eng', str(leaf), '-m', 'plus ten', '--refactor', 'main@eng')
    assert saved.returncode == 0, saved.stderr
    mid3 = current(pool, 'mid')  # one current mid...
    assert mid3 != mid2
    main = current(pool, 'main')
    assert pool.run(main, 1) == '33'  # ...with both changes: (1 + 10) * 3
    assert 'No matches found' in pool.bb('search', '--diverged').stdout


def test_rm_refuses_a_function_still_in_use(tmp_path):
    pool = Pool(tmp_path)
    leaf = hash_of(pool.edit('leaf', 'def leaf(n):\n    """Leaf."""\n    return n + 1\n'))
    user = hash_of(pool.edit('user', 'import bb\nleaf = bb.latest("leaf")\n\n\ndef user(n):\n    """User."""\n    return leaf(n)\n'))
    refused = pool.bb('rm', leaf)
    assert refused.returncode == 1 and f'used by user {user}' in refused.stderr
    assert pool.bb('latest', 'leaf@eng').returncode == 0  # still there
    assert pool.bb('rm', user).returncode == 0  # nothing uses user
    assert pool.bb('rm', leaf).returncode == 0  # and now nothing uses leaf
    assert pool.bb('rm', '--force', 'x' * 64).returncode == 1  # a missing function is still an error
