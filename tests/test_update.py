"""bb update brings a function and everything it uses up to date in one step; --outdated lists the whole way up."""
from tests.test_edit_file import set_code
from tests.test_import_by_name import Pool, hash_of
from tests.test_restore import QUX, current

MID = 'import bb\nqux = bb.latest("qux")\n\n\ndef mid(n):\n    """Mid."""\n    return qux(n) * 2\n'
TOP = 'import bb\nmid = bb.latest("mid")\n\n\ndef top(n):\n    """Top."""\n    return mid(n) + 1\n'


def chain(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'qux.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(1))
    v1 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v1'))
    pool.edit('mid', MID)
    pool.edit('top', TOP)
    set_code(draft, QUX.format(2))
    v2 = hash_of(pool.bb('edit', 'qux@eng', str(draft), '-m', 'v2'))
    return pool, v1, v2


def test_outdated_lists_every_function_still_to_reach_and_suggests_update(tmp_path):
    pool, _, _ = chain(tmp_path)
    outdated = pool.bb('search', '--outdated').stdout
    # mid uses an old qux; top uses the current mid, but must follow once mid is saved
    assert 'Name: mid (eng)' in outdated and 'Name: top (eng)' in outdated
    assert 'behind: uses mid, which is out of date' in outdated
    assert 'bb update top@eng' in outdated


def test_update_rewrites_the_chain_deepest_first_in_one_step(tmp_path):
    pool, _, v2 = chain(tmp_path)
    updated = pool.bb('update', 'top@eng', '-m', 'catch up')
    assert updated.returncode == 0, updated.stderr
    assert 'Updated 2 function(s)' in updated.stdout
    assert updated.stdout.index('mid:') < updated.stdout.index('top:')
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout
    assert f"bb.ref('{v2}')" in pool.bb('show', current(pool, 'mid') + '@eng').stdout or \
        "qux = bb.resolve('qux')" in pool.bb('show', 'mid@eng').stdout
    assert current(pool, 'top') == hash_of(updated)
    assert 'is up to date' in pool.bb('update', 'top@eng', '-m', 'again').stdout


def test_update_keeps_pins(tmp_path):
    pool, v1, _ = chain(tmp_path)
    pinned = 'import bb\nqux = bb.ref("{}")\n\n\ndef keep(n):\n    """Keep."""\n    return qux(n) + 0\n'.format(v1)
    pool.edit('keep', pinned)
    assert 'is up to date' in pool.bb('update', 'keep@eng', '-m', 'no').stdout
    assert f"qux = bb.ref('{v1}')" in pool.bb('show', 'keep@eng').stdout


def test_rm_name_without_lang_removes_the_whole_function(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('lonely', 'def lonely():\n    """L."""\n    return 7\n')
    removed = pool.bb('rm', 'lonely')
    assert removed.returncode == 0 and 'Removed function' in removed.stdout, removed.stdout + removed.stderr
    assert 'No language mappings remain' not in removed.stdout
    assert 'Functions (0 current versions)' in pool.bb('search').stdout


def test_refactor_lists_the_functions_it_rewrote(tmp_path):
    pool, _, _ = chain(tmp_path)
    pool.bb('update', 'top@eng', '-m', 'catch up')
    draft = tmp_path / 'qux3.md'
    pool.bb('edit', 'qux@eng', str(draft))
    set_code(draft, QUX.format(3))
    out = pool.bb('edit', 'qux@eng', str(draft), '-m', 'v3', '--refactor', 'top@eng').stdout
    assert 'Rewrote 1 function(s) on the way' in out and 'mid:' in out and 'stale' in out
