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


FIND = ('def {name}(records, key, value):\n    """Find a record."""\n    found = []\n'
        '    for record in records:\n        if record.get(key) == value and record.get("{flag}"):\n'
        '            found.append(record)\n        elif record.get(key) is None:\n            continue\n'
        '        elif len(found) > 10:\n            break\n'
        '    found.sort(key=lambda r: r.get("created", ""))\n    return found[0] if found else None\n')


def test_search_similar_lists_functions_whose_code_looks_alike(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('order_find', FIND.format(name='order_find', flag='paid'))
    pool.edit('product_find', FIND.format(name='product_find', flag='listed'))
    pool.edit('lonely', 'def lonely():\n    """L."""\n    return 7\n')
    out = pool.bb('search', '--similar').stdout
    assert 'matches for: --similar' in out
    assert 'Name: order_find (eng)' in out and 'looks like product_find' in out
    assert 'lonely' not in out


def test_rm_name_says_when_its_previous_version_is_current_again(tmp_path):
    pool = Pool(tmp_path)
    first = hash_of(pool.edit('card', 'def card():\n    """First."""\n    return 1\n'))
    pool.edit('card', 'def card():\n    """Second."""\n    return 2\n')
    removed = pool.bb('rm', 'card@eng')
    assert removed.returncode == 0, removed.stderr
    assert f'card@eng is current again at its previous version {first}' in removed.stdout
    assert 'Warning' not in removed.stdout and 'has no name left' in removed.stdout
    again = pool.bb('rm', 'card@eng')
    assert again.returncode == 0 and 'current again' not in again.stdout
    assert 'Functions (0 current versions)' in pool.bb('search').stdout


TWO_STRINGS = ('import bb\nqux = bb.latest("qux")\n\n\ndef noted(n):\n    """Noted."""\n'
               '    "a string that is not the docstring"\n    return qux(n) * 2\n')


def test_update_and_refactor_hash_the_code_they_store(tmp_path):
    """A body that starts with a string after its docstring keeps it: the rewrite's hash is that of the stored code"""
    import json
    import bb
    pool, _, v2 = chain(tmp_path)
    assert pool.edit('noted', TWO_STRINGS).returncode == 0

    def stored_matches(func_hash):
        path = tmp_path / 'bb' / 'pool' / 'functions' / func_hash[:2] / func_hash[2:] / 'object.json'
        data = json.loads(path.read_text())
        assert 'not the docstring' in data['normalized_code']
        # What bb run checks before executing: the stored code hashes to its name
        return pool.run(func_hash, 1) != '' and bb.object_hash_matches(func_hash, data)

    draft = tmp_path / 'qux.md'
    set_code(draft, QUX.format(3))
    assert pool.bb('edit', 'qux@eng', str(draft), '-m', 'v3').returncode == 0
    updated = pool.bb('update', 'noted@eng', '-m', 'catch up')
    assert 'Updated 1 function(s)' in updated.stdout, updated.stdout
    assert updated.returncode == 0, updated.stderr
    assert stored_matches(current(pool, 'noted')), updated.stdout + updated.stderr

    # The same through bb edit --refactor
    set_code(draft, QUX.format(4))
    saved = pool.bb('edit', 'qux@eng', str(draft), '-m', 'v4', '--refactor', 'noted@eng')
    assert saved.returncode == 0, saved.stderr
    assert stored_matches(current(pool, 'noted'))



def test_update_runs_no_check_on_a_body_nobody_approved(tmp_path):
    """The checks of a rewrite call the body of the function rewritten: it is approved or yours, or nothing runs"""
    import subprocess
    import sys
    from tests.test_import_by_name import BB
    pool, _, _ = chain(tmp_path)
    marker = tmp_path / 'ran'
    draft = tmp_path / 'spy.md'
    pool.bb('edit', 'spy@eng', str(draft))
    set_code(draft, 'import bb\nqux = bb.latest("qux")\n\n\ndef spy(n):\n    """Spy."""\n'
                    f'    open({str(marker)!r}, "w").close()\n    return qux(n)\n')
    draft.write_text(draft.read_text().rstrip('\n') + '\n\n### spy_returns\n\n```python def\n'
                     'def spy_returns(spy):\n    """It answers."""\n    assert spy(1) is not None\n```\n')
    saved = pool.bb('edit', 'spy@eng', str(draft), '-m', 'spy')
    assert saved.returncode == 0 and 'PASS spy_returns' in saved.stdout, saved.stderr
    spy = hash_of(saved)
    marker.unlink()  # its author ran it; from here on nobody should
    qux = tmp_path / 'qux.md'
    set_code(qux, QUX.format(5))
    assert pool.bb('edit', 'qux@eng', str(qux), '-m', 'v5').returncode == 0

    # Someone else approved the new qux and the check, but never read spy
    other = Pool(tmp_path)
    other.env = {**pool.env, 'BB_STATE_DIRECTORY': str(tmp_path / 'other-state')}
    for name in ('qux', 'spy_returns'):
        subprocess.run([sys.executable, str(BB), 'review', f'{name}@eng'], input='y\n' * 9, text=True,
                       capture_output=True, env=other.env, check=True)
    refused = other.bb('update', 'spy@eng', '-m', 'catch up')
    assert refused.returncode == 1 and f'not run, review first: bb review {spy}' in refused.stderr, refused.stderr
    assert not marker.exists()
    assert current(other, 'spy') == spy

    # Its author may: the body is theirs
    updated = pool.bb('update', 'spy@eng', '-m', 'catch up')
    assert updated.returncode == 0 and marker.exists(), updated.stderr


def test_update_and_refactor_refuse_to_rewrite_code_swapped_under_its_hash(tmp_path):
    """A rewrite of swapped code would be saved under a new hash it matches: the swap could no longer be told"""
    import json
    pool, v1, v2 = chain(tmp_path)
    mid = current(pool, 'mid')
    path = tmp_path / 'bb' / 'pool' / 'functions' / mid[:2] / mid[2:] / 'object.json'
    data = json.loads(path.read_text())
    genuine = path.read_text()
    data['normalized_code'] = data['normalized_code'].replace('* 2', '* 666')
    data.pop('public_key', None)
    path.write_text(json.dumps(data))
    functions = lambda: sorted(p.parent.name for p in (tmp_path / 'bb' / 'pool' / 'functions').glob('*/*/object.json'))
    before = functions()

    refused = pool.bb('update', 'mid@eng', '-m', 'catch up')
    assert refused.returncode == 1, refused.stdout
    assert 'does not match its stored code' in refused.stderr and 'refusing to update it' in refused.stderr
    refused = pool.bb('refactor', mid, v1, v2, '-m', 'by hand')
    assert refused.returncode == 1, refused.stdout
    assert 'refusing to refactor it' in refused.stderr
    # ... also when it is on the way up to the function asked for
    refused = pool.bb('update', 'top@eng', '-m', 'catch up')
    assert refused.returncode == 1 and 'refusing to update it' in refused.stderr
    assert functions() == before and current(pool, 'mid') == mid

    # Code not saved yet is loaded like the version it rewrites: that one is checked too
    code = ('import bb, sys\ntry:\n    bb.code_load_normalized("0" * 64, source=sys.argv[2], like=sys.argv[1])\n'
            'except ValueError as e:\n    print(e)\n')
    import subprocess
    import sys
    from tests.test_import_by_name import BB
    loaded = subprocess.run([sys.executable, '-c', code, mid, json.loads(genuine)['normalized_code']],
                            capture_output=True, text=True, env=pool.env, cwd=str(BB.parent))
    assert 'does not match its stored code' in loaded.stdout, loaded.stderr

    path.write_text(genuine)
    assert pool.bb('update', 'top@eng', '-m', 'catch up').returncode == 0
