"""bb edit NAME FILE: write FILE when it is missing, save from it when it is there."""
import json
import re

from tests.test_import_by_name import Pool, hash_of


def step_of(tmp_path, draft):
    """The lineage step a draft's bb:parent marker names."""
    step_id = re.search(r'bb:parent ([0-9a-f]{64})', draft.read_text()).group(1)
    return json.loads((tmp_path / 'bb' / 'pool' / 'lineage' / step_id[:2] / f'{step_id[2:]}.json').read_text())


def set_code(path, code):
    """Replace the implementation's python def block in an edit file."""
    text = path.read_text()
    start = text.index('```python def\n') + len('```python def\n')
    end = text.index('```', start)
    path.write_text(text[:start] + code + text[end:])


def test_a_missing_file_is_written_and_nothing_is_saved(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'double.md'
    wrote = pool.bb('edit', 'double@eng', str(draft), editor='false')
    assert wrote.returncode == 0, wrote.stderr
    assert 'Wrote' in wrote.stdout and 'def double():' in draft.read_text()
    assert pool.bb('latest', 'double@eng').returncode == 1  # nothing saved


def test_saving_from_the_file_keeps_it_as_the_saved_version(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'double.md'
    pool.bb('edit', 'double@eng', str(draft))
    set_code(draft, 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    first = pool.bb('edit', 'double@eng', str(draft), '-m', 'new')
    assert first.returncode == 0, first.stderr
    first_hash = hash_of(first)
    # The draft now continues the lineage step just saved, on the saved version
    assert step_of(tmp_path, draft)['function'] == first_hash
    # Editing goes on in the same file
    set_code(draft, 'def double(n):\n    """Twice n."""\n    return n + n\n')
    second = pool.bb('edit', 'double@eng', str(draft), '-m', 'sum')
    assert second.returncode == 0, second.stderr
    assert f'Parent: {first_hash}' in second.stdout
    assert pool.bb('edit', 'double@eng', str(draft), '-m', 'again').stdout.strip().endswith('No changes.')


def test_a_stale_file_is_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    old = tmp_path / 'old.md'
    pool.bb('edit', 'double@eng', str(old))
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return n + n\n')
    set_code(old, 'def double(n):\n    """Twice n."""\n    return n * 2\n')
    refused = pool.bb('edit', 'double@eng', str(old), '-m', 'late')
    assert refused.returncode == 1 and 'was drafted from' in refused.stderr
    assert refused.stdout == ''  # refused before anything runs
    assert "Current versions of 'double@eng': 1" in pool.bb('latest', 'double@eng').stdout


def test_a_failing_save_leaves_the_file_alone(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'broken.md'
    pool.bb('edit', 'broken@eng', str(draft))
    set_code(draft, 'def broken(:\n')
    text = draft.read_text()
    failed = pool.bb('edit', 'broken@eng', str(draft), '-m', 'x')
    assert failed.returncode == 1 and 'Not saved: fix' in failed.stderr
    assert draft.read_text() == text


def test_a_new_name_may_share_code_already_in_the_pool(tmp_path):
    pool = Pool(tmp_path)
    double = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n'))
    same = pool.edit('twice', 'def twice(n):\n    """Twice n."""\n    return 2 * n\n')
    assert same.returncode == 0, same.stderr
    assert hash_of(same) == double
    # Each name has its own mapping and its own lineage
    assert pool.bb('latest', 'twice@eng', '--hash').stdout.split() == [double]
    assert pool.bb('latest', 'double@eng', '--hash').stdout.split() == [double]
    assert 'def twice(n):' in pool.bb('show', 'twice@eng').stdout
    assert 'def double(n):' in pool.bb('show', 'double@eng').stdout


def test_a_bare_newline_stays_escaped():
    import ast
    import bb
    assert bb.code_unparse_readable(ast.parse('x = "\\n"\n')) == "x = '\\n'"
    assert bb.code_unparse_readable(ast.parse('x = "a\\nb"\n')) == 'x = """a\nb"""'


def test_diverged_lists_forks_not_unrelated_namesakes(tmp_path):
    pool = Pool(tmp_path)
    # Two unrelated functions called validates (as two checks often are)
    one = tmp_path / 'one.md'
    pool.bb('edit', 'validates@eng', str(one))
    set_code(one, 'def validates(f):\n    """First."""\n    return f(1) == 2\n')
    pool.bb('edit', 'validates@eng', str(one), '-m', 'one')
    other = hash_of(pool.add('validates', 'def validates(f):\n    """Second."""\n    return f(2) == 4\n'))
    diverged = pool.bb('search', '--diverged')
    assert other not in diverged.stdout
    assert 'another origin' in pool.bb('search', 'validates').stdout
    # A fork: two saves from the same version
    first = tmp_path / 'first.md'
    second = tmp_path / 'second.md'
    pool.bb('edit', f'{other}@eng', str(first))
    pool.bb('edit', f'{other}@eng', str(second))
    set_code(first, 'def validates(f):\n    """Second."""\n    return f(2) == 4 and True\n')
    set_code(second, 'def validates(f):\n    """Second."""\n    return f(3) == 6\n')
    assert pool.bb('edit', f'{other}@eng', str(first), '-m', 'a').returncode == 0
    assert pool.bb('edit', f'{other}@eng', str(second), '-m', 'b').returncode == 0
    assert 'the same lineage, another end' in pool.bb('search', '--diverged').stdout


def fork(pool, tmp_path, identifier, code, comment, name='fork.md'):
    """bb edit identifier --fork, with the implementation replaced by code."""
    draft = tmp_path / name
    pool.bb('edit', identifier, str(draft))
    set_code(draft, code)
    return pool.bb('edit', identifier, str(draft), '--fork', '-m', comment)


def test_a_fork_leaves_its_origin_current_and_nothing_out_of_date(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.edit('is_weekend', 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n'
                               '    return day in (5, 6)\n'))
    user = pool.edit('opening', 'import bb\nis_weekend = bb.resolve("is_weekend")\n\n\ndef opening(day):\n'
                     '    """Open on working days."""\n    return not is_weekend(day)\n')
    assert user.returncode == 0, user.stderr
    forked = fork(pool, tmp_path, 'is_weekend@eng', 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n'
                  '    return day in (4, 5)\n', 'Friday and Saturday, as in Algeria')
    assert forked.returncode == 0, forked.stderr
    assert f'Forked from: {origin} (still current)' in forked.stdout
    other = hash_of(forked)
    # Both answer to the name; neither is a newer version of the other
    assert sorted(pool.bb('latest', 'is_weekend@eng', '--hash').stdout.split()) == sorted([origin, other])
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout
    assert 'No matches found' in pool.bb('search', '--diverged').stdout
    found = json.loads(pool.bb('search', 'is_weekend', '--json').stdout)
    relatives = {r['hash']: r['relatives'] for r in found if r['name'] == 'is_weekend'}
    assert [(r['hash'], r['relation'], r['comment']) for r in relatives[other]] == \
        [(origin, 'fork', 'Friday and Saturday, as in Algeria')]
    assert [(r['hash'], r['relation']) for r in relatives[origin]] == [(other, 'forked')]
    shown = pool.bb('search', 'is_weekend').stdout
    assert f'Also is_weekend: {origin}, this one forked from it (Friday and Saturday, as in Algeria)' in shown
    assert f'Also is_weekend: {other}, forked from this one (Friday and Saturday, as in Algeria)' in shown
    # The name is not guessed; a hash picks one, and each goes on along its own lineage
    refused = pool.bb('show', 'is_weekend@eng')
    assert refused.returncode == 1 and "2 functions answer to 'is_weekend@eng'" in refused.stderr
    draft = tmp_path / 'on.md'
    pool.bb('edit', f'{other}@eng', str(draft))
    set_code(draft, 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return 4 <= day <= 5\n')
    later = pool.bb('edit', f'{other}@eng', str(draft), '-m', 'a range')
    assert later.returncode == 0 and f'Parent: {other}' in later.stdout
    assert sorted(pool.bb('latest', 'is_weekend@eng', '--hash').stdout.split()) == sorted([origin, hash_of(later)])
    assert 'No matches found' in pool.bb('search', '--diverged').stdout


def test_a_fork_needs_other_code_and_a_function_to_leave(tmp_path):
    pool = Pool(tmp_path)
    assert pool.bb('edit', 'nowhere@eng', str(tmp_path / 'n.md'), '--fork').returncode == 1
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    same = fork(pool, tmp_path, 'double@eng', 'def double(n):\n    """Two times n."""\n    return 2 * n\n', 'words')
    assert same.returncode == 1 and 'a fork is another function' in same.stderr
    assert "Current versions of 'double@eng': 1" in pool.bb('latest', 'double@eng').stdout


def test_a_fork_under_a_new_name_keeps_the_old_name(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n'))
    forked = fork(pool, tmp_path, 'double@eng', 'def triple(n):\n    """Thrice n."""\n    return 3 * n\n', 'thrice')
    assert forked.returncode == 0 and 'Renamed' not in forked.stdout
    assert pool.bb('latest', 'double@eng', '--hash').stdout.split() == [origin]
    assert pool.bb('latest', 'triple@eng', '--hash').stdout.split() == [hash_of(forked)]


def test_two_forks_of_one_function_are_kin(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n'))
    one = hash_of(fork(pool, tmp_path, f'{origin}@eng', 'def double(n):\n    """Twice n."""\n    return n + n\n',
                       'sums', 'one.md'))
    two = hash_of(fork(pool, tmp_path, f'{origin}@eng', 'def double(n):\n    """Twice n."""\n    return n << 1\n',
                       'shifts', 'two.md'))
    found = {r['hash']: r for r in json.loads(pool.bb('search', 'double', '--json').stdout)}
    assert {r['hash']: r['relation'] for r in found[one]['relatives']} == {origin: 'fork', two: 'kin'}
    assert not found[one]['diverged']


def test_a_fork_that_changes_a_check_leaves_the_origin_its_check(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'weekend.md'
    pool.bb('edit', 'is_weekend@eng', str(draft))
    set_code(draft, 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return day in (5, 6)\n')
    draft.write_text(draft.read_text() + '\n### saturday\n\n```python def\ndef saturday(f):\n'
                     '    """Saturday rests."""\n    assert f(5)\n```\n')
    origin = pool.bb('edit', 'is_weekend@eng', str(draft), '-m', 'with a check')
    assert origin.returncode == 0, origin.stderr
    check = pool.bb('latest', 'saturday@eng', '--hash').stdout.split()
    stricter = tmp_path / 'stricter.md'
    pool.bb('edit', 'is_weekend@eng', str(stricter))
    stricter.write_text(stricter.read_text().replace('assert f(5)', 'assert f(5) is True'))
    forked = pool.bb('edit', 'is_weekend@eng', str(stricter), '--fork', '-m', 'a stricter check')
    assert forked.returncode == 0, forked.stderr
    # The origin's check is still current beside the fork's, and neither is an undeclared split
    both = pool.bb('latest', 'saturday@eng', '--hash').stdout.split()
    assert len(both) == 2 and check[0] in both
    assert 'No matches found' in pool.bb('search', '--diverged').stdout
    # The origin goes on editing its check along its own lineage
    again = tmp_path / 'again.md'
    pool.bb('edit', f'{hash_of(origin)}@eng', str(again))
    again.write_text(again.read_text().replace('assert f(5)', 'assert f(5) and f(6)'))
    later = pool.bb('edit', f'{hash_of(origin)}@eng', str(again), '-m', 'Sunday too')
    assert later.returncode == 0, later.stderr
    assert len(pool.bb('latest', 'saturday@eng', '--hash').stdout.split()) == 2
    assert 'No matches found' in pool.bb('search', '--diverged').stdout


def test_a_fork_edited_to_its_origins_code_is_refused_and_to_an_older_version_restores_nothing(tmp_path):
    pool = Pool(tmp_path)
    code = 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return day in {days}\n'
    origin = hash_of(pool.edit('is_weekend', code.format(days='(5, 6)')))
    other = hash_of(fork(pool, tmp_path, 'is_weekend@eng', code.format(days='(4, 5)'), 'Friday and Saturday'))
    draft = tmp_path / 'back.md'
    pool.bb('edit', f'{other}@eng', str(draft))
    set_code(draft, code.format(days='(5, 6)'))
    # One name, one code is one function: the fork would be its origin, and nothing could follow one of them
    back = pool.bb('edit', f'{other}@eng', str(draft), '-m', 'the same days after all')
    assert back.returncode == 1 and 'Nothing saved' in back.stderr
    assert f'is_weekend@eng is already this code: {origin} (this one forked from it)' in back.stderr
    # Nor the origin its fork: the other way round is refused as well
    forth = tmp_path / 'forth.md'
    pool.bb('edit', f'{origin}@eng', str(forth))
    set_code(forth, code.format(days='(4, 5)'))
    refused = pool.bb('edit', f'{origin}@eng', str(forth), '-m', 'Friday too')
    assert refused.returncode == 1 and f'is already this code: {other} (forked from this one)' in refused.stderr
    assert sorted(pool.bb('latest', 'is_weekend@eng', '--hash').stdout.split()) == sorted([origin, other])
    # The origin moves on: its old code is nobody's now, and the fork may take it.
    # Its own lineage never held that code: an edit, not a restore
    set_code(forth, code.format(days='(6,)'))
    assert pool.bb('edit', f'{origin}@eng', str(forth), '-m', 'Sunday').returncode == 0
    back = pool.bb('edit', f'{other}@eng', str(draft), '-m', 'the days the origin left')
    assert back.returncode == 0 and 'Restored' not in back.stdout and f'Parent: {other}' in back.stdout
    assert hash_of(back) == origin
    assert 'is valid' in pool.bb('validate').stdout


def test_a_fork_that_renames_a_check_leaves_the_origin_its_name(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'weekend.md'
    pool.bb('edit', 'is_weekend@eng', str(draft))
    set_code(draft, 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return day in (5, 6)\n')
    draft.write_text(draft.read_text() + '\n### saturday\n\n```python def\ndef saturday(f):\n'
                     '    """Saturday rests."""\n    assert f(5)\n```\n')
    assert pool.bb('edit', 'is_weekend@eng', str(draft), '-m', 'with a check').returncode == 0
    check = pool.bb('latest', 'saturday@eng', '--hash').stdout.split()
    # The fork renames the check and keeps its code: one code, two names, each with its lineage
    renamed = tmp_path / 'renamed.md'
    pool.bb('edit', 'is_weekend@eng', str(renamed))
    renamed.write_text(renamed.read_text().replace('(5, 6)', '(4, 5)').replace('saturday', 'samedi'))
    forked = pool.bb('edit', 'is_weekend@eng', str(renamed), '--fork', '-m', 'Friday and Saturday')
    assert forked.returncode == 0, forked.stderr
    assert pool.bb('latest', 'saturday@eng', '--hash').stdout.split() == check
    assert pool.bb('latest', 'samedi@eng', '--hash').stdout.split() == check
    assert 'Fork: samedi@eng' in pool.bb('log').stdout
    assert 'is valid' in pool.bb('validate').stdout
    # A check that keeps its code and its name cannot be two functions: it remains the origin's
    reworded = tmp_path / 'reworded.md'
    origin = pool.bb('latest', 'is_weekend@eng', '--hash').stdout.split()
    origin.remove(hash_of(forked))
    pool.bb('edit', f'{origin[0]}@eng', str(reworded))
    reworded.write_text(reworded.read_text().replace('(5, 6)', '(5,)').replace('Saturday rests.', 'Rest on day 5.'))
    again = pool.bb('edit', f'{origin[0]}@eng', str(reworded), '--fork', '-m', 'Saturday only')
    assert again.returncode == 0, again.stderr
    assert "check 'saturday' keeps its code and its name: it remains the origin's" in again.stderr
    assert pool.bb('latest', 'saturday@eng', '--hash').stdout.split() == check
    assert 'Saturday rests.' in pool.bb('show', 'saturday@eng').stdout
    assert 'is valid' in pool.bb('validate').stdout


def test_a_doc_only_edit_of_a_name_with_two_ends_on_one_code_is_not_refused(tmp_path):
    """The 'one name, one code' guard is about code a name lands on, not code it stays on"""
    import subprocess
    import sys
    from tests.test_import_by_name import BB
    pool = Pool(tmp_path)
    code = 'def double(n):\n    """{}"""\n    return 2 * n\n'
    origin = hash_of(pool.edit('double', code.format('Twice n.')))
    # Two people document the same version, each in their clone, then merge: one code, two ends
    merge = ('import bb, sys\n'
             'func = sys.argv[1]\n'
             '[step] = bb.lineage_current_steps("eng", "double", func)\n'
             'for doc in ("Two times n.", "The double of n."):\n'
             '    mapping = bb.mapping_save(func, "eng", doc, {"_bb_v_0": "double", "_bb_v_1": "n"}, {})\n'
             '    bb.lineage_save(step, func, "eng", mapping, "document", doc)\n'
             'print(len(bb.lineage_current_steps("eng", "double", func)))\n')
    merged = subprocess.run([sys.executable, '-c', merge, origin], capture_output=True, text=True, env=pool.env,
                            cwd=str(BB.parent))
    assert merged.stdout.split()[-1] == '2', merged.stderr
    # One of them goes on documenting
    draft = tmp_path / 'double.md'
    assert pool.bb('edit', 'double@eng', str(draft)).returncode == 0
    set_code(draft, code.format('Two times n, exactly.'))
    again = pool.bb('edit', 'double@eng', str(draft), '-m', 'more')
    assert again.returncode == 0, again.stderr
    assert 'is already this code' not in again.stderr
