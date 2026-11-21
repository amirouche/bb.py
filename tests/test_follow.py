"""bb.follow("<hash>"): a reference to a lineage, which needs no agreement on names."""
import json

from tests.test_edit_file import fork, set_code
from tests.test_import_by_name import Pool, hash_of

WEEKEND = 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return day in {days}\n'
OPENING = ('import bb\nis_weekend = {ref}\n\n\ndef {name}(day):\n    """Open on working days."""\n'
           '    return not is_weekend(day)\n')


def edit_by_hash(pool, tmp_path, func_hash, code, name='draft.md'):
    draft = tmp_path / name
    pool.bb('edit', f'{func_hash}@eng', str(draft))
    set_code(draft, code)
    return pool.bb('edit', f'{func_hash}@eng', str(draft), '-m', 'change')


def test_a_caller_keeps_following_its_function_when_another_takes_the_name(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    caller = pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening'))
    assert caller.returncode == 0, caller.stderr
    assert "is_weekend = bb.resolve('is_weekend')" in pool.bb('show', 'opening@eng').stdout
    other = hash_of(fork(pool, tmp_path, 'is_weekend@eng', WEEKEND.format(days='(4, 5)'), 'Friday and Saturday'))
    # Two functions answer to the name: the draft says which one this caller means, by its lineage
    shown = pool.bb('show', 'opening@eng').stdout
    assert f"is_weekend = bb.follow('{origin}')" in shown and 'bb.ref' not in shown
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout
    # The followed function moves on: saving the caller follows it, never the fork, and pins nothing
    newer = hash_of(edit_by_hash(pool, tmp_path, origin, WEEKEND.format(days='(6, 5)')))
    assert f'saving picks {newer[:12]}' in pool.bb('show', 'opening@eng').stdout
    draft = tmp_path / 'opening.md'
    pool.bb('edit', 'opening@eng', str(draft))
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    saved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'words')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {newer} (was {origin})' in saved.stdout
    found = {r['name']: r for r in json.loads(pool.bb('search', 'opening', '--json').stdout)}
    assert found['opening']['pinned'] == [] and found['opening']['outdated'] == []
    assert other not in pool.bb('show', 'opening@eng').stdout
    assert pool.run(hash_of(saved), 5) == 'False'


def test_a_shared_name_is_refused_with_lines_to_paste(tmp_path):
    pool = Pool(tmp_path)
    one = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    two = hash_of(pool.add('is_weekend', WEEKEND.format(days='(4, 5)')))
    refused = pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening'))
    assert refused.returncode == 1
    for func_hash in (one, two):
        assert f'is_weekend = bb.follow("{func_hash}")' in refused.stderr
    chosen = pool.add('opening', OPENING.format(ref=f'bb.follow("{two}")', name='opening'))
    assert chosen.returncode == 0, chosen.stderr
    assert pool.run(hash_of(chosen), 4) == 'False' and pool.run(hash_of(chosen), 6) == 'True'


def test_following_an_old_version_saves_the_current_one(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    saved = pool.add('opening', OPENING.format(ref=f'bb.follow("{old}")', name='opening'))
    assert saved.returncode == 0 and f'Import: is_weekend -> {new}' in saved.stdout
    stored = tmp_path / 'bb' / 'pool' / 'functions' / hash_of(saved)[:2] / hash_of(saved)[2:] / 'object.json'
    assert f"bb.ref('{new}')" in stored.read_text() and 'follow' not in stored.read_text()


def test_a_lineage_with_two_ends_is_not_guessed(tmp_path):
    pool = Pool(tmp_path)
    root = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    one = hash_of(edit_by_hash(pool, tmp_path, root, WEEKEND.format(days='(6, 5)'), 'one.md'))
    two = hash_of(edit_by_hash(pool, tmp_path, root, WEEKEND.format(days='[5, 6]'), 'two.md'))
    refused = pool.add('opening', OPENING.format(ref=f'bb.follow("{root}")', name='opening'))
    assert refused.returncode == 1 and 'has 2 ends' in refused.stderr
    assert f'bb.follow("{one}")' in refused.stderr and f'bb.follow("{two}")' in refused.stderr
    assert pool.add('opening', OPENING.format(ref=f'bb.follow("{two}")', name='opening')).returncode == 0


def test_a_hash_the_pool_does_not_have_is_refused(tmp_path):
    pool = Pool(tmp_path)
    refused = pool.add('opening', OPENING.format(ref=f'bb.follow("{"ab" * 32}")', name='opening'))
    assert refused.returncode == 1 and 'no function abababababab... in the pool' in refused.stderr


def test_a_caller_follows_its_function_through_a_rename(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening'))
    renamed = edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)').replace('is_weekend', 'is_rest_day'))
    assert renamed.returncode == 0 and 'Renamed' in renamed.stdout
    shown = pool.bb('show', 'opening@eng').stdout
    assert f"is_weekend = bb.follow('{old}')" in shown and f'saving picks {hash_of(renamed)[:12]}' in shown
