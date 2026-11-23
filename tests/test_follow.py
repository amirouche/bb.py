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


def test_a_reference_inside_the_function_body_is_refused(tmp_path):
    pool = Pool(tmp_path)
    weekend = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    for written in ('bb.resolve("is_weekend")', f'bb.follow("{weekend}")'):
        refused = pool.add('opening', 'import bb\n\n\ndef opening(day):\n    """Open on working days."""\n'
                           f'    is_weekend = {written}\n    return not is_weekend(day)\n')
        assert refused.returncode == 1, refused.stdout
        assert 'is inside the function: bind it above the def' in refused.stderr
    inline = pool.add('opening', 'import bb\n\n\ndef opening(day):\n    """Open on working days."""\n'
                      '    return not bb.resolve("is_weekend")(day)\n')
    assert inline.returncode == 1 and 'is inside the function' in inline.stderr
    assert pool.bb('latest', 'opening@eng').returncode == 1  # nothing saved


def test_a_hash_inside_the_body_gets_a_draft_that_saves(tmp_path):
    """bb.ref in a body has no alias: the draft keeps the hash, with `import bb` and a note."""
    pool = Pool(tmp_path)
    weekend = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    saved = pool.add('opening', 'import bb\n\n\ndef opening(day):\n    """Open on working days."""\n'
                     f'    is_weekend = bb.ref("{weekend}")\n    return not is_weekend(day)\n')
    assert saved.returncode == 0, saved.stderr
    shown = pool.bb('show', 'opening@eng').stdout
    assert shown.startswith('import bb\n\n# bb: a reference inside the function stays on its version')
    assert f"is_weekend = bb.ref('{weekend}')" in shown
    # The draft bb writes can be saved as it is
    draft = tmp_path / 'opening.md'
    pool.bb('edit', 'opening@eng', str(draft))
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    again = pool.bb('edit', 'opening@eng', str(draft), '-m', 'words')
    assert again.returncode == 0, again.stderr
    # Bound above the def, it follows by name again
    draft.write_text(draft.read_text().replace(f"    is_weekend = bb.ref('{weekend}')\n", '')
                     .replace('def opening', f"is_weekend = bb.follow('{weekend}')\n\n\ndef opening"))
    moved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'follow')
    assert moved.returncode == 0, moved.stderr
    shown = pool.bb('show', 'opening@eng').stdout
    assert "is_weekend = bb.resolve('is_weekend')" in shown and '# bb:' not in shown


def test_a_follow_that_names_no_hash_says_what_it_takes(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    for written in ('is_weekend', origin[:12], origin.upper()):
        refused = pool.add('opening', OPENING.format(ref=f'bb.follow("{written}")', name='opening'))
        assert refused.returncode == 1
        assert "bb.follow takes one pool function's hash" in refused.stderr
        assert 'outside its function' not in refused.stderr


def outdated(pool):
    return [r['name'] for r in json.loads(pool.bb('search', '--outdated', '--json').stdout)]


def test_a_fork_back_to_old_code_does_not_take_over_who_used_that_code(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    assert pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening')).returncode == 0
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    assert outdated(pool) == ['opening']
    # A fork of the new version, with the old version's code: one code, now in two lineages
    back = fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    assert back.returncode == 0 and hash_of(back) == old
    # opening follows the step it was saved on: its lineage went on to new, whatever the fork holds
    assert outdated(pool) == ['opening']
    shown = pool.bb('show', 'opening@eng').stdout
    assert f"is_weekend = bb.follow('{old}')" in shown and f'saving picks {new[:12]}' in shown
    updated = pool.bb('update', 'opening@eng', '-m', 'follow')
    assert updated.returncode == 0, updated.stderr
    assert outdated(pool) == [] and 'out of date' not in pool.bb('show', 'opening@eng').stdout
    assert pool.run(pool.bb('latest', 'opening@eng', '--hash').stdout.strip(), 5) == 'False'
    # A function saved on the fork follows the fork: it is up to date on the same code
    assert pool.add('closing', OPENING.format(ref=f'bb.follow("{old}")', name='closing')).returncode == 0
    assert outdated(pool) == []


def test_saving_a_draft_follows_the_lineage_its_step_stands_on(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening'))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    draft = tmp_path / 'opening.md'
    pool.bb('edit', 'opening@eng', str(draft))
    assert f"bb.follow('{old}')" in draft.read_text()
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    saved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'words')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {new} (was {old})' in saved.stdout


def forget_follows(pool_dir):
    """Rewrite a pool's lineage as it was before steps said what they follow (ids are content hashes)."""
    import bb
    lineage = pool_dir / 'pool' / 'lineage'
    steps = {p.parent.name + p.stem: json.loads(p.read_text()) for p in lineage.glob('*/*.json')}
    renamed = {}
    while len(renamed) < len(steps):
        for step_id, record in steps.items():
            links = [record.get(k) for k in ('from', 'translates') if record.get(k) in steps]
            if step_id in renamed or any(link not in renamed for link in links):
                continue
            record = {k: renamed.get(v, v) if k in ('from', 'translates') else v
                      for k, v in record.items() if k != 'follows'}
            renamed[step_id] = bb.lineage_step_id(record)
            path = lineage / renamed[step_id][:2] / f'{renamed[step_id][2:]}.json'
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(record, indent=2, ensure_ascii=False))
    for step_id in set(steps) - set(renamed.values()):
        (lineage / step_id[:2] / f'{step_id[2:]}.json').unlink()


def test_migrate_says_what_older_steps_follow_without_rewriting_them(tmp_path):
    import time
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', OPENING.format(ref='bb.resolve("is_weekend")', name='opening'))
    time.sleep(1.1)  # steps are dated to the second, and the migration reads dates
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    forget_follows(tmp_path / 'bb')
    lineage = tmp_path / 'bb' / 'pool' / 'lineage'
    before = {p: p.read_text() for p in lineage.glob('*/*.json')}
    assert not any('follows' in text for text in before.values())
    # Followed by hash, the fork's step on the old code hides the debt
    assert outdated(pool) == []
    draft = tmp_path / 'opening.md'
    pool.bb('edit', 'opening@eng', str(draft))
    migrated = pool.bb('migrate')
    assert migrated.returncode == 0 and 'Recorded what 1 lineage step(s) follow' in migrated.stdout
    after = {p: p.read_text() for p in lineage.glob('*/*.json')}
    assert all(after[p] == text for p, text in before.items()) and len(after) == len(before) + 1
    assert outdated(pool) == ['opening']
    assert 'Follow: opening@eng' in pool.bb('log').stdout
    assert 'Already migrated' in pool.bb('migrate').stdout
    assert 'is valid' in pool.bb('validate').stdout
    # A draft made before the migration is not stale after it, and saving it follows the right lineage
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    saved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'words')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {new} (was {old})' in saved.stdout
    assert outdated(pool) == []
