"""A name several functions answer to: the caller's lineage step remembers which one it follows."""
import json
import os
import subprocess
import sys
import time

from tests.test_edit_file import fork, set_code
from tests.test_import_by_name import BB, Pool, hash_of

WEEKEND = 'def is_weekend(day):\n    """A day of rest (0 is Monday)."""\n    return day in {days}\n'
OPENING = ('import bb\nis_weekend = {ref}\n\n\ndef {name}(day):\n    """Open on working days."""\n'
           '    return not is_weekend(day)\n')
BY_NAME = OPENING.format(ref='bb.resolve("is_weekend")', name='{name}')


def edit_by_hash(pool, tmp_path, func_hash, code, name='draft.md'):
    draft = tmp_path / name
    pool.bb('edit', f'{func_hash}@eng', str(draft))
    set_code(draft, code)
    return pool.bb('edit', f'{func_hash}@eng', str(draft), '-m', 'change')


def add(pool, name, code, *args, answer=None):
    """bb add name with arguments; with answer, at a terminal (a pty) where answer is typed."""
    source = pool.tmp / f'{name}_eng.py'
    source.write_text(code)
    command = [sys.executable, str(BB), 'add', f'{source}@eng', *args]
    if answer is None:
        return subprocess.run(command, capture_output=True, text=True, env=pool.env, stdin=subprocess.DEVNULL)
    master, slave = os.openpty()
    try:
        os.write(master, answer.encode())
        return subprocess.run(command, capture_output=True, text=True, env=pool.env, stdin=slave, timeout=60)
    finally:
        os.close(master)
        os.close(slave)


def reword(pool, tmp_path, *args, name='opening.md'):
    """Save opening with other words in its docstring: an edit that changes no reference."""
    draft = tmp_path / name
    pool.bb('edit', 'opening@eng', str(draft))
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    return pool.bb('edit', 'opening@eng', str(draft), '-m', 'words', *args)


def outdated(pool):
    return [r['name'] for r in json.loads(pool.bb('search', '--outdated', '--json').stdout)]


def test_a_caller_keeps_following_its_function_when_another_takes_the_name(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    caller = pool.add('opening', BY_NAME.format(name='opening'))
    assert caller.returncode == 0, caller.stderr
    assert pool.bb('show', 'opening@eng').stdout.startswith("import bb\nis_weekend = bb.resolve('is_weekend')")
    other = hash_of(fork(pool, tmp_path, 'is_weekend@eng', WEEKEND.format(days='(4, 5)'), 'Friday and Saturday'))
    # Two functions answer to the name: the draft still names it, and says which one is followed and what else there is
    shown = pool.bb('show', 'opening@eng').stdout
    assert "is_weekend = bb.resolve('is_weekend')" in shown and 'bb.ref' not in shown
    assert f'# bb: 2 functions are called is_weekend: this follows {origin[:12]}...' in shown
    assert f'# bb:   {origin[:12]}...  followed' in shown
    assert f'# bb:   {other[:12]}...  --pick is_weekend={other[:12]}: a fork of {origin[:12]}... ' \
           f'(Friday and Saturday)' in shown
    assert 'nothing outdated' in pool.bb('search', '--outdated').stdout
    # The followed function moves on: saving the caller follows it, never the fork, asks nothing and pins nothing
    newer = hash_of(edit_by_hash(pool, tmp_path, origin, WEEKEND.format(days='(6, 5)')))
    assert f'saving picks {newer[:12]}' in pool.bb('show', 'opening@eng').stdout
    saved = reword(pool, tmp_path)
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {newer} (was {origin})' in saved.stdout
    found = {r['name']: r for r in json.loads(pool.bb('search', 'opening', '--json').stdout)}
    assert found['opening']['pinned'] == [] and found['opening']['outdated'] == []
    assert pool.run(hash_of(saved), 5) == 'False'


def test_a_new_reference_to_a_shared_name_needs_a_pick(tmp_path):
    pool = Pool(tmp_path)
    one = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    two = hash_of(pool.add('is_weekend', WEEKEND.format(days='(4, 5)')))
    refused = add(pool, 'opening', BY_NAME.format(name='opening'))
    assert refused.returncode == 1 and "2 functions answer to 'is_weekend'" in refused.stderr
    for func_hash in (one, two):
        assert f'--pick is_weekend={func_hash[:12]}' in refused.stderr
    wrong = add(pool, 'opening', BY_NAME.format(name='opening'), '--pick', 'is_weekend=ffffffffffff')
    assert wrong.returncode == 1 and 'names none of the functions' in wrong.stderr
    chosen = add(pool, 'opening', BY_NAME.format(name='opening'), '--pick', f'is_weekend={two[:12]}')
    assert chosen.returncode == 0, chosen.stderr
    assert pool.run(hash_of(chosen), 4) == 'False' and pool.run(hash_of(chosen), 6) == 'True'
    # Chosen once: the step remembers, later saves ask nothing
    again = reword(pool, tmp_path)
    assert again.returncode == 0, again.stderr
    assert pool.run(hash_of(again), 4) == 'False'
    assert f'this follows {two[:12]}' in pool.bb('show', 'opening@eng').stdout


def test_someone_at_the_terminal_is_asked_which_one(tmp_path):
    pool = Pool(tmp_path)
    hashes = [hash_of(pool.add('is_weekend', WEEKEND.format(days=days))) for days in ('(5, 6)', '(4, 5)')]
    asked = add(pool, 'opening', BY_NAME.format(name='opening'), answer='2\n')
    assert asked.returncode == 0, asked.stderr
    assert "2 functions answer to 'is_weekend'" in asked.stdout and 'Which one?' in asked.stdout
    second = next(line for line in asked.stdout.splitlines() if line.startswith('  2. '))
    picked = next(h for h in hashes if h[:12] in second)
    assert f'Import: is_weekend -> {picked}' in asked.stdout
    # --no-ask: a terminal with nobody at it (an agent's) gets the refusal, not the question
    unasked = add(pool, 'closing', BY_NAME.format(name='closing'), '--no-ask', answer='2\n')
    assert unasked.returncode == 1 and 'Which one?' not in unasked.stdout
    assert all(f'--pick is_weekend={h[:12]}' in unasked.stderr for h in hashes)
    assert pool.bb('latest', 'closing@eng').returncode == 1  # nothing saved
    # No answer: nothing is guessed
    silent = add(pool, 'closing', BY_NAME.format(name='closing'), answer='\n')
    assert silent.returncode == 1 and '--pick is_weekend=' in silent.stderr


def test_a_pick_moves_a_caller_to_another_fork(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', BY_NAME.format(name='opening'))
    other = hash_of(fork(pool, tmp_path, 'is_weekend@eng', WEEKEND.format(days='(4, 5)'), 'Friday and Saturday'))
    draft = tmp_path / 'move.md'
    pool.bb('edit', 'opening@eng', str(draft))
    moved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'the other weekend', '--pick', f'is_weekend={other[:12]}')
    assert moved.returncode == 0, moved.stderr
    assert f'Import: is_weekend -> {other} (was {origin})' in moved.stdout
    shown = pool.bb('show', 'opening@eng').stdout
    assert f'this follows {other[:12]}' in shown and f'--pick is_weekend={origin[:12]}' in shown
    # And stays there
    again = reword(pool, tmp_path)
    assert again.returncode == 0 and pool.run(hash_of(again), 4) == 'False'


def test_a_chosen_fork_is_shown_by_name_and_followed_while_a_pin_is_no_debt(tmp_path):
    pool = Pool(tmp_path)
    origin = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    other = hash_of(fork(pool, tmp_path, 'is_weekend@eng', WEEKEND.format(days='(4, 5)'), 'Friday and Saturday'))
    add(pool, 'opening', BY_NAME.format(name='opening'), '--pick', f'is_weekend={other[:12]}')
    # (other code than opening: two names of one code would share their pins)
    add(pool, 'closing', OPENING.format(ref=f'bb.ref("{other}")', name='closing').replace('not is_weekend', 'is_weekend'))
    # Choosing is not pinning: the chosen fork is shown by name, the pin as a hash
    assert "is_weekend = bb.resolve('is_weekend')" in pool.bb('show', 'opening@eng').stdout
    assert f"is_weekend = bb.ref('{other}')" in pool.bb('show', 'closing@eng').stdout
    assert outdated(pool) == []
    # The fork moves on: who chose it is behind, who pinned it is not
    newer = hash_of(edit_by_hash(pool, tmp_path, other, WEEKEND.format(days='[4, 5]')))
    assert outdated(pool) == ['opening']
    assert f'saving picks {newer[:12]}' in pool.bb('show', 'opening@eng').stdout
    pinned = pool.bb('show', 'closing@eng').stdout
    assert f"is_weekend = bb.ref('{other}')" in pinned and 'out of date' not in pinned
    found = {r['name']: r for r in json.loads(pool.bb('search', 'closing', '--json').stdout)}
    assert found['closing']['pinned'] == [other] and found['closing']['outdated'] == []
    assert origin not in pool.bb('show', 'opening@eng').stdout.split('def opening')[1]


def test_a_lineage_with_two_ends_is_not_guessed(tmp_path):
    pool = Pool(tmp_path)
    root = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', BY_NAME.format(name='opening'))
    one = hash_of(edit_by_hash(pool, tmp_path, root, WEEKEND.format(days='(6, 5)'), 'one.md'))
    two = hash_of(edit_by_hash(pool, tmp_path, root, WEEKEND.format(days='[5, 6]'), 'two.md'))
    shown = pool.bb('show', 'opening@eng').stdout
    assert 'has 2 ends: saving asks which' in shown and "is_weekend = bb.resolve('is_weekend')" in shown
    refused = reword(pool, tmp_path)
    assert refused.returncode == 1
    assert f'--pick is_weekend={one[:12]}' in refused.stderr and f'--pick is_weekend={two[:12]}' in refused.stderr
    saved = reword(pool, tmp_path, '--pick', f'is_weekend={two[:12]}', name='again.md')
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {two} (was {root})' in saved.stdout


def test_a_caller_follows_its_function_through_a_rename(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', BY_NAME.format(name='opening'))
    renamed = edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)').replace('is_weekend', 'is_rest_day'))
    assert renamed.returncode == 0 and 'Renamed' in renamed.stdout
    # The draft names it as it is called now, under the alias the code uses
    shown = pool.bb('show', 'opening@eng').stdout
    assert "is_weekend = bb.resolve('is_rest_day')" in shown and f'saving picks {hash_of(renamed)[:12]}' in shown
    saved = reword(pool, tmp_path)
    assert saved.returncode == 0, saved.stderr
    assert pool.run(hash_of(saved), 5) == 'False' and outdated(pool) == []


def test_a_reference_inside_the_function_body_is_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.add('is_weekend', WEEKEND.format(days='(5, 6)'))
    refused = pool.add('opening', 'import bb\n\n\ndef opening(day):\n    """Open on working days."""\n'
                       '    is_weekend = bb.resolve("is_weekend")\n    return not is_weekend(day)\n')
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
                     .replace('def opening', 'is_weekend = bb.resolve("is_weekend")\n\n\ndef opening'))
    moved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'by name')
    assert moved.returncode == 0, moved.stderr
    shown = pool.bb('show', 'opening@eng').stdout
    assert "is_weekend = bb.resolve('is_weekend')" in shown and '# bb:' not in shown


def test_a_fork_back_to_old_code_does_not_take_over_who_used_that_code(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    assert pool.add('opening', BY_NAME.format(name='opening')).returncode == 0
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    assert outdated(pool) == ['opening']
    # A fork of the new version, with the old version's code: one code, now in two lineages
    back = fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    assert back.returncode == 0 and hash_of(back) == old
    # opening follows the step it was saved on: its lineage went on to new, whatever the fork holds
    assert outdated(pool) == ['opening']
    shown = pool.bb('show', 'opening@eng').stdout
    assert f'saving picks {new[:12]}' in shown and f'this follows {new[:12]}' in shown
    assert f'--pick is_weekend={old[:12]}: a fork of {new[:12]}... (as it was)' in shown
    saved = reword(pool, tmp_path)
    assert saved.returncode == 0, saved.stderr
    assert f'Import: is_weekend -> {new} (was {old})' in saved.stdout and outdated(pool) == []
    # A function saved on the fork follows the fork: it is up to date on the same code
    closing = add(pool, 'closing', BY_NAME.format(name='closing'), '--pick', f'is_weekend={old[:12]}')
    assert closing.returncode == 0, closing.stderr
    assert outdated(pool) == []


def test_update_follows_the_lineage_a_step_stands_on(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', BY_NAME.format(name='opening'))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    updated = pool.bb('update', 'opening@eng', '-m', 'follow')
    assert updated.returncode == 0, updated.stderr
    assert outdated(pool) == [] and 'out of date' not in pool.bb('show', 'opening@eng').stdout
    assert pool.run(pool.bb('latest', 'opening@eng', '--hash').stdout.strip(), 5) == 'False'


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
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    pool.add('opening', BY_NAME.format(name='opening'))
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
