"""A name several functions answer to: the caller's lineage step remembers which one it follows."""
import json
import os
import subprocess
import sys

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


def add(pool, name, code, *args, answer=None, piped=False):
    """
    bb add name with arguments; with answer, at a terminal (a pty) where answer is typed.

    The terminal is the input and the output, as when someone sits at it
    (stdout and stderr are then one screen); with piped, the input only
    (the output is captured, as in `bb add ... | grep`).
    """
    source = pool.tmp / f'{name}_eng.py'
    source.write_text(code)
    command = [sys.executable, str(BB), 'add', f'{source}@eng', *args]
    if answer is None:
        return subprocess.run(command, capture_output=True, text=True, env=pool.env, stdin=subprocess.DEVNULL)
    master, slave = os.openpty()
    try:
        os.write(master, answer.encode())
        if piped:
            return subprocess.run(command, capture_output=True, text=True, env=pool.env, stdin=slave, timeout=60)
        done = subprocess.run(command, stdin=slave, stdout=slave, stderr=slave, env=pool.env, timeout=60)
        os.set_blocking(master, False)
        shown = b''
        try:
            while chunk := os.read(master, 65536):
                shown += chunk
        except OSError:  # nothing more to read
            pass
        done.stdout = done.stderr = shown.decode().replace('\r\n', '\n')  # one screen
        return done
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
    # Output piped or captured (`bb add ... | grep Hash`): the question would not be seen, so it is not asked
    unseen = add(pool, 'closing', BY_NAME.format(name='closing'), answer='2\n', piped=True)
    assert unseen.returncode == 1 and 'Which one?' not in unseen.stdout
    assert all(f'--pick is_weekend={h[:12]}' in unseen.stderr for h in hashes)
    assert pool.bb('latest', 'closing@eng').returncode == 1


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


def test_a_pin_belongs_to_a_name_not_to_the_code_two_names_share(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    # One code under two names: opening follows is_weekend, closing pins it
    followed = pool.add('opening', BY_NAME.format(name='opening'))
    pinned = pool.add('closing', OPENING.format(ref=f'bb.ref("{old}")', name='closing'))
    assert hash_of(followed) == hash_of(pinned)
    assert outdated(pool) == []
    edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)'))
    # The name that follows is behind; the status says which, since the other is not
    found = json.loads(pool.bb('search', '--outdated', '--json').stdout)
    assert [r['hash'] for r in found] == [hash_of(followed)]
    assert found[0]['outdated'] == [old] and found[0]['outdated_as'] == {old: ['opening@eng']}
    assert 'out-of-date version(s): is_weekend (as opening@eng)' in pool.bb('search', '--outdated').stdout
    assert '[out of date]' in pool.bb('tree', 'opening@eng').stdout
    # Each draft still says its own: one out of date, the other pinned
    assert 'out of date' in pool.bb('show', 'opening@eng').stdout
    assert f"is_weekend = bb.ref('{old}')" in pool.bb('show', 'closing@eng').stdout


def test_pinned_callers_can_be_found(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    follower = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    holder = hash_of(pool.add('closing', OPENING.format(ref=f'bb.ref("{old}")', name='closing')
                              .replace('not is_weekend', 'is_weekend')))
    # bb caller says who holds on to the version
    listed = pool.bb('caller', old).stdout.splitlines()
    assert f'bb show {holder}  # pinned' in listed and f'bb show {follower}' in listed
    callers = {c['hash']: c for c in json.loads(pool.bb('caller', old, '--json').stdout)}
    assert callers[holder]['pinned'] is True and callers[follower]['pinned'] is False
    # bb search --pinned lists them, and says whether the pinned version is still current
    found = json.loads(pool.bb('search', '--pinned', '--json').stdout)
    assert [r['name'] for r in found] == ['closing'] and found[0]['pins'][0]['now'] == [old]
    assert f'Pinned: is_weekend {old}, still current' in pool.bb('search', '--pinned').stdout
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    assert f'Pinned: is_weekend {old}, its lineage went on to {new[:12]}...' in pool.bb('search', '--pinned').stdout
    assert [r['name'] for r in json.loads(pool.bb('search', '--pinned', '--uses', old, '--json').stdout)] == ['closing']
    assert outdated(pool) == ['opening']


def test_a_pin_under_one_of_two_names_says_which(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    both = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    assert hash_of(pool.add('closing', OPENING.format(ref=f'bb.ref("{old}")', name='closing'))) == both
    assert f'bb show {both}  # pinned as closing@eng' in pool.bb('caller', old).stdout
    assert f'Pinned: is_weekend {old}, still current, as closing@eng' in pool.bb('search', '--pinned').stdout


def test_a_pick_moves_a_caller_to_a_fork_holding_the_code_it_already_uses(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    opening = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    assert outdated(pool) == ['opening']
    # The fork holds the code opening uses: picking it changes no code, only the lineage followed
    draft = tmp_path / 'move.md'
    pool.bb('edit', 'opening@eng', str(draft))
    moved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'as it was', '--pick', f'is_weekend={old[:12]}')
    assert moved.returncode == 0, moved.stderr
    assert 'No changes.' not in moved.stdout and f'Follows: is_weekend {old}' in moved.stdout
    assert pool.bb('latest', 'opening@eng', '--hash').stdout.split() == [opening]
    assert outdated(pool) == []
    assert 'Follow: opening@eng' in pool.bb('log').stdout
    assert 'is valid' in pool.bb('validate').stdout
    # And stays there: the next save keeps to the fork, and picking it again changes nothing
    assert 'No changes.' in pool.bb('edit', 'opening@eng', str(draft), '--pick', f'is_weekend={old[:12]}').stdout
    again = reword(pool, tmp_path)
    assert again.returncode == 0 and pool.run(hash_of(again), 5) == 'False' and outdated(pool) == []


WEEK = 'import bb\nopening = bb.resolve("opening")\n\n\ndef week():\n    """The days open."""\n    return [d for d in range(7) if opening(d)]\n'


def latest(pool, name):
    return pool.bb('latest', name, '--hash').stdout.split()


def test_update_pays_the_debt_of_the_name_asked_and_leaves_a_name_that_pins(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    both = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    assert hash_of(pool.add('closing', OPENING.format(ref=f'bb.ref("{old}")', name='closing'))) == both
    edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)'))
    assert outdated(pool) == ['opening']
    updated = pool.bb('update', 'opening@eng', '-m', 'catch up')
    assert updated.returncode == 0 and 'Updated 1 function(s)' in updated.stdout, updated.stdout + updated.stderr
    assert outdated(pool) == []
    assert latest(pool, 'opening@eng') == [hash_of(updated)] != [both]
    assert latest(pool, 'closing@eng') == [both]
    assert f"is_weekend = bb.ref('{old}')" in pool.bb('show', 'closing@eng').stdout


def test_update_moves_only_the_language_asked(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    both = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    french = tmp_path / 'ouverture_fra.py'
    french.write_text('import bb\nis_weekend = bb.resolve("is_weekend")\n\n\ndef ouverture(jour):\n'
                      '    """Ouvert les jours ouvrables."""\n    return not is_weekend(jour)\n')
    assert hash_of(pool.bb('add', f'{french}@fra')) == both
    edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)'))
    updated = pool.bb('update', 'opening@eng', '-m', 'catch up')
    assert updated.returncode == 0, updated.stderr
    # Whoever updates cannot sign the other languages' names: they stay, and can be updated in turn
    assert latest(pool, 'opening@eng') == [hash_of(updated)] != [both]
    assert latest(pool, 'ouverture@fra') == [both]
    again = pool.bb('update', 'ouverture@fra', '-m', 'rattraper')
    assert again.returncode == 0 and latest(pool, 'ouverture@fra') == [hash_of(updated)], again.stdout + again.stderr


def test_update_follows_what_it_uses_through_a_rename(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    opening = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    assert pool.add('week', WEEK).returncode == 0
    # opening goes on under another name: week follows it there
    renamed = edit_by_hash(pool, tmp_path, opening, BY_NAME.format(name='hours').replace('not is_weekend(day)',
                                                                                         'is_weekend(day) is False'),
                           name='hours.md')
    assert renamed.returncode == 0 and 'Renamed' in renamed.stdout, renamed.stdout + renamed.stderr
    # What hours uses goes on too, and its old code comes back in a fork: by hash alone it would look current
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    assert sorted(outdated(pool)) == ['hours', 'week']
    updated = pool.bb('update', 'week@eng', '-m', 'catch up')
    assert updated.returncode == 0 and 'Updated 2 function(s)' in updated.stdout, updated.stdout + updated.stderr
    assert outdated(pool) == []
    assert pool.run(hash_of(updated)) == '[0, 1, 2, 3, 4]'


def test_update_follows_the_step_a_caller_stands_on_when_one_code_has_two(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    opening = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    # opening's code in two lineages: the first one, behind on is_weekend, and a fork of a fork, on the fork
    always = hash_of(fork(pool, tmp_path, f'{opening}@eng', 'def opening(day):\n    """Always open."""\n    return True\n',
                          'never closed', name='always.md'))
    draft = tmp_path / 'back.md'
    pool.bb('edit', f'{always}@eng', str(draft))
    set_code(draft, BY_NAME.format(name='opening'))
    back = pool.bb('edit', f'{always}@eng', str(draft), '--fork', '-m', 'closed again', '--pick', f'is_weekend={old[:12]}')
    assert back.returncode == 0 and hash_of(back) == opening, back.stdout + back.stderr
    assert outdated(pool) == ['opening']
    # week stands on the first lineage (not a fork's): that one is brought up to date
    assert add(pool, 'week', WEEK, '--pick', f'opening={opening[:12]}').returncode == 0
    updated = pool.bb('update', 'week@eng', '-m', 'catch up')
    assert updated.returncode == 0 and 'Updated 2 function(s)' in updated.stdout, updated.stdout + updated.stderr
    assert outdated(pool) == []
    assert pool.run(hash_of(updated)) == '[0, 1, 2, 3, 4]'
    assert opening in latest(pool, 'opening@eng')  # the fork that holds the code stays


def test_the_tree_of_a_name_says_what_that_name_pins_and_follows(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    both = hash_of(pool.add('opening', BY_NAME.format(name='opening')))
    assert hash_of(pool.add('closing', OPENING.format(ref=f'bb.ref("{old}")', name='closing'))) == both
    edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)'))
    assert '[out of date]' in pool.bb('tree', 'opening@eng').stdout
    pinned = pool.bb('tree', 'closing@eng').stdout
    assert pinned.startswith('closing') and '[pinned]' in pinned and 'out of date' not in pinned


AUDITED = ('import bb\naudit = bb.resolve("audit")\nis_weekend = bb.resolve("is_weekend")\n\n\ndef opening(day):\n'
           '    """Open on working days."""\n    return audit(day) and not is_weekend(day)\n')


def test_a_fork_comment_with_a_line_break_writes_no_code_into_its_callers(tmp_path):
    import ast
    pool = Pool(tmp_path)
    pool.add('is_weekend', WEEKEND.format(days='(5, 6)'))
    audit = hash_of(pool.add('audit', 'def audit(day):\n    """A day of the week."""\n    return 0 <= day <= 6\n'))
    other = hash_of(pool.add('lenient', 'def lenient(day):\n    """Any day."""\n    return day is not None\n'))
    assert pool.add('opening', AUDITED).returncode == 0
    # The comment closes the `# bb:` line it is quoted on and binds audit to another function
    forked = fork(pool, tmp_path, 'is_weekend@eng', WEEKEND.format(days='(4, 5)'),
                  f"Algiers)\naudit = bb.ref('{other}')  # (")
    assert forked.returncode == 0, forked.stderr
    shown = pool.bb('show', 'opening@eng').stdout
    bindings = [node for node in ast.parse(shown).body if isinstance(node, ast.Assign)]
    assert [node.targets[0].id for node in bindings] == ['audit', 'is_weekend']
    assert f"(Algiers) audit = bb.ref('{other}') # ()" in shown
    saved = reword(pool, tmp_path)
    assert saved.returncode == 0, saved.stderr
    assert f'Import: audit -> {audit}' in saved.stdout
    assert other not in pool.bb('show', 'opening@eng').stdout.replace(f"bb.ref('{other}') # ()", '')
    assert json.loads(pool.bb('search', '--pinned', '--json').stdout) == []


def test_a_name_bound_to_two_pool_functions_is_refused(tmp_path):
    pool = Pool(tmp_path)
    one = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    two = hash_of(pool.add('is_rest', WEEKEND.format(days='(4, 5)').replace('is_weekend', 'is_rest')))
    twice = pool.add('opening', OPENING.format(ref=f'bb.ref("{one}")\nis_weekend = bb.ref("{two}")', name='opening'))
    assert twice.returncode == 1
    assert 'is_weekend is bound twice' in twice.stderr and 'keep one binding' in twice.stderr


def test_a_pick_is_given_by_the_name_searched_whatever_the_code_calls_it(tmp_path):
    pool = Pool(tmp_path)
    old = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    opening = hash_of(pool.add('opening', 'import bb\nrest = bb.resolve("is_weekend")\n\n\ndef opening(day):\n'
                                          '    """Open on working days."""\n    return not rest(day)\n'))
    new = hash_of(edit_by_hash(pool, tmp_path, old, WEEKEND.format(days='(6, 5)')))
    fork(pool, tmp_path, f'{new}@eng', WEEKEND.format(days='(5, 6)'), 'as it was')
    assert outdated(pool) == ['opening']
    draft = tmp_path / 'move.md'
    pool.bb('edit', 'opening@eng', str(draft))
    moved = pool.bb('edit', 'opening@eng', str(draft), '-m', 'as it was', '--pick', f'is_weekend={old[:12]}')
    assert moved.returncode == 0, moved.stderr
    assert 'No changes.' not in moved.stdout and f'Follows: rest {old}' in moved.stdout
    assert pool.bb('latest', 'opening@eng', '--hash').stdout.split() == [opening]
    assert outdated(pool) == []


def test_a_pick_for_a_name_nothing_searches_for_is_refused(tmp_path):
    pool = Pool(tmp_path)
    weekend = hash_of(pool.add('is_weekend', WEEKEND.format(days='(5, 6)')))
    # One function answers, so the name alone would do: a pick under another name is a typo, not a choice
    typo = add(pool, 'opening', BY_NAME.format(name='opening'), '--pick', f'is_wekend={weekend[:12]}')
    assert typo.returncode == 1 and 'Nothing saved' in typo.stderr
    assert f'--pick is_wekend={weekend[:12]}' in typo.stderr and 'it names: is_weekend' in typo.stderr
    assert pool.bb('latest', 'opening@eng').returncode == 1
    opening = hash_of(add(pool, 'opening', BY_NAME.format(name='opening'), '--pick', f'is_weekend={weekend[:12]}'))
    # bb edit refuses it too, and saves nothing of the edit
    draft = tmp_path / 'opening.md'
    pool.bb('edit', 'opening@eng', str(draft))
    draft.write_text(draft.read_text().replace('Open on working days', 'Open when people work'))
    refused = pool.bb('edit', 'opening@eng', str(draft), '-m', 'words', '--pick', f'weekend={weekend[:12]}')
    assert refused.returncode == 1 and 'Nothing saved' in refused.stderr
    assert pool.bb('latest', 'opening@eng', '--hash').stdout.split() == [opening]
    assert 'Open on working days' in pool.bb('show', 'opening@eng').stdout
    # bb scratchpad gives its picks to every section: one is enough to use it, none is refused
    pad = tmp_path / 'pad.md'
    pool.bb('scratchpad', 'is_weekend@eng', 'opening@eng', str(pad))
    pad.write_text(pad.read_text().replace('Open on working days', 'Open when people work'))
    unused = pool.bb('scratchpad', 'is_weekend@eng', 'opening@eng', str(pad), '-m', 'words', '--pick',
                     f'weekend={weekend[:12]}')
    assert unused.returncode == 1 and 'Nothing saved' in unused.stderr
    assert 'Open on working days' in pool.bb('show', 'opening@eng').stdout
    used = pool.bb('scratchpad', 'is_weekend@eng', 'opening@eng', str(pad), '-m', 'words', '--pick',
                   f'is_weekend={weekend[:12]}')
    assert used.returncode == 0 and 'saved opening' in used.stdout, used.stdout + used.stderr
