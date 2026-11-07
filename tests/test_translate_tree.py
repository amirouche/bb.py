"""bb translate --tree ROOT@lang TARGET FILE: a whole tree in one file, each function under one name."""
from tests.test_import_by_name import Pool, hash_of
from tests.test_translate_file import fill

MONEY = 'def money(cents):\n    """Cents as dollars."""\n    return f"${cents / 100:.2f}"\n'
LABEL = ('import bb\nmoney = bb.resolve("money")\n\n\ndef label(cents):\n    """A price label."""\n'
         '    return "Price: " + money(cents)\n')
APP = ('import bb\nlabel = bb.resolve("label")\nmoney = bb.resolve("money")\n\n\ndef app(cents):\n'
       '    """The app."""\n    return label(cents) + " / " + money(cents)\n')


def tree(tmp_path):
    pool = Pool(tmp_path)
    hashes = {name: hash_of(pool.edit(name, code)) for name, code in (('money', MONEY), ('label', LABEL), ('app', APP))}
    return pool, hashes


def latest(pool, name):
    return pool.bb('latest', name, '--hash').stdout.split()


def test_a_tree_is_translated_in_one_file_with_one_name_per_function(tmp_path):
    pool, hashes = tree(tmp_path)
    draft = tmp_path / 'app.fra.md'
    wrote = pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft))
    assert wrote.returncode == 0 and 'with 3 function(s) to translate' in wrote.stdout, wrote.stderr
    text = draft.read_text()
    assert sorted(l for l in text.split('\n') if l.startswith('# ')) == ['# app', '# label', '# money']
    assert '## Aliases' not in text  # callers take each function's own name

    # Incomplete: every problem at once, nothing saved
    fill(draft, {'app': 'appli', 'label': 'etiquette'})
    refused = pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft))
    assert refused.returncode == 1 and 'nothing saved' in refused.stderr
    assert "app: Names: 'cents' has no fra name" in refused.stderr and "money: Names: 'money' has no fra name" in refused.stderr
    assert not latest(pool, 'appli@fra')

    fill(draft, {'money': 'argent', 'cents': 'centimes'}, docstring='En français.')
    saved = pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft), '-m', 'en français')
    assert saved.returncode == 0, saved.stderr
    assert 'Saved 3 translation(s) into fra:' in saved.stdout
    assert all(pair in saved.stdout for pair in ('app -> appli', 'label -> etiquette', 'money -> argent'))
    # Same code, same hashes; every caller uses each function's one French name
    assert [latest(pool, f'{n}@fra') for n in ('appli', 'etiquette', 'argent')] == \
        [[hashes['app']], [hashes['label']], [hashes['money']]]
    shown = pool.bb('show', 'appli@fra').stdout
    assert 'etiquette(centimes)' in shown and 'argent(centimes)' in shown
    assert 'argent(centimes)' in pool.bb('show', 'etiquette@fra').stdout
    assert 'Nothing to translate' in pool.bb('translate', '--tree', 'app@eng', 'fra', str(tmp_path / 'again.md')).stdout


def test_functions_already_translated_are_left_out_and_named_as_they_are(tmp_path):
    pool, _ = tree(tmp_path)
    leaf = tmp_path / 'money.fra.md'
    pool.bb('translate', 'money@eng', 'fra', str(leaf))
    fill(leaf, {'money': 'monnaie', 'cents': 'centimes'})
    assert pool.bb('translate', 'money@eng', 'fra', str(leaf)).returncode == 0
    draft = tmp_path / 'app.fra.md'
    wrote = pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft))
    assert 'with 2 function(s) to translate (1 already in fra)' in wrote.stdout
    assert 'money -> monnaie' in draft.read_text() and '\n# money\n' not in draft.read_text()
    fill(draft, {'app': 'appli', 'label': 'etiquette', 'cents': 'centimes'})
    assert pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft)).returncode == 0
    assert 'monnaie(centimes)' in pool.bb('show', 'appli@fra').stdout


def test_one_name_for_two_functions_is_refused(tmp_path):
    pool, _ = tree(tmp_path)
    draft = tmp_path / 'app.fra.md'
    pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft))
    fill(draft, {'app': 'appli', 'label': 'prix', 'money': 'prix', 'cents': 'centimes'})
    refused = pool.bb('translate', '--tree', 'app@eng', 'fra', str(draft))
    assert refused.returncode == 1 and "'prix' is given to label, money" in refused.stderr
    assert 'given to two names' not in refused.stderr
    assert not latest(pool, 'appli@fra')
