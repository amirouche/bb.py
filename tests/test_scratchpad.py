"""bb scratchpad: several functions in one file, written when missing, saved when present."""
from tests.test_import_by_name import Pool, hash_of

MONEY = 'def money(cents):\n    """Cents as dollars."""\n    return f"${cents / 100:.2f}"\n'
LABEL = 'import bb\nmoney = bb.resolve("money")\n\n\ndef label(cents):\n    """A price label."""\n    return "Price: " + money(cents)\n'
BADGE = 'def badge(text):\n    """A badge."""\n    return "[" + text + "]"\n'


def latest(pool, name):
    return pool.bb('latest', f'{name}@eng', '--hash').stdout.split()


def test_write_then_save_unchanged_then_edit_two_of_three(tmp_path):
    pool = Pool(tmp_path)
    for name, code in (('money', MONEY), ('label', LABEL), ('badge', BADGE)):
        pool.edit(name, code)
    pad = tmp_path / 'pad.md'
    written = pool.bb('scratchpad', 'money@eng', 'label@eng', 'badge@eng', str(pad))
    assert 'Wrote' in written.stdout and [l for l in pad.read_text().split('\n') if l.startswith('# ')] == \
        ['# money', '# label', '# badge']
    again = pool.bb('scratchpad', 'money@eng', 'label@eng', 'badge@eng', str(pad), '-m', 'same')
    assert 'saved nothing; unchanged money, label, badge' in again.stdout
    before = latest(pool, 'badge')
    text = pad.read_text().replace("cents / 100:.2f", "cents / 100:.1f").replace("'Price: '", "'Cost: '")
    pad.write_text(text)
    saved = pool.bb('scratchpad', 'money@eng', 'label@eng', 'badge@eng', str(pad), '-m', 'two')
    assert saved.returncode == 0, saved.stderr
    assert 'saved money, label; unchanged badge' in saved.stdout
    assert latest(pool, 'badge') == before
    # label was saved after money and uses its new version
    assert f"Import: money -> {latest(pool, 'money')[0]}" in saved.stdout


def test_a_new_function_in_the_file_is_saved_before_what_uses_it(tmp_path):
    pool = Pool(tmp_path)
    pad = tmp_path / 'pad.md'
    pad.write_text(f'# label\n\n## Implementation\n\n```python def\n{LABEL}```\n\n'
                   f'# money\n\n## Implementation\n\n```python def\n{MONEY}```\n')
    saved = pool.bb('scratchpad', 'label@eng', str(pad), '-m', 'new')
    assert saved.returncode == 0, saved.stderr
    assert saved.stdout.index('── money') < saved.stdout.index('── label')
    assert latest(pool, 'money') and latest(pool, 'label')
    assert 'bb:parent' in pad.read_text()  # rewritten in stored form, ready for the next edit


def test_a_stale_section_stops_the_save_and_keeps_the_rest_as_written(tmp_path):
    pool = Pool(tmp_path)
    for name, code in (('money', MONEY), ('label', LABEL), ('badge', BADGE)):
        pool.edit(name, code)
    pad = tmp_path / 'pad.md'
    pool.bb('scratchpad', 'badge@eng', 'money@eng', 'label@eng', str(pad))
    pool.edit('label', LABEL.replace('Price', 'Total'))  # label changes after the scratchpad was written
    pad.write_text(pad.read_text().replace("'[' + text", "'<' + text").replace("'Price: '", "'Due: '"))
    result = pool.bb('scratchpad', 'badge@eng', 'money@eng', 'label@eng', str(pad), '-m', 'try')
    assert result.returncode == 1
    assert 'Stopped at label: saved badge; unchanged money' in result.stderr
    assert "'Due: '" in pad.read_text()  # the refused section keeps the text written


def test_a_failing_check_stops_and_names_the_section(tmp_path):
    pool = Pool(tmp_path)
    pad = tmp_path / 'pad.md'
    check = ('\n## Checks\n\n### check_money\n\n```python def\ndef check_money(money):\n'
             '    """Two decimals."""\n    return money(150) == "$1.5"\n```\n')
    pad.write_text(f'# money\n\n## Implementation\n\n```python def\n{MONEY}```\n{check}\n'
                   f'# badge\n\n## Implementation\n\n```python def\n{BADGE}```\n')
    result = pool.bb('scratchpad', 'money@eng', 'badge@eng', str(pad), '-m', 'try')
    assert result.returncode == 1 and 'Stopped at money' in result.stderr
    assert "FAIL check_money: returned False ('$1.50' == '$1.5')" in result.stdout
    assert not latest(pool, 'badge')  # not tried


def test_a_removed_section_is_not_saved_and_languages_cannot_mix(tmp_path):
    pool = Pool(tmp_path)
    for name, code in (('money', MONEY), ('badge', BADGE)):
        pool.edit(name, code)
    pad = tmp_path / 'pad.md'
    pool.bb('scratchpad', 'money@eng', 'badge@eng', str(pad))
    text = pad.read_text()
    pad.write_text(text[:text.index('# badge')].replace(':.2f', ':.3f'))
    saved = pool.bb('scratchpad', 'money@eng', str(pad), '-m', 'one')
    assert 'saved money' in saved.stdout and latest(pool, 'badge')
    mixed = pool.bb('scratchpad', 'money@eng', 'badge@fra', str(tmp_path / 'x.md'))
    assert mixed.returncode == 1 and 'one language per scratchpad' in mixed.stderr


def test_tree_opens_a_function_and_all_it_uses_and_one_section_is_an_edit_file(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('money', MONEY)
    pool.edit('label', LABEL)
    pad = tmp_path / 'tree.md'
    pool.bb('scratchpad', '--tree', 'label@eng', str(pad))
    assert [l for l in pad.read_text().split('\n') if l.startswith('# ')] == ['# label', '# money']
    one = tmp_path / 'one.md'
    edit = tmp_path / 'edit.md'
    pool.bb('scratchpad', 'money@eng', str(one))
    pool.bb('edit', 'money@eng', str(edit))
    assert one.read_text() == edit.read_text()
