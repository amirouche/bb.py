"""bb edit NAME FILE: write FILE when it is missing, save from it when it is there."""
from tests.test_import_by_name import Pool, hash_of


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
    assert f'bb:parent {first_hash}' in draft.read_text()
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
    assert "Current versions of 'double@eng': 1" in pool.bb('latest', 'double@eng').stdout


def test_a_failing_save_leaves_the_file_alone(tmp_path):
    pool = Pool(tmp_path)
    draft = tmp_path / 'broken.md'
    pool.bb('edit', 'broken@eng', str(draft))
    set_code(draft, 'def broken(:\n')
    text = draft.read_text()
    failed = pool.bb('edit', 'broken@eng', str(draft), '-m', 'x')
    assert failed.returncode == 1 and 'unchanged' in failed.stderr
    assert draft.read_text() == text


def test_a_new_name_for_code_already_in_the_pool_is_refused(tmp_path):
    pool = Pool(tmp_path)
    pool.edit('double', 'def double(n):\n    """Twice n."""\n    return 2 * n\n')
    same = pool.edit('twice', 'def twice(n):\n    """Twice n."""\n    return 2 * n\n')
    assert same.returncode == 1 and 'already in the pool as double' in same.stderr


def test_a_bare_newline_stays_escaped():
    import ast
    import bb
    assert bb.code_unparse_readable(ast.parse('x = "\\n"\n')) == "x = '\\n'"
    assert bb.code_unparse_readable(ast.parse('x = "a\\nb"\n')) == 'x = """a\nb"""'
