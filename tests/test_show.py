"""Tests for bb show command, including 0x hash prefix and full hash resolution."""
from tests.test_import_by_name import Pool, hash_of

ECHO_CODE = '''import sys

def echo(args):
    """Print args."""
    print(args)
'''

SUM_CODE = '''def add(a, b):
    """Add two numbers."""
    return a + b
'''


def test_show_with_0x_hash_prefix_lists_languages(tmp_path):
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    # 40-character prefix as in Issue #96
    prefix = full_hash[:40]

    show_res = pool.bb('show', f'0x{prefix}')
    assert show_res.returncode == 0, show_res.stderr
    assert f'Available languages for 0x{full_hash}:' in show_res.stdout
    assert 'eng - 1 mapping(s)' in show_res.stdout


def test_show_with_0x_hash_prefix_and_lang_displays_code(tmp_path):
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    prefix = full_hash[:20]

    show_res = pool.bb('show', f'0x{prefix}@eng')
    assert show_res.returncode == 0, show_res.stderr
    assert 'def echo(args):' in show_res.stdout
    assert 'print(args)' in show_res.stdout


def test_show_with_full_0x_hash(tmp_path):
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    # Full 64-char hash with 0x
    show_res = pool.bb('show', f'0x{full_hash}')
    assert show_res.returncode == 0, show_res.stderr
    assert f'Available languages for 0x{full_hash}:' in show_res.stdout
    assert 'eng - 1 mapping(s)' in show_res.stdout

    # With @eng
    show_res_lang = pool.bb('show', f'0x{full_hash}@eng')
    assert show_res_lang.returncode == 0, show_res_lang.stderr
    assert 'def echo(args):' in show_res_lang.stdout


def test_show_backward_compatibility_without_0x(tmp_path):
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    # By function name
    show_by_name = pool.bb('show', 'echo@eng')
    assert show_by_name.returncode == 0, show_by_name.stderr
    assert 'def echo(args):' in show_by_name.stdout

    # By full 64-char hash
    show_by_hash = pool.bb('show', f'{full_hash}@eng')
    assert show_by_hash.returncode == 0, show_by_hash.stderr
    assert 'def echo(args):' in show_by_hash.stdout


def test_show_unknown_0x_hash_prefix_error(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    show_res = pool.bb('show', '0xdeadbeef1234')
    assert show_res.returncode != 0
    assert 'Error: No function matches hash prefix: 0xdeadbeef1234' in show_res.stderr


def test_show_invalid_hex_error(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    show_res = pool.bb('show', '0xxyz123')
    assert show_res.returncode != 0
    assert 'Error: Invalid hex characters in hash: 0xxyz123' in show_res.stderr


def test_helper_resolve_hash_prefix_ambiguous(monkeypatch):
    import bb
    import pytest

    # Mock storage_list_functions to return two hashes starting with 'abcd'
    monkeypatch.setattr(bb, 'storage_list_functions', lambda: {'abcd1111' + '0' * 56, 'abcd2222' + '0' * 56})

    with pytest.raises(SystemExit):
        bb.helper_resolve_hash_prefix('0xabcd')

