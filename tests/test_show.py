"""Tests for bb show command, including 0x hash prefix and full hash resolution."""
from tests.test_import_by_name import Pool, hash_of

ECHO_CODE = '''import sys

def echo(args):
    """Print args."""
    print(args)
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


def test_show_empty_0x_prefix_error(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    show_res = pool.bb('show', '0x')
    assert show_res.returncode != 0
    assert 'Error: Empty hash prefix: 0x' in show_res.stderr


def test_show_prefix_too_short_error(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    show_res = pool.bb('show', '0x123')
    assert show_res.returncode != 0
    assert 'Error: Hash prefix too short: 0x123 (minimum 6 hex characters)' in show_res.stderr


def test_show_invalid_hex_error(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    show_res = pool.bb('show', '0xxyz123')
    assert show_res.returncode != 0
    assert 'Error: Invalid hex characters in hash: 0xxyz123' in show_res.stderr


def test_show_ambiguous_prefix_cli(tmp_path):
    pool = Pool(tmp_path)
    pool.add('echo', ECHO_CODE)

    # Manually seed two functions sharing the prefix 'abcdef' in the pool
    h1 = 'abcdef' + '1' * 58
    h2 = 'abcdef' + '2' * 58
    func_dir1 = tmp_path / 'bb' / 'pool' / 'functions' / h1[:2] / h1[2:]
    func_dir2 = tmp_path / 'bb' / 'pool' / 'functions' / h2[:2] / h2[2:]
    func_dir1.mkdir(parents=True, exist_ok=True)
    func_dir2.mkdir(parents=True, exist_ok=True)
    (func_dir1 / 'object.json').write_text('{}', encoding='utf-8')
    (func_dir2 / 'object.json').write_text('{}', encoding='utf-8')

    show_res = pool.bb('show', '0xabcdef')
    assert show_res.returncode != 0
    assert 'Error: Several functions match hash prefix: 0xabcdef' in show_res.stderr
    assert f'0x{h1}' in show_res.stderr
    assert f'0x{h2}' in show_res.stderr


def test_log_with_0x_hash_prefix(tmp_path):
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    prefix = full_hash[:12]
    log_res = pool.bb('log', f'0x{prefix}')
    assert log_res.returncode == 0, log_res.stderr
    assert f'Hash: {full_hash}' in log_res.stdout


def test_show_triple_at_with_mapping_prefix(tmp_path):
    import json
    pool = Pool(tmp_path)
    res = pool.add('echo', ECHO_CODE)
    assert res.returncode == 0, res.stderr
    full_hash = hash_of(res)

    mappings_dir = tmp_path / 'bb' / 'pool' / 'mappings' / full_hash[:2] / full_hash[2:] / 'eng'
    mapping_files = list(mappings_dir.glob('*.json'))
    assert len(mapping_files) >= 1
    mapping_hash = mapping_files[0].stem

    # Test with 8-character prefix of mapping hash
    show_res = pool.bb('show', f'0x{full_hash[:12]}@eng@{mapping_hash[:8]}')
    assert show_res.returncode == 0, show_res.stderr
    assert 'def echo(args):' in show_res.stdout
