"""
Tests for 'bb.py validate' command.

Grey-box integration tests for function validation.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import normalize_code_for_test


def cli_run(args: list, env: dict = None) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / 'bb.py')] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=run_env
    )


def test_validate_valid_function(tmp_path):
    """Test that validate succeeds for valid function"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Add a function
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['validate', func_hash], env=env)

    # Assert
    assert result.returncode == 0
    assert 'valid' in result.stdout.lower()


def test_validate_nonexistent_function_fails(tmp_path):
    """Test that validate fails for nonexistent function"""
    bb_dir = tmp_path / '.bb'
    (bb_dir / 'pool').mkdir(parents=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    fake_hash = 'f' * 64
    result = cli_run(['validate', fake_hash], env=env)

    assert result.returncode != 0
    assert 'invalid' in result.stderr.lower()
    assert 'object.json not found' in result.stderr


def test_validate_corrupted_object_json_fails(tmp_path):
    """Test that validate fails for corrupted object.json"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Create corrupted function
    fake_hash = 'a' * 64
    func_dir = bb_dir / 'pool' / fake_hash[:2] / fake_hash[2:]
    func_dir.mkdir(parents=True)
    (func_dir / 'object.json').write_text('not valid json')

    # Test
    result = cli_run(['validate', fake_hash], env=env)

    # Assert
    assert result.returncode != 0
    assert 'invalid' in result.stderr.lower()


def test_validate_missing_fields_fails(tmp_path):
    """Test that validate fails when required fields are missing"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Create function with incomplete object.json
    fake_hash = 'b' * 64
    func_dir = bb_dir / 'pool' / fake_hash[:2] / fake_hash[2:]
    func_dir.mkdir(parents=True)
    (func_dir / 'object.json').write_text(json.dumps({
        'schema_version': 1,
        'hash': fake_hash
        # Missing: normalized_code, metadata
    }))

    # Test
    result = cli_run(['validate', fake_hash], env=env)

    # Assert
    assert result.returncode != 0
    assert 'invalid' in result.stderr.lower()
    assert 'Missing required field' in result.stderr


def test_validate_wrong_schema_version_fails(tmp_path):
    """Test that validate fails for wrong schema version"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Create function with wrong schema version
    fake_hash = 'c' * 64
    func_dir = bb_dir / 'pool' / fake_hash[:2] / fake_hash[2:]
    func_dir.mkdir(parents=True)
    (func_dir / 'object.json').write_text(json.dumps({
        'schema_version': 99,
        'hash': fake_hash,
        'normalized_code': normalize_code_for_test('def _bb_v_0(): pass'),
        'metadata': {}
    }))

    # Test
    result = cli_run(['validate', fake_hash], env=env)

    # Assert
    assert result.returncode != 0
    assert 'Invalid schema version' in result.stderr


def test_validate_no_language_mapping_fails(tmp_path):
    """Test that validate fails when no language mapping exists"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Create function without language mapping
    fake_hash = 'd' * 64
    func_dir = bb_dir / 'pool' / fake_hash[:2] / fake_hash[2:]
    func_dir.mkdir(parents=True)
    (func_dir / 'object.json').write_text(json.dumps({
        'schema_version': 1,
        'hash': fake_hash,
        'normalized_code': normalize_code_for_test('def _bb_v_0(): pass'),
        'metadata': {'created': '2025-01-01', 'name': 'test', 'email': 'test@example.com'}
    }))

    # Test
    result = cli_run(['validate', fake_hash], env=env)

    # Assert
    assert result.returncode != 0
    assert 'No language mappings found' in result.stderr


# =============================================================================
# Tests for validate --all (entire directory)
# =============================================================================

def test_validate_all_empty_pool(tmp_path):
    """Test that validate --all works with empty pool"""
    bb_dir = tmp_path / '.bb'
    (bb_dir / 'pool').mkdir(parents=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Test
    result = cli_run(['validate', '--all'], env=env)

    # Assert: Should succeed with 0 functions
    assert result.returncode == 0
    assert 'Functions total:   0' in result.stdout
    assert 'valid' in result.stdout.lower()


def test_validate_all_valid_pool(tmp_path):
    """Test that validate --all succeeds for valid pool"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Add multiple functions
    for i in range(3):
        test_file = tmp_path / f"func{i}.py"
        test_file.write_text(f'def func{i}(): return {i}')
        cli_run(['add', f'{test_file}@eng'], env=env)

    # Test
    result = cli_run(['validate', '--all'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Functions total:   3' in result.stdout
    assert 'Functions valid:   3' in result.stdout
    assert 'Functions invalid: 0' in result.stdout
    assert 'valid' in result.stdout.lower()


def test_validate_all_shows_statistics(tmp_path):
    """Test that validate --all shows statistics"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Add a function
    test_file = tmp_path / "func.py"
    test_file.write_text('def stats_test(): pass')
    cli_run(['add', f'{test_file}@eng'], env=env)

    # Test
    result = cli_run(['validate', '--all'], env=env)

    # Assert: Should show statistics
    assert result.returncode == 0
    assert 'BB Directory Validation' in result.stdout
    assert 'Functions total' in result.stdout
    assert 'Functions valid' in result.stdout
    assert 'Languages found' in result.stdout
    assert 'Missing deps' in result.stdout


def test_validate_all_detects_invalid_function(tmp_path):
    """Test that validate --all detects invalid functions"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Setup: Add a valid function first
    test_file = tmp_path / "valid.py"
    test_file.write_text('def valid(): pass')
    cli_run(['add', f'{test_file}@eng'], env=env)

    # Create an invalid function manually
    fake_hash = 'e' * 64
    func_dir = bb_dir / 'pool' / fake_hash[:2] / fake_hash[2:]
    func_dir.mkdir(parents=True)
    (func_dir / 'object.json').write_text('invalid json')

    # Test
    result = cli_run(['validate', '--all'], env=env)

    # Assert: Should fail and report error
    assert result.returncode != 0
    assert 'Functions invalid: 1' in result.stdout


def test_validate_no_args_validates_directory(tmp_path):
    """Test that validate without hash validates entire directory"""
    bb_dir = tmp_path / '.bb'
    (bb_dir / 'pool').mkdir(parents=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Test: No hash provided, should validate directory
    result = cli_run(['validate'], env=env)

    # Assert: Should run directory validation
    assert result.returncode == 0
    assert 'BB Directory Validation' in result.stdout
