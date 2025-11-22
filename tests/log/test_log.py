"""
Tests for 'mobius.py log' command.

Grey-box integration tests for pool log display.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def cli_run(args: list, env: dict = None) -> subprocess.CompletedProcess:
    """Run mobius.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / 'mobius.py')] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=run_env
    )


def test_log_empty_pool(tmp_path):
    """Test that log handles empty pool gracefully"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['log'], env=env)

    assert result.returncode == 0
    assert 'No functions in pool' in result.stdout


def test_log_empty_pool_with_pool_dir(tmp_path):
    """Test that log handles empty pool directory"""
    mobius_dir = tmp_path / '.mobius'
    (mobius_dir / 'pool').mkdir(parents=True)
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['log'], env=env)

    assert result.returncode == 0
    assert '0 functions' in result.stdout or 'No functions' in result.stdout


def test_log_displays_function_info(tmp_path):
    """Test that log displays function hash, date, and author"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['log'], env=env)

    # Assert
    assert result.returncode == 0
    assert func_hash in result.stdout
    assert 'Date:' in result.stdout
    assert 'Author:' in result.stdout


def test_log_shows_header_with_count(tmp_path):
    """Test that log shows header with function count"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Add a function
    test_file = tmp_path / "func.py"
    test_file.write_text('def bar(): pass')
    cli_run(['add', f'{test_file}@eng'], env=env)

    # Test
    result = cli_run(['log'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Function Pool Log' in result.stdout
    assert '1 functions' in result.stdout


def test_log_shows_languages(tmp_path):
    """Test that log displays available languages"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    cli_run(['add', f'{test_file}@eng'], env=env)

    # Test
    result = cli_run(['log'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Languages:' in result.stdout
    assert 'eng' in result.stdout


def test_log_multiple_languages(tmp_path):
    """Test that log shows multiple languages for same function"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Add same function in multiple languages
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet():
    """Hello"""
    pass
''')
    cli_run(['add', f'{test_file}@eng'], env=env)

    fra_file = tmp_path / "func_fra.py"
    fra_file.write_text('''def greet():
    """Bonjour"""
    pass
''')
    cli_run(['add', f'{fra_file}@fra'], env=env)

    # Test
    result = cli_run(['log'], env=env)

    # Assert: Should show both languages
    assert result.returncode == 0
    assert 'eng' in result.stdout
    assert 'fra' in result.stdout


def test_log_multiple_functions(tmp_path):
    """Test that log shows multiple functions"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Add multiple different functions
    test_file1 = tmp_path / "func1.py"
    test_file1.write_text('def one(): return 1')
    cli_run(['add', f'{test_file1}@eng'], env=env)

    test_file2 = tmp_path / "func2.py"
    test_file2.write_text('def two(): return 2')
    cli_run(['add', f'{test_file2}@eng'], env=env)

    # Test
    result = cli_run(['log'], env=env)

    # Assert
    assert result.returncode == 0
    assert '2 functions' in result.stdout
