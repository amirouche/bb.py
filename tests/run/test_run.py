"""
Tests for 'mobius.py run' command.

Grey-box integration tests for function execution.
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


def test_run_missing_language_fails(tmp_path):
    """Test that run fails without language suffix"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    fake_hash = '0' * 64
    result = cli_run(['run', fake_hash], env=env)

    assert result.returncode != 0
    assert 'Missing language suffix' in result.stderr


def test_run_invalid_language_fails(tmp_path):
    """Test that run fails with invalid language code"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    fake_hash = '0' * 64
    result = cli_run(['run', f'{fake_hash}@invalid'], env=env)

    assert result.returncode != 0
    assert 'Language code must be 3 characters' in result.stderr


def test_run_invalid_hash_fails(tmp_path):
    """Test that run fails with invalid hash format"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['run', 'not-valid-hash@eng'], env=env)

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_run_nonexistent_function_fails(tmp_path):
    """Test that run fails for nonexistent function"""
    mobius_dir = tmp_path / '.mobius'
    (mobius_dir / 'pool').mkdir(parents=True)
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    fake_hash = 'f' * 64
    result = cli_run(['run', f'{fake_hash}@eng'], env=env)

    assert result.returncode != 0
    assert 'Could not load function' in result.stderr or 'not found' in result.stderr.lower()


def test_run_with_integer_argument(tmp_path):
    """Test running function with integer argument"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def double(x):
    """Double a number"""
    return x * 2
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['run', f'{func_hash}@eng', '--', '5'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Result: 10' in result.stdout


def test_run_with_float_argument(tmp_path):
    """Test running function with float argument"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def triple(x):
    """Triple a number"""
    return x * 3
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['run', f'{func_hash}@eng', '--', '2.5'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Result: 7.5' in result.stdout


def test_run_with_string_argument(tmp_path):
    """Test running function with string argument"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['run', f'{func_hash}@eng', '--', 'World'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Hello, World!' in result.stdout


def test_run_with_multiple_arguments(tmp_path):
    """Test running function with multiple arguments"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def add(a, b):
    """Add two numbers"""
    return a + b
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['run', f'{func_hash}@eng', '--', '3', '4'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Result: 7' in result.stdout


def test_run_displays_function_code(tmp_path):
    """Test that run displays the function code"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def my_func(value):
    """Process value"""
    return value + 1
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['run', f'{func_hash}@eng', '--', '10'], env=env)

    # Assert
    assert result.returncode == 0
    assert 'def my_func(value):' in result.stdout
    assert 'Running function: my_func' in result.stdout


def test_run_function_with_exception(tmp_path):
    """Test that run handles function exceptions gracefully"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Function that raises exception
    test_file = tmp_path / "func.py"
    test_file.write_text('''def divide(a, b):
    """Divide a by b"""
    return a / b
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test: Division by zero
    result = cli_run(['run', f'{func_hash}@eng', '--', '10', '0'], env=env)

    # Assert: Should fail with error message
    assert result.returncode != 0
    assert 'Error' in result.stderr or 'ZeroDivisionError' in result.stderr
