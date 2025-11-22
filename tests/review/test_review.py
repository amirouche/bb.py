"""
Tests for 'mobius.py review' command.

Grey-box integration tests for function review with dependency resolution.
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


def test_review_invalid_hash_fails(tmp_path):
    """Test that review fails with invalid hash format"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['review', 'not-a-valid-hash'], env=env)

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_review_nonexistent_function_warns(tmp_path):
    """Test that review warns for nonexistent function"""
    mobius_dir = tmp_path / '.mobius'
    (mobius_dir / 'pool').mkdir(parents=True)
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    fake_hash = 'f' * 64
    result = cli_run(['review', fake_hash], env=env)

    # Review continues but warns about missing function
    assert 'not found' in result.stderr.lower() or 'not available' in result.stderr.lower()


def test_review_displays_function_code(tmp_path):
    """Test that review displays function code"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    cli_run(['init'], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process(data):
    """Process some data"""
    return data * 2
''')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['review', func_hash], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Function: process (eng)' in result.stdout
    assert f'Hash: {func_hash}' in result.stdout
    assert 'def process(data):' in result.stdout
    assert 'Dependencies: None' in result.stdout


def test_review_shows_function_review_header(tmp_path):
    """Test that review shows proper header"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup
    cli_run(['init'], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['review', func_hash], env=env)

    # Assert
    assert result.returncode == 0
    assert 'Function Review' in result.stdout


def test_review_uses_preferred_language(tmp_path):
    """Test that review uses user's preferred languages"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Initialize and set French as preferred language
    cli_run(['init'], env=env)
    cli_run(['whoami', 'language', 'fra'], env=env)

    # Add function in French
    test_file = tmp_path / "func.py"
    test_file.write_text('''def calculer(valeur):
    """Calculer le resultat"""
    return valeur * 2
''')
    add_result = cli_run(['add', f'{test_file}@fra'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['review', func_hash], env=env)

    # Assert: Should show French version
    assert result.returncode == 0
    assert 'calculer (fra)' in result.stdout
    assert 'Calculer le resultat' in result.stdout


def test_review_fallback_when_language_unavailable(tmp_path):
    """Test that review warns when function not in preferred language"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Initialize with Spanish as preferred language
    cli_run(['init'], env=env)
    cli_run(['whoami', 'language', 'spa'], env=env)

    # Add function in English only
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test
    result = cli_run(['review', func_hash], env=env)

    # Assert: Should warn about unavailable language
    assert 'not available in any preferred language' in result.stderr


def test_review_default_language_fallback(tmp_path):
    """Test that review falls back to 'eng' when no preferred languages set"""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # Setup: Add function without init (no preferred languages)
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = add_result.stdout.split('Hash:')[1].strip().split()[0]

    # Test: Review without init (config doesn't exist)
    result = cli_run(['review', func_hash], env=env)

    # Assert: Should still work using 'eng' as default
    assert result.returncode == 0
    assert 'foo (eng)' in result.stdout
