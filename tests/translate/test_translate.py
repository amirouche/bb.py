"""
Tests for 'bb.py translate' command.

Grey-box integration tests for adding translations to functions.
Note: translate is interactive, so some tests use stdin injection.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def cli_run(
    args: list, env: dict = None, input_text: str = None
) -> subprocess.CompletedProcess:
    """Run bb.py CLI command with optional stdin input."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / "bb.py")] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(
        cmd, capture_output=True, text=True, env=run_env, input=input_text
    )


def test_translate_missing_source_language_fails(tmp_path):
    """Test that translate fails without source language suffix"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add a function first
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: translate without @lang
    result = cli_run(["translate", func_hash, "fra"], env=env)

    # Assert
    assert result.returncode != 0
    assert "Missing language suffix" in result.stderr


def test_translate_invalid_source_language_fails(tmp_path):
    """Test that translate fails with too short source language code"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "0" * 64

    # Test with too short language code (must be 3-256 chars)
    result = cli_run(["translate", f"{fake_hash}@ab", "fra"], env=env)

    assert result.returncode != 0
    assert "Source language code must be 3-256 characters" in result.stderr


def test_translate_invalid_target_language_fails(tmp_path):
    """Test that translate fails with too short target language code"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test with too short language code (must be 3-256 chars)
    result = cli_run(["translate", f"{func_hash}@eng", "ab"], env=env)

    assert result.returncode != 0
    assert "Target language code must be 3-256 characters" in result.stderr


def test_translate_invalid_hash_fails(tmp_path):
    """Test that translate fails with invalid hash format"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["translate", "not-valid@eng", "fra"], env=env)

    assert result.returncode != 0
    assert "Invalid hash format" in result.stderr


def test_translate_nonexistent_function_fails(tmp_path):
    """Test that translate fails for nonexistent function"""
    bb_dir = tmp_path / ".bb"
    (bb_dir / "pool").mkdir(parents=True)
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "f" * 64

    result = cli_run(["translate", f"{fake_hash}@eng", "fra"], env=env)

    assert result.returncode != 0
    assert (
        "Could not load function" in result.stderr
        or "not found" in result.stderr.lower()
    )


def test_translate_shows_source_function(tmp_path):
    """Test that translate displays the source function"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Provide interactive input (will fail on empty input but we can check output)
    # Provide translations: function name, variable name, docstring, comment
    input_text = "saluer\nnom\nDire bonjour\nFrench translation\n"
    result = cli_run(
        ["translate", f"{func_hash}@eng", "fra"], env=env, input_text=input_text
    )

    # Assert: Should show source function
    assert "Source function (eng):" in result.stdout
    assert "def greet" in result.stdout


def test_translate_creates_mapping(tmp_path):
    """Test that translate creates a new language mapping"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Provide translations for both names
    # _bb_v_0 = greet, _bb_v_1 = name
    input_text = "saluer\nnom\nDire bonjour\n\n"  # empty comment
    result = cli_run(
        ["translate", f"{func_hash}@eng", "fra"], env=env, input_text=input_text
    )

    # Assert
    assert result.returncode == 0
    assert "Translation saved" in result.stdout

    # Verify mapping was created
    func_dir = bb_dir / "pool" / func_hash[:2] / func_hash[2:]
    assert (func_dir / "fra").exists()


def test_translate_prompts_for_all_names(tmp_path):
    """Test that translate prompts for all variable names"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Function with multiple variables
    test_file = tmp_path / "func.py"
    test_file.write_text('''def calculate(value, multiplier):
    """Calculate result"""
    result = value * multiplier
    return result
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Provide translations for all names
    # 4 names: function + 3 variables
    input_text = "calculer\nresultat\nvaleur\nmultiplicateur\nCalculer le resultat\nTest comment\n"
    result = cli_run(
        ["translate", f"{func_hash}@eng", "fra"], env=env, input_text=input_text
    )

    # Assert
    assert result.returncode == 0
    assert "Translation saved" in result.stdout
