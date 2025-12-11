"""
Tests for 'bb.py review' command.

Grey-box integration tests for function review with dependency resolution.
Note: review is now interactive, so some tests use stdin injection.
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


def test_review_invalid_hash_fails(tmp_path):
    """Test that review fails with invalid hash format"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["review", "not-a-valid-hash"], env=env)

    assert result.returncode != 0
    assert "Invalid hash format" in result.stderr


def test_review_nonexistent_function_warns(tmp_path):
    """Test that review warns for nonexistent function"""
    bb_dir = tmp_path / ".bb"
    (bb_dir / "pool").mkdir(parents=True)
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "f" * 64
    result = cli_run(["review", fake_hash], env=env)

    # Review continues but warns about missing function
    assert (
        "not found" in result.stderr.lower() or "not available" in result.stderr.lower()
    )


def test_review_displays_function_code(tmp_path):
    """Test that review displays function code"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    cli_run(["init"], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process(data):
    """Process some data"""
    return data * 2
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - provide 'y' to approve the function
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert
    assert result.returncode == 0
    assert "Function: process (eng)" in result.stdout
    assert f"Hash: {func_hash}" in result.stdout
    assert "def process(data):" in result.stdout
    assert "Dependencies: None" in result.stdout


def test_review_shows_function_review_header(tmp_path):
    """Test that review shows proper header"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    cli_run(["init"], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - provide 'y' to approve
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert
    assert result.returncode == 0
    assert "Interactive Function Review" in result.stdout


def test_review_uses_preferred_language(tmp_path):
    """Test that review uses user's preferred languages"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Initialize and set French as preferred language
    cli_run(["init"], env=env)
    cli_run(["whoami", "language", "fra"], env=env)

    # Add function in French
    test_file = tmp_path / "func.py"
    test_file.write_text('''def calculer(valeur):
    """Calculer le resultat"""
    return valeur * 2
''')
    add_result = cli_run(["add", f"{test_file}@fra"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - provide 'y' to approve
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert: Should show French version
    assert result.returncode == 0
    assert "calculer (fra)" in result.stdout
    assert "Calculer le resultat" in result.stdout


def test_review_fallback_when_language_unavailable(tmp_path):
    """Test that review warns when function not in preferred language"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Initialize with Spanish as preferred language
    cli_run(["init"], env=env)
    cli_run(["whoami", "language", "spa"], env=env)

    # Add function in English only
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - run will exit early due to no matching language
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert: Should warn about unavailable language
    assert "not available in any preferred language" in result.stderr


def test_review_default_language_fallback(tmp_path):
    """Test that review falls back to 'eng' when no preferred languages set"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add function without init (no preferred languages)
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Review without init (config doesn't exist) - provide 'y'
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert: Should still work using 'eng' as default
    assert result.returncode == 0
    assert "foo (eng)" in result.stdout


def test_review_saves_state(tmp_path):
    """Test that review saves reviewed functions to state file"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    cli_run(["init"], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text("def bar(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - approve the function
    result = cli_run(["review", func_hash], env=env, input_text="y\n")

    # Assert: State file should contain the approved hash
    assert result.returncode == 0
    assert "approved" in result.stdout.lower()

    state_file = bb_dir / "review_state.json"
    assert state_file.exists()

    with open(state_file, "r") as f:
        state = json.load(f)
    assert func_hash in state["reviewed"]


def test_review_skips_already_reviewed(tmp_path):
    """Test that review skips already reviewed functions"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    cli_run(["init"], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text("def baz(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # First review - approve
    cli_run(["review", func_hash], env=env, input_text="y\n")

    # Second review - should skip
    result = cli_run(["review", func_hash], env=env)

    # Assert: Should say all already reviewed
    assert result.returncode == 0
    assert "already been reviewed" in result.stdout


def test_review_quit_saves_progress(tmp_path):
    """Test that 'q' quits review and saves progress"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    cli_run(["init"], env=env)
    test_file = tmp_path / "func.py"
    test_file.write_text("def qux(): pass")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - quit without approving
    result = cli_run(["review", func_hash], env=env, input_text="q\n")

    # Assert: Should indicate paused
    assert result.returncode == 0
    assert "paused" in result.stdout.lower() or "Review paused" in result.stdout
