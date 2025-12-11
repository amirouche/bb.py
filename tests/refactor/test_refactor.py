"""
Tests for 'bb.py refactor' command.

Grey-box integration tests for hash replacement in functions.
"""

import os
import subprocess
import sys
from pathlib import Path



def cli_run(args: list, env: dict = None) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / "bb.py")] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(cmd, capture_output=True, text=True, env=run_env)


def test_refactor_invalid_what_hash_fails(tmp_path):
    """Test that refactor fails with invalid what hash"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_from = "a" * 64
    fake_to = "b" * 64
    result = cli_run(["refactor", "invalid-hash", fake_from, fake_to], env=env)

    assert result.returncode != 0
    assert "Invalid" in result.stderr
    assert "what" in result.stderr.lower()


def test_refactor_invalid_from_hash_fails(tmp_path):
    """Test that refactor fails with invalid from hash"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_what = "a" * 64
    fake_to = "b" * 64
    result = cli_run(["refactor", fake_what, "invalid", fake_to], env=env)

    assert result.returncode != 0
    assert "Invalid" in result.stderr


def test_refactor_invalid_to_hash_fails(tmp_path):
    """Test that refactor fails with invalid to hash"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_what = "a" * 64
    fake_from = "b" * 64
    result = cli_run(["refactor", fake_what, fake_from, "invalid"], env=env)

    assert result.returncode != 0
    assert "Invalid" in result.stderr


def test_refactor_nonexistent_what_function_fails(tmp_path):
    """Test that refactor fails when what function doesn't exist"""
    bb_dir = tmp_path / ".bb"
    (bb_dir / "pool").mkdir(parents=True)
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_what = "a" * 64
    fake_from = "b" * 64
    fake_to = "c" * 64
    result = cli_run(["refactor", fake_what, fake_from, fake_to], env=env)

    assert result.returncode != 0
    assert "not found" in result.stderr.lower()


def test_refactor_nonexistent_to_function_fails(tmp_path):
    """Test that refactor fails when to function doesn't exist"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add a function (what)
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): return 42")
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    what_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    fake_from = "b" * 64
    fake_to = "c" * 64
    result = cli_run(["refactor", what_hash, fake_from, fake_to], env=env)

    assert result.returncode != 0
    assert "not found" in result.stderr.lower()
