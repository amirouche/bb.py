"""
Tests for 'bb.py run' command.

Grey-box integration tests for function execution.
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


def test_run_without_language_works(tmp_path):
    """Test that run works without language suffix when function exists"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add a function first
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Run without @lang
    result = cli_run(["run", func_hash, "--", "World"], env=env)

    # Assert: Should succeed
    assert result.returncode == 0
    assert "Hello, World!" in result.stdout


def test_run_without_language_nonexistent_fails(tmp_path):
    """Test that run fails without language suffix when function doesn't exist"""
    bb_dir = tmp_path / ".bb"
    (bb_dir / "pool").mkdir(parents=True)
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "0" * 64
    result = cli_run(["run", fake_hash], env=env)

    assert result.returncode != 0
    assert "No language mappings found" in result.stderr


def test_run_debug_requires_language(tmp_path):
    """Test that run --debug requires language suffix"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add a function first
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Run --debug without @lang
    result = cli_run(["run", "--debug", func_hash], env=env)

    # Assert: Should fail requiring language
    assert result.returncode != 0
    assert "Language suffix required when using --debug" in result.stderr


def test_run_invalid_language_fails(tmp_path):
    """Test that run fails with too short language code"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "0" * 64
    result = cli_run(["run", f"{fake_hash}@ab"], env=env)

    assert result.returncode != 0
    assert "Language code must be 3-256 characters" in result.stderr


def test_run_invalid_hash_fails(tmp_path):
    """Test that run fails with invalid hash format"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["run", "not-valid-hash@eng"], env=env)

    assert result.returncode != 0
    assert "Invalid hash format" in result.stderr


def test_run_nonexistent_function_fails(tmp_path):
    """Test that run fails for nonexistent function"""
    bb_dir = tmp_path / ".bb"
    (bb_dir / "pool").mkdir(parents=True)
    env = {"BB_DIRECTORY": str(bb_dir)}

    fake_hash = "f" * 64
    result = cli_run(["run", f"{fake_hash}@eng"], env=env)

    assert result.returncode != 0
    assert (
        "Could not load function" in result.stderr
        or "not found" in result.stderr.lower()
    )


def test_run_with_string_argument(tmp_path):
    """Test running function with string argument"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - arguments are passed as strings, no implicit coercion
    result = cli_run(["run", f"{func_hash}@eng", "--", "World"], env=env)

    # Assert
    assert result.returncode == 0
    assert "Hello, World!" in result.stdout


def test_run_with_multiple_string_arguments(tmp_path):
    """Test running function with multiple string arguments (no implicit coercion)"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup - function concatenates strings
    test_file = tmp_path / "func.py"
    test_file.write_text('''def concat(a, b):
    """Concatenate two strings"""
    return a + b
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test - arguments passed as strings
    result = cli_run(["run", f"{func_hash}@eng", "--", "Hello", "World"], env=env)

    # Assert
    assert result.returncode == 0
    assert "HelloWorld" in result.stdout


def test_run_displays_function_code(tmp_path):
    """Test that run displays the function code"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def my_func(value):
    """Process value"""
    return value + 1
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test
    result = cli_run(["run", f"{func_hash}@eng", "--", "10"], env=env)

    # Assert
    assert result.returncode == 0
    assert "def my_func(value):" in result.stdout
    assert "Running function: my_func" in result.stdout


def test_run_function_with_exception(tmp_path):
    """Test that run handles function exceptions gracefully"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Function that raises exception
    test_file = tmp_path / "func.py"
    test_file.write_text('''def divide(a, b):
    """Divide a by b"""
    return a / b
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test: Division by zero
    result = cli_run(["run", f"{func_hash}@eng", "--", "10", "0"], env=env)

    # Assert: Should fail with error message
    assert result.returncode != 0
    assert "Error" in result.stderr or "ZeroDivisionError" in result.stderr
