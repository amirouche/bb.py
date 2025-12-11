"""
Tests for 'bb.py search' command.

Grey-box integration tests for function search.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest


def cli_run(args: list, env: dict = None) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / "bb.py")] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(cmd, capture_output=True, text=True, env=run_env)


def test_search_no_query_fails(tmp_path):
    """Test that search fails without query"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["search"], env=env)

    assert result.returncode != 0


def test_search_empty_pool(tmp_path):
    """Test that search handles empty pool gracefully"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["search", "foo"], env=env)

    assert result.returncode == 0
    assert "No functions in pool" in result.stdout


def test_search_finds_by_function_name(tmp_path):
    """Test that search finds function by docstring content (name in docstring)"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Include search term in docstring since names are normalized
    test_file = tmp_path / "func.py"
    test_file.write_text('''def calculate_average(numbers):
    """Calculate the average of numbers"""
    return sum(numbers) / len(numbers)
''')
    cli_run(["add", f"{test_file}@eng"], env=env)

    # Test: Search for term in docstring
    result = cli_run(["search", "calculate"], env=env)

    # Assert
    assert result.returncode == 0
    assert "calculate_average" in result.stdout
    assert "docstring" in result.stdout.lower()


def test_search_finds_by_docstring(tmp_path):
    """Test that search finds function by docstring content"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process(data):
    """Transform the input data using special algorithm"""
    return data * 2
''')
    cli_run(["add", f"{test_file}@eng"], env=env)

    # Test
    result = cli_run(["search", "algorithm"], env=env)

    # Assert
    assert result.returncode == 0
    assert "process" in result.stdout
    assert "docstring" in result.stdout.lower()


def test_search_case_insensitive(tmp_path):
    """Test that search is case insensitive"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Put searchable term in docstring
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process():
    """MySpecialFunction docstring"""
    pass
''')
    cli_run(["add", f"{test_file}@eng"], env=env)

    # Test: Search with different case
    result = cli_run(["search", "MYSPECIALFUNCTION"], env=env)

    # Assert
    assert result.returncode == 0
    assert "1 matches" in result.stdout


def test_search_no_matches(tmp_path):
    """Test that search handles no matches gracefully"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Add a function
    test_file = tmp_path / "func.py"
    test_file.write_text("def foo(): pass")
    cli_run(["add", f"{test_file}@eng"], env=env)

    # Test: Search for non-existent term
    result = cli_run(["search", "nonexistent"], env=env)

    # Assert
    assert result.returncode == 0
    assert "0 matches" in result.stdout


def test_search_shows_view_command(tmp_path):
    """Test that search results include view command"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup: Put searchable term in docstring
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process():
    """A searchable docstring"""
    pass
''')
    add_result = cli_run(["add", f"{test_file}@eng"], env=env)
    func_hash = add_result.stdout.split("Hash:")[1].strip().split()[0]

    # Test
    result = cli_run(["search", "searchable"], env=env)

    # Assert
    assert result.returncode == 0
    assert f"bb.py show {func_hash}@eng" in result.stdout


def test_search_multiple_terms(tmp_path):
    """Test that search works with multiple terms"""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def calculate_total(items):
    """Calculate total value"""
    return sum(items)
''')
    cli_run(["add", f"{test_file}@eng"], env=env)

    # Test: Multiple search terms
    result = cli_run(["search", "calculate", "total"], env=env)

    # Assert
    assert result.returncode == 0
    assert "calculate_total" in result.stdout
