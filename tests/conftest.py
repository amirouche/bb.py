"""
Shared pytest fixtures for mobius tests.

This module provides common fixtures used across all test modules.

Test Philosophy:
- Most tests should be integration tests (grey-box style)
- Setup: Use CLI commands (mobius.py add, init, etc.)
- Test: Call CLI commands
- Assert: Check CLI output and/or files directly
- Unit tests only for complex low-level aspects (AST, hashing, schema, migration)
"""
import ast
import subprocess
import sys
from pathlib import Path

import pytest

# Add parent directory to path so we can import mobius
sys.path.insert(0, str(Path(__file__).parent.parent))

import mobius

# Export fixtures and helpers
__all__ = ['normalize_code_for_test', 'mock_mobius_dir', 'sample_function_code',
           'sample_function_file', 'sample_async_function_code', 'sample_async_function_file',
           'cli_run', 'cli_runner']


def normalize_code_for_test(code: str) -> str:
    """
    Normalize code string to match ast.unparse() output format.

    All normalized code strings in tests MUST go through this function to ensure
    they match the format that mobius produces. This is because ast.unparse()
    always outputs code with proper line breaks and indentation, regardless of
    the input format.

    The function:
    1. Parses code into AST
    2. Clears all line/column information recursively (using mobius.code_clear_locations)
    3. Fixes missing locations
    4. Unparses back to string

    Example:
        # Wrong - this format never exists in practice:
        normalized_code = "def _mobius_v_0(): return 42"

        # Correct - use this helper:
        normalized_code = normalize_code_for_test("def _mobius_v_0(): return 42")
        # Returns: "def _mobius_v_0():\\n    return 42"
    """
    tree = ast.parse(code)
    mobius.code_clear_locations(tree)
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


def cli_run(args: list, env: dict = None, cwd: str = None) -> subprocess.CompletedProcess:
    """
    Run mobius.py CLI command.

    Args:
        args: Command arguments (without 'python mobius.py' prefix)
        env: Environment variables (merged with current env)
        cwd: Working directory

    Returns:
        CompletedProcess with stdout, stderr, returncode

    Example:
        result = cli_run(['add', 'test.py@eng'])
        assert result.returncode == 0
        assert 'Hash:' in result.stdout
    """
    import os

    cmd = [sys.executable, str(Path(__file__).parent.parent / 'mobius.py')] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=run_env,
        cwd=cwd
    )


class CLIRunner:
    """Helper class for running CLI commands with a specific mobius directory.

    Directory structure:
        mobius_dir/
        ├── pool/          # Pool directory
        └── config.json    # Configuration file
    """

    def __init__(self, mobius_dir: Path):
        self.mobius_dir = mobius_dir
        self.pool_dir = mobius_dir / 'pool'
        self.env = {
            'MOBIUS_DIRECTORY': str(mobius_dir)
        }

    def run(self, args: list, cwd: str = None) -> subprocess.CompletedProcess:
        """Run CLI command with this runner's mobius directory."""
        return cli_run(args, env=self.env, cwd=cwd)

    def add(self, file_path: str, lang: str) -> str:
        """Add a function and return its hash."""
        result = self.run(['add', f'{file_path}@{lang}'])
        if result.returncode != 0:
            raise RuntimeError(f"add failed: {result.stderr}")
        # Extract hash from output
        for line in result.stdout.split('\n'):
            if 'Hash:' in line:
                return line.split('Hash:')[1].strip()
        raise RuntimeError(f"Could not find hash in output: {result.stdout}")

    def show(self, hash_lang: str) -> str:
        """Show a function and return its code."""
        result = self.run(['show', hash_lang])
        if result.returncode != 0:
            raise RuntimeError(f"show failed: {result.stderr}")
        return result.stdout

    def get(self, hash_lang: str) -> str:
        """Get a function and return its code."""
        result = self.run(['get', hash_lang])
        if result.returncode != 0:
            raise RuntimeError(f"get failed: {result.stderr}")
        return result.stdout


@pytest.fixture
def mock_mobius_dir(tmp_path, monkeypatch):
    """
    Fixture to monkey patch directory functions to return a temp directory.
    This ensures tests work with pytest-xdist (parallel test runner).

    Directory structure:
        tmp_path/.mobius/
        ├── pool/          # Pool directory
        └── config.json    # Configuration file
    """
    base_dir = tmp_path / '.mobius'
    pool_dir = base_dir / 'pool'

    def _get_temp_mobius_dir():
        return base_dir

    def _get_temp_pool_dir():
        return pool_dir

    monkeypatch.setattr(mobius, 'storage_get_mobius_directory', _get_temp_mobius_dir)
    monkeypatch.setattr(mobius, 'storage_get_pool_directory', _get_temp_pool_dir)
    return tmp_path


@pytest.fixture
def cli_runner(tmp_path):
    """
    Fixture providing a CLIRunner with isolated mobius directory.

    Use this for integration tests that call CLI commands.
    Creates the directory structure:
        tmp_path/.mobius/
        ├── pool/           # Pool directory
        └── config.json     # Configuration file
    """
    mobius_dir = tmp_path / '.mobius'
    pool_dir = mobius_dir / 'pool'

    pool_dir.mkdir(parents=True, exist_ok=True)

    return CLIRunner(mobius_dir)


@pytest.fixture
def sample_function_code():
    """Sample function code for testing."""
    return '''def calculate_sum(first, second):
    """Add two numbers"""
    result = first + second
    return result'''


@pytest.fixture
def sample_function_file(tmp_path, sample_function_code):
    """Create a temporary file with sample function code."""
    test_file = tmp_path / "sample.py"
    test_file.write_text(sample_function_code, encoding='utf-8')
    return test_file


@pytest.fixture
def sample_async_function_code():
    """Sample async function code for testing."""
    return '''async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response'''


@pytest.fixture
def sample_async_function_file(tmp_path, sample_async_function_code):
    """Create a temporary file with sample async function code."""
    test_file = tmp_path / "async_sample.py"
    test_file.write_text(sample_async_function_code, encoding='utf-8')
    return test_file
