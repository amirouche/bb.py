"""
Tests for the commit command.

Integration tests for CLI validation and functionality.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

import bb
from tests.conftest import normalize_code_for_test


# Helper to run CLI commands
def cli_run(args: list, env: dict = None, cwd: str = None) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / 'bb.py')] + args
    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=cwd)


# =============================================================================
# Integration tests for commit CLI validation
# =============================================================================

def test_commit_invalid_hash_format_fails(tmp_path):
    """Test that commit fails with invalid hash format"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    result = cli_run(['commit', 'not-a-valid-hash', '--comment', 'test'], env=env)

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_commit_nonexistent_function_fails(tmp_path):
    """Test that commit fails for nonexistent function"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}
    fake_hash = 'f' * 64

    result = cli_run(['commit', fake_hash, '--comment', 'test'], env=env)

    assert result.returncode != 0
    assert 'not found' in result.stderr.lower()


def test_commit_help_shows_usage():
    """Test that commit --help shows usage information"""
    result = cli_run(['commit', '--help'])

    assert result.returncode == 0
    assert 'hash' in result.stdout.lower()
    assert '--comment' in result.stdout


# =============================================================================
# Integration tests for commit functionality
# =============================================================================

def test_commit_copies_function_to_git_directory(tmp_path):
    """Test that commit copies function to git directory"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create a test file and add it
    test_file = tmp_path / 'test_func.py'
    test_file.write_text('''def hello():
    """Say hello"""
    return "hello"
''', encoding='utf-8')

    # Add function to pool
    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    assert add_result.returncode == 0

    # Extract hash from output
    func_hash = None
    for line in add_result.stdout.split('\n'):
        if 'Hash:' in line:
            func_hash = line.split('Hash:')[1].strip()
            break
    assert func_hash is not None

    # Commit the function
    result = cli_run(['commit', func_hash, '--comment', 'Add hello function'], env=env)

    assert result.returncode == 0
    assert 'Committed' in result.stdout

    # Verify function was copied to git directory
    git_dir = bb_dir / 'git'
    assert git_dir.exists()
    func_in_git = git_dir / func_hash[:2] / func_hash[2:] / 'object.json'
    assert func_in_git.exists()


def test_commit_copies_all_language_mappings(tmp_path):
    """Test that commit copies all language mappings"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create English version
    test_file_eng = tmp_path / 'hello_eng.py'
    test_file_eng.write_text('''def hello():
    """Say hello"""
    return "hello"
''', encoding='utf-8')

    # Create French version (same logic, different names)
    test_file_fra = tmp_path / 'bonjour_fra.py'
    test_file_fra.write_text('''def bonjour():
    """Dire bonjour"""
    return "hello"
''', encoding='utf-8')

    # Add both versions
    add_eng = cli_run(['add', f'{test_file_eng}@eng'], env=env)
    assert add_eng.returncode == 0
    func_hash = None
    for line in add_eng.stdout.split('\n'):
        if 'Hash:' in line:
            func_hash = line.split('Hash:')[1].strip()
            break

    add_fra = cli_run(['add', f'{test_file_fra}@fra'], env=env)
    assert add_fra.returncode == 0

    # Commit the function
    result = cli_run(['commit', func_hash, '--comment', 'Add multilingual hello'], env=env)
    assert result.returncode == 0

    # Verify both language mappings were copied
    git_dir = bb_dir / 'git'
    func_git_dir = git_dir / func_hash[:2] / func_hash[2:]
    assert (func_git_dir / 'eng').exists()
    assert (func_git_dir / 'fra').exists()


def test_commit_copies_dependencies_recursively(tmp_path):
    """Test that commit copies all dependencies recursively"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create helper function
    helper_file = tmp_path / 'helper.py'
    helper_file.write_text('''def helper():
    """Helper function"""
    return 42
''', encoding='utf-8')

    # Add helper
    add_helper = cli_run(['add', f'{helper_file}@eng'], env=env)
    assert add_helper.returncode == 0
    helper_hash = None
    for line in add_helper.stdout.split('\n'):
        if 'Hash:' in line:
            helper_hash = line.split('Hash:')[1].strip()
            break

    # Create main function that depends on helper
    main_file = tmp_path / 'main.py'
    main_file.write_text(f'''from bb.pool import object_{helper_hash} as helper

def main():
    """Main function"""
    return helper()
''', encoding='utf-8')

    # Add main
    add_main = cli_run(['add', f'{main_file}@eng'], env=env)
    assert add_main.returncode == 0
    main_hash = None
    for line in add_main.stdout.split('\n'):
        if 'Hash:' in line:
            main_hash = line.split('Hash:')[1].strip()
            break

    # Commit the main function
    result = cli_run(['commit', main_hash, '--comment', 'Add main with dependency'], env=env)
    assert result.returncode == 0
    assert '2 function(s)' in result.stdout  # main + helper

    # Verify both functions were copied
    git_dir = bb_dir / 'git'
    main_in_git = git_dir / main_hash[:2] / main_hash[2:] / 'object.json'
    helper_in_git = git_dir / helper_hash[:2] / helper_hash[2:] / 'object.json'
    assert main_in_git.exists()
    assert helper_in_git.exists()


def test_commit_initializes_git_repo(tmp_path):
    """Test that commit initializes git repository if not present"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create and add a function
    test_file = tmp_path / 'test.py'
    test_file.write_text('''def test():
    """Test function"""
    return 1
''', encoding='utf-8')

    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = None
    for line in add_result.stdout.split('\n'):
        if 'Hash:' in line:
            func_hash = line.split('Hash:')[1].strip()
            break

    # Commit
    result = cli_run(['commit', func_hash, '--comment', 'Initial commit'], env=env)
    assert result.returncode == 0

    # Verify git was initialized
    git_dir = bb_dir / 'git'
    assert (git_dir / '.git').exists()


def test_commit_creates_git_commit(tmp_path):
    """Test that commit creates an actual git commit"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create and add a function
    test_file = tmp_path / 'test.py'
    test_file.write_text('''def test():
    """Test function"""
    return 1
''', encoding='utf-8')

    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = None
    for line in add_result.stdout.split('\n'):
        if 'Hash:' in line:
            func_hash = line.split('Hash:')[1].strip()
            break

    # Commit with specific message
    commit_msg = 'Test commit message'
    result = cli_run(['commit', func_hash, '--comment', commit_msg], env=env)
    assert result.returncode == 0

    # Verify git log shows the commit
    git_dir = bb_dir / 'git'
    git_log = subprocess.run(
        ['git', 'log', '--oneline', '-1'],
        cwd=str(git_dir),
        capture_output=True,
        text=True
    )
    assert commit_msg in git_log.stdout


def test_commit_no_changes_when_already_committed(tmp_path):
    """Test that commit reports no changes when function already committed"""
    bb_dir = tmp_path / '.bb'
    pool_dir = bb_dir / 'pool'
    pool_dir.mkdir(parents=True, exist_ok=True)
    env = {'BB_DIRECTORY': str(bb_dir)}

    # Create and add a function
    test_file = tmp_path / 'test.py'
    test_file.write_text('''def test():
    """Test function"""
    return 1
''', encoding='utf-8')

    add_result = cli_run(['add', f'{test_file}@eng'], env=env)
    func_hash = None
    for line in add_result.stdout.split('\n'):
        if 'Hash:' in line:
            func_hash = line.split('Hash:')[1].strip()
            break

    # First commit
    cli_run(['commit', func_hash, '--comment', 'First commit'], env=env)

    # Second commit of same function
    result = cli_run(['commit', func_hash, '--comment', 'Second commit'], env=env)
    assert result.returncode == 0
    assert 'No new changes to commit' in result.stdout


# =============================================================================
# Unit tests for commit helper functions
# =============================================================================

def test_storage_get_git_directory():
    """Test that storage_get_git_directory returns correct path"""
    import os

    # Test with BB_DIRECTORY set
    original = os.environ.get('BB_DIRECTORY')
    try:
        os.environ['BB_DIRECTORY'] = '/test/bb'
        result = bb.storage_get_git_directory()
        assert result == Path('/test/bb/git')
    finally:
        if original:
            os.environ['BB_DIRECTORY'] = original
        elif 'BB_DIRECTORY' in os.environ:
            del os.environ['BB_DIRECTORY']


def test_git_init_commit_repo_creates_directory(tmp_path, monkeypatch):
    """Test that git_init_commit_repo creates git directory"""
    git_dir = tmp_path / 'git'

    monkeypatch.setattr(bb, 'storage_get_git_directory', lambda: git_dir)

    result = bb.git_init_commit_repo()

    assert result == git_dir
    assert git_dir.exists()
    assert (git_dir / '.git').exists()


def test_git_init_commit_repo_idempotent(tmp_path, monkeypatch):
    """Test that git_init_commit_repo is idempotent"""
    git_dir = tmp_path / 'git'

    monkeypatch.setattr(bb, 'storage_get_git_directory', lambda: git_dir)

    # Call twice
    bb.git_init_commit_repo()
    result = bb.git_init_commit_repo()

    assert result == git_dir
    assert (git_dir / '.git').exists()
