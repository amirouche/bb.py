"""
Tests for remote functionality.

Unit tests for low-level git operations (URL parsing, type detection).
Integration tests for CLI commands (remote add/list/remove/pull/push).
"""
import pytest

import ouverture


# =============================================================================
# Unit tests for low-level git operations
# =============================================================================

def test_remote_type_detect_file():
    """Test detecting file:// remote type"""
    assert ouverture.remote_type_detect("file:///path/to/pool") == "file"


def test_remote_type_detect_git_ssh():
    """Test detecting git SSH remote type"""
    assert ouverture.remote_type_detect("git@github.com:user/repo.git") == "git-ssh"


def test_remote_type_detect_git_https():
    """Test detecting git HTTPS remote type"""
    assert ouverture.remote_type_detect("git+https://github.com/user/repo.git") == "git-https"


def test_remote_type_detect_git_file():
    """Test detecting local git remote type"""
    assert ouverture.remote_type_detect("git+file:///path/to/repo") == "git-file"


def test_remote_type_detect_unknown():
    """Test detecting unknown remote type"""
    assert ouverture.remote_type_detect("ftp://example.com") == "unknown"


def test_git_url_parse_ssh():
    """Test parsing SSH Git URL"""
    result = ouverture.git_url_parse("git@github.com:user/repo.git")
    assert result['protocol'] == 'ssh'
    assert result['host'] == 'github.com'
    assert result['git_url'] == 'git@github.com:user/repo.git'


def test_git_url_parse_https():
    """Test parsing HTTPS Git URL"""
    result = ouverture.git_url_parse("git+https://github.com/user/repo.git")
    assert result['protocol'] == 'https'
    assert result['git_url'] == 'https://github.com/user/repo.git'


def test_git_url_parse_file():
    """Test parsing file Git URL"""
    result = ouverture.git_url_parse("git+file:///home/user/repo")
    assert result['protocol'] == 'file'
    assert result['git_url'] == 'file:///home/user/repo'


def test_git_url_parse_invalid():
    """Test parsing invalid Git URL raises error"""
    with pytest.raises(ValueError):
        ouverture.git_url_parse("invalid://url")


# =============================================================================
# Integration tests for remote CLI commands
# =============================================================================

def test_remote_add_file(cli_runner, tmp_path):
    """Test adding a file:// remote via CLI"""
    remote_path = tmp_path / "remote_pool"
    remote_path.mkdir()

    result = cli_runner.run(['remote', 'add', 'local', f'file://{remote_path}'])

    assert result.returncode == 0
    assert 'Added remote' in result.stdout
    assert 'local' in result.stdout


def test_remote_add_git_ssh(cli_runner):
    """Test adding a git SSH remote via CLI"""
    result = cli_runner.run(['remote', 'add', 'origin', 'git@github.com:user/pool.git'])

    assert result.returncode == 0
    assert 'Added remote' in result.stdout
    assert 'git-ssh' in result.stdout


def test_remote_add_git_https(cli_runner):
    """Test adding a git HTTPS remote via CLI"""
    result = cli_runner.run(['remote', 'add', 'upstream', 'git+https://github.com/org/pool.git'])

    assert result.returncode == 0
    assert 'Added remote' in result.stdout
    assert 'git-https' in result.stdout


def test_remote_add_invalid_url_fails(cli_runner):
    """Test adding invalid URL format fails"""
    result = cli_runner.run(['remote', 'add', 'bad', 'ftp://invalid'])

    assert result.returncode != 0
    assert 'Invalid URL format' in result.stderr


def test_remote_add_duplicate_fails(cli_runner, tmp_path):
    """Test adding duplicate remote name fails"""
    remote_path = tmp_path / "remote"
    remote_path.mkdir()

    cli_runner.run(['remote', 'add', 'dup', f'file://{remote_path}'])
    result = cli_runner.run(['remote', 'add', 'dup', f'file://{remote_path}'])

    assert result.returncode != 0
    assert 'already exists' in result.stderr


def test_remote_list_empty(cli_runner):
    """Test listing remotes when none configured"""
    result = cli_runner.run(['remote', 'list'])

    assert result.returncode == 0
    assert 'No remotes configured' in result.stdout


def test_remote_list_shows_remotes(cli_runner, tmp_path):
    """Test listing configured remotes"""
    remote_path = tmp_path / "remote"
    remote_path.mkdir()

    cli_runner.run(['remote', 'add', 'myremote', f'file://{remote_path}'])
    result = cli_runner.run(['remote', 'list'])

    assert result.returncode == 0
    assert 'myremote' in result.stdout


def test_remote_remove(cli_runner, tmp_path):
    """Test removing a remote"""
    remote_path = tmp_path / "remote"
    remote_path.mkdir()

    cli_runner.run(['remote', 'add', 'toremove', f'file://{remote_path}'])
    result = cli_runner.run(['remote', 'remove', 'toremove'])

    assert result.returncode == 0
    assert 'Removed remote' in result.stdout


def test_remote_remove_nonexistent_fails(cli_runner):
    """Test removing nonexistent remote fails"""
    result = cli_runner.run(['remote', 'remove', 'doesnotexist'])

    assert result.returncode != 0
    assert 'not found' in result.stderr


def test_remote_pull_file(cli_runner, tmp_path):
    """Test pulling from file:// remote"""
    # Setup: Create remote pool with a function
    remote_pool = tmp_path / "remote_pool"
    remote_pool.mkdir()
    remote_objects = remote_pool / "objects" / "sha256" / "ab"
    remote_objects.mkdir(parents=True)

    # Create a minimal v1 function
    func_dir = remote_objects / ("c123" + "0" * 56)
    func_dir.mkdir()
    (func_dir / "object.json").write_text('{"schema_version": 1, "normalized_code": "def _ouverture_v_0(): pass"}')

    # Add remote and pull
    cli_runner.run(['remote', 'add', 'source', f'file://{remote_pool}'])
    result = cli_runner.run(['remote', 'pull', 'source'])

    assert result.returncode == 0
    assert 'Pulling from remote' in result.stdout


def test_remote_push_file(cli_runner, tmp_path):
    """Test pushing to file:// remote"""
    # Setup: Add a function to local pool
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    cli_runner.add(str(test_file), 'eng')

    # Create remote and push
    remote_pool = tmp_path / "remote_pool"
    cli_runner.run(['remote', 'add', 'dest', f'file://{remote_pool}'])
    result = cli_runner.run(['remote', 'push', 'dest'])

    assert result.returncode == 0
    assert 'Pushing to remote' in result.stdout


def test_remote_pull_nonexistent_fails(cli_runner):
    """Test pulling from nonexistent remote fails"""
    result = cli_runner.run(['remote', 'pull', 'noremote'])

    assert result.returncode != 0
    assert 'not found' in result.stderr


def test_remote_push_nonexistent_fails(cli_runner):
    """Test pushing to nonexistent remote fails"""
    result = cli_runner.run(['remote', 'push', 'noremote'])

    assert result.returncode != 0
    assert 'not found' in result.stderr
