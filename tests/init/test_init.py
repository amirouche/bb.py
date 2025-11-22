"""
Tests for 'mobius.py init' command.

Grey-box integration tests that verify CLI behavior and internal storage state.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def cli_run(args: list, env: dict = None, cwd: str = None) -> subprocess.CompletedProcess:
    """Run mobius.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / 'mobius.py')] + args

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


def test_init_creates_pool_directory(tmp_path):
    """Test that init creates the pool directory structure."""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    assert (mobius_dir / 'pool').exists()
    assert (mobius_dir / 'pool').is_dir()


def test_init_creates_config_file(tmp_path):
    """Test that init creates config.json with correct structure."""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    config_path = mobius_dir / 'config.json'
    assert config_path.exists()

    config = json.loads(config_path.read_text())
    assert 'user' in config
    assert 'remotes' in config
    assert 'username' in config['user']
    assert 'email' in config['user']
    assert 'public_key' in config['user']
    assert 'languages' in config['user']
    assert config['user']['languages'] == ['eng']


def test_init_uses_username_from_environment(tmp_path, monkeypatch):
    """Test that init uses USER environment variable for username."""
    mobius_dir = tmp_path / '.mobius'
    monkeypatch.setenv('USER', 'testuser123')
    env = {'MOBIUS_DIRECTORY': str(mobius_dir), 'USER': 'testuser123'}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    config = json.loads((mobius_dir / 'config.json').read_text())
    assert config['user']['username'] == 'testuser123'


def test_init_output_messages(tmp_path):
    """Test that init outputs correct messages."""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    assert 'Created config file' in result.stdout
    assert 'Initialized mobius directory' in result.stdout


def test_init_existing_config_not_overwritten(tmp_path):
    """Test that init does not overwrite existing config."""
    mobius_dir = tmp_path / '.mobius'
    mobius_dir.mkdir(parents=True)
    config_path = mobius_dir / 'config.json'

    # Create existing config with custom content
    existing_config = {'user': {'username': 'existing_user', 'custom': 'value'}}
    config_path.write_text(json.dumps(existing_config))

    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}
    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    assert 'already exists' in result.stdout

    # Verify original config is preserved
    preserved_config = json.loads(config_path.read_text())
    assert preserved_config['user']['username'] == 'existing_user'
    assert preserved_config['user']['custom'] == 'value'


def test_init_idempotent(tmp_path):
    """Test that running init twice is safe."""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    # First init
    result1 = cli_run(['init'], env=env)
    assert result1.returncode == 0

    # Get config after first init
    config1 = json.loads((mobius_dir / 'config.json').read_text())

    # Second init
    result2 = cli_run(['init'], env=env)
    assert result2.returncode == 0

    # Config should be unchanged
    config2 = json.loads((mobius_dir / 'config.json').read_text())
    assert config1 == config2


def test_init_creates_empty_remotes(tmp_path):
    """Test that init creates empty remotes dict."""
    mobius_dir = tmp_path / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(mobius_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    config = json.loads((mobius_dir / 'config.json').read_text())
    assert config['remotes'] == {}


def test_init_respects_mobius_directory_env(tmp_path):
    """Test that MOBIUS_DIRECTORY env var is respected."""
    custom_dir = tmp_path / 'custom_mobius_location'
    env = {'MOBIUS_DIRECTORY': str(custom_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    assert custom_dir.exists()
    assert (custom_dir / 'pool').exists()
    assert (custom_dir / 'config.json').exists()


def test_init_creates_parent_directories(tmp_path):
    """Test that init creates parent directories if they don't exist."""
    nested_dir = tmp_path / 'deeply' / 'nested' / 'path' / '.mobius'
    env = {'MOBIUS_DIRECTORY': str(nested_dir)}

    result = cli_run(['init'], env=env)

    assert result.returncode == 0
    assert nested_dir.exists()
    assert (nested_dir / 'pool').exists()
