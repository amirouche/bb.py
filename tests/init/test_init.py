"""
Tests for 'bb.py init' command.

Grey-box integration tests that verify CLI behavior and internal storage state.
"""

import json
import os
import subprocess
import sys
from pathlib import Path



def cli_run(
    args: list, env: dict = None, cwd: str = None
) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / "bb.py")] + args

    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    return subprocess.run(cmd, capture_output=True, text=True, env=run_env, cwd=cwd)


def test_init_creates_pool_directory(tmp_path):
    """Test that init creates the pool directory structure."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    assert (bb_dir / "pool").exists()
    assert (bb_dir / "pool").is_dir()


def test_init_creates_config_file(tmp_path):
    """Test that init creates config.json with correct structure."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    config_path = bb_dir / "config.json"
    assert config_path.exists()

    config = json.loads(config_path.read_text())
    assert "user" in config
    assert "remotes" in config
    assert "username" in config["user"]
    assert "email" in config["user"]
    assert "public_key" in config["user"]
    assert "languages" in config["user"]
    assert config["user"]["languages"] == ["eng"]


def test_init_uses_username_from_environment(tmp_path, monkeypatch):
    """Test that init uses USER environment variable for username."""
    bb_dir = tmp_path / ".bb"
    monkeypatch.setenv("USER", "testuser123")
    env = {"BB_DIRECTORY": str(bb_dir), "USER": "testuser123"}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    config = json.loads((bb_dir / "config.json").read_text())
    assert config["user"]["username"] == "testuser123"


def test_init_output_messages(tmp_path):
    """Test that init outputs correct messages."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    assert "Created config file" in result.stdout
    assert "Initialized bb directory" in result.stdout


def test_init_existing_config_not_overwritten(tmp_path):
    """Test that init does not overwrite existing config."""
    bb_dir = tmp_path / ".bb"
    bb_dir.mkdir(parents=True)
    config_path = bb_dir / "config.json"

    # Create existing config with custom content
    existing_config = {"user": {"username": "existing_user", "custom": "value"}}
    config_path.write_text(json.dumps(existing_config))

    env = {"BB_DIRECTORY": str(bb_dir)}
    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    assert "already exists" in result.stdout

    # Verify original config is preserved
    preserved_config = json.loads(config_path.read_text())
    assert preserved_config["user"]["username"] == "existing_user"
    assert preserved_config["user"]["custom"] == "value"


def test_init_idempotent(tmp_path):
    """Test that running init twice is safe."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # First init
    result1 = cli_run(["init"], env=env)
    assert result1.returncode == 0

    # Get config after first init
    config1 = json.loads((bb_dir / "config.json").read_text())

    # Second init
    result2 = cli_run(["init"], env=env)
    assert result2.returncode == 0

    # Config should be unchanged
    config2 = json.loads((bb_dir / "config.json").read_text())
    assert config1 == config2


def test_init_creates_empty_remotes(tmp_path):
    """Test that init creates empty remotes dict."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    config = json.loads((bb_dir / "config.json").read_text())
    assert config["remotes"] == {}


def test_init_respects_bb_directory_env(tmp_path):
    """Test that BB_DIRECTORY env var is respected."""
    custom_dir = tmp_path / "custom_bb_location"
    env = {"BB_DIRECTORY": str(custom_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    assert custom_dir.exists()
    assert (custom_dir / "pool").exists()
    assert (custom_dir / "config.json").exists()


def test_init_creates_parent_directories(tmp_path):
    """Test that init creates parent directories if they don't exist."""
    nested_dir = tmp_path / "deeply" / "nested" / "path" / ".bb"
    env = {"BB_DIRECTORY": str(nested_dir)}

    result = cli_run(["init"], env=env)

    assert result.returncode == 0
    assert nested_dir.exists()
    assert (nested_dir / "pool").exists()
