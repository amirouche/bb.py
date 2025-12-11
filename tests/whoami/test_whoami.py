"""
Tests for 'bb.py whoami' command.

Grey-box integration tests for user configuration management.
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


def test_whoami_get_name_empty_without_init(tmp_path):
    """Test getting name returns empty when config doesn't exist."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["whoami", "name"], env=env)

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_whoami_set_and_get_name(tmp_path):
    """Test setting and getting name."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Initialize first
    cli_run(["init"], env=env)

    # Set name
    result = cli_run(["whoami", "name", "testuser"], env=env)
    assert result.returncode == 0
    assert "Set name: testuser" in result.stdout

    # Get name
    result = cli_run(["whoami", "name"], env=env)
    assert result.returncode == 0
    assert result.stdout.strip() == "testuser"


def test_whoami_set_and_get_email(tmp_path):
    """Test setting and getting email."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)

    # Set email
    result = cli_run(["whoami", "email", "test@example.com"], env=env)
    assert result.returncode == 0
    assert "Set email: test@example.com" in result.stdout

    # Get email
    result = cli_run(["whoami", "email"], env=env)
    assert result.returncode == 0
    assert result.stdout.strip() == "test@example.com"


def test_whoami_set_and_get_public_key(tmp_path):
    """Test setting and getting public key."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)

    # Set public key
    result = cli_run(
        ["whoami", "public-key", "https://keys.example.com/key.pub"], env=env
    )
    assert result.returncode == 0
    assert "Set public-key: https://keys.example.com/key.pub" in result.stdout

    # Get public key
    result = cli_run(["whoami", "public-key"], env=env)
    assert result.returncode == 0
    assert result.stdout.strip() == "https://keys.example.com/key.pub"


def test_whoami_set_and_get_languages(tmp_path):
    """Test setting and getting languages (multiple values)."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)

    # Set multiple languages
    result = cli_run(["whoami", "language", "eng", "fra", "spa"], env=env)
    assert result.returncode == 0
    assert "Set language: eng fra spa" in result.stdout

    # Get languages
    result = cli_run(["whoami", "language"], env=env)
    assert result.returncode == 0
    assert result.stdout.strip() == "eng fra spa"


def test_whoami_languages_replace_not_append(tmp_path):
    """Test that setting languages replaces previous values."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)

    # Set initial languages
    cli_run(["whoami", "language", "eng", "fra"], env=env)

    # Set new languages (should replace)
    cli_run(["whoami", "language", "deu", "ita"], env=env)

    # Get languages
    result = cli_run(["whoami", "language"], env=env)
    assert result.returncode == 0
    assert result.stdout.strip() == "deu ita"


def test_whoami_invalid_subcommand(tmp_path):
    """Test that invalid subcommand fails with argparse error."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["whoami", "invalid"], env=env)

    assert result.returncode == 2
    assert "invalid choice" in result.stderr


def test_whoami_persists_to_config_file(tmp_path):
    """Test that whoami changes are persisted to config.json."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)
    cli_run(["whoami", "name", "persisteduser"], env=env)
    cli_run(["whoami", "email", "persisted@example.com"], env=env)

    # Read config directly
    config = json.loads((bb_dir / "config.json").read_text())

    assert config["user"]["name"] == "persisteduser"
    assert config["user"]["email"] == "persisted@example.com"


def test_whoami_corrupted_config_fails_gracefully(tmp_path):
    """Test that corrupted config file shows helpful error."""
    bb_dir = tmp_path / ".bb"
    bb_dir.mkdir(parents=True)
    (bb_dir / "config.json").write_text("corrupted json")
    env = {"BB_DIRECTORY": str(bb_dir)}

    result = cli_run(["whoami", "name"], env=env)

    assert result.returncode == 1
    assert "Error" in result.stderr
    assert "config" in result.stderr.lower()


def test_whoami_get_empty_languages(tmp_path):
    """Test getting languages when none are set."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    # Don't init - default config has empty languages list
    result = cli_run(["whoami", "language"], env=env)

    assert result.returncode == 0
    assert result.stdout.strip() == ""


def test_whoami_single_language(tmp_path):
    """Test setting a single language."""
    bb_dir = tmp_path / ".bb"
    env = {"BB_DIRECTORY": str(bb_dir)}

    cli_run(["init"], env=env)
    result = cli_run(["whoami", "language", "fra"], env=env)

    assert result.returncode == 0
    assert "Set language: fra" in result.stdout

    result = cli_run(["whoami", "language"], env=env)
    assert result.stdout.strip() == "fra"
