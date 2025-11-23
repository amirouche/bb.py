"""
Tests for the fork command.

Integration tests for CLI validation and unit tests for lineage tracking.
"""
import subprocess
import sys
from pathlib import Path

import pytest

import bb
from tests.conftest import normalize_code_for_test


# Helper to run CLI commands
def cli_run(args: list, env: dict = None, cwd: str = None, input_text: str = None) -> subprocess.CompletedProcess:
    """Run bb.py CLI command."""
    cmd = [sys.executable, str(Path(__file__).parent.parent.parent / 'bb.py')] + args
    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=cwd, input=input_text)


# =============================================================================
# Integration tests for fork CLI validation
# =============================================================================

def test_fork_missing_language_suffix_fails(tmp_path):
    """Test that fork fails without language suffix"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}
    fake_hash = 'a' * 64

    result = cli_run(['fork', fake_hash], env=env)

    assert result.returncode != 0
    assert 'Missing language suffix' in result.stderr


def test_fork_invalid_hash_format_fails(tmp_path):
    """Test that fork fails with invalid hash format"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}

    result = cli_run(['fork', 'not-a-valid-hash@eng'], env=env)

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_fork_too_short_language_code_fails(tmp_path):
    """Test that fork fails with too short language code"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}
    fake_hash = 'a' * 64

    result = cli_run(['fork', f'{fake_hash}@ab'], env=env)

    assert result.returncode != 0
    assert 'Language code must be 3-256 characters' in result.stderr


def test_fork_nonexistent_function_fails(tmp_path):
    """Test that fork fails for nonexistent function"""
    bb_dir = tmp_path / '.bb'
    env = {'BB_DIRECTORY': str(bb_dir)}
    fake_hash = 'f' * 64

    result = cli_run(['fork', f'{fake_hash}@eng'], env=env)

    assert result.returncode != 0
    assert 'not found' in result.stderr.lower()


# =============================================================================
# Unit tests for parent lineage in metadata
# =============================================================================

def test_metadata_without_parent():
    """Test that metadata without parent doesn't include parent field"""
    metadata = bb.code_create_metadata()

    assert 'created' in metadata
    assert 'name' in metadata
    assert 'email' in metadata
    assert 'parent' not in metadata


def test_metadata_with_parent():
    """Test that metadata with parent includes parent field"""
    parent_hash = 'a' * 64

    metadata = bb.code_create_metadata(parent=parent_hash)

    assert 'created' in metadata
    assert 'name' in metadata
    assert 'email' in metadata
    assert 'parent' in metadata
    assert metadata['parent'] == parent_hash


def test_fork_saves_parent_in_metadata(mock_bb_dir, tmp_path):
    """Test that forking saves parent hash in metadata"""
    import json

    # Create original function
    original_hash = "original" + "0" * 56
    original_code = normalize_code_for_test("def _bb_v_0(): return 42")
    bb.code_save(original_hash, "eng", original_code, "Original function",
                     {"_bb_v_0": "answer"}, {})

    # Create forked function with parent reference
    forked_hash = "forked00" + "0" * 56
    forked_code = normalize_code_for_test("def _bb_v_0(): return 100")
    bb.code_save(forked_hash, "eng", forked_code, "Forked function",
                     {"_bb_v_0": "answer"}, {}, parent=original_hash)

    # Verify parent is stored in metadata
    pool_dir = bb.storage_get_pool_directory()
    object_json_path = pool_dir / forked_hash[:2] / forked_hash[2:] / 'object.json'

    with open(object_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert 'metadata' in data
    assert 'parent' in data['metadata']
    assert data['metadata']['parent'] == original_hash


def test_fork_original_has_no_parent(mock_bb_dir, tmp_path):
    """Test that original function (not forked) has no parent in metadata"""
    import json

    # Create original function
    original_hash = "nofork00" + "0" * 56
    original_code = normalize_code_for_test("def _bb_v_0(): return 42")
    bb.code_save(original_hash, "eng", original_code, "Original function",
                     {"_bb_v_0": "answer"}, {})

    # Verify parent is NOT stored in metadata
    pool_dir = bb.storage_get_pool_directory()
    object_json_path = pool_dir / original_hash[:2] / original_hash[2:] / 'object.json'

    with open(object_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert 'metadata' in data
    assert 'parent' not in data['metadata']
