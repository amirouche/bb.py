"""
Tests for the 'migrate' CLI command and schema migration functions.

Tests migrating functions from v0 to v1 schema format.
"""
import json
from unittest.mock import patch

import pytest

import ouverture
from tests.conftest import normalize_code_for_test


def test_schema_migrate_function_v0_to_v1_basic(mock_ouverture_dir):
    """Test basic function migration from v0 to v1"""
    func_hash = "migrate01" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    docstring = "Return 42"
    name_mapping = {"_ouverture_v_0": "get_answer"}
    alias_mapping = {}

    # Create v0 function
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate to v1 (keep v0 by default in test)
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=True)

    # Verify v1 format exists
    version = ouverture.schema_detect_version(func_hash)
    assert version == 1

    # Verify v0 still exists (keep_v0=True)
    pool_dir = mock_ouverture_dir / '.ouverture' / 'pool'
    v0_path = pool_dir / func_hash[:2] / f'{func_hash[2:]}.json'
    assert v0_path.exists()

    # Load from v1 and verify data
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_schema_migrate_function_v0_to_v1_delete_v0(mock_ouverture_dir):
    """Test migration with v0 deletion"""
    func_hash = "migrate02" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 100")
    docstring = "Return 100"
    name_mapping = {"_ouverture_v_0": "get_hundred"}
    alias_mapping = {}

    # Create v0 function
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate to v1 (delete v0)
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Verify v1 format exists
    version = ouverture.schema_detect_version(func_hash)
    assert version == 1

    # Verify v0 was deleted
    pool_dir = mock_ouverture_dir / '.ouverture' / 'pool'
    v0_path = pool_dir / func_hash[:2] / f'{func_hash[2:]}.json'
    assert not v0_path.exists()

    # Load from v1 and verify data
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_schema_migrate_function_v0_to_v1_multiple_languages(mock_ouverture_dir):
    """Test migration with multiple languages"""
    func_hash = "migrate03" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 50")

    # Create v0 function with two languages
    ouverture.function_save_v0(func_hash, "eng", normalized_code, "English doc", {"_ouverture_v_0": "fifty"}, {})
    ouverture.function_save_v0(func_hash, "fra", normalized_code, "French doc", {"_ouverture_v_0": "cinquante"}, {})

    # Migrate to v1
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Verify both languages are available in v1
    loaded_code_eng, loaded_name_eng, _, loaded_doc_eng = ouverture.function_load(func_hash, "eng")
    loaded_code_fra, loaded_name_fra, _, loaded_doc_fra = ouverture.function_load(func_hash, "fra")

    assert loaded_code_eng == normalized_code
    assert loaded_name_eng == {"_ouverture_v_0": "fifty"}
    assert loaded_doc_eng == "English doc"

    assert loaded_code_fra == normalized_code
    assert loaded_name_fra == {"_ouverture_v_0": "cinquante"}
    assert loaded_doc_fra == "French doc"


def test_schema_migrate_all_v0_to_v1(mock_ouverture_dir):
    """Test migrating all v0 functions at once"""
    # Create three v0 functions
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")
    for i in range(3):
        func_hash = f"migrall{i}" + "0" * 56
        ouverture.function_save_v0(func_hash, "eng", normalized_code, f"Doc {i}", {"_ouverture_v_0": f"func{i}"}, {})

    # Migrate all
    ouverture.schema_migrate_all_v0_to_v1(keep_v0=False, dry_run=False)

    # Verify all three were migrated
    for i in range(3):
        func_hash = f"migrall{i}" + "0" * 56
        version = ouverture.schema_detect_version(func_hash)
        assert version == 1

        # Verify v0 files deleted
        pool_dir = mock_ouverture_dir / '.ouverture' / 'pool'
        v0_path = pool_dir / func_hash[:2] / f'{func_hash[2:]}.json'
        assert not v0_path.exists()


def test_schema_migrate_all_v0_to_v1_dry_run(mock_ouverture_dir):
    """Test dry-run doesn't actually migrate"""
    func_hash = "dryrun01" + "0" * 56
    ouverture.function_save_v0(func_hash, "eng", normalize_code_for_test("def _ouverture_v_0(): pass"), "Doc", {"_ouverture_v_0": "func"}, {})

    # Dry run
    result = ouverture.schema_migrate_all_v0_to_v1(keep_v0=True, dry_run=True)

    # Verify nothing changed (still v0)
    version = ouverture.schema_detect_version(func_hash)
    assert version == 0

    # Verify result contains the function that would be migrated
    assert len(result) == 1
    assert result[0] == func_hash


def test_schema_validate_v1_valid_function(mock_ouverture_dir):
    """Test validation of valid v1 function"""
    func_hash = "valid001" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 1")
    docstring = "Return 1"
    name_mapping = {"_ouverture_v_0": "one"}
    alias_mapping = {}

    # Create valid v1 function
    ouverture.function_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is True
    assert len(errors) == 0


def test_schema_validate_v1_missing_object_json(mock_ouverture_dir):
    """Test validation detects missing object.json"""
    func_hash = "invalid01" + "0" * 56

    # Don't create anything, just validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is False
    assert len(errors) > 0
    assert any("not found" in err.lower() or "missing" in err.lower() for err in errors)


def test_schema_validate_v1_missing_mappings(mock_ouverture_dir):
    """Test validation detects function with no mappings"""
    func_hash = "invalid02" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 1")
    metadata = ouverture.metadata_create()

    # Create function without any mappings
    ouverture.function_save_v1(func_hash, normalized_code, metadata)

    # Validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is False
    assert len(errors) > 0
    assert any("no mappings" in err.lower() or "no language" in err.lower() for err in errors)


def test_schema_migrate_function_preserves_alias_mappings(mock_ouverture_dir):
    """Test migration preserves alias mappings"""
    func_hash = "alias001" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return abc123._ouverture_v_0()")
    docstring = "Call helper"
    name_mapping = {"_ouverture_v_0": "caller"}
    alias_mapping = {"abc123": "helper"}

    # Create v0 function with alias mapping
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Load and verify alias mapping preserved
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_alias == alias_mapping
