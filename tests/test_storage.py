"""
Tests for storage functions (Schema v1 write/read path).

Tests for saving and loading functions in v1 format.
"""
import json

import pytest

import mobius
from tests.conftest import normalize_code_for_test


# ============================================================================
# Tests for V1 Write Path
# ============================================================================

def test_function_save_v1_creates_object_json(mock_mobius_dir):
    """Test that function_save_v1 creates proper object.json"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(): pass")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['test'],
        'dependencies': []
    }

    mobius.function_save_v1(test_hash, normalized_code, metadata)

    # Check that object.json was created - with sha256 in path
    pool_dir = mock_mobius_dir / '.mobius' / 'pool'
    objects_dir = pool_dir
    func_dir = objects_dir / 'sha256' / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    assert object_json.exists()

    # Load and verify structure
    with open(object_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert data['schema_version'] == 1
    assert data['hash'] == test_hash
    assert data['hash_algorithm'] == 'sha256'
    assert data['normalized_code'] == normalized_code
    assert data['encoding'] == 'none'
    assert data['metadata'] == metadata


def test_function_save_v1_no_language_data(mock_mobius_dir):
    """Test that function_save_v1 does NOT include language-specific data"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(): pass")
    metadata = mobius.metadata_create()

    mobius.function_save_v1(test_hash, normalized_code, metadata)

    pool_dir = mock_mobius_dir / '.mobius' / 'pool'
    objects_dir = pool_dir
    func_dir = objects_dir / 'sha256' / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Should NOT have docstrings, name_mappings, alias_mappings
    assert 'docstrings' not in data
    assert 'name_mappings' not in data
    assert 'alias_mappings' not in data


def test_mapping_save_v1_creates_mapping_json(mock_mobius_dir):
    """Test that mapping_save_v1 creates proper mapping.json"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_mobius_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # First create the function (object.json must exist)
    normalized_code = normalize_code_for_test("def _mobius_v_0(): pass")
    metadata = mobius.metadata_create()
    mobius.function_save_v1(func_hash, normalized_code, metadata)

    # Now save the mapping
    mapping_hash = mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Check that mapping.json was created - with sha256 in paths
    pool_dir = mock_mobius_dir / '.mobius' / 'pool'
    objects_dir = pool_dir
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    mapping_dir = func_dir / lang / 'sha256' / mapping_hash[:2] / mapping_hash[2:]
    mapping_json = mapping_dir / 'mapping.json'

    assert mapping_json.exists()

    # Load and verify structure
    with open(mapping_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert data['docstring'] == docstring
    assert data['name_mapping'] == name_mapping
    assert data['alias_mapping'] == alias_mapping
    assert data['comment'] == comment


def test_mapping_save_v1_returns_hash(mock_mobius_dir):
    """Test that mapping_save_v1 returns the mapping hash"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_mobius_v_0": "test"}
    alias_mapping = {}
    comment = ""

    # Create function first
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())

    # Save mapping
    mapping_hash = mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Verify it's a valid hash
    assert len(mapping_hash) == 64
    assert all(c in '0123456789abcdef' for c in mapping_hash)

    # Verify it matches computed hash
    expected_hash = mobius.mapping_compute_hash(docstring, name_mapping, alias_mapping, comment)
    assert mapping_hash == expected_hash


def test_mapping_save_v1_deduplication(mock_mobius_dir):
    """Test that identical mappings share the same file (deduplication)"""
    func_hash1 = "aaaa" + "0" * 60
    func_hash2 = "bbbb" + "0" * 60
    lang = "eng"
    docstring = "Identical docstring"
    name_mapping = {"_mobius_v_0": "identical"}
    alias_mapping = {}
    comment = "Same comment"

    # Create two different functions
    mobius.function_save_v1(func_hash1, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())
    mobius.function_save_v1(func_hash2, normalize_code_for_test("def _mobius_v_0(): return 42"), mobius.metadata_create())

    # Save identical mappings for both
    mapping_hash1 = mobius.mapping_save_v1(func_hash1, lang, docstring, name_mapping, alias_mapping, comment)
    mapping_hash2 = mobius.mapping_save_v1(func_hash2, lang, docstring, name_mapping, alias_mapping, comment)

    # Hashes should be identical
    assert mapping_hash1 == mapping_hash2


def test_mapping_save_v1_different_comments_different_hashes(mock_mobius_dir):
    """Test that different comments produce different mapping hashes"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_mobius_v_0": "test"}
    alias_mapping = {}

    # Create function
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())

    # Save two mappings with different comments
    hash1 = mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, "Formal")
    hash2 = mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, "Informal")

    # Hashes should be different
    assert hash1 != hash2


def test_v1_write_integration_full_structure(mock_mobius_dir):
    """Integration test: verify complete v1 directory structure"""
    func_hash = "test1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(_mobius_v_1): return _mobius_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['math'],
        'dependencies': []
    }

    # Save function
    mobius.function_save_v1(func_hash, normalized_code, metadata)

    # Save mappings in two languages
    eng_hash = mobius.mapping_save_v1(
        func_hash, "eng",
        "Double the input",
        {"_mobius_v_0": "double", "_mobius_v_1": "value"},
        {},
        "Simple English"
    )

    fra_hash = mobius.mapping_save_v1(
        func_hash, "fra",
        "Doubler l'entrée",
        {"_mobius_v_0": "doubler", "_mobius_v_1": "valeur"},
        {},
        "Français simple"
    )

    # Verify directory structure - with sha256 in paths
    pool_dir = mock_mobius_dir / '.mobius' / 'pool'
    objects_dir = pool_dir
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]

    # Check object.json exists
    assert (func_dir / 'object.json').exists()

    # Check language directories exist
    assert (func_dir / 'eng').exists()
    assert (func_dir / 'fra').exists()

    # Check mapping files exist - with sha256 in mapping paths
    assert (func_dir / 'eng' / 'sha256' / eng_hash[:2] / eng_hash[2:] / 'mapping.json').exists()
    assert (func_dir / 'fra' / 'sha256' / fra_hash[:2] / fra_hash[2:] / 'mapping.json').exists()


# ============================================================================
# Tests for V1 Read Path
# ============================================================================

def test_function_load_v1_loads_object_json(mock_mobius_dir):
    """Test that function_load_v1 loads object.json correctly"""
    func_hash = "test5678" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(_mobius_v_1): return _mobius_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['test'],
        'dependencies': []
    }

    # Save function first
    mobius.function_save_v1(func_hash, normalized_code, metadata)

    # Load it back
    loaded_data = mobius.function_load_v1(func_hash)

    # Verify data
    assert loaded_data['schema_version'] == 1
    assert loaded_data['hash'] == func_hash
    assert loaded_data['hash_algorithm'] == 'sha256'
    assert loaded_data['normalized_code'] == normalized_code
    assert loaded_data['encoding'] == 'none'
    assert loaded_data['metadata'] == metadata


def test_mappings_list_v1_single_mapping(mock_mobius_dir):
    """Test that mappings_list_v1 returns single mapping correctly"""
    func_hash = "list1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_mobius_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # Create function and mapping
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())
    mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # List mappings
    mappings = mobius.mappings_list_v1(func_hash, lang)

    # Should have exactly one mapping
    assert len(mappings) == 1
    mapping_hash, mapping_comment = mappings[0]
    assert len(mapping_hash) == 64
    assert mapping_comment == comment


def test_mappings_list_v1_multiple_mappings(mock_mobius_dir):
    """Test that mappings_list_v1 returns multiple mappings"""
    func_hash = "list5678" + "0" * 56
    lang = "eng"

    # Create function
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())

    # Add two mappings with different comments
    mobius.mapping_save_v1(func_hash, lang, "Doc 1", {"_mobius_v_0": "func1"}, {}, "Formal")
    mobius.mapping_save_v1(func_hash, lang, "Doc 2", {"_mobius_v_0": "func2"}, {}, "Casual")

    # List mappings
    mappings = mobius.mappings_list_v1(func_hash, lang)

    # Should have two mappings
    assert len(mappings) == 2

    # Extract comments
    comments = [comment for _, comment in mappings]
    assert "Formal" in comments
    assert "Casual" in comments


def test_mappings_list_v1_no_mappings(mock_mobius_dir):
    """Test that mappings_list_v1 returns empty list when no mappings exist"""
    func_hash = "nomaps12" + "0" * 56

    # Create function without any mappings
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())

    # List mappings for a language that doesn't exist
    mappings = mobius.mappings_list_v1(func_hash, "fra")

    # Should be empty
    assert len(mappings) == 0


def test_mapping_load_v1_loads_correctly(mock_mobius_dir):
    """Test that mapping_load_v1 loads a specific mapping"""
    func_hash = "load1234" + "0" * 56
    lang = "eng"
    docstring = "Test docstring"
    name_mapping = {"_mobius_v_0": "test_func", "_mobius_v_1": "param"}
    alias_mapping = {"abc123": "helper"}
    comment = "Test variant"

    # Create function and mapping
    mobius.function_save_v1(func_hash, normalize_code_for_test("def _mobius_v_0(): pass"), mobius.metadata_create())
    mapping_hash = mobius.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Load the mapping
    loaded_doc, loaded_name, loaded_alias, loaded_comment = mobius.mapping_load_v1(func_hash, lang, mapping_hash)

    # Verify data
    assert loaded_doc == docstring
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_comment == comment


def test_function_load_v1_integration(mock_mobius_dir):
    """Integration test: write v1, read v1, verify correctness"""
    func_hash = "integ123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _mobius_v_0(_mobius_v_1): return _mobius_v_1 + 1")
    docstring = "Increment by one"
    name_mapping = {"_mobius_v_0": "increment", "_mobius_v_1": "value"}
    alias_mapping = {}
    comment = "Simple increment"

    # Write v1 format
    mobius.function_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping, comment)

    # Read back using dispatch (should detect v1)
    loaded_code, loaded_name, loaded_alias, loaded_doc = mobius.function_load(func_hash, lang)

    # Verify correctness
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_function_load_dispatch_multiple_mappings(mock_mobius_dir):
    """Test that dispatch with multiple mappings defaults to first one"""
    func_hash = "multi123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _mobius_v_0(): pass")

    # Create function with two mappings
    mobius.function_save_v1(func_hash, normalized_code, mobius.metadata_create())
    hash1 = mobius.mapping_save_v1(func_hash, lang, "Doc 1", {"_mobius_v_0": "func1"}, {}, "First")
    hash2 = mobius.mapping_save_v1(func_hash, lang, "Doc 2", {"_mobius_v_0": "func2"}, {}, "Second")

    # Load without specifying mapping_hash (should return first alphabetically)
    loaded_code, loaded_name, loaded_alias, loaded_doc = mobius.function_load(func_hash, lang)

    # Should load one of the mappings (implementation will pick first alphabetically)
    assert loaded_code == normalized_code
    assert loaded_name in [{"_mobius_v_0": "func1"}, {"_mobius_v_0": "func2"}]
    assert loaded_doc in ["Doc 1", "Doc 2"]


def test_function_load_dispatch_explicit_mapping(mock_mobius_dir):
    """Test that dispatch can load specific mapping by hash"""
    func_hash = "explicit1" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _mobius_v_0(): pass")

    # Create function with two mappings
    mobius.function_save_v1(func_hash, normalized_code, mobius.metadata_create())
    hash1 = mobius.mapping_save_v1(func_hash, lang, "Doc 1", {"_mobius_v_0": "func1"}, {}, "First")
    hash2 = mobius.mapping_save_v1(func_hash, lang, "Doc 2", {"_mobius_v_0": "func2"}, {}, "Second")

    # Load with specific mapping_hash
    loaded_code, loaded_name, loaded_alias, loaded_doc = mobius.function_load(func_hash, lang, mapping_hash=hash2)

    # Should load the second mapping
    assert loaded_code == normalized_code
    assert loaded_name == {"_mobius_v_0": "func2"}
    assert loaded_doc == "Doc 2"
