"""
Test script for SQLite storage implementation.
"""

import ast
import os
import tempfile

from bb import (
    init_sqlite_storage,
    code_save_sqlite,
    code_load_sqlite,
    mapping_save_sqlite,
    mapping_load_sqlite,
    list_mappings_sqlite,
    save_dependencies_sqlite,
    load_dependencies_sqlite,
    code_detect_storage,
    code_load_unified,
    code_save_unified,
)


def test_sqlite_storage():
    """Test SQLite storage functions"""

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set BB_DIRECTORY to temp dir
        os.environ["BB_DIRECTORY"] = temp_dir

        # Initialize SQLite storage
        conn, aston_nstore, deps_nstore = init_sqlite_storage()

        # Test function data
        test_code = """
def add(a, b):
    '''Add two numbers'''
    return a + b
"""

        test_hash = "0" * 64  # Fake hash for testing
        test_metadata = {
            "created": "2023-01-01T00:00:00Z",
            "name": "test",
            "email": "test@example.com",
        }

        # Save function to SQLite
        code_save_sqlite(conn, aston_nstore, test_hash, test_code, test_metadata)

        # Load function from SQLite
        loaded_data = code_load_sqlite(conn, aston_nstore, test_hash)

        # Verify basic properties
        assert loaded_data["hash"] == test_hash, "Hash mismatch"
        assert loaded_data["schema_version"] == 2, "Schema version mismatch"
        assert len(loaded_data["normalized_code"]) > 0, "Empty normalized code"

        # Verify the code is the same (after AST round-trip)
        original_tree = ast.parse(test_code)
        loaded_tree = ast.parse(loaded_data["normalized_code"])

        assert ast.dump(original_tree) == ast.dump(loaded_tree), "AST round-trip failed"

        # Test mapping storage
        test_lang = "eng"
        test_docstring = "Add two numbers"
        test_name_mapping = {"_bb_v_0": "add", "_bb_v_1": "a", "_bb_v_2": "b"}
        test_alias_mapping = {}
        test_comment = "Test mapping"

        mapping_hash = mapping_save_sqlite(
            conn,
            test_hash,
            test_lang,
            test_docstring,
            test_name_mapping,
            test_alias_mapping,
            test_comment,
        )

        # Load mapping back
        loaded_mapping = mapping_load_sqlite(conn, test_hash, test_lang, mapping_hash)

        # Verify mapping data
        assert loaded_mapping[0] == test_docstring, "Docstring mismatch"
        assert loaded_mapping[1] == test_name_mapping, "Name mapping mismatch"
        assert loaded_mapping[2] == test_alias_mapping, "Alias mapping mismatch"
        assert loaded_mapping[3] == test_comment, "Comment mismatch"

        # Test listing mappings
        mappings = list_mappings_sqlite(conn, test_hash, test_lang)
        assert len(mappings) == 1, "Expected 1 mapping"
        assert mappings[0][0] == mapping_hash, "Mapping hash mismatch"

        # Test dependency storage
        test_deps = ["1" * 64, "2" * 64, "3" * 64]  # Fake dependency hashes

        save_dependencies_sqlite(conn, deps_nstore, test_hash, test_deps)
        loaded_deps = load_dependencies_sqlite(conn, deps_nstore, test_hash)

        assert set(loaded_deps) == set(test_deps), "Dependency mismatch"

        # Test storage detection
        storage_type = code_detect_storage()
        assert storage_type == "sqlite", f"Expected sqlite storage, got {storage_type}"

        # Test unified functions
        code_save_unified(
            test_hash,
            test_lang,
            test_code,
            test_docstring,
            test_name_mapping,
            test_alias_mapping,
            test_comment,
        )

        loaded_data = code_load_unified(test_hash, test_lang, mapping_hash)
        assert loaded_data is not None, "Unified load failed"

        # Close connection
        conn.close()


def test_migration_simulation():
    """Simulate migration process"""

    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["BB_DIRECTORY"] = temp_dir

        # Create some test functions in file-based storage
        from bb import code_save_v1, mapping_save_v1

        pool_dir = os.path.join(temp_dir, "pool")
        os.makedirs(pool_dir, exist_ok=True)

        # Create a test function
        test_hash = "a" * 64
        test_code = "def test(): return 42"
        test_metadata = {"created": "2023-01-01T00:00:00Z"}

        code_save_v1(test_hash, test_code, test_metadata)

        # Add a mapping
        mapping_save_v1(test_hash, "eng", "Test function", {"_bb_v_0": "test"}, {}, "test mapping")

        # Test that we can detect storage types
        storage_before = code_detect_storage()
        assert storage_before == "file", f"Expected file storage, got {storage_before}"

        # Test that migration function is available
        from bb import command_migrate

        assert callable(command_migrate), "Migration function not callable"
