"""
Test SQLite integration with bb.py core functionality.
"""

import os
import tempfile
import subprocess
import re
import pytest


def test_sqlite_integration():
    """Test SQLite integration"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ['BB_DIRECTORY'] = temp_dir
        
        from bb import (
            init_sqlite_storage, code_save_sqlite, code_load_sqlite,
            mapping_save_sqlite, mapping_load_sqlite, list_mappings_sqlite,
            save_dependencies_sqlite, load_dependencies_sqlite,
            code_detect_storage, code_load_unified, code_save_unified
        )
        
        # Initialize SQLite storage
        conn, aston_nstore, deps_nstore = init_sqlite_storage()
        
        # Test function data
        test_code = """
def test_function(x, y):
    '''A test function'''
    return x + y
"""
        
        test_hash = "a" * 64
        test_metadata = {"created": "2023-01-01T00:00:00Z"}
        
        # Save function
        code_save_sqlite(conn, aston_nstore, test_hash, test_code, test_metadata)
        
        # Load function
        loaded_data = code_load_sqlite(conn, aston_nstore, test_hash)
        assert loaded_data['hash'] == test_hash, "Hash mismatch"
        assert len(loaded_data['normalized_code']) > 0, "Empty normalized code"
        
        # Test mapping
        test_lang = "eng"
        test_docstring = "A test function"
        test_name_mapping = {"_bb_v_0": "test_function", "_bb_v_1": "x", "_bb_v_2": "y"}
        test_alias_mapping = {}
        test_comment = "Test mapping"
        
        mapping_hash = mapping_save_sqlite(
            conn, test_hash, test_lang, test_docstring,
            test_name_mapping, test_alias_mapping, test_comment
        )
        
        # Load mapping
        loaded_mapping = mapping_load_sqlite(conn, test_hash, test_lang, mapping_hash)
        assert loaded_mapping[0] == test_docstring, "Docstring mismatch"
        assert loaded_mapping[1] == test_name_mapping, "Name mapping mismatch"
        
        # Test dependencies
        test_deps = ["b" * 64, "c" * 64]
        save_dependencies_sqlite(conn, deps_nstore, test_hash, test_deps)
        loaded_deps = load_dependencies_sqlite(conn, deps_nstore, test_hash)
        assert set(loaded_deps) == set(test_deps), "Dependency mismatch"
        
        # Test storage detection
        storage_type = code_detect_storage()
        assert storage_type == "sqlite", f"Expected sqlite storage, got {storage_type}"
        
        # Test unified functions
        code_save_unified(
            test_hash, test_lang, test_code, test_docstring,
            test_name_mapping, test_alias_mapping, test_comment
        )
        
        loaded_unified = code_load_unified(test_hash, test_lang, mapping_hash)
        assert loaded_unified is not None, "Unified load failed"
        
        conn.close()
        
        # Test that bb.py commands work with SQLite storage
        bb_path = os.path.abspath("bb.py")
        
        # Create a simple test file (without language suffix in filename)
        test_file_base = "simple_test.py"
        test_file_path = os.path.join(temp_dir, test_file_base)
        with open(test_file_path, 'w') as f:
            f.write('''
def simple_add(a, b):
    """Add two numbers"""
    return a + b
''')
        
        # Add the function (with language suffix)
        cmd_add = ["python3", bb_path, "add", "simple_test.py@eng"]
        result = subprocess.run(cmd_add, capture_output=True, text=True, cwd=temp_dir)
        
        assert result.returncode == 0, f"Add command failed: {result.stderr}"
        
        # Extract the hash from the output
        match = re.search(r'Hash: ([a-f0-9]{64})', result.stdout)
        assert match is not None, "Could not find function hash in add output"
        
        func_hash = match.group(1)
        assert len(func_hash) == 64, "Invalid hash length"
        
        # Test show command
        cmd_show = ["python3", bb_path, "show", func_hash]
        result_show = subprocess.run(cmd_show, capture_output=True, text=True, cwd=temp_dir)
        
        assert result_show.returncode == 0, f"Show command failed: {result_show.stderr}"
        
        # Test run command
        cmd_run = ["python3", bb_path, "run", func_hash, "--", "2", "3"]
        result_run = subprocess.run(cmd_run, capture_output=True, text=True, cwd=temp_dir)
        
        assert result_run.returncode == 0, f"Run command failed: {result_run.stderr}"
        assert "5" in result_run.stdout, f"Run command did not return expected result: {result_run.stdout}"
        
        # Test migrate command
        cmd_migrate = ["python3", bb_path, "migrate"]
        result_migrate = subprocess.run(cmd_migrate, capture_output=True, text=True, cwd=temp_dir)
        
        assert result_migrate.returncode == 0, f"Migrate command failed: {result_migrate.stderr}"