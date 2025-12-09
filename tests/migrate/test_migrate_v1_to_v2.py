"""
Test real migration from file-based to SQLite storage.
"""

import os
import tempfile
import pytest
import ast

from bb import (
    init_sqlite_storage, code_save_v1, mapping_save_v1,
    command_migrate, code_load_unified, code_detect_storage
)


def test_real_migration():
    """Test real migration process from file-based to SQLite storage"""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ['BB_DIRECTORY'] = temp_dir
        
        # Create some test functions in file-based storage
        pool_dir = os.path.join(temp_dir, 'pool')
        os.makedirs(pool_dir, exist_ok=True)
        
        # Function 1
        func1_hash = "a" * 64
        func1_code = "def add(x, y): return x + y"
        func1_metadata = {"created": "2023-01-01T00:00:00Z"}
        code_save_v1(func1_hash, func1_code, func1_metadata)
        mapping_save_v1(func1_hash, "eng", "Add two numbers", 
                       {"_bb_v_0": "add", "_bb_v_1": "x", "_bb_v_2": "y"}, {}, comment="test mapping 1")
        
        # Function 2
        func2_hash = "b" * 64
        func2_code = "def subtract(a, b): return a - b"
        func2_metadata = {"created": "2023-01-02T00:00:00Z"}
        code_save_v1(func2_hash, func2_code, func2_metadata)
        mapping_save_v1(func2_hash, "eng", "Subtract two numbers",
                       {"_bb_v_0": "subtract", "_bb_v_1": "a", "_bb_v_2": "b"}, {}, comment="test mapping 2")
        
        # Verify file-based storage works
        storage_before = code_detect_storage()
        assert storage_before == "file", f"Expected file-based storage, got {storage_before}"
        
        # Load functions from file-based storage
        code1, names1, aliases1, doc1 = code_load_unified(func1_hash, "eng")
        code2, names2, aliases2, doc2 = code_load_unified(func2_hash, "eng")
        
        # Verify loaded data
        assert code1.strip() == func1_code.strip(), "Function 1 code mismatch"
        assert code2.strip() == func2_code.strip(), "Function 2 code mismatch"
        assert doc1 == "Add two numbers", "Function 1 docstring mismatch"
        assert doc2 == "Subtract two numbers", "Function 2 docstring mismatch"
        
        # Perform migration
        command_migrate()
        
        # Verify SQLite storage works
        storage_after = code_detect_storage()
        assert storage_after == "sqlite", f"Expected SQLite storage after migration, got {storage_after}"
        
        # Load functions from SQLite storage
        code1_sqlite, names1_sqlite, aliases1_sqlite, doc1_sqlite = code_load_unified(func1_hash, "eng")
        code2_sqlite, names2_sqlite, aliases2_sqlite, doc2_sqlite = code_load_unified(func2_hash, "eng")
        
        # Verify data integrity by comparing AST structure
        tree1_file = ast.parse(code1)
        tree1_sqlite = ast.parse(code1_sqlite)
        tree2_file = ast.parse(code2)
        tree2_sqlite = ast.parse(code2_sqlite)
        
        # AST structure should be identical
        assert ast.dump(tree1_file) == ast.dump(tree1_sqlite), "Function 1 AST structure mismatch after migration"
        assert ast.dump(tree2_file) == ast.dump(tree2_sqlite), "Function 2 AST structure mismatch after migration"
        
        # Docstrings should be identical
        assert doc1 == doc1_sqlite, f"Function 1 docstring mismatch after migration: {doc1} != {doc1_sqlite}"
        assert doc2 == doc2_sqlite, f"Function 2 docstring mismatch after migration: {doc2} != {doc2_sqlite}"
        
        # Names and aliases should be preserved
        assert names1 == names1_sqlite, "Function 1 names mismatch after migration"
        assert names2 == names2_sqlite, "Function 2 names mismatch after migration"
        assert aliases1 == aliases1_sqlite, "Function 1 aliases mismatch after migration"
        assert aliases2 == aliases2_sqlite, "Function 2 aliases mismatch after migration"