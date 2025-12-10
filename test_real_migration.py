#!/usr/bin/env python3
"""
Test real migration from file-based to SQLite storage.
"""

import sys
import os
import tempfile

sys.path.insert(0, ".")

from bb import (
    code_save_v1,
    mapping_save_v1,
    command_migrate,
    code_load_unified,
    code_detect_storage,
)


def test_real_migration():
    """Test real migration process"""
    print("=" * 60)
    print("Testing Real Migration Process")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["BB_DIRECTORY"] = temp_dir

        # Create some test functions in file-based storage
        pool_dir = os.path.join(temp_dir, "pool")
        os.makedirs(pool_dir, exist_ok=True)

        print("1. Creating test functions in file-based storage...")

        # Function 1
        func1_hash = "a" * 64
        func1_code = "def add(x, y): return x + y"
        func1_metadata = {"created": "2023-01-01T00:00:00Z"}
        code_save_v1(func1_hash, func1_code, func1_metadata)
        mapping_save_v1(
            func1_hash,
            "eng",
            "Add two numbers",
            {"_bb_v_0": "add", "_bb_v_1": "x", "_bb_v_2": "y"},
            {},
            "test mapping 1",
        )

        # Function 2
        func2_hash = "b" * 64
        func2_code = "def subtract(a, b): return a - b"
        func2_metadata = {"created": "2023-01-02T00:00:00Z"}
        code_save_v1(func2_hash, func2_code, func2_metadata)
        mapping_save_v1(
            func2_hash,
            "eng",
            "Subtract two numbers",
            {"_bb_v_0": "subtract", "_bb_v_1": "a", "_bb_v_2": "b"},
            {},
            "test mapping 2",
        )

        print("   ✓ Created 2 functions in file-based storage")

        # Verify file-based storage works
        print("\n2. Verifying file-based storage...")
        storage_before = code_detect_storage()
        print(f"   ✓ Storage type: {storage_before}")

        # Load functions from file-based storage
        try:
            code1, names1, aliases1, doc1 = code_load_unified(func1_hash, "eng")
            code2, names2, aliases2, doc2 = code_load_unified(func2_hash, "eng")
            print("   ✓ Successfully loaded functions from file-based storage")
        except Exception as e:
            print(f"   ✗ Error loading from file-based storage: {e}")
            return False

        # Perform migration
        print("\n3. Performing migration to SQLite...")
        try:
            command_migrate()
            print("   ✓ Migration completed")
        except Exception as e:
            print(f"   ✗ Migration failed: {e}")
            import traceback

            traceback.print_exc()
            return False

        # Verify SQLite storage works
        print("\n4. Verifying SQLite storage...")
        storage_after = code_detect_storage()
        print(f"   ✓ Storage type: {storage_after}")

        # Load functions from SQLite storage
        try:
            code1_sqlite, names1_sqlite, aliases1_sqlite, doc1_sqlite = (
                code_load_unified(func1_hash, "eng")
            )
            code2_sqlite, names2_sqlite, aliases2_sqlite, doc2_sqlite = (
                code_load_unified(func2_hash, "eng")
            )
            print("   ✓ Successfully loaded functions from SQLite storage")

            # Verify data integrity (allow for AST normalization differences)
            import ast

            try:
                # Parse both versions and compare AST structure
                tree1_file = ast.parse(code1)
                tree1_sqlite = ast.parse(code1_sqlite)
                tree2_file = ast.parse(code2)
                tree2_sqlite = ast.parse(code2_sqlite)

                if (
                    ast.dump(tree1_file) == ast.dump(tree1_sqlite)
                    and ast.dump(tree2_file) == ast.dump(tree2_sqlite)
                    and doc1 == doc1_sqlite
                    and doc2 == doc2_sqlite
                ):
                    print("   ✓ Data integrity verified (AST structure matches)")
                else:
                    print("   ✗ Data mismatch after migration")
                    print(f"     File code1 AST: {ast.dump(tree1_file)[:100]}...")
                    print(f"     SQLite code1 AST: {ast.dump(tree1_sqlite)[:100]}...")
                    return False
            except Exception as e:
                print(f"   ✗ Error comparing AST: {e}")
                return False

        except Exception as e:
            print(f"   ✗ Error loading from SQLite storage: {e}")
            import traceback

            traceback.print_exc()
            return False

        print("\n" + "=" * 60)
        print("✓ Real migration test PASSED!")
        print("=" * 60)
        return True


if __name__ == "__main__":
    if test_real_migration():
        print("\n🎉 Migration functionality is working correctly!")
    else:
        print("\n❌ Migration test FAILED!")
        sys.exit(1)
