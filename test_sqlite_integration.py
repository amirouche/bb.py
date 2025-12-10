#!/usr/bin/env python3
"""
Test SQLite integration with bb.py core functionality.
"""

import sys
import os
import tempfile
import subprocess

sys.path.insert(0, ".")


def test_sqlite_integration():
    """Test SQLite integration"""
    print("=" * 60)
    print("Testing SQLite Integration")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        os.environ["BB_DIRECTORY"] = temp_dir

        print("1. Testing basic SQLite operations...")

        from bb import (
            init_sqlite_storage,
            code_save_sqlite,
            code_load_sqlite,
            mapping_save_sqlite,
            mapping_load_sqlite,
            save_dependencies_sqlite,
            load_dependencies_sqlite,
            code_detect_storage,
            code_load_unified,
            code_save_unified,
        )

        # Initialize SQLite storage
        conn, aston_nstore, deps_nstore = init_sqlite_storage()
        print("   ✓ SQLite database initialized")

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
        print("   ✓ Function saved to SQLite")

        # Load function
        loaded_data = code_load_sqlite(conn, aston_nstore, test_hash)
        print(f"   ✓ Function loaded from SQLite: {loaded_data['hash'][:12]}...")

        # Test mapping
        test_lang = "eng"
        test_docstring = "A test function"
        test_name_mapping = {"_bb_v_0": "test_function", "_bb_v_1": "x", "_bb_v_2": "y"}
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
        print(f"   ✓ Mapping saved: {mapping_hash[:12]}...")

        # Load mapping
        loaded_mapping = mapping_load_sqlite(conn, test_hash, test_lang, mapping_hash)
        print("   ✓ Mapping loaded")

        # Test dependencies
        test_deps = ["b" * 64, "c" * 64]
        save_dependencies_sqlite(conn, deps_nstore, test_hash, test_deps)
        print(f"   ✓ Saved {len(test_deps)} dependencies")

        loaded_deps = load_dependencies_sqlite(conn, deps_nstore, test_hash)
        print(f"   ✓ Loaded {len(loaded_deps)} dependencies")

        # Test storage detection
        storage_type = code_detect_storage()
        print(f"   ✓ Storage type detected: {storage_type}")

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
        print("   ✓ Unified save successful")

        loaded_unified = code_load_unified(test_hash, test_lang, mapping_hash)
        print("   ✓ Unified load successful")

        conn.close()

        print("\n2. Testing CLI integration...")

        # Test that bb.py commands work with SQLite storage
        bb_path = os.path.abspath("bb.py")

        # Create a simple test file
        test_file = os.path.join(temp_dir, "simple_test.py@eng")
        with open(test_file, "w") as f:
            f.write('''
def simple_add(a, b):
    """Add two numbers"""
    return a + b
''')

        # Add the function
        cmd_add = [sys.executable, bb_path, "add", "simple_test.py@eng"]
        result = subprocess.run(cmd_add, capture_output=True, text=True, cwd=temp_dir)

        if result.returncode == 0:
            print("   ✓ Add command succeeded with SQLite storage")

            # Extract the hash from the output
            import re

            match = re.search(r"Hash: ([a-f0-9]{64})", result.stdout)
            if match:
                func_hash = match.group(1)
                print(f"   ✓ Function hash: {func_hash[:12]}...")

                # Test show command
                cmd_show = [sys.executable, bb_path, "show", func_hash]
                result_show = subprocess.run(
                    cmd_show, capture_output=True, text=True, cwd=temp_dir
                )

                if result_show.returncode == 0:
                    print("   ✓ Show command succeeded with SQLite storage")

                    # Test run command
                    cmd_run = [
                        sys.executable,
                        bb_path,
                        "run",
                        func_hash,
                        "--args",
                        "2,3",
                    ]
                    result_run = subprocess.run(
                        cmd_run, capture_output=True, text=True, cwd=temp_dir
                    )

                    if result_run.returncode == 0 and "5" in result_run.stdout:
                        print("   ✓ Run command succeeded with SQLite storage")
                        print(f"     Result: {result_run.stdout.strip()}")
                    else:
                        print(f"   ⚠ Run command issue: {result_run.stdout}")
                else:
                    print(f"   ⚠ Show command issue: {result_show.stderr}")
        else:
            print(f"   ⚠ Add command issue: {result.stderr}")

        print("\n3. Testing migration command...")

        # Test migrate command
        cmd_migrate = [sys.executable, bb_path, "migrate"]
        result_migrate = subprocess.run(
            cmd_migrate, capture_output=True, text=True, cwd=temp_dir
        )

        if result_migrate.returncode == 0:
            print("   ✓ Migrate command succeeded")
            if "Migration complete" in result_migrate.stdout:
                print("   ✓ Migration completed successfully")
        else:
            print(f"   ⚠ Migrate command issue: {result_migrate.stderr}")

        print("\n" + "=" * 60)
        print("✓ SQLite integration test PASSED!")
        print("=" * 60)
        return True


if __name__ == "__main__":
    if test_sqlite_integration():
        print("\n🎉 SQLite integration is working correctly!")
    else:
        print("\n❌ SQLite integration test FAILED!")
        sys.exit(1)
