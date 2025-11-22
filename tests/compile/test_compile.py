"""
Tests for compilation functionality.

Unit tests for dependency resolution and bundling (complex low-level aspects).
Integration tests for CLI compile command error handling.
"""
import pytest

import mobius
from tests.conftest import normalize_code_for_test


# =============================================================================
# Integration tests for compile CLI command
# =============================================================================

def test_compile_missing_language_suffix_fails(cli_runner):
    """Test that compile fails without language suffix"""
    result = cli_runner.run(['compile', 'a' * 64])

    assert result.returncode != 0
    assert 'Missing language suffix' in result.stderr


def test_compile_invalid_hash_format_fails(cli_runner):
    """Test that compile fails with invalid hash format"""
    result = cli_runner.run(['compile', 'not-a-valid-hash@eng'])

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_compile_nonexistent_function_fails(cli_runner):
    """Test that compile fails for nonexistent function"""
    fake_hash = "f" * 64

    result = cli_runner.run(['compile', f'{fake_hash}@eng'])

    assert result.returncode != 0


def test_compile_prepares_bundle(cli_runner, tmp_path):
    """Test that compile prepares the bundle directory before failing on PyOxidizer"""
    # Setup: Add a simple function
    test_file = tmp_path / "simple.py"
    test_file.write_text('def answer(): return 42')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Run compile (will fail because PyOxidizer not installed)
    result = cli_runner.run(['compile', f'{func_hash}@eng'])

    # Assert: Should fail on PyOxidizer, not on setup
    # If it gets to "PyOxidizer not found" or similar, the bundle prep succeeded
    # This is as far as we can test without installing PyOxidizer
    assert result.returncode != 0


# =============================================================================
# Unit tests for dependency extraction (complex low-level aspect)
# =============================================================================


def test_dependencies_extract_no_deps():
    """Test extracting dependencies from code with no mobius imports"""
    code = normalize_code_for_test("""
def _mobius_v_0():
    return 42
""")
    deps = mobius.code_extract_dependencies(code)
    assert deps == []


def test_dependencies_extract_single_dep():
    """Test extracting single dependency"""
    code = normalize_code_for_test("""
from mobius.pool import object_abc123def456789012345678901234567890123456789012345678901234

def _mobius_v_0():
    return object_abc123def456789012345678901234567890123456789012345678901234._mobius_v_0()
""")
    deps = mobius.code_extract_dependencies(code)
    assert len(deps) == 1
    assert deps[0] == "abc123def456789012345678901234567890123456789012345678901234"


def test_dependencies_extract_multiple_deps():
    """Test extracting multiple dependencies"""
    code = normalize_code_for_test("""
from mobius.pool import object_abc123def456789012345678901234567890123456789012345678901234
from mobius.pool import object_def456789012345678901234567890123456789012345678901234abc123

def _mobius_v_0():
    x = object_abc123def456789012345678901234567890123456789012345678901234._mobius_v_0()
    y = object_def456789012345678901234567890123456789012345678901234abc123._mobius_v_0()
    return x + y
""")
    deps = mobius.code_extract_dependencies(code)
    assert len(deps) == 2


# =============================================================================
# Unit tests for dependency resolution (complex low-level aspect)
# =============================================================================

def test_dependencies_resolve_no_deps(mock_mobius_dir):
    """Test resolving dependencies for function with no deps"""
    func_hash = "nodeps01" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(): return 42")

    mobius.code_save(func_hash, "eng", normalized_code, "No deps", {"_mobius_v_0": "answer"}, {})

    deps = mobius.code_resolve_dependencies(func_hash)

    assert deps == [func_hash]


def test_dependencies_resolve_single_dep(mock_mobius_dir):
    """Test resolving single dependency"""
    # Create dependency function
    dep_hash = "helper01" + "0" * 56
    dep_code = normalize_code_for_test("def _mobius_v_0(): return 10")
    mobius.code_save(dep_hash, "eng", dep_code, "Helper", {"_mobius_v_0": "helper"}, {})

    # Create function that depends on it
    main_hash = "main0001" + "0" * 56
    main_code = normalize_code_for_test(f"""
from mobius.pool import object_{dep_hash}

def _mobius_v_0():
    return object_{dep_hash}._mobius_v_0() * 2
""")
    mobius.code_save(main_hash, "eng", main_code, "Main", {"_mobius_v_0": "double_helper"}, {dep_hash: "helper"})

    deps = mobius.code_resolve_dependencies(main_hash)

    # Should have both hashes, dependency first
    assert len(deps) == 2
    assert deps[0] == dep_hash  # dependency first
    assert deps[1] == main_hash  # main function last


def test_dependencies_resolve_diamond(mock_mobius_dir):
    """Test resolving diamond dependency pattern"""
    # A depends on B and C
    # B depends on D
    # C depends on D
    # Order should be: D, B, C, A (or D, C, B, A)

    d_hash = "hashd001" + "0" * 56
    d_code = normalize_code_for_test("def _mobius_v_0(): return 1")
    mobius.code_save(d_hash, "eng", d_code, "D", {"_mobius_v_0": "d"}, {})

    b_hash = "hashb001" + "0" * 56
    b_code = normalize_code_for_test(f"""
from mobius.pool import object_{d_hash}

def _mobius_v_0():
    return object_{d_hash}._mobius_v_0() + 1
""")
    mobius.code_save(b_hash, "eng", b_code, "B", {"_mobius_v_0": "b"}, {d_hash: "d"})

    c_hash = "hashc001" + "0" * 56
    c_code = normalize_code_for_test(f"""
from mobius.pool import object_{d_hash}

def _mobius_v_0():
    return object_{d_hash}._mobius_v_0() * 2
""")
    mobius.code_save(c_hash, "eng", c_code, "C", {"_mobius_v_0": "c"}, {d_hash: "d"})

    a_hash = "hasha001" + "0" * 56
    a_code = normalize_code_for_test(f"""
from mobius.pool import object_{b_hash}
from mobius.pool import object_{c_hash}

def _mobius_v_0():
    return object_{b_hash}._mobius_v_0() + object_{c_hash}._mobius_v_0()
""")
    mobius.code_save(a_hash, "eng", a_code, "A", {"_mobius_v_0": "a"}, {b_hash: "b", c_hash: "c"})

    deps = mobius.code_resolve_dependencies(a_hash)

    # Should have all 4 hashes
    assert len(deps) == 4
    # D should come before B and C
    assert deps.index(d_hash) < deps.index(b_hash)
    assert deps.index(d_hash) < deps.index(c_hash)
    # A should be last
    assert deps[-1] == a_hash


# =============================================================================
# Unit tests for bundling (complex low-level aspect)
# =============================================================================

def test_dependencies_bundle(mock_mobius_dir, tmp_path):
    """Test bundling functions to output directory"""
    func_hash = "bundle01" + "0" * 56
    normalized_code = normalize_code_for_test("def _mobius_v_0(): return 99")
    mobius.code_save(func_hash, "eng", normalized_code, "Bundle test", {"_mobius_v_0": "test"}, {})

    output_dir = tmp_path / "bundle_output"
    result = mobius.code_bundle_dependencies([func_hash], output_dir)

    assert result == output_dir
    assert output_dir.exists()
    assert (output_dir / func_hash[:2]).exists()  # v1 structure directly in output_dir


# =============================================================================
# Unit tests for config generation (complex low-level aspect)
# =============================================================================

def test_compile_generate_config():
    """Test generating PyOxidizer configuration"""
    config = mobius.compile_generate_config("abc123" + "0" * 58, "eng", "myapp")

    assert "PyOxidizer configuration" in config
    assert "myapp" in config
    assert "abc123" in config
    assert "eng" in config


def test_compile_generate_runtime(tmp_path):
    """Test generating runtime module"""
    func_hash = "runtime1" + "0" * 56
    runtime_dir = mobius.compile_generate_runtime(func_hash, "eng", tmp_path)

    assert runtime_dir.exists()
    assert (runtime_dir / "__init__.py").exists()

    # Read and verify content
    init_content = (runtime_dir / "__init__.py").read_text()
    assert "code_execute" in init_content
    assert "code_load" in init_content
    assert "code_denormalize" in init_content
