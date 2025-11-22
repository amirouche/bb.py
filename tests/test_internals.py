"""
Internal tests for mobius.py

Tests for core functionality that doesn't map directly to CLI commands:
- AST normalization and transformation
- Name mapping and unmapping
- Import handling
- Hash computation
- Schema detection and validation
"""
import ast
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
import mobius

from tests.conftest import normalize_code_for_test


# ============================================================================
# Tests for ASTNormalizer class
# ============================================================================

def test_ast_normalizer_visit_name_with_mapping():
    """Test that Name nodes are renamed according to mapping"""
    mapping = {"x": "_mobius_v_1", "y": "_mobius_v_2"}
    normalizer = mobius.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_mobius_v_1" in result
    assert "_mobius_v_2" in result


def test_ast_normalizer_visit_name_without_mapping():
    """Test that unmapped names remain unchanged"""
    mapping = {"x": "_mobius_v_1"}
    normalizer = mobius.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_mobius_v_1" in result
    assert "y" in result  # y should remain unchanged


def test_ast_normalizer_visit_arg_with_mapping():
    """Test that function arguments are renamed"""
    mapping = {"x": "_mobius_v_1", "y": "_mobius_v_2"}
    normalizer = mobius.ASTNormalizer(mapping)

    code = "def foo(x, y): return x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_mobius_v_1" in result
    assert "_mobius_v_2" in result


def test_ast_normalizer_visit_functiondef_with_mapping():
    """Test that function names are renamed"""
    mapping = {"foo": "_mobius_v_0"}
    normalizer = mobius.ASTNormalizer(mapping)

    code = "def foo(): pass"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_mobius_v_0" in result
    assert "foo" not in result


# ============================================================================
# Tests for names_collect function
# ============================================================================

def test_collect_names_simple_names():
    """Test collecting variable names"""
    code = "x = 1\ny = 2\nz = x + y"
    tree = ast.parse(code)
    names = mobius.code_collect_names(tree)

    assert "x" in names
    assert "y" in names
    assert "z" in names


def test_collect_names_function_names():
    """Test collecting function names and arguments"""
    code = "def foo(a, b): return a + b"
    tree = ast.parse(code)
    names = mobius.code_collect_names(tree)

    assert "foo" in names
    assert "a" in names
    assert "b" in names


def test_collect_names_empty_tree():
    """Test collecting names from empty module"""
    tree = ast.parse("")
    names = mobius.code_collect_names(tree)

    assert len(names) == 0


# ============================================================================
# Tests for imports_get_names function
# ============================================================================

def test_get_imported_names_import_statement():
    """Test extracting names from import statement"""
    code = "import math"
    tree = ast.parse(code)
    names = mobius.code_get_import_names(tree)

    assert "math" in names


def test_get_imported_names_import_with_alias():
    """Test extracting aliased import names"""
    code = "import numpy as np"
    tree = ast.parse(code)
    names = mobius.code_get_import_names(tree)

    assert "np" in names
    assert "numpy" not in names


def test_get_imported_names_from_import():
    """Test extracting names from from-import"""
    code = "from collections import Counter"
    tree = ast.parse(code)
    names = mobius.code_get_import_names(tree)

    assert "Counter" in names


def test_get_imported_names_from_import_with_alias():
    """Test extracting aliased from-import names"""
    code = "from collections import Counter as C"
    tree = ast.parse(code)
    names = mobius.code_get_import_names(tree)

    assert "C" in names
    assert "Counter" not in names


def test_get_imported_names_multiple_imports():
    """Test extracting names from multiple imports"""
    code = """
import math
from collections import Counter
import numpy as np
"""
    tree = ast.parse(code)
    names = mobius.code_get_import_names(tree)

    assert "math" in names
    assert "Counter" in names
    assert "np" in names


# ============================================================================
# Tests for imports_check_unused function
# ============================================================================

def test_check_unused_imports_all_imports_used():
    """Test when all imports are used"""
    code = """
import math
def foo():
    return math.sqrt(4)
"""
    tree = ast.parse(code)
    imported = mobius.code_get_import_names(tree)
    all_names = mobius.code_collect_names(tree)

    result = mobius.code_check_unused_imports(tree, imported, all_names)
    assert result is True


def test_check_unused_imports_unused_import():
    """Test when an import is unused"""
    code = """
import math
def foo():
    return 4
"""
    tree = ast.parse(code)
    imported = mobius.code_get_import_names(tree)
    all_names = mobius.code_collect_names(tree)

    result = mobius.code_check_unused_imports(tree, imported, all_names)
    assert result is False


# ============================================================================
# Tests for imports_sort function
# ============================================================================

def test_sort_imports_simple_imports():
    """Test sorting import statements"""
    code = """
import sys
import ast
import os
"""
    tree = ast.parse(code)
    sorted_tree = mobius.code_sort_imports(tree)
    result = ast.unparse(sorted_tree)

    # ast should come before os, os before sys
    assert result.index("ast") < result.index("os")
    assert result.index("os") < result.index("sys")


def test_sort_imports_from_imports():
    """Test sorting from-import statements"""
    code = """
from os import path
from collections import Counter
from ast import parse
"""
    tree = ast.parse(code)
    sorted_tree = mobius.code_sort_imports(tree)
    result = ast.unparse(sorted_tree)

    # Should be sorted by module name
    assert result.index("from ast") < result.index("from collections")
    assert result.index("from collections") < result.index("from os")


def test_sort_imports_imports_before_code():
    """Test that imports remain before code"""
    code = """
import sys
def foo():
    pass
import os
"""
    tree = ast.parse(code)
    sorted_tree = mobius.code_sort_imports(tree)
    result = ast.unparse(sorted_tree)

    # All imports should come before the function
    func_pos = result.index("def foo")
    assert result.index("import") < func_pos


# ============================================================================
# Tests for function_extract_definition function
# ============================================================================

def test_extract_function_def_simple_function():
    """Test extracting a simple function"""
    code = """
def foo():
    return 42
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    assert func_def is not None
    assert func_def.name == "foo"
    assert len(imports) == 0


def test_extract_function_def_function_with_imports():
    """Test extracting function with imports"""
    code = """
import math
from collections import Counter

def process():
    return 42
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    assert func_def is not None
    assert func_def.name == "process"
    assert len(imports) == 2


def test_extract_function_def_no_function_raises_error():
    """Test that missing function raises ValueError"""
    code = "x = 42"
    tree = ast.parse(code)

    with pytest.raises(ValueError, match="No function definition found"):
        mobius.code_extract_definition(tree)


def test_extract_function_def_multiple_functions_raises_error():
    """Test that multiple functions raise ValueError"""
    code = """
def foo():
    pass

def bar():
    pass
"""
    tree = ast.parse(code)

    with pytest.raises(ValueError, match="Only one function definition is allowed"):
        mobius.code_extract_definition(tree)


# ============================================================================
# Tests for mapping_create_name function
# ============================================================================

def test_create_name_mapping_function_name_always_v0():
    """Test that function name always maps to _mobius_v_0"""
    code = "def my_function(x): return x"
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    forward, reverse = mobius.code_create_name_mapping(func_def, imports)

    assert forward["my_function"] == "_mobius_v_0"
    assert reverse["_mobius_v_0"] == "my_function"


def test_create_name_mapping_sequential_numbering():
    """Test that variables get sequential numbers"""
    code = """
def foo(a, b):
    c = a + b
    return c
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    forward, reverse = mobius.code_create_name_mapping(func_def, imports)

    # foo should be v_0
    assert forward["foo"] == "_mobius_v_0"
    # Other names should be numbered sequentially
    assert all(name.startswith("_mobius_v_") for name in forward.values())


def test_create_name_mapping_builtins_not_renamed():
    """Test that built-in functions are not renamed"""
    code = """
def foo(items):
    return len(items)
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    forward, reverse = mobius.code_create_name_mapping(func_def, imports)

    # len is a built-in and should not be in the mapping
    assert "len" not in forward
    assert "items" in forward


def test_create_name_mapping_imported_names_not_renamed():
    """Test that imported names are not renamed"""
    code = """
import math

def foo(x):
    return math.sqrt(x)
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    forward, reverse = mobius.code_create_name_mapping(func_def, imports)

    # math is imported and should not be renamed
    assert "math" not in forward
    assert "x" in forward


def test_create_name_mapping_mobius_aliases_not_renamed():
    """Test that mobius aliases are excluded from renaming"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    # Simulate that 'helper' is an mobius alias
    mobius_aliases = {"helper"}
    forward, reverse = mobius.code_create_name_mapping(func_def, imports, mobius_aliases)

    # helper should not be renamed
    assert "helper" not in forward
    assert "x" in forward


# ============================================================================
# Tests for imports_rewrite_mobius function
# ============================================================================

def test_rewrite_mobius_imports_with_alias():
    """Test rewriting mobius import with alias"""
    code = "from mobius.pool import abc123 as helper"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = mobius.code_rewrite_mobius_imports(imports)

    # Should remove alias but keep mobius.pool module name
    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from mobius.pool import abc123" in result
    assert "as helper" not in result

    # Should track the alias
    assert alias_mapping["abc123"] == "helper"


def test_rewrite_mobius_imports_without_alias():
    """Test rewriting mobius import without alias"""
    code = "from mobius.pool import abc123"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = mobius.code_rewrite_mobius_imports(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from mobius.pool import abc123" in result
    assert len(alias_mapping) == 0


def test_rewrite_mobius_imports_non_mobius_imports_unchanged():
    """Test that non-mobius imports remain unchanged"""
    code = "import math\nfrom collections import Counter"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = mobius.code_rewrite_mobius_imports(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "import math" in result
    assert "from collections import Counter" in result
    assert len(alias_mapping) == 0


# ============================================================================
# Tests for calls_replace_mobius function
# ============================================================================

def test_replace_mobius_calls_aliased_call():
    """Test replacing aliased mobius function calls"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = mobius.code_replace_mobius_calls(tree, alias_mapping, name_mapping)
    result = ast.unparse(new_tree)

    # helper(x) should become abc123._mobius_v_0(x)
    assert "abc123._mobius_v_0" in result
    assert "helper" not in result


def test_replace_mobius_calls_non_aliased_names_unchanged():
    """Test that non-aliased names remain unchanged"""
    code = """
def foo(x):
    return other(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = mobius.code_replace_mobius_calls(tree, alias_mapping, name_mapping)
    result = ast.unparse(new_tree)

    # other should remain unchanged
    assert "other" in result


# ============================================================================
# Tests for ast_clear_locations function
# ============================================================================

def test_clear_locations_all_location_info():
    """Test that all location info is cleared"""
    code = "def foo(x): return x + 1"
    tree = ast.parse(code)

    # Verify locations exist initially
    for node in ast.walk(tree):
        if hasattr(node, 'lineno'):
            assert node.lineno is not None
            break

    mobius.code_clear_locations(tree)

    # Verify all locations are None
    for node in ast.walk(tree):
        if hasattr(node, 'lineno'):
            assert node.lineno is None
        if hasattr(node, 'col_offset'):
            assert node.col_offset is None


# ============================================================================
# Tests for docstring_extract function
# ============================================================================

def test_extract_docstring_existing_docstring():
    """Test extracting an existing docstring"""
    code = '''
def foo():
    """This is a docstring"""
    return 42
'''
    tree = ast.parse(code)
    func_def, _ = mobius.code_extract_definition(tree)

    docstring, func_without_doc = mobius.code_extract_docstring(func_def)

    assert docstring == "This is a docstring"
    assert len(func_without_doc.body) == 1  # Only return statement
    assert isinstance(func_without_doc.body[0], ast.Return)


def test_extract_docstring_function_without_docstring():
    """Test function without docstring returns empty string"""
    code = """
def foo():
    return 42
"""
    tree = ast.parse(code)
    func_def, _ = mobius.code_extract_definition(tree)

    docstring, func_without_doc = mobius.code_extract_docstring(func_def)

    assert docstring == ""
    assert len(func_without_doc.body) == 1


def test_extract_docstring_multiline_docstring():
    """Test extracting multiline docstring"""
    code = '''
def foo():
    """
    This is a multiline
    docstring
    """
    return 42
'''
    tree = ast.parse(code)
    func_def, _ = mobius.code_extract_definition(tree)

    docstring, func_without_doc = mobius.code_extract_docstring(func_def)

    assert "multiline" in docstring
    assert "docstring" in docstring


# ============================================================================
# Tests for ast_normalize function
# ============================================================================

def test_normalize_ast_simple_function():
    """Test normalizing a simple function"""
    code = """
def calculate_sum(first, second):
    \"\"\"Add two numbers\"\"\"
    result = first + second
    return result
"""
    tree = ast.parse(code)

    code_with_doc, code_without_doc, docstring, name_mapping, alias_mapping = \
        mobius.code_normalize(tree, "eng")

    assert "_mobius_v_0" in code_with_doc  # Function name normalized
    assert docstring == "Add two numbers"
    assert "_mobius_v_0" in name_mapping.keys()
    assert code_without_doc != code_with_doc  # Should differ by docstring


def test_normalize_ast_imports_sorted():
    """Test that imports are sorted during normalization"""
    code = """
import sys
import ast
import os

def foo():
    return 42
"""
    tree = ast.parse(code)

    code_with_doc, _, _, _, _ = mobius.code_normalize(tree, "eng")

    # Verify imports are sorted
    assert code_with_doc.index("import ast") < code_with_doc.index("import os")
    assert code_with_doc.index("import os") < code_with_doc.index("import sys")


def test_normalize_ast_with_mobius_import():
    """Test normalizing function with mobius import"""
    code = """
from mobius.pool import abc123 as helper

def foo(x):
    \"\"\"Process with helper\"\"\"
    return helper(x)
"""
    tree = ast.parse(code)

    code_with_doc, code_without_doc, docstring, name_mapping, alias_mapping = \
        mobius.code_normalize(tree, "eng")

    # Should remove alias but keep mobius.pool module name
    assert "from mobius.pool import abc123" in code_with_doc
    assert "as helper" not in code_with_doc

    # Should track alias
    assert alias_mapping["abc123"] == "helper"

    # Should replace calls
    assert "abc123._mobius_v_0" in code_with_doc


# ============================================================================
# Tests for hash_compute function
# ============================================================================

def test_compute_hash_deterministic():
    """Test that same input produces same hash"""
    code = "def foo(): return 42"

    hash1 = mobius.hash_compute(code)
    hash2 = mobius.hash_compute(code)

    assert hash1 == hash2


def test_compute_hash_format():
    """Test that hash is 64 hex characters"""
    code = "def foo(): return 42"

    hash_value = mobius.hash_compute(code)

    assert len(hash_value) == 64
    assert all(c in '0123456789abcdef' for c in hash_value)


def test_compute_hash_different_code_different_hash():
    """Test that different code produces different hash"""
    code1 = "def foo(): return 42"
    code2 = "def bar(): return 43"

    hash1 = mobius.hash_compute(code1)
    hash2 = mobius.hash_compute(code2)

    assert hash1 != hash2


def test_hash_compute_with_algorithm_parameter():
    """Test that hash_compute works with algorithm parameter"""
    code = "def foo(): pass"

    # Default should be sha256
    hash_default = mobius.hash_compute(code)
    hash_sha256 = mobius.hash_compute(code, algorithm='sha256')

    assert hash_default == hash_sha256
    assert len(hash_default) == 64


def test_hash_compute_algorithm_deterministic():
    """Test that hash_compute with algorithm produces deterministic results"""
    code = "def foo(): pass"

    hash1 = mobius.hash_compute(code, algorithm='sha256')
    hash2 = mobius.hash_compute(code, algorithm='sha256')

    assert hash1 == hash2


# ============================================================================
# Tests for docstring_replace function
# ============================================================================

def test_replace_docstring_existing_docstring():
    """Test replacing an existing docstring"""
    code = '''
def foo():
    """Old docstring"""
    return 42
'''
    new_doc = "New docstring"

    result = mobius.code_replace_docstring(code, new_doc)

    assert "New docstring" in result
    assert "Old docstring" not in result


def test_replace_docstring_add_when_none_exists():
    """Test adding docstring to function without one"""
    code = """
def foo():
    return 42
"""
    new_doc = "Added docstring"

    result = mobius.code_replace_docstring(code, new_doc)

    assert "Added docstring" in result


def test_replace_docstring_remove_with_empty_string():
    """Test removing docstring by passing empty string"""
    code = '''
def foo():
    """Remove this"""
    return 42
'''
    result = mobius.code_replace_docstring(code, "")

    assert "Remove this" not in result
    tree = ast.parse(result)
    func_def = tree.body[0]

    # Should only have return statement
    assert len(func_def.body) == 1
    assert isinstance(func_def.body[0], ast.Return)


# ============================================================================
# Tests for code_denormalize function
# ============================================================================

def test_denormalize_code_variable_names():
    """Test denormalizing variable names"""
    normalized = """
def _mobius_v_0(_mobius_v_1, _mobius_v_2):
    _mobius_v_3 = _mobius_v_1 + _mobius_v_2
    return _mobius_v_3
"""
    name_mapping = {
        "_mobius_v_0": "calculate",
        "_mobius_v_1": "first",
        "_mobius_v_2": "second",
        "_mobius_v_3": "result"
    }
    alias_mapping = {}

    result = mobius.code_denormalize(normalized, name_mapping, alias_mapping)

    assert "calculate" in result
    assert "first" in result
    assert "second" in result
    assert "result" in result
    assert "_mobius_v_" not in result


def test_denormalize_code_mobius_imports():
    """Test denormalizing mobius imports"""
    normalized = """
from mobius.pool import abc123

def _mobius_v_0(_mobius_v_1):
    return abc123._mobius_v_0(_mobius_v_1)
"""
    name_mapping = {
        "_mobius_v_0": "process",
        "_mobius_v_1": "data"
    }
    alias_mapping = {
        "abc123": "helper"
    }

    result = mobius.code_denormalize(normalized, name_mapping, alias_mapping)

    # Should restore import with alias
    assert "from mobius.pool import abc123 as helper" in result

    # Should restore function calls
    assert "helper(data)" in result
    assert "abc123._mobius_v_0" not in result


# ============================================================================
# Tests for Schema v1 - Foundation
# ============================================================================

def test_mapping_compute_hash_deterministic():
    """Test that mapping_compute_hash produces deterministic hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_mobius_v_0": "calculate_average", "_mobius_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}
    comment = "Formal terminology"

    # Compute hash twice - should be identical
    hash1 = mobius.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment)
    hash2 = mobius.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment)

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex characters
    assert all(c in '0123456789abcdef' for c in hash1)


def test_mapping_compute_hash_different_comments():
    """Test that different comments produce different hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_mobius_v_0": "calculate_average", "_mobius_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}

    hash1 = mobius.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "Formal")
    hash2 = mobius.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "Informal")

    assert hash1 != hash2


def test_mapping_compute_hash_empty_comment():
    """Test that mapping hash works with empty comment"""
    docstring = "Calculate the average"
    name_mapping = {"_mobius_v_0": "calculate_average"}
    alias_mapping = {}

    hash_val = mobius.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "")

    assert len(hash_val) == 64
    assert all(c in '0123456789abcdef' for c in hash_val)


def test_mapping_compute_hash_canonical_json():
    """Test that mapping hash is based on canonical JSON (order-independent)"""
    docstring = "Test"
    # Different key orders should produce same hash
    name_mapping1 = {"_mobius_v_0": "foo", "_mobius_v_1": "bar"}
    name_mapping2 = {"_mobius_v_1": "bar", "_mobius_v_0": "foo"}
    alias_mapping = {}

    hash1 = mobius.code_compute_mapping_hash(docstring, name_mapping1, alias_mapping, "")
    hash2 = mobius.code_compute_mapping_hash(docstring, name_mapping2, alias_mapping, "")

    assert hash1 == hash2


def test_schema_detect_version_v1(mock_mobius_dir):
    """Test that schema_detect_version correctly identifies v1 format"""
    pool_dir = mock_mobius_dir / '.mobius' / 'pool'
    test_hash = "abcd1234" + "0" * 56

    # Create v1 format: pool/XX/YYYYYY.../object.json
    func_dir = pool_dir / test_hash[:2] / test_hash[2:]
    func_dir.mkdir(parents=True, exist_ok=True)
    object_json = func_dir / 'object.json'

    v1_data = {
        'schema_version': 1,
        'hash': test_hash,
        'normalized_code': 'def _mobius_v_0(): pass',
        'metadata': {}
    }

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(v1_data, f)

    version = mobius.code_detect_schema(test_hash)
    assert version == 1


def test_schema_detect_version_not_found(mock_mobius_dir):
    """Test that schema_detect_version returns None for non-existent function"""
    test_hash = "nonexistent" + "0" * 54

    version = mobius.code_detect_schema(test_hash)
    assert version is None


def test_metadata_create_basic():
    """Test that metadata_create generates proper metadata structure"""
    metadata = mobius.code_create_metadata()

    assert 'created' in metadata
    assert 'author' in metadata


def test_metadata_create_with_author():
    """Test that metadata_create uses author from environment"""
    with patch.dict(os.environ, {'USER': 'testuser'}):
        metadata = mobius.code_create_metadata()
        assert metadata['author'] == 'testuser'


def test_metadata_create_timestamp_format():
    """Test that metadata_create uses ISO 8601 timestamp format"""
    metadata = mobius.code_create_metadata()
    created = metadata['created']

    # Should be ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
    assert 'T' in created
    assert len(created) >= 19  # At minimum: 2025-01-01T00:00:00
