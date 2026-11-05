"""
Internal tests for bb.py

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
import re
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))
import bb

from tests.conftest import normalize_code_for_test


# ============================================================================
# Tests for ASTNormalizer class
# ============================================================================

def test_ast_normalizer_visit_name_with_mapping():
    """Test that Name nodes are renamed according to mapping"""
    mapping = {"x": "_bb_v_1", "y": "_bb_v_2"}
    normalizer = bb.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_bb_v_1" in result
    assert "_bb_v_2" in result


def test_ast_normalizer_visit_name_without_mapping():
    """Test that unmapped names remain unchanged"""
    mapping = {"x": "_bb_v_1"}
    normalizer = bb.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_bb_v_1" in result
    assert "y" in result  # y should remain unchanged


def test_ast_normalizer_visit_arg_with_mapping():
    """Test that function arguments are renamed"""
    mapping = {"x": "_bb_v_1", "y": "_bb_v_2"}
    normalizer = bb.ASTNormalizer(mapping)

    code = "def foo(x, y): return x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_bb_v_1" in result
    assert "_bb_v_2" in result


def test_ast_normalizer_visit_functiondef_with_mapping():
    """Test that function names are renamed"""
    mapping = {"foo": "_bb_v_0"}
    normalizer = bb.ASTNormalizer(mapping)

    code = "def foo(): pass"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_bb_v_0" in result
    assert "foo" not in result


# ============================================================================
# Tests for names_collect function
# ============================================================================

def test_collect_names_simple_names():
    """Test collecting variable names"""
    code = "x = 1\ny = 2\nz = x + y"
    tree = ast.parse(code)
    names = bb.code_collect_names(tree)

    assert "x" in names
    assert "y" in names
    assert "z" in names


def test_collect_names_function_names():
    """Test collecting function names and arguments"""
    code = "def foo(a, b): return a + b"
    tree = ast.parse(code)
    names = bb.code_collect_names(tree)

    assert "foo" in names
    assert "a" in names
    assert "b" in names


def test_collect_names_empty_tree():
    """Test collecting names from empty module"""
    tree = ast.parse("")
    names = bb.code_collect_names(tree)

    assert len(names) == 0


# ============================================================================
# Tests for imports_get_names function
# ============================================================================

def test_get_imported_names_import_statement():
    """Test extracting names from import statement"""
    code = "import math"
    tree = ast.parse(code)
    names = bb.code_get_import_names(tree)

    assert "math" in names


def test_get_imported_names_import_with_alias():
    """Test extracting aliased import names"""
    code = "import numpy as np"
    tree = ast.parse(code)
    names = bb.code_get_import_names(tree)

    assert "np" in names
    assert "numpy" not in names


def test_get_imported_names_from_import():
    """Test extracting names from from-import"""
    code = "from collections import Counter"
    tree = ast.parse(code)
    names = bb.code_get_import_names(tree)

    assert "Counter" in names


def test_get_imported_names_from_import_with_alias():
    """Test extracting aliased from-import names"""
    code = "from collections import Counter as C"
    tree = ast.parse(code)
    names = bb.code_get_import_names(tree)

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
    names = bb.code_get_import_names(tree)

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
    imported = bb.code_get_import_names(tree)
    all_names = bb.code_collect_names(tree)

    result = bb.code_check_unused_imports(tree, imported, all_names)
    assert result is True


def test_check_unused_imports_unused_import():
    """Test when an import is unused"""
    code = """
import math
def foo():
    return 4
"""
    tree = ast.parse(code)
    imported = bb.code_get_import_names(tree)
    all_names = bb.code_collect_names(tree)

    result = bb.code_check_unused_imports(tree, imported, all_names)
    assert result is False


# ============================================================================
# Tests for imports_sort function
# ============================================================================

def test_sort_imports_simple_imports():
    """Test sorting import statements"""
    code = """
import sys
import ast
import re
import os
"""
    tree = ast.parse(code)
    sorted_tree = bb.code_sort_imports(tree)
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
    sorted_tree = bb.code_sort_imports(tree)
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
    sorted_tree = bb.code_sort_imports(tree)
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
    func_def, imports = bb.code_extract_definition(tree)

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
    func_def, imports = bb.code_extract_definition(tree)

    assert func_def is not None
    assert func_def.name == "process"
    assert len(imports) == 2


def test_extract_function_def_no_function_raises_error():
    """Test that missing function raises ValueError"""
    code = "x = 42"
    tree = ast.parse(code)

    with pytest.raises(ValueError, match="No function definition found"):
        bb.code_extract_definition(tree)


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
        bb.code_extract_definition(tree)


# ============================================================================
# Tests for mapping_create_name function
# ============================================================================

def test_create_name_mapping_function_name_always_v0():
    """Test that function name always maps to _bb_v_0"""
    code = "def my_function(x): return x"
    tree = ast.parse(code)
    func_def, imports = bb.code_extract_definition(tree)

    forward, reverse = bb.code_create_name_mapping(func_def, imports)

    assert forward["my_function"] == "_bb_v_0"
    assert reverse["_bb_v_0"] == "my_function"


def test_create_name_mapping_sequential_numbering():
    """Test that variables get sequential numbers"""
    code = """
def foo(a, b):
    c = a + b
    return c
"""
    tree = ast.parse(code)
    func_def, imports = bb.code_extract_definition(tree)

    forward, reverse = bb.code_create_name_mapping(func_def, imports)

    # foo should be v_0
    assert forward["foo"] == "_bb_v_0"
    # Other names should be numbered sequentially
    assert all(name.startswith("_bb_v_") for name in forward.values())


def test_create_name_mapping_builtins_not_renamed():
    """Test that built-in functions are not renamed"""
    code = """
def foo(items):
    return len(items)
"""
    tree = ast.parse(code)
    func_def, imports = bb.code_extract_definition(tree)

    forward, reverse = bb.code_create_name_mapping(func_def, imports)

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
    func_def, imports = bb.code_extract_definition(tree)

    forward, reverse = bb.code_create_name_mapping(func_def, imports)

    # math is imported and should not be renamed
    assert "math" not in forward
    assert "x" in forward


def test_create_name_mapping_bb_aliases_not_renamed():
    """Test that bb aliases are excluded from renaming"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    func_def, imports = bb.code_extract_definition(tree)

    # Simulate that 'helper' is a bb alias
    bb_aliases = {"helper"}
    forward, reverse = bb.code_create_name_mapping(func_def, imports, bb_aliases)

    # helper should not be renamed
    assert "helper" not in forward
    assert "x" in forward


# ============================================================================
# Tests for imports_rewrite_bb function
# ============================================================================

def test_rewrite_bb_imports_with_alias():
    """Test rewriting bb import with alias"""
    code = "from bb.pool import abc123 as helper"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = bb.code_rewrite_bb_imports(imports)

    # Should remove alias but keep bb.pool module name
    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from bb.pool import abc123" in result
    assert "as helper" not in result

    # Should track the alias
    assert alias_mapping["abc123"] == "helper"


def test_rewrite_bb_imports_without_alias():
    """Test rewriting bb import without alias"""
    code = "from bb.pool import abc123"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = bb.code_rewrite_bb_imports(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from bb.pool import abc123" in result
    assert len(alias_mapping) == 0


def test_rewrite_bb_imports_non_bb_imports_unchanged():
    """Test that non-bb imports remain unchanged"""
    code = "import math\nfrom collections import Counter"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = bb.code_rewrite_bb_imports(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "import math" in result
    assert "from collections import Counter" in result
    assert len(alias_mapping) == 0


# ============================================================================
# Tests for calls_replace_bb function
# ============================================================================

def test_replace_bb_calls_aliased_call():
    """Test replacing aliased bb function calls"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = bb.code_replace_bb_calls(tree, alias_mapping, name_mapping)
    result = ast.unparse(new_tree)

    # helper(x) should become abc123._bb_v_0(x)
    assert "abc123._bb_v_0" in result
    assert "helper" not in result


def test_replace_bb_calls_non_aliased_names_unchanged():
    """Test that non-aliased names remain unchanged"""
    code = """
def foo(x):
    return other(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = bb.code_replace_bb_calls(tree, alias_mapping, name_mapping)
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

    bb.code_clear_locations(tree)

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
    func_def, _ = bb.code_extract_definition(tree)

    docstring, func_without_doc = bb.code_extract_docstring(func_def)

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
    func_def, _ = bb.code_extract_definition(tree)

    docstring, func_without_doc = bb.code_extract_docstring(func_def)

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
    func_def, _ = bb.code_extract_definition(tree)

    docstring, func_without_doc = bb.code_extract_docstring(func_def)

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
        bb.code_normalize(tree, "eng")

    assert "_bb_v_0" in code_with_doc  # Function name normalized
    assert docstring == "Add two numbers"
    assert "_bb_v_0" in name_mapping.keys()
    assert code_without_doc != code_with_doc  # Should differ by docstring


def test_normalize_ast_imports_sorted():
    """Test that imports are sorted during normalization"""
    code = """
import sys
import ast
import re
import os

def foo():
    return 42
"""
    tree = ast.parse(code)

    code_with_doc, _, _, _, _ = bb.code_normalize(tree, "eng")

    # Verify imports are sorted
    assert code_with_doc.index("import ast") < code_with_doc.index("import os")
    assert code_with_doc.index("import os") < code_with_doc.index("import sys")


def test_normalize_ast_with_bb_import():
    """Test normalizing function with bb import"""
    code = """
from bb.pool import abc123 as helper

def foo(x):
    \"\"\"Process with helper\"\"\"
    return helper(x)
"""
    tree = ast.parse(code)

    code_with_doc, code_without_doc, docstring, name_mapping, alias_mapping = \
        bb.code_normalize(tree, "eng")

    # Should remove alias but keep bb.pool module name
    assert "from bb.pool import abc123" in code_with_doc
    assert "as helper" not in code_with_doc

    # Should track alias
    assert alias_mapping["abc123"] == "helper"

    # Should replace calls
    assert "abc123._bb_v_0" in code_with_doc


# ============================================================================
# Tests for hash_compute function
# ============================================================================

def test_compute_hash_deterministic():
    """Test that same input produces same hash"""
    code = "def foo(): return 42"

    hash1 = bb.hash_compute(code)
    hash2 = bb.hash_compute(code)

    assert hash1 == hash2


def test_compute_hash_format():
    """Test that hash is 64 hex characters"""
    code = "def foo(): return 42"

    hash_value = bb.hash_compute(code)

    assert len(hash_value) == 64
    assert all(c in '0123456789abcdef' for c in hash_value)


def test_compute_hash_different_code_different_hash():
    """Test that different code produces different hash"""
    code1 = "def foo(): return 42"
    code2 = "def bar(): return 43"

    hash1 = bb.hash_compute(code1)
    hash2 = bb.hash_compute(code2)

    assert hash1 != hash2


def test_hash_compute_with_algorithm_parameter():
    """Test that hash_compute works with algorithm parameter"""
    code = "def foo(): pass"

    # Default should be sha256
    hash_default = bb.hash_compute(code)
    hash_sha256 = bb.hash_compute(code, algorithm='sha256')

    assert hash_default == hash_sha256
    assert len(hash_default) == 64


def test_hash_compute_algorithm_deterministic():
    """Test that hash_compute with algorithm produces deterministic results"""
    code = "def foo(): pass"

    hash1 = bb.hash_compute(code, algorithm='sha256')
    hash2 = bb.hash_compute(code, algorithm='sha256')

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

    result = bb.code_replace_docstring(code, new_doc)

    assert "New docstring" in result
    assert "Old docstring" not in result


def test_replace_docstring_add_when_none_exists():
    """Test adding docstring to function without one"""
    code = """
def foo():
    return 42
"""
    new_doc = "Added docstring"

    result = bb.code_replace_docstring(code, new_doc)

    assert "Added docstring" in result


def test_replace_docstring_remove_with_empty_string():
    """Test removing docstring by passing empty string"""
    code = '''
def foo():
    """Remove this"""
    return 42
'''
    result = bb.code_replace_docstring(code, "")

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
def _bb_v_0(_bb_v_1, _bb_v_2):
    _bb_v_3 = _bb_v_1 + _bb_v_2
    return _bb_v_3
"""
    name_mapping = {
        "_bb_v_0": "calculate",
        "_bb_v_1": "first",
        "_bb_v_2": "second",
        "_bb_v_3": "result"
    }
    alias_mapping = {}

    result = bb.code_denormalize(normalized, name_mapping, alias_mapping)

    assert "calculate" in result
    assert "first" in result
    assert "second" in result
    assert "result" in result
    assert "_bb_v_" not in result


def test_denormalize_code_bb_imports():
    """Test denormalizing bb imports"""
    normalized = """
from bb.pool import abc123

def _bb_v_0(_bb_v_1):
    return abc123._bb_v_0(_bb_v_1)
"""
    name_mapping = {
        "_bb_v_0": "process",
        "_bb_v_1": "data"
    }
    alias_mapping = {
        "abc123": "helper"
    }

    result = bb.code_denormalize(normalized, name_mapping, alias_mapping)

    # Should restore import with alias
    assert "from bb.pool import abc123 as helper" in result

    # Should restore function calls
    assert "helper(data)" in result
    assert "abc123._bb_v_0" not in result


# ============================================================================
# Tests for Schema v1 - Foundation
# ============================================================================

def test_mapping_compute_hash_deterministic():
    """Test that mapping_compute_hash produces deterministic hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_bb_v_0": "calculate_average", "_bb_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}
    comment = "Formal terminology"

    # Compute hash twice - should be identical
    hash1 = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment)
    hash2 = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment)

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex characters
    assert all(c in '0123456789abcdef' for c in hash1)


def test_mapping_compute_hash_different_comments():
    """Test that different comments produce different hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_bb_v_0": "calculate_average", "_bb_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}

    hash1 = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "Formal")
    hash2 = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "Informal")

    assert hash1 != hash2


def test_mapping_compute_hash_empty_comment():
    """Test that mapping hash works with empty comment"""
    docstring = "Calculate the average"
    name_mapping = {"_bb_v_0": "calculate_average"}
    alias_mapping = {}

    hash_val = bb.code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "")

    assert len(hash_val) == 64
    assert all(c in '0123456789abcdef' for c in hash_val)


def test_mapping_compute_hash_canonical_json():
    """Test that mapping hash is based on canonical JSON (order-independent)"""
    docstring = "Test"
    # Different key orders should produce same hash
    name_mapping1 = {"_bb_v_0": "foo", "_bb_v_1": "bar"}
    name_mapping2 = {"_bb_v_1": "bar", "_bb_v_0": "foo"}
    alias_mapping = {}

    hash1 = bb.code_compute_mapping_hash(docstring, name_mapping1, alias_mapping, "")
    hash2 = bb.code_compute_mapping_hash(docstring, name_mapping2, alias_mapping, "")

    assert hash1 == hash2


def test_schema_detect_version(mock_bb_dir):
    """Test that schema_detect_version correctly identifies v1 format"""
    pool_dir = mock_bb_dir / '.bb' / 'pool'
    test_hash = "abcd1234" + "0" * 56

    # Create v1 format: pool/XX/YYYYYY.../object.json
    func_dir = pool_dir / test_hash[:2] / test_hash[2:]
    func_dir.mkdir(parents=True, exist_ok=True)
    object_json = func_dir / 'object.json'

    v1_data = {
        'schema_version': 1,
        'hash': test_hash,
        'normalized_code': normalize_code_for_test('def _bb_v_0(): pass'),
        'metadata': {}
    }

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(v1_data, f)

    version = bb.code_detect_schema(test_hash)
    assert version == 1


def test_schema_detect_version_not_found(mock_bb_dir):
    """Test that schema_detect_version returns None for non-existent function"""
    test_hash = "nonexistent" + "0" * 54

    version = bb.code_detect_schema(test_hash)
    assert version is None


def test_metadata_create_basic():
    """Test that metadata_create generates proper metadata structure"""
    metadata = bb.code_create_metadata()

    assert 'created' in metadata
    assert 'name' in metadata
    assert 'email' in metadata


def test_metadata_create_reads_from_config(mock_bb_dir):
    """Test that metadata_create reads name and email from config"""
    # Write config with name and email
    config = {
        'user': {
            'name': 'testuser',
            'email': 'test@example.com',
            'signing_key': '',
            'languages': []
        }
    }
    config_path = bb.storage_get_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w') as f:
        json.dump(config, f)

    metadata = bb.code_create_metadata()
    assert metadata['name'] == 'testuser'
    assert metadata['email'] == 'test@example.com'


def test_metadata_create_timestamp_format():
    """Test that metadata_create uses ISO 8601 timestamp format"""
    metadata = bb.code_create_metadata()
    created = metadata['created']

    # Should be ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
    assert 'T' in created
    assert len(created) >= 19  # At minimum: 2025-01-01T00:00:00


# ============================================================================
# Tests for hash determinism across languages
# ============================================================================

def test_hash_determinism_multilingual_same_logic():
    """Test that functions with identical logic but different names produce the same hash.

    This verifies the core BB principle: same logic = same hash, regardless of
    variable names or human language used. Uses the example files:
    - examples/example_simple.py (English)
    - examples/example_simple_french.py (French)
    """
    examples_dir = Path(__file__).parent.parent / 'examples'
    english_file = examples_dir / 'example_simple.py'
    french_file = examples_dir / 'example_simple_french.py'

    # Read both files
    english_code = english_file.read_text(encoding='utf-8')
    french_code = french_file.read_text(encoding='utf-8')

    # Parse to AST
    english_tree = ast.parse(english_code)
    french_tree = ast.parse(french_code)

    # Normalize both
    eng_with_doc, eng_without_doc, eng_docstring, eng_name_mapping, eng_alias_mapping = \
        bb.code_normalize(english_tree, "eng")
    fra_with_doc, fra_without_doc, fra_docstring, fra_name_mapping, fra_alias_mapping = \
        bb.code_normalize(french_tree, "fra")

    # Compute hashes on code WITHOUT docstring (this is critical for multilingual support)
    english_hash = bb.hash_compute(eng_without_doc)
    french_hash = bb.hash_compute(fra_without_doc)

    # Core assertion: same logic = same hash
    assert english_hash == french_hash, (
        f"Hash mismatch! English and French versions should have identical hashes.\n"
        f"English hash: {english_hash}\n"
        f"French hash: {french_hash}\n"
        f"English normalized (without doc):\n{eng_without_doc}\n"
        f"French normalized (without doc):\n{fra_without_doc}"
    )

    # Additional sanity checks
    assert len(english_hash) == 64, "Hash should be 64 hex characters (SHA256)"
    assert eng_docstring != fra_docstring, "Docstrings should differ (different languages)"
    assert eng_name_mapping != fra_name_mapping, "Name mappings should differ (different variable names)"


# ============================================================================
# Tests for the bb edit file format
# ============================================================================

EDIT_IMPLEMENTATION = {
    'source': 'def double(x: int) -> int:\n    return x + x\n',
    'markdown': 'Doubles a number.\n\n```python\n>>> double(3)\n6\n```',
    'parent': 'a' * 64,
}
EDIT_CHECK = {
    'title': 'check_small',
    'source': 'def check_small(double):\n    return double(3) == 6\n',
    'markdown': 'Small integers.',
    'parent': 'b' * 64,
}


def test_edit_format_round_trip():
    """Rendering then parsing an edit file gives back the same sections"""
    text = bb.markdown_render_edit('double', EDIT_IMPLEMENTATION, [EDIT_CHECK])
    parsed = bb.markdown_parse_edit(text)
    assert parsed['implementation'] == EDIT_IMPLEMENTATION
    assert parsed['checks'] == [EDIT_CHECK]


def test_edit_format_round_trip_without_checks():
    """The hint written into an empty Checks section is not read back as a check"""
    implementation = dict(EDIT_IMPLEMENTATION, parent=None, markdown='')
    parsed = bb.markdown_parse_edit(bb.markdown_render_edit('double', implementation, []))
    assert parsed == {'implementation': implementation, 'checks': []}


def test_edit_format_definition_anywhere_in_section():
    """The python def block may come before the prose; the prose is still documentation"""
    text = "## Implementation\n\n```python def\ndef f():\n    return 1\n```\n\nReturns one.\n"
    section = bb.markdown_parse_edit(text)['implementation']
    assert section['source'] == 'def f():\n    return 1\n'
    assert section['markdown'] == 'Returns one.'
    assert section['parent'] is None


def test_edit_format_headings_inside_code_blocks_are_ignored():
    """A '## ' line inside a fenced block does not start a section"""
    text = ("## Implementation\n\n```text\n## Checks\n```\n\n"
            "```python def\ndef f():\n    return 1\n```\n")
    parsed = bb.markdown_parse_edit(text)
    assert parsed['checks'] == []
    assert parsed['implementation']['markdown'] == "```text\n## Checks\n```"


def test_edit_format_longer_fence_can_contain_shorter_one():
    """A ```` fence can hold a ``` block without closing early"""
    text = ("## Implementation\n\n````markdown\n```python def\nnot this\n```\n````\n\n"
            "```python def\ndef f():\n    return 1\n```\n")
    section = bb.markdown_parse_edit(text)['implementation']
    assert section['source'] == 'def f():\n    return 1\n'
    assert 'not this' in section['markdown']


@pytest.mark.parametrize("text, message", [
    ("# f\n", "Missing '## Implementation'"),
    ("## Implementation\n\nNo code here.\n", "missing ```python def"),
    ("## Implementation\n\n```python def\ndef f(): pass\n```\n\n```python def\ndef g(): pass\n```\n",
     "more than one ```python def"),
    ("## Implementation\n\n```python def\ndef f(): pass\n", "Unclosed fenced code block"),
    ("## Usage\n", "Unknown section"),
    ("Stray prose\n\n## Implementation\n\n```python def\ndef f(): pass\n```\n", "would be lost"),
    ("## Implementation\n\n```python def\ndef f(): pass\n```\n\n## Checks\n\nStray\n", "outside a ### subsection"),
    ("## Implementation\n\n```python def\ndef f(): pass\n```\n\n## Checks\n\n### c\n\nNo code.\n",
     "Check 'c': missing"),
])
def test_edit_format_errors(text, message):
    """Malformed edit files are rejected with a clear message instead of losing text"""
    with pytest.raises(bb.EditFormatError, match=re.escape(message)):
        bb.markdown_parse_edit(text)


# ============================================================================
# Tests for functor checks
# ============================================================================

def test_check_run_statuses():
    """A check passes on a truthy result, fails on a falsy one, errors on an exception"""
    def double(x):
        return x * 2

    assert bb.check_run(lambda f: f(3) == 6, double) == ('PASS', '')
    assert bb.check_run(lambda f: f(3) == 7, double) == ('FAIL', '')
    status, detail = bb.check_run(lambda f: f(), double)
    assert status == 'ERROR'
    assert detail.startswith('TypeError:')


def test_check_report_summary(capsys):
    """check_report prints one line per check and returns whether all passed"""
    assert bb.check_report([]) is True
    assert bb.check_report([('PASS', 'a', ''), ('ERROR', 'b', 'boom')]) is False
    out = capsys.readouterr().out
    assert out == "No checks.\nPASS a\nERROR b: boom\n2 checks: 1 passed, 1 errors\n"


def test_code_execute_source_returns_function(mock_bb_dir):
    """Unsaved source from an edit file is executed and its function returned"""
    func = bb.code_execute_source("import math\n\ndef area(r):\n    return math.pi * r * r\n", 'eng')
    assert func(1) == pytest.approx(3.14159, rel=1e-5)


def test_code_execute_source_reports_load_errors(mock_bb_dir):
    """Errors while defining the function surface as ValueError with the function name"""
    with pytest.raises(ValueError, match="broken failed to load: ZeroDivisionError"):
        bb.code_execute_source("def broken(x=1 / 0):\n    return x\n", 'eng')


def test_code_execute_source_ignores_statements_that_are_not_stored(mock_bb_dir):
    """Only imports and the function run, matching what bb stores"""
    func = bb.code_execute_source("print(1 / 0)\n\ndef one():\n    return 1\n", 'eng')
    assert func() == 1


# ============================================================================
# Tests for check lineage without markers
# ============================================================================

def test_edit_assign_parents_keeps_markers():
    """Markers win; their hashes are not handed out again by name or position"""
    originals = [('a', 'h1'), ('b', 'h2')]
    assert bb.edit_assign_parents([('a', 'h2'), ('b', None)], originals) == ['h2', None]


def test_edit_assign_parents_by_name_then_position():
    """Same name is matched first, then the remaining check at the same position"""
    originals = [('a', 'h1'), ('b', 'h2'), ('c', 'h3')]
    checks = [('renamed', None), ('c', None), ('b', None)]
    assert bb.edit_assign_parents(checks, originals) == ['h1', 'h3', 'h2']


def test_edit_assign_parents_new_checks_have_no_parent():
    """A check beyond the loaded ones, with a new name, has no parent"""
    assert bb.edit_assign_parents([('a', None), ('new', None)], [('a', 'h1')]) == ['h1', None]


# ============================================================================
# Tests for review-gated checks (nothing runs unread)
# ============================================================================

def _pool_add(source, lang='eng', checks=None):
    """Add a function to the mocked pool from source; returns its hash."""
    prepared = bb.code_prepare_source(source, lang)
    func_hash = bb.code_compute_function_hash(prepared['normalized_code'], checks)
    bb.object_save(func_hash, prepared['normalized_code'], bb.code_create_metadata(), checks=checks)
    bb.mapping_save(func_hash, lang, prepared['docstring'], prepared['name_mapping'], prepared['alias_mapping'])
    return func_hash


def test_review_state_lives_outside_the_shared_directory(mock_bb_dir):
    """Approvals committed into BB_DIRECTORY are not trusted"""
    shared = bb.storage_get_bb_directory() / 'review_state.json'
    shared.parent.mkdir(parents=True, exist_ok=True)
    shared.write_text(json.dumps({'reviewed': ['f' * 64]}), encoding='utf-8')

    assert bb.storage_get_bb_directory() not in bb.review_state_path().parents
    assert bb.review_load_state() == set()


def test_checks_do_not_run_until_everything_is_approved(mock_bb_dir, tmp_path):
    """An unapproved check never executes, even when the function is approved"""
    marker = tmp_path / 'ran'
    check = _pool_add(f"def check_side_effect(f):\n    open({str(marker)!r}, 'w').close()\n    return True\n")
    func = _pool_add("def one():\n    return 1\n", checks=[check])

    results, missing = bb.check_run_reviewed(func, {func})
    assert results is None and missing == [check]
    assert not marker.exists()

    results, missing = bb.check_run_reviewed(func, {func, check})
    assert missing == [] and [r[0] for r in results] == ['PASS']
    assert marker.exists()


def test_review_closure_includes_check_dependencies(mock_bb_dir):
    """A check's own imports must be approved too"""
    helper = _pool_add("def six():\n    return 6\n")
    check = _pool_add(f"from bb.pool import object_{helper} as six\n\n"
                      f"def check_double(f):\n    return f(3) == six()\n")
    func = _pool_add("def double(x):\n    return x * 2\n", checks=[check])
    assert set(bb.review_closure(func)) == {func, helper, check}
    results, _ = bb.check_run_reviewed(func, {func, helper, check})
    assert results == [('PASS', 'check_double', '')]


def test_code_load_normalized_runs_the_hashed_code(mock_bb_dir):
    """What runs comes from object.json, not from a language mapping"""
    func = _pool_add("def double(x):\n    return x * 2\n")
    # A mapping cannot change what runs: the loaded function is the normalized one
    assert bb.code_load_normalized(func)(21) == 42
    assert bb.code_load_normalized(func).__name__ == '_bb_v_0'


def test_edit_refuses_unreviewed_imports(mock_bb_dir):
    """bb edit does not run pool functions you have not reviewed"""
    helper = _pool_add("def six():\n    return 6\n")
    text = (f"## Implementation\n\n```python def\nfrom bb.pool import object_{helper} as six\n\n"
            f"def twelve():\n    return six() * 2\n```\n")
    with pytest.raises(ValueError, match=f"bb review {helper}"):
        bb.edit_prepare(text, 'eng')

    bb.review_save_state({helper})
    assert bb.edit_prepare(text, 'eng')['results'] == []


def test_review_order_puts_dependencies_and_checks_first(mock_bb_dir):
    """Levels follow the dependency order; a function's checks come before it"""
    helper = _pool_add("def six():\n    return 6\n")
    check = _pool_add(f"from bb.pool import object_{helper} as six\n\n"
                      f"def check_double(f):\n    return f(3) == six()\n")
    leaf = _pool_add("def two():\n    return 2\n")
    func = _pool_add(f"from bb.pool import object_{leaf} as two\n\n"
                     f"def double(x):\n    return x * two()\n", checks=[check])

    levels = bb.review_order(func)
    assert levels[0] == sorted([helper, leaf])
    assert levels[1] == [check]
    assert levels[2] == [func]


def test_review_require_refuses_unreviewed_code(mock_bb_dir, capsys):
    """bb run and bb compile stop unless the function and its dependencies are approved"""
    leaf = _pool_add("def two():\n    return 2\n")
    func = _pool_add(f"from bb.pool import object_{leaf} as two\n\ndef four():\n    return two() * 2\n")

    bb.review_save_state({func})
    with pytest.raises(SystemExit):
        bb.review_require(func, "run")
    assert f"bb review {func}" in capsys.readouterr().err

    bb.review_save_state({func, leaf})
    bb.review_require(func, "run")  # no exit


def test_validate_dangerously_unreviewed_runs_everything(mock_bb_dir, capsys):
    """--dangerously-unreviewed runs unapproved checks, warns, and approves nothing"""
    check = _pool_add("def check_one(f):\n    return f() == 1\n")
    func = _pool_add("def one():\n    return 1\n", checks=[check])

    bb.command_validate_checks()
    assert "Not reviewed" in capsys.readouterr().out

    bb.command_validate_checks(unreviewed=True)
    out = capsys.readouterr()
    assert f"one {func}" in out.out and "PASS check_one" in out.out
    assert "dangerously-unreviewed" in out.err
    assert bb.review_load_state() == set()


# ============================================================================
# Tests for signed reviews and trusted peers
# ============================================================================

needs_ssh_keygen = pytest.mark.skipif(shutil.which('ssh-keygen') is None, reason="needs ssh-keygen")


def _become(monkeypatch, tmp_path, name):
    """Switch to a reviewer with their own SSH key, config and private state."""
    key = tmp_path / f"{name}_key"
    if not key.exists():
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-C', name, '-f', str(key)], check=True)
    monkeypatch.setenv('BB_STATE_DIRECTORY', str(tmp_path / f"{name}_state"))
    bb.storage_write_config({'user': {'name': name, 'email': f"{name}@example.org",
                                      'signing_key': str(key), 'languages': ['eng']}})
    return key


@needs_ssh_keygen
def test_signed_review_status(mock_bb_dir, monkeypatch, tmp_path):
    """A review is untrusted until you trust its key, and invalid once altered"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    path = bb.review_publish(func, 'approve', 'fine')
    assert path is not None and path.with_suffix('.json.sig').exists()

    _become(monkeypatch, tmp_path, 'bob')
    assert [r['status'] for r in bb.review_list_shared(func)] == ['untrusted']
    assert bb.review_approvals([func]) == set()

    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert [r['status'] for r in bb.review_list_shared(func)] == ['trusted']
    assert bb.review_approvals([func]) == {func}

    path.write_text(path.read_text(encoding='utf-8').replace('fine', 'great'), encoding='utf-8')
    assert [r['status'] for r in bb.review_list_shared(func)] == ['invalid']
    assert bb.review_approvals([func]) == set()


@needs_ssh_keygen
def test_signed_review_cannot_be_moved_to_another_function(mock_bb_dir, monkeypatch, tmp_path):
    """Copying a valid review under another hash does not approve that hash"""
    good = _pool_add("def one():\n    return 1\n")
    evil = _pool_add("def two():\n    return 2\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    path = bb.review_publish(good, 'approve')

    target = bb.storage_get_review_directory() / evil[:2] / evil[2:]
    target.mkdir(parents=True)
    shutil.copy(path, target / path.name)
    shutil.copy(str(path) + '.sig', target / (path.name + '.sig'))

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.review_approvals([good, evil]) == {good}


@needs_ssh_keygen
def test_trusted_rejection_blocks_and_own_verdict_wins(mock_bb_dir, monkeypatch, tmp_path):
    """A trusted peer's rejection cancels trusted approvals; your own verdict decides"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    bb.review_publish(func, 'approve')
    carol_key = _become(monkeypatch, tmp_path, 'carol')
    bb.review_publish(func, 'reject', 'unsafe')

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.review_approvals([func]) == {func}
    bb.command_trust_add('carol@example.org', str(carol_key) + '.pub')
    assert bb.review_approvals([func]) == set()

    bb.review_save_state({func})
    assert bb.review_approvals([func]) == {func}
    bb.review_save_state(set())
    bb.review_save_rejections({func})
    bb.command_trust_remove('carol@example.org')
    assert bb.review_approvals([func]) == set()


@needs_ssh_keygen
def test_trust_import_asks_for_each_peer(mock_bb_dir, monkeypatch, tmp_path):
    """The shared list is never trusted as a whole: each entry is accepted or not"""
    _become(monkeypatch, tmp_path, 'alice')
    bb.command_trust_publish()
    _become(monkeypatch, tmp_path, 'eve')
    bb.command_trust_publish()

    _become(monkeypatch, tmp_path, 'bob')
    assert bb.trust_parse(bb.trust_private_path()) == []
    answers = iter(['y', 'n'])
    monkeypatch.setattr('builtins.input', lambda _: next(answers))
    bb.command_trust_import()
    assert [email for email, _ in bb.trust_parse(bb.trust_private_path())] == ['alice@example.org']


def test_config_is_private_and_migrated_once(mock_bb_dir, capsys):
    """Config lives in the private state directory; an old shared one is copied over"""
    old = bb.storage_get_bb_directory() / 'config.json'
    old.parent.mkdir(parents=True, exist_ok=True)
    old.write_text(json.dumps({'user': {'name': 'old', 'email': '', 'languages': ['eng']}}), encoding='utf-8')

    path = bb.storage_get_config_path()
    assert path == bb.storage_get_state_directory() / 'config.json'
    assert bb.storage_get_bb_directory() not in path.parents
    assert bb.storage_read_config()['user']['name'] == 'old'
    assert "moved your config" in capsys.readouterr().err

    bb.storage_get_config_path()
    assert capsys.readouterr().err == ""


def test_inner_imports_are_not_renamed():
    """A module imported inside the body keeps its name in the stored code"""
    src = "def pick(n):\n    import random\n    from os import path as p\n    return random.randint(1, n), p\n"
    _, code, _, names, _ = bb.code_normalize(ast.parse(src), 'eng')
    assert "random.randint" in code and "return (random.randint(1, _bb_v_1), p)" in code
    assert set(names.values()) == {'pick', 'n'}


def test_inner_imports_run_from_normalized_code(mock_bb_dir):
    """Such functions run directly from their normalized code"""
    func = _pool_add("def pick(n):\n    import random\n    random.seed(1)\n    return random.randint(1, n) <= n\n")
    assert bb.code_load_normalized(func)(6) is True


def _save_legacy_inner_import(module_name):
    """Store a function the way older bb normalized inner imports: the use renamed, the import not."""
    code = normalize_code_for_test("def _bb_v_0(_bb_v_1):\n    import random\n    return _bb_v_2.randint(1, _bb_v_1)")
    func = bb.hash_compute(code + module_name)
    bb.object_save(func, code, bb.code_create_metadata(), normalization=1)
    bb.mapping_save(func, 'eng', '', {'_bb_v_0': 'pick', '_bb_v_1': 'n', '_bb_v_2': module_name}, {})
    return func


def test_legacy_inner_imports_still_run(mock_bb_dir):
    """Objects stored before the fix get the module name back from the mapping"""
    assert 1 <= bb.code_load_normalized(_save_legacy_inner_import('random'))(6) <= 6


def test_mapping_cannot_rename_to_names_the_code_does_not_import(mock_bb_dir):
    """A hostile mapping pointing the module at another name is ignored"""
    with pytest.raises(NameError):
        bb.code_load_normalized(_save_legacy_inner_import('os'))(6)


def test_find_similar_reports_near_copies_only(mock_bb_dir):
    """Near-copies are reported; rewrites, tiny functions and lineage are not"""
    body = ("def tokenize(text):\n    import string\n    words = []\n    for word in text.split():\n"
            "        cleaned = word.strip(string.punctuation).lower()\n        if cleaned:\n"
            "            words.append(cleaned)\n    return words\n")
    original = _pool_add(body)
    near_copy = _pool_add(body.replace("if cleaned:", "if len(cleaned) > 0:").replace("tokenize", "split_words"))
    rewrite = _pool_add("def tokenize(text):\n    import string\n"
                        "    return [w for w in (x.strip(string.punctuation).lower() for x in text.split()) if w]\n")
    tiny_one = _pool_add("def double(x):\n    return x * 2\n")
    tiny_two = _pool_add("def triple(x):\n    return x * 3\n")

    similar = [h for _, h in bb.code_find_similar(near_copy)]
    assert similar == [original]
    assert rewrite not in [h for _, h in bb.code_find_similar(original, {near_copy})]
    assert bb.code_find_similar(tiny_two) == []
    assert bb.code_find_similar(near_copy, exclude={original}) == []


def test_shape_tokens_encode_variables_by_distance():
    """Variables become distances to their previous use; built-ins keep their names"""
    code = bb.code_normalize(ast.parse("def f(x):\n    y = len(x)\n    return y\n"), 'eng')[1]
    tokens = bb.code_shape_tokens(code)
    assert 'Name:len' in tokens
    assert not any(t.startswith('_bb_v_') for t in tokens)
    assert tokens.count('var0') == 1          # y's first use; x was bound as an argument
    assert [t for t in tokens if t.startswith('var') and t != 'var0']   # later uses are distances


def test_find_similar_survives_an_extra_early_variable(mock_bb_dir):
    """Adding a variable at the top renumbers every _bb_v_N, but the shape stays close"""
    loop = ("def compute_pi(n):\n    n = int(n)\n    result = 0.0\n    for i in range(n):\n"
            "        result += ((-1) ** i) / (2 * i + 1)\n    return result * 4\n")
    original = _pool_add(loop)
    shifted = _pool_add(loop.replace("    n = int(n)\n", "    sign = 1\n    n = int(n)\n"))
    [(score, found)] = bb.code_find_similar(shifted)
    assert found == original and score >= 0.75



def test_objects_record_their_normalization(mock_bb_dir):
    """New objects say which normalization produced them; old ones read as version 1"""
    func = _pool_add("def one():\n    return 1\n")
    assert bb.object_load(func)['normalization'] == bb.NORMALIZATION_VERSION == 2
    assert bb.object_normalization(func) == 2

    legacy = _save_legacy_inner_import('random')
    assert bb.object_normalization(legacy) == 1
    data = bb.object_load(legacy)
    data.pop('normalization')
    path = bb.storage_get_pool_directory() / legacy[:2] / legacy[2:] / 'object.json'
    path.write_text(json.dumps(data), encoding='utf-8')
    assert bb.object_normalization(legacy) == 1


@pytest.mark.parametrize("source, line", [
    ("LIMIT = 10\n\ndef f():\n    return LIMIT\n", "line 1: LIMIT = 10"),
    ("def f():\n    return 1\n\nif __name__ == '__main__':\n    f()\n", "line 4: if __name__ == '__main__':"),
    ("import flask\n\napp = flask.Flask(__name__)\n\ndef f():\n    return app\n", "line 3: app = flask.Flask(__name__)"),
])
def test_prepare_refuses_code_outside_the_function(mock_bb_dir, source, line):
    """Module-level code would be silently lost: refuse it and say which line"""
    with pytest.raises(ValueError, match="code outside its function") as error:
        bb.code_prepare_source(source, 'eng', where="app.py")
    assert line in str(error.value)


def test_prepare_allows_a_module_docstring(mock_bb_dir):
    """A module docstring is documentation, not code: dropping it loses nothing that runs"""
    assert bb.code_prepare_source('"""Helpers."""\n\ndef f():\n    return 1\n', 'eng')['hash']
