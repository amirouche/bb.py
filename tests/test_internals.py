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

    # helper(x) should become bb.ref('abc123')(x)
    assert "bb.ref('abc123')(x)" in result
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
    helper_hash = "abc123" + "0" * 58
    code = f"""
import bb
helper = bb.ref('{helper_hash}')

def foo(x):
    \"\"\"Process with helper\"\"\"
    return helper(x)
"""
    tree = ast.parse(code)

    code_with_doc, code_without_doc, docstring, name_mapping, alias_mapping = \
        bb.code_normalize(tree, "eng")

    # The import goes: the pool function is referred to where it is used
    assert "bb.pool" not in code_without_doc and "helper" not in code_without_doc
    assert f"return bb.ref('{helper_hash}')(_bb_v_1)" in code_with_doc

    # Should track alias
    assert alias_mapping[helper_hash] == "helper"


def test_normalize_names_a_pool_function_once_in_any_form():
    """An import, a bb.ref binding and a bb.ref call of the same function hash alike"""
    helper_hash = "abc123" + "0" * 58
    forms = [
        f"import bb\nhelper = bb.ref('{helper_hash}')\n\ndef foo(x):\n    return helper(x)\n",
        f"helper = bb.ref('{helper_hash}')\n\ndef foo(x):\n    return helper(x)\n",
        f"import bb\n\ndef foo(x):\n    return bb.ref('{helper_hash}')(x)\n",
    ]
    hashes = {bb.code_compute_function_hash(bb.code_normalize(ast.parse(f), "eng")[1]) for f in forms}
    assert len(hashes) == 1


def test_normalize_refuses_imports_by_hash():
    """from bb.pool import object_<hash> is gone: bb.ref names an exact version"""
    with pytest.raises(ValueError, match="import bb"):
        bb.code_normalize(ast.parse(f"from bb.pool import object_{'a' * 64} as h\n\ndef f():\n    return h()\n"), "eng")


@pytest.mark.parametrize("code, message", [
    ("def foo(bb):\n    return bb\n", "reserved"),
    ("def foo(x):\n    return bb.ref(x)(1)\n", "bb.ref takes one"),
    ("def foo(x):\n    return bb.pool\n", "bb provides bb.resolve"),
    ("def foo(x):\n    return bb.latest('double')(x)\n", "resolved when saving"),
])
def test_normalize_refuses_other_uses_of_bb(code, message):
    with pytest.raises(ValueError, match=message):
        bb.code_normalize(ast.parse(code), "eng")


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

    # Storage v2: pool/functions/XX/YYYYYY.../object.json
    func_dir = pool_dir / 'functions' / test_hash[:2] / test_hash[2:]
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
    'source': 'def check_small(double):\n    assert double(3) == 6\n',
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

def _check_function(source: str):
    """The function a check's source defines, compiled the way bb compiles checks."""
    namespace = {}
    exec(compile(source, '<check>', 'exec', optimize=0), namespace)
    return namespace[next(name for name in namespace if name.startswith('check_'))]


def test_check_run_statuses():
    """A check passes when it runs, fails on a failed assert, errors on another exception"""
    def double(x):
        return x * 2

    six = _check_function('def check_six(f):\n    assert f(3) == 6\n')
    seven = _check_function('def check_seven(f):\n    assert f(3) == 7\n')
    arity = _check_function('def check_arity(f):\n    f()\n    assert True\n')
    assert bb.check_run(six, double) == ('PASS', '')
    assert bb.check_run(seven, double) == ('FAIL', 'assertion failed')
    status, detail = bb.check_run(arity, double)
    assert status == 'ERROR'
    assert detail.startswith('TypeError:')


def test_a_check_ignores_what_it_returns():
    check = _check_function('def check_false(f):\n    assert f(1) == 1\n    return False\n')
    assert bb.check_run(check, lambda n: n) == ('PASS', '')


def test_a_check_without_assert_is_an_error_when_run():
    source = 'def check_none(f):\n    return f(1) == 2\n'
    status, detail = bb.check_run(_check_function(source), lambda n: n, source)
    assert (status, detail) == ('ERROR', bb.CHECK_NO_ASSERT)
    assert bb.check_has_assert('def check_one(f):\n    assert f(1)\n')
    assert not bb.check_has_assert(source)


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
    mapping = bb.mapping_save(func_hash, lang, prepared['docstring'], prepared['name_mapping'],
                              prepared['alias_mapping'])
    bb.lineage_save(None, func_hash, lang, mapping, 'add')  # as bb add does: the name is current
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
    check = _pool_add(f"def check_side_effect(f):\n    open({str(marker)!r}, 'w').close()\n    assert True\n")
    func = _pool_add("def one():\n    return 1\n", checks=[check])

    results, missing = bb.check_run_reviewed(func, {func})
    assert results is None and missing == [check]
    assert not marker.exists()

    results, missing = bb.check_run_reviewed(func, {func, check})
    assert missing == [] and [r[0] for r in results] == ['PASS']
    assert marker.exists()


def test_a_stored_check_without_assert_is_an_error_when_its_checks_run(mock_bb_dir):
    """Checks saved before the assert protocol return a value: nothing could fail them, so they do not pass"""
    check = _pool_add("def check_old(f):\n    return f() == 2\n")
    func = _pool_add("def one():\n    return 1\n", checks=[check])
    results, missing = bb.check_run_reviewed(func, {func, check})
    assert missing == [] and results == [('ERROR', 'check_old', bb.CHECK_NO_ASSERT)]


def test_review_closure_includes_check_dependencies(mock_bb_dir):
    """A check's own imports must be approved too"""
    helper = _pool_add("def six():\n    return 6\n")
    check = _pool_add(f"import bb\nsix = bb.ref('{helper}')\n\n"
                      f"def check_double(f):\n    assert f(3) == six()\n")
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


def test_running_refuses_code_swapped_under_an_approved_hash(mock_bb_dir):
    """object.json is git-shared; running it recomputes the hash, so a swapped body is caught"""
    func = _pool_add("def double(x):\n    return x * 2\n")
    assert bb.object_hash_matches(func, bb.object_load(func))

    # An attacker rewrites the body in place, under the hash the reviewer approved
    path = bb.storage_object_path(func)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['normalized_code'] = normalize_code_for_test("def _bb_v_0(_bb_v_1):\n    return 'pwned'")
    data.pop('public_key', None)
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()

    assert not bb.object_hash_matches(func, bb.object_load(func))
    # Approval binds to the hash, so the swap must be caught where the code runs
    with pytest.raises(ValueError, match="does not match its stored code"):
        bb.code_load_normalized(func)
    is_valid, errors = bb.schema_validate(func)
    assert not is_valid and any("does not match its hash" in e for e in errors)


def test_running_refuses_tampered_checks_under_an_approved_hash(mock_bb_dir):
    """Checks are part of the hash, so swapping the 'checks' list is caught too"""
    func = _pool_add("def one():\n    return 1\n", checks=['a' * 64])
    path = bb.storage_object_path(func)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['checks'] = ['b' * 64]  # same code, different checks -> different hash
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()

    assert not bb.object_hash_matches(func, bb.object_load(func))
    with pytest.raises(ValueError, match="does not match its stored code"):
        bb.code_load_normalized(func)


@pytest.mark.parametrize('checks', ['a' * 64, [['a' * 64]], [{}], {'a' * 64: 1}, [1], 7, False])
def test_checks_that_are_not_a_list_of_strings_are_a_mismatch_not_a_crash(mock_bb_dir, checks):
    """A tampered 'checks' (a string, nested lists, numbers) is reported like any altered object"""
    func = _pool_add("def one():\n    return 1\n")
    path = bb.storage_object_path(func)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['checks'] = checks
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()

    assert not bb.object_hash_matches(func, bb.object_load(func))
    is_valid, errors = bb.schema_validate(func)
    assert not is_valid and errors
    with pytest.raises(ValueError):
        bb.code_load_normalized(func)


def _swap_body(func_hash, body="def _bb_v_0(_bb_v_1):\n    return 'swapped'"):
    """Rewrite a stored object's code in place so it no longer hashes to its name."""
    path = bb.storage_object_path(func_hash)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['normalized_code'] = normalize_code_for_test(body)
    data.pop('public_key', None)
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()
    assert not bb.object_hash_matches(func_hash, bb.object_load(func_hash))


def test_show_edit_and_compile_refuse_code_that_does_not_match_its_hash(mock_bb_dir, tmp_path, capsys):
    """bb show, bb edit and bb compile display or bundle stored code, so each binds to the hash.

    object.json is git-shared and can be swapped under a hash; these paths
    put the stored bytes in front of a human (show, edit) or into a program
    (compile), so they refuse bytes that are not the ones the hash names,
    the same guard bb run, bb validate and bb review apply.
    """
    func = _pool_add("def double(x):\n    return x * 2\n")
    _swap_body(func)

    with pytest.raises(SystemExit):
        bb.code_show(f"{func}@eng")
    out = capsys.readouterr()
    assert "does not match its stored code" in out.err and "swapped" not in out.out

    with pytest.raises(SystemExit):
        bb.edit_load_section(func, 'eng')
    assert "does not match its stored code" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        bb.command_compile(f"{func}@eng", output_path=str(tmp_path / 'out.py'))
    assert "does not match its stored code" in capsys.readouterr().err
    assert not (tmp_path / 'out.py').exists()  # nothing was written


def test_edit_runs_nothing_without_checks_and_checks_need_reviewed_imports(mock_bb_dir):
    """bb edit runs nothing without checks; with checks, not pool functions you have not reviewed"""
    helper = _pool_add("def six():\n    return 6\n")
    implementation = (f"## Implementation\n\n```python def\nimport bb\nsix = bb.ref('{helper}')\n\n"
                      f"def twelve():\n    return six() * 2\n```\n")
    assert bb.edit_prepare(implementation, 'eng')['results'] == []  # saved without running

    with_check = implementation + ("\n## Checks\n\n### check_twelve\n\n```python def\n"
                                   "def check_twelve(twelve):\n    assert twelve() == 12\n```\n")
    with pytest.raises(ValueError, match=f"bb review {helper}"):
        bb.edit_prepare(with_check, 'eng')

    bb.review_save_state({helper})
    assert [status for status, _, _ in bb.edit_prepare(with_check, 'eng')['results']] == ['PASS']


def test_review_order_puts_dependencies_and_checks_first(mock_bb_dir):
    """Levels follow the dependency order; a function's checks come before it"""
    helper = _pool_add("def six():\n    return 6\n")
    check = _pool_add(f"import bb\nsix = bb.ref('{helper}')\n\n"
                      f"def check_double(f):\n    assert f(3) == six()\n")
    leaf = _pool_add("def two():\n    return 2\n")
    func = _pool_add(f"import bb\ntwo = bb.ref('{leaf}')\n\n"
                     f"def double(x):\n    return x * two()\n", checks=[check])

    levels = bb.review_order(func)
    assert levels[0] == sorted([helper, leaf])
    assert levels[1] == [check]
    assert levels[2] == [func]


def test_review_require_refuses_unreviewed_code(mock_bb_dir, capsys):
    """bb run and bb compile stop unless the function and its dependencies are approved"""
    leaf = _pool_add("def two():\n    return 2\n")
    func = _pool_add(f"import bb\ntwo = bb.ref('{leaf}')\n\ndef four():\n    return two() * 2\n")

    bb.review_save_state({func})
    with pytest.raises(SystemExit):
        bb.review_require(func, "run")
    assert f"bb review {func}" in capsys.readouterr().err

    bb.review_save_state({func, leaf})
    bb.review_require(func, "run")  # no exit


def test_validate_dangerously_unreviewed_runs_everything(mock_bb_dir, capsys):
    """--dangerously-unreviewed runs unapproved checks, warns, and approves nothing"""
    check = _pool_add("def check_one(f):\n    assert f() == 1\n")
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
    path = bb.review_publish(func, 'approve', 'fine', ack=bb.REVIEW_ACKS)
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
def test_your_own_signed_review_is_trusted(mock_bb_dir, monkeypatch, tmp_path):
    """A review signed with your key counts without adding yourself to your peers: on your other machines too"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    bb.review_publish(func, 'approve', ack=bb.REVIEW_ACKS)
    assert bb.trust_parse(bb.trust_private_path()) == []
    assert [r['status'] for r in bb.review_list_shared(func)] == ['trusted']

    # Another machine: the same key and email, no private approval there
    monkeypatch.setenv('BB_STATE_DIRECTORY', str(tmp_path / 'alice_laptop'))
    bb.storage_write_config({'user': {'name': 'alice', 'email': 'alice@example.org',
                                      'signing_key': str(alice_key), 'languages': ['eng']}})
    assert bb.review_load_state() == set()
    assert bb.review_approvals([func]) == {func}
    bb.review_save_rejections({func})
    assert bb.review_approvals([func]) == set()

    # The key under another email is not you
    bb.review_save_rejections(set())
    bb.storage_write_config({'user': {'name': 'alice', 'email': 'other@example.org',
                                      'signing_key': str(alice_key), 'languages': ['eng']}})
    assert [r['status'] for r in bb.review_list_shared(func)] == ['untrusted']
    assert bb.review_approvals([func]) == set()


@needs_ssh_keygen
def test_signed_review_names_the_mapping_read(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """bb review records under which names the function was read; the verdict is on the function"""
    func = _pool_add("def one():\n    return 1\n")
    _pool_add("def un():\n    return 1\n", lang='fra')
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    answers = iter(['y', 'y', 'y'])
    monkeypatch.setattr('builtins.input', lambda *_: next(answers))
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: True, raising=False)
    bb.command_review(func, 'fine')
    [review] = bb.review_list_shared(func)
    mapping = bb.mapping_select(func, 'eng')
    assert (review['lang'], review['mapping']) == ('eng', mapping)
    assert review['id'] == bb.review_compute_id({k: v for k, v in review.items()
                                                 if k not in ('id', 'status', 'current')})
    capsys.readouterr()
    bb.command_reviews(func)
    assert f"read as one@eng (mapping {mapping[:12]}...)" in capsys.readouterr().out

    # The same verdict after reading it in French is another review; the approval covers both
    french = bb.mapping_select(func, 'fra')
    second = bb.review_publish(func, 'approve', 'fine', 'fra', french, ack=bb.REVIEW_ACKS)
    assert 'supersedes' not in json.loads(second.read_text(encoding='utf-8'))
    assert bb.review_publish(func, 'approve', 'fine', 'fra', french, ack=bb.REVIEW_ACKS) == second
    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.review_approvals([func]) == {func}

    # Naming another mapping in a signed review breaks its signature
    text = second.read_text(encoding='utf-8')
    second.write_text(text.replace(french, mapping), encoding='utf-8')
    # ... and her earlier review is the one that counts again
    assert sorted((r['status'], r['current']) for r in bb.review_list_shared(func)) == [
        ('invalid', False), ('trusted', True)]


@needs_ssh_keygen
def test_review_refuses_to_approve_code_that_does_not_match_its_hash(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """bb review must bind to the hash it approves: the reviewed bytes are the ones the hash names.

    A git-shared object.json can show benign code under a hash while the real,
    hash-matching body runs later (bb run recomputes the hash). If review
    displayed the swapped bytes and let them be approved, the reviewer would
    sign off on code they never saw. Review refuses the mismatch instead.
    """
    func = _pool_add("def double(x):\n    return x * 2\n")
    path = bb.storage_object_path(func)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['normalized_code'] = normalize_code_for_test("def _bb_v_0(_bb_v_1):\n    return 'benign'")
    data.pop('public_key', None)
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()
    assert not bb.object_hash_matches(func, bb.object_load(func))

    _become(monkeypatch, tmp_path, 'alice')
    monkeypatch.setattr('builtins.input', lambda *_: 'y')  # a reviewer trying to approve
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: True, raising=False)
    with pytest.raises(SystemExit) as exit_info:
        bb.command_review(func, 'looks fine')
    assert exit_info.value.code == 1

    captured = capsys.readouterr()
    assert "does not match its stored code" in captured.err
    assert "benign" not in captured.out  # the swapped body was never shown
    # Nothing was approved, privately or as a shared review
    assert bb.review_load_state() == set()
    assert bb.review_list_shared(func) == []


@needs_ssh_keygen
def test_signed_review_cannot_be_moved_to_another_function(mock_bb_dir, monkeypatch, tmp_path):
    """Copying a valid review under another hash does not approve that hash"""
    good = _pool_add("def one():\n    return 1\n")
    evil = _pool_add("def two():\n    return 2\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    path = bb.review_publish(good, 'approve', ack=bb.REVIEW_ACKS)

    target = bb.storage_get_review_directory() / evil[:2] / evil[2:] / path.parent.name
    target.mkdir(parents=True)
    shutil.copy(path, target / path.name)
    shutil.copy(str(path) + '.sig', target / (path.name + '.sig'))

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.review_approvals([good, evil]) == {good}


@needs_ssh_keygen
def test_review_changed_mind_is_another_review(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """A new verdict is another review naming no other: both stay, the latest of the key counts"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    first = bb.review_publish(func, 'approve', 'fine', ack=bb.REVIEW_ACKS)
    before = first.read_bytes()
    assert bb.review_publish(func, 'approve', 'fine', ack=bb.REVIEW_ACKS) == first
    second = bb.review_publish(func, 'reject', 'on second thought')
    assert second != first and second.parent == first.parent
    assert first.read_bytes() == before and Path(str(first) + '.sig').exists()
    assert 'supersedes' not in json.loads(second.read_text(encoding='utf-8'))
    assert second.stem == bb.review_compute_id(json.loads(second.read_text(encoding='utf-8')))

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert sorted((r['verdict'], r['current']) for r in bb.review_list_shared(func)) == [
        ('approve', False), ('reject', True)]
    assert bb.review_approvals([func]) == set()
    capsys.readouterr()
    bb.command_reviews(func)
    out = capsys.readouterr().out
    assert out.index('current') < out.index('earlier')
    assert "superseded" not in out

    # Changing her mind again, within the same second: the approval counts, being her latest
    _become(monkeypatch, tmp_path, 'alice')
    third = bb.review_publish(func, 'approve', 'fine after all', ack=bb.REVIEW_ACKS)
    _become(monkeypatch, tmp_path, 'bob')
    assert [r['id'] for r in bb.review_list_shared(func) if r['current']] == [third.stem]
    assert bb.review_approvals([func]) == {func}


def test_review_pick_current_reads_the_latest_trusted_review_of_each_key():
    """Latest 'created' of a key counts, a tie goes to a rejection, and only trusted reviews count"""
    def review(id, key, verdict, created, status='trusted'):
        return {'id': id, 'public_key': f'ssh-ed25519 {key}', 'verdict': verdict, 'created': created,
                'status': status}

    reviews = [review('a1', 'AAAA', 'approve', '2026-01-01T00:00:00Z'),
               review('a2', 'AAAA', 'reject', '2026-01-02T00:00:00Z'),
               review('a3', 'AAAA', 'approve', '2026-01-03T00:00:00Z'),
               review('b1', 'BBBB', 'approve', '2026-01-04T00:00:00Z'),
               review('b2', 'BBBB', 'reject', '2026-01-04T00:00:00Z'),
               review('c1', 'CCCC', 'reject', '2026-01-05T00:00:00Z', status='untrusted'),
               review('d1', 'DDDD', 'reject', '2026-01-05T00:00:00Z', status='invalid')]
    # an untrusted or invalid review never counts, nor hides a trusted one of its own key
    assert bb.review_pick_current(reviews) == {'a3', 'b2'}
    assert bb.review_pick_current(reviews + [review('a4', 'AAAA', 'reject', '2026-01-09T00:00:00Z', 'untrusted')]
                                  ) == {'a3', 'b2'}


@needs_ssh_keygen
def test_review_asks_each_aspect_and_records_the_acknowledgements(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """bb review asks security, correctness and readability one by one; the review records the answers"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    monkeypatch.setattr(sys.stdin, 'isatty', lambda: True, raising=False)

    def review(*answers):
        asked = []
        answers = iter(answers)
        monkeypatch.setattr('builtins.input', lambda prompt='': asked.append(prompt) or next(answers))
        bb.command_review(func, '')
        return asked

    # A no, then skip: nothing is saved
    asked = review('y', 'n', 's')
    assert [q.split(':')[0] for q in asked] == ['Security', 'Correctness', 'Not acknowledged']
    assert bb.review_load_state() == set() and bb.review_load_rejections() == set()
    assert bb.review_list_shared(func) == []

    # A no, then reject: the rejection records what did hold
    review('y', 'y', 'n', 'r')
    assert bb.review_load_rejections() == {func}
    [rejection] = bb.review_list_shared(func)
    assert (rejection['verdict'], rejection['ack']) == ('reject', ['correctness', 'security'])

    # Yes to all three approves, with no further question
    asked = review('y', 'y', 'y')
    assert [q.split(':')[0] for q in asked] == ['Security', 'Correctness', 'Readability']
    assert bb.review_load_state() == {func}
    [approval] = [r for r in bb.review_list_shared(func) if r['current']]
    assert (approval['verdict'], approval['ack']) == ('approve', sorted(bb.REVIEW_ACKS))
    assert sorted(r['verdict'] for r in bb.review_list_shared(func)) == ['approve', 'reject']
    capsys.readouterr()
    bb.command_reviews(func)
    assert "acknowledged: correctness, readability, security" in capsys.readouterr().out


@needs_ssh_keygen
def test_peer_approval_counts_only_with_every_acknowledgement(mock_bb_dir, monkeypatch, tmp_path):
    """A trusted approval that does not acknowledge every aspect approves nothing"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    partial = bb.review_publish(func, 'approve', ack=['security', 'correctness'])
    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert [r['status'] for r in bb.review_list_shared(func)] == ['trusted']
    assert bb.review_approvals([func]) == set()

    _become(monkeypatch, tmp_path, 'alice')
    full = bb.review_publish(func, 'approve', ack=bb.REVIEW_ACKS)
    assert full != partial
    _become(monkeypatch, tmp_path, 'bob')
    assert bb.review_approvals([func]) == {func}

    # An aspect bb does not know makes the review invalid, signed or not
    record = json.loads(full.read_text(encoding='utf-8'))
    assert bb.review_acks(dict(record, ack=['security', 'vibes'])) is None
    assert bb.review_acks(dict(record, ack='security')) is None


@needs_ssh_keygen
def test_a_supersedes_field_of_an_older_review_retires_nothing(mock_bb_dir, monkeypatch, tmp_path):
    """A signed review still carrying 'supersedes' loads; the field is ignored, the latest review counts"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    rejection = bb.review_publish(func, 'reject', 'unsafe')
    record = json.loads(rejection.read_text(encoding='utf-8'))
    record.update(verdict='approve', ack=sorted(bb.REVIEW_ACKS), comment='', created='2000-01-01T00:00:00Z',
                  supersedes=[rejection.stem])
    old = rejection.parent / f"{bb.review_compute_id(record)}.json"
    old.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(alice_key), '-n', bb.REVIEW_SIGNATURE_NAMESPACE,
                    str(old)], check=True, capture_output=True)
    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert sorted((r['verdict'], r['status'], r['current']) for r in bb.review_list_shared(func)) == [
        ('approve', 'trusted', False), ('reject', 'trusted', True)]
    assert bb.review_approvals([func]) == set()


@needs_ssh_keygen
def test_a_forged_or_foreign_review_never_replaces_anyones_verdict(mock_bb_dir, monkeypatch, tmp_path):
    """Neither a forged file nor another reviewer can take the place of someone's review"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    rejection = bb.review_publish(func, 'reject', 'unsafe')

    # Forged: an unsigned, later approval in Alice's directory
    record = json.loads(rejection.read_text(encoding='utf-8'))
    record.update(verdict='approve', comment='', created='2999-01-01T00:00:00Z')
    forged = rejection.parent / f"{bb.review_compute_id(record)}.json"
    forged.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    # Eve signs a review of her own naming Alice's
    eve_key = _become(monkeypatch, tmp_path, 'eve')
    public_key = bb.helper_signing_identity()[1]
    record.update(reviewer={'name': 'eve', 'email': 'eve@example.org'}, public_key=public_key)
    hers = rejection.parent.parent / bb.helper_key_id(public_key) / f"{bb.review_compute_id(record)}.json"
    hers.parent.mkdir()
    hers.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(eve_key), '-n', bb.REVIEW_SIGNATURE_NAMESPACE,
                    str(hers)], check=True, capture_output=True)

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    current = {(r['reviewer']['name'], r['verdict'], r['status']) for r in bb.review_list_shared(func)}
    assert ('alice', 'reject', 'trusted') in current
    assert ('alice', 'approve', 'invalid') in current
    assert ('eve', 'approve', 'untrusted') in current
    assert [r['verdict'] for r in bb.review_list_shared(func) if r['current']] == ['reject']
    assert bb.review_approvals([func]) == set()


@needs_ssh_keygen
def test_trusted_rejection_blocks_and_own_verdict_wins(mock_bb_dir, monkeypatch, tmp_path):
    """A trusted peer's rejection cancels trusted approvals; your own verdict decides"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    bb.review_publish(func, 'approve', ack=bb.REVIEW_ACKS)
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
def test_forged_review_cannot_inject_a_second_signer_through_public_key(mock_bb_dir, monkeypatch, tmp_path):
    """A newline in public_key must not smuggle an attacker key into review_verify"""
    func = _pool_add("def two():\n    return 2\n")
    victim_key = _become(monkeypatch, tmp_path, 'victim')
    victim_pub = bb.helper_signing_identity()[1]
    attacker_key = _become(monkeypatch, tmp_path, 'attacker')
    attacker_pub = bb.helper_signing_identity()[1]

    # The forged record claims the victim's key (so helper_key_id, which reads
    # only the first two tokens, files it in the victim's directory) but appends
    # a second allowed_signers line binding the victim's email to the attacker's
    # key. It is signed with the attacker's key.
    record = {
        'schema_version': 2, 'function': func, 'verdict': 'approve',
        'reviewer': {'name': 'victim', 'email': 'victim@example.org'},
        'public_key': victim_pub + '\nvictim@example.org ' + attacker_pub,
        'created': '2026-11-24T00:00:00Z', 'comment': '', 'supersedes': [],
    }
    key_id = bb.helper_key_id(record['public_key'])
    assert key_id == bb.helper_key_id(victim_pub)  # files under the victim's directory
    directory = bb.storage_get_review_directory() / func[:2] / func[2:] / key_id
    directory.mkdir(parents=True)
    path = directory / f"{bb.review_compute_id(record)}.json"
    path.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(attacker_key),
                    '-n', bb.REVIEW_SIGNATURE_NAMESPACE, str(path)], check=True, capture_output=True)

    # The victim themselves must not see this as their own trusted approval
    _become(monkeypatch, tmp_path, 'victim')
    bb.storage_write_config({'user': {'name': 'victim', 'email': 'victim@example.org',
                                      'signing_key': str(victim_key), 'languages': ['eng']}})
    assert [r['status'] for r in bb.review_list_shared(func)] == ['invalid']
    assert bb.review_approvals([func]) == set()


def _only_step(func):
    [step] = bb.lineage_index()['by_function'][func]
    return step


@needs_ssh_keygen
def test_lineage_step_is_signed_when_you_have_a_key(mock_bb_dir, monkeypatch, tmp_path):
    """A step saved with a signing key names it; it is untrusted until you trust its author"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    step = _only_step(func)
    record = bb.lineage_index()['steps'][step]
    assert record['public_key'] == bb.helper_signing_identity()[1]
    assert bb.lineage_step_id(record) == step
    assert bb.lineage_signature_path(step).exists()
    assert bb.lineage_signature_path(step) in bb.anchor_list_files()
    assert bb.lineage_signature_path(step) in bb.anchor_list_files(func)

    _become(monkeypatch, tmp_path, 'bob')
    assert bb.lineage_signature_status(step) == 'untrusted'
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.lineage_signature_status(step) == 'trusted'
    # Trusting another email with her key does not make her steps trusted
    bb.command_trust_remove('alice@example.org')
    bb.command_trust_add('mallory@example.org', str(alice_key) + '.pub')
    assert bb.lineage_signature_status(step) == 'untrusted'


def test_lineage_step_without_a_key_is_unsigned(mock_bb_dir):
    """No signing key: the step is what it always was"""
    func = _pool_add("def one():\n    return 1\n")
    step = _only_step(func)
    assert 'public_key' not in bb.lineage_index()['steps'][step]
    assert not bb.lineage_signature_path(step).exists()
    assert bb.lineage_signature_status(step) == 'unsigned'


@needs_ssh_keygen
def test_lineage_step_given_its_metadata_is_not_signed(mock_bb_dir, monkeypatch, tmp_path):
    """Only your own step, saved now, is signed: never one recorded for another author"""
    _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    mapping = bb.lineage_index()['steps'][_only_step(func)]['mapping']
    other = bb.lineage_save(_only_step(func), func, 'eng', mapping, 'document',
                            metadata={'created': '2020-01-01T00:00:00Z', 'name': 'carol', 'email': 'c@example.org'})
    assert bb.lineage_signature_status(other) == 'unsigned'


@needs_ssh_keygen
def test_lineage_signature_tampering_is_invalid(mock_bb_dir, monkeypatch, tmp_path):
    """An altered step, a missing signature, a signature of something else: all invalid, never unsigned"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    step = _only_step(func)
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    path, signature = bb.storage_step_path(step), bb.lineage_signature_path(step)
    original, original_signature = path.read_text(encoding='utf-8'), signature.read_bytes()

    def status():
        bb.storage_cache_clear()
        return bb.lineage_signature_status(step)

    assert status() == 'trusted'
    path.write_text(original.replace('"comment": ""', '"comment": "mine"'), encoding='utf-8')
    assert status() == 'invalid'
    path.write_text(original, encoding='utf-8')

    signature.unlink()
    assert status() == 'invalid'

    # Stripping the key along with the signature does not make it an unsigned step
    record = json.loads(original)
    del record['public_key']
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding='utf-8')
    assert status() == 'invalid'
    path.write_text(original, encoding='utf-8')

    # A review signature by the same key is not a step signature
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(alice_key), '-n', bb.REVIEW_SIGNATURE_NAMESPACE,
                    str(path)], check=True, capture_output=True)
    assert status() == 'invalid'
    signature.write_bytes(original_signature)
    assert status() == 'trusted'


@needs_ssh_keygen
def test_lineage_step_is_saved_unsigned_when_signing_fails(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """A save never fails because of signing"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    alice_key.write_text('not a key', encoding='utf-8')
    bb._SIGNATURE_WARNED.clear()
    func = _pool_add("def one():\n    return 1\n")
    step = _only_step(func)
    assert capsys.readouterr().err.count("could not sign what you save") == 1  # said once, not per record
    assert 'public_key' not in bb.object_load(func) and bb.object_signature_status(func) == ('unsigned', '')
    assert not list(bb.storage_get_pool_directory().rglob('*.sig'))
    assert bb.lineage_signature_status(step) == 'unsigned'
    assert [p.name for p in bb.storage_get_lineage_directory().rglob('*') if p.is_file()] == [step[2:] + '.json']


@needs_ssh_keygen
def test_lineage_prune_takes_the_signature_along(mock_bb_dir, monkeypatch, tmp_path):
    _become(monkeypatch, tmp_path, 'alice')
    _pool_add("def one():\n    return 1\n")
    bb.command_rm('one@eng')
    assert [p for p in bb.storage_get_lineage_directory().rglob('*') if p.is_file()] == []


@needs_ssh_keygen
def test_log_says_who_signed(mock_bb_dir, monkeypatch, tmp_path, capsys):
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    _pool_add("def one():\n    return 1\n")
    _become(monkeypatch, tmp_path, 'bob')
    _pool_add("def two():\n    return 2\n")
    bb.command_log('one')
    assert "Signed: untrusted" in capsys.readouterr().out
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    capsys.readouterr()
    bb.command_log('one')
    assert "Signed: trusted" in capsys.readouterr().out
    bb.command_log('two')
    assert "Signed: trusted" in capsys.readouterr().out  # his own key


@needs_ssh_keygen
def test_object_and_mapping_are_signed(mock_bb_dir, monkeypatch, tmp_path):
    """What you add is signed: the object names your key, the mapping's signature holds it"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    mapping = bb.lineage_index()['steps'][_only_step(func)]['mapping']
    assert bb.object_load(func)['public_key'] == bb.helper_signing_identity()[1]
    assert 'public_key' not in bb.mapping_load_json(func, 'eng', mapping)  # content-addressed: the same for all
    assert bb.object_signature_status(func) == ('trusted', 'alice@example.org')
    assert bb.mapping_signature_status(func, 'eng', mapping) == ('trusted', 'alice@example.org')
    assert bb.signature_read_key(bb.signature_path(bb.storage_mapping_path(func, 'eng', mapping))) == \
        bb.helper_signing_identity()[1]

    _become(monkeypatch, tmp_path, 'bob')
    assert bb.object_signature_status(func) == ('untrusted', 'alice@example.org')
    assert bb.mapping_signature_status(func, 'eng', mapping) == ('untrusted', '')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert bb.object_signature_status(func)[0] == 'trusted'
    assert bb.mapping_signature_status(func, 'eng', mapping) == ('trusted', 'alice@example.org')

    # Bob saving the same code and names writes nothing: the files, and who signed them, stay
    _pool_add("def one():\n    return 1\n")
    assert bb.object_signature_status(func) == ('trusted', 'alice@example.org')
    assert len(bb.lineage_index()['by_function'][func]) == 2  # his own step, signed by him


@needs_ssh_keygen
def test_object_and_mapping_tampering_is_invalid(mock_bb_dir, monkeypatch, tmp_path):
    _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    mapping = bb.lineage_index()['steps'][_only_step(func)]['mapping']
    object_path, mapping_path = bb.storage_object_path(func), bb.storage_mapping_path(func, 'eng', mapping)

    # Signatures are for one kind of record: the mapping's does not sign the object
    object_signature = bb.signature_path(object_path).read_bytes()
    bb.signature_path(object_path).write_bytes(bb.signature_path(mapping_path).read_bytes())
    assert bb.object_signature_status(func)[0] == 'invalid'
    bb.signature_path(object_path).write_bytes(object_signature)
    assert bb.object_signature_status(func)[0] == 'trusted'

    object_path.write_text(object_path.read_text(encoding='utf-8').replace('alice', 'mallory', 1), encoding='utf-8')
    assert bb.object_signature_status(func)[0] == 'invalid'
    bb.signature_path(object_path).unlink()
    assert bb.object_signature_status(func)[0] == 'invalid'  # it names a key: not merely unsigned

    mapping_path.write_text(mapping_path.read_text(encoding='utf-8').replace('one', 'won'), encoding='utf-8')
    assert bb.mapping_signature_status(func, 'eng', mapping)[0] == 'invalid'
    bb.signature_path(mapping_path).unlink()
    assert bb.mapping_signature_status(func, 'eng', mapping)[0] == 'invalid'  # not what its hash names


def test_signature_read_key_refuses_what_is_not_a_signature(tmp_path):
    path = tmp_path / 'x.sig'
    for text in ['', 'garbage', '-----BEGIN SSH SIGNATURE-----\nAAAA\n-----END SSH SIGNATURE-----\n',
                 '-----BEGIN SSH SIGNATURE-----\n!!!\n-----END SSH SIGNATURE-----\n']:
        path.write_text(text, encoding='utf-8')
        assert bb.signature_read_key(path) is None
    assert bb.signature_read_key(tmp_path / 'missing.sig') is None


@needs_ssh_keygen
def test_worklog_and_its_messages_are_signed(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """A worklog names its original message and its key; a translation is signed by whoever wrote it"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    worklog_id, original = bb.worklog_save('eng', 'Explored', [])
    worklog = bb.worklog_load(worklog_id)
    assert worklog['message'] == original and worklog['public_key'] == bb.helper_signing_identity()[1]
    assert bb.worklog_signature_status(worklog) == ('trusted', 'alice@example.org')
    assert bb.worklog_message_signature_status(worklog_id, 'eng', original) == ('trusted', 'alice@example.org')

    _become(monkeypatch, tmp_path, 'bob')
    french = bb.worklog_save_message(worklog_id, 'fra', 'Exploré')
    assert bb.worklog_signature_status(worklog)[0] == 'untrusted'
    assert bb.worklog_message_signature_status(worklog_id, 'eng', original) == ('untrusted', '')
    assert bb.worklog_message_signature_status(worklog_id, 'fra', french) == ('trusted', 'bob@example.org')
    bb.command_worklog_show(f"{worklog_id}@fra")
    out = capsys.readouterr().out
    assert "Signed: untrusted" in out and "Message signed: trusted (bob@example.org)" in out
    bb.command_log()
    assert "Signed: untrusted" in capsys.readouterr().out

    path = bb.worklog_message_path(worklog_id, 'eng', original)
    path.write_text(path.read_text(encoding='utf-8').replace('Explored', 'Ignored'), encoding='utf-8')
    assert bb.worklog_message_signature_status(worklog_id, 'eng', original)[0] == 'invalid'
    signatures = [p for p in bb.anchor_list_files() if p.suffix == '.sig']
    assert len(signatures) == 3  # the worklog and its two messages


@needs_ssh_keygen
def test_show_says_what_is_signed(mock_bb_dir, monkeypatch, tmp_path, capsys):
    _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    capsys.readouterr()
    bb.code_show('one@eng')
    out = capsys.readouterr().out
    assert [line.split()[2] for line in out.splitlines() if line.startswith('# bb:')] == ['function', 'mapping', 'step']
    assert out.count('signed: trusted') == 3
    ast.parse(out)  # still the function's source
    assert func == bb.code_compute_function_hash(bb.code_prepare_source(out, 'eng')['normalized_code'], None)


def test_show_says_nothing_of_what_is_neither_signed_nor_anchored(mock_bb_dir, capsys):
    _pool_add("def one():\n    return 1\n")
    capsys.readouterr()
    bb.code_show('one@eng')
    assert '# bb:' not in capsys.readouterr().out


@needs_ssh_keygen
def test_rm_takes_the_signatures_along(mock_bb_dir, monkeypatch, tmp_path):
    _become(monkeypatch, tmp_path, 'alice')
    func = _pool_add("def one():\n    return 1\n")
    bb.command_rm('one@eng')
    assert sorted(p.name for p in bb.storage_get_pool_directory().rglob('*') if p.is_file()) == [
        'object.json', 'object.json.sig']
    bb.command_rm(func)
    assert [p for p in bb.storage_get_pool_directory().rglob('*') if p.is_file()] == []


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
    # Legacy objects were content-addressed too: store under the real hash of the
    # code, so object_hash_matches (which guards execution) accepts it.
    func = bb.code_compute_function_hash(code)
    bb.object_save(func, code, bb.code_create_metadata(), normalization=1)
    bb.mapping_save(func, 'eng', '', {'_bb_v_0': 'pick', '_bb_v_1': 'n', '_bb_v_2': module_name}, {})
    return func


@pytest.mark.parametrize('module_name', ['random', 'os'])
def test_legacy_inner_imports_are_not_repaired_from_the_mapping(mock_bb_dir, module_name):
    """Neither a mapping nor the 'normalization' field is hashed: neither changes what runs"""
    func = _save_legacy_inner_import(module_name)
    with pytest.raises(NameError):
        bb.code_load_normalized(func)(6)


def test_normalization_field_cannot_change_what_approved_code_does(mock_bb_dir):
    """Marking a function as old and mapping a variable to a module it imports renames nothing"""
    func = _pool_add("def pick(n):\n    import os\n    low = 1\n    return low + n\n")
    path = bb.storage_object_path(func)
    data = json.loads(path.read_text(encoding='utf-8'))
    data['normalization'] = 1
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    [mapping] = bb.storage_mapping_directory(func).glob('eng/*.json')
    names = json.loads(mapping.read_text(encoding='utf-8'))
    names['name_mapping'] = {norm: ('os' if name == 'low' else name) for norm, name in names['name_mapping'].items()}
    mapping.write_text(json.dumps(names), encoding='utf-8')
    bb.storage_cache_clear()
    assert bb.object_hash_matches(func, bb.object_load(func))  # neither is in the hash
    assert bb.code_load_normalized(func)(6) == 7


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
    assert bb.object_load(func)['normalization'] == bb.NORMALIZATION_VERSION == 4
    assert bb.object_normalization(func) == 4

    legacy = _save_legacy_inner_import('random')
    assert bb.object_normalization(legacy) == 1
    data = bb.object_load(legacy)
    data.pop('normalization')
    path = bb.storage_object_path(legacy)
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


def test_unparse_readable_after_ast_unparse():
    """Multi-line strings show triple-quoted, even after ast.unparse ran (Python 3.14 loads it lazily)."""
    import ast
    import bb
    tree = ast.parse('x = "a\\nb"\n')
    ast.unparse(tree)
    assert bb.code_unparse_readable(tree) == 'x = """a\nb"""'


def test_code_pretty_breaks_long_views_and_keeps_the_tree():
    import ast
    source = ('from beyondlive import h\n\n\ndef page(session):\n    """A page."""\n'
              "    return h.main(Class='page')[h.h1()['Studio ‹›'], h.p(id='summary')[describe(session['choices'], 'eng')], "
              "h.div(Class='row')[h.button(onClick=prev_option, title='Previous')['‹ Prev'], h.button(onClick=next_option)['Next ›']], "
              "h.p()[str(n) + (' item' if n == 1 else ' items') + ' left.'], [h.li()[x] for x in xs if x]]\n")
    pretty = bb.code_pretty(source)
    assert ast.dump(ast.parse(pretty)) == ast.dump(ast.parse(source))
    assert max(len(line) for line in pretty.split("\n")) <= 100
    assert "    return h.main(Class='page')[\n        h.h1()['Studio ‹›'],\n" in pretty
    assert "(' item' if n == 1 else ' items')" in pretty  # parentheses kept where precedence needs them


def test_code_pretty_leaves_short_code_and_lone_tuple_children_alone():
    import ast
    short = 'def f(x):\n    """F."""\n    return [x, x]\n'
    assert bb.code_pretty(short) == short
    lone_tuple = 'def f(h):\n    """F."""\n    return h.div()[(' + ', '.join(f"'item number {i}'" for i in range(12)) + '),]\n'
    pretty = bb.code_pretty(lone_tuple)
    assert ast.dump(ast.parse(pretty)) == ast.dump(ast.parse(lone_tuple))


def test_a_failing_check_says_which_part_was_false_and_what_it_compared():
    source = ('def check_double(double):\n    """Doubles."""\n    expected = 4\n'
              '    assert double(1) == 2 and double(2) == expected\n')
    status, detail = bb.check_run(_check_function(source), lambda n: n * 3, source)
    assert status == 'FAIL'
    assert detail == 'assertion failed: double(1) == 2 (3 == 2)'


def test_a_failing_assert_shows_its_message_first():
    source = 'def check_keep(f):\n    assert f([2, 1, 2]) == [1, 2, 2], "duplicates are kept"\n'
    assert bb.check_run(_check_function(source), lambda numbers: sorted(set(numbers)), source) == \
        ('FAIL', 'duplicates are kept: f([2, 1, 2]) == [1, 2, 2] ([1, 2] == [1, 2, 2])')


def test_a_failing_check_without_source_only_says_the_assert_failed():
    zero = _check_function('def check_zero(f):\n    assert f(0)\n')
    assert bb.check_run(zero, lambda n: n) == ('FAIL', 'assertion failed')
    source = 'def check_all(f):\n    """All."""\n    assert all(f(x) for x in [1, 2])\n'
    assert bb.check_run(_check_function(source), lambda n: n > 1, source) == \
        ('FAIL', 'assertion failed: all((f(x) for x in [1, 2])) is False')


def test_an_assert_raised_by_a_helper_is_reported_without_guessing():
    source = ('def check_helper(f):\n    def inner():\n        assert False, "inner broke"\n'
              '    assert True\n    inner()\n')
    assert bb.check_run(_check_function(source), lambda n: n, source) == ('FAIL', 'inner broke')


def test_asserts_in_a_check_survive_python_optimization():
    """python -O removes asserts it compiles; bb compiles checks with optimize=0"""
    code = ("import bb\nsource = 'def check_f(f):\\n    assert f(1) == 2\\n'\n"
            "namespace = {}\nexec(compile(source, '<c>', 'exec', optimize=0), namespace)\n"
            "print(bb.check_run(namespace['check_f'], lambda n: n, source)[0])")
    result = subprocess.run([sys.executable, '-O', '-c', code], cwd=Path(bb.__file__).parent,
                            capture_output=True, text=True)
    assert result.stdout.strip() == 'FAIL', result.stderr


def test_a_class_defined_inside_a_function_is_renamed_with_its_uses(mock_bb_dir):
    """The class name was kept while its uses were renamed: the stored code failed with a NameError"""
    english = ('def make(name):\n    class Member:\n        def __init__(self, n):\n            self.n = n\n\n'
               '        def label(self):\n            return "@" + self.n\n    return Member(name).label()\n')
    french = english.replace('Member', 'Membre').replace('name', 'nom')
    func_hash = _pool_add(english)
    assert bb.code_load_normalized(func_hash)('ada') == '@ada'
    assert _pool_add(french, lang='fra') == func_hash  # the class name is a name like any other
    prepared = bb.code_prepare_source(english, 'eng')
    assert 'class Member' not in prepared['normalized_code']
    shown = bb.code_denormalize(prepared['normalized_code'], prepared['name_mapping'], prepared['alias_mapping'])
    assert 'class Member:' in shown and 'return Member(name).label()' in shown


def test_search_compare_tells_namesakes_apart_by_their_checks(mock_bb_dir, capsys):
    """bb search --compare runs functions sharing a name against each other's checks, approved code only"""
    def check(name, body):
        return _pool_add(f"def {name}(f):\n    assert {body}\n")

    twice, sum_ = check('twice_two', 'f(2) == 4'), check('twice_three', 'f(3) == 6')
    product = _pool_add("def double(x):\n    return x * 2\n", checks=[twice])
    added = _pool_add("def double(x):\n    return x + x\n", checks=[sum_])
    partial = _pool_add("def double(x):\n    return 0 if x == 3 else x * 2\n", checks=[check('zero', 'f(0) == 0')])
    text = _pool_add("def double(x):\n    return str(x) * 2\n", checks=[check('text', "f(1) == '11'")])
    bare = _pool_add("def double(x):\n    return 2 * x\n")

    def search(*flags):
        capsys.readouterr()
        bb.command_search(['double'], as_json=True, compare='--compare' in flags)
        found = json.loads(capsys.readouterr().out)
        return {r['hash']: {o['hash']: o for o in r['relatives']} for r in found}

    # Without --compare nothing runs and nothing is said
    assert 'behaviour' not in search()[product][added]

    # Nothing runs unread: both functions and their checks are to approve
    unread = search('--compare')[product][added]
    assert (unread['behaviour'], unread['missing']) == (None, 4)

    bb.review_save_state(set(bb.storage_pool_index()['functions']))
    told = search('--compare')
    behaviour = lambda a, b: told[a][b]['behaviour']
    assert behaviour(product, added) == behaviour(added, product) == 'interchangeable'
    # partial passes the check of product, not that of added; both pass its own
    assert behaviour(partial, product) == 'interchangeable'
    assert (behaviour(added, partial), behaviour(partial, added)) == ('covers', 'covered')
    assert told[partial][added]['failed'] == ['twice_three']
    # Another kind of function altogether: each fails the other's check
    assert behaviour(text, product) == behaviour(product, text) == 'incompatible'
    # A function without checks claims nothing: passing the other's checks, it covers it
    assert (behaviour(bare, product), behaviour(product, bare)) == ('covers', 'covered')
    assert (behaviour(bare, text), behaviour(text, bare)) == ('covered', 'covers')

    capsys.readouterr()
    bb.command_search(['double'], compare=True)
    shown = capsys.readouterr().out
    assert f"Also double: {added}, another origin" in shown
    assert "interchangeable (each passes the other's checks)" in shown
    assert "incompatible (each fails a check of the other)" in shown


@pytest.mark.parametrize('name', ["x):\n    import os\n    return (0", "x; import os", "a.b", "class", "", "1x", 7, None])
def test_denormalize_refuses_mapping_names_that_are_not_identifiers(name):
    """ast.unparse writes names verbatim: a mapping's name must not be able to become code"""
    code = normalize_code_for_test("def _bb_v_0(_bb_v_1):\n    return bb.ref('" + 'a' * 64 + "')(_bb_v_1)")
    with pytest.raises(bb.MappingAlteredError, match="Mapping altered"):
        bb.code_denormalize(code, {'_bb_v_0': 'double', '_bb_v_1': name}, {'a' * 64: 'helper'})
    with pytest.raises(bb.MappingAlteredError, match="Mapping altered"):
        bb.code_denormalize(code, {'_bb_v_0': 'double', '_bb_v_1': 'x'}, {'a' * 64: name})
    # Names in any script are identifiers all the same
    assert 'def 二倍(数):' in bb.code_denormalize(code, {'_bb_v_0': '二倍', '_bb_v_1': '数'}, {'a' * 64: 'aide'})


def test_an_altered_mapping_is_not_shown_reviewed_or_compiled(mock_bb_dir, monkeypatch, capsys):
    """A mapping is not hashed with the code: one written by hand cannot put code in what is read or bundled"""
    func = _pool_add("def double(x):\n    return x * 2\n")
    [path] = bb.storage_mapping_directory(func).glob('eng/*.json')
    data = json.loads(path.read_text(encoding='utf-8'))
    data['name_mapping']['_bb_v_1'] = "x):\n    import os; os.system('echo pwned')\n    return (0"
    path.write_text(json.dumps(data), encoding='utf-8')
    bb.storage_cache_clear()

    monkeypatch.setattr('builtins.input', lambda *_: 'y')
    with pytest.raises(bb.MappingAlteredError):
        bb.command_review(func, '')
    assert bb.review_load_state() == set()
    with pytest.raises(SystemExit):
        bb.code_show(f'{func}@eng')
    captured = capsys.readouterr()
    assert 'os.system' not in captured.out and 'Mapping altered' in captured.err
    # The command line reports it instead of a traceback
    monkeypatch.setattr(sys, 'argv', ['bb', 'edit', f'{func}@eng', str(path.parent / 'draft.md')])
    with pytest.raises(SystemExit) as exit_info:
        bb.main()
    assert exit_info.value.code == 1
    assert 'Error: Mapping altered' in capsys.readouterr().err


@needs_ssh_keygen
def test_a_trusted_peer_cannot_speak_for_another_peers_key(mock_bb_dir, monkeypatch, tmp_path):
    """A review is verified against the key it names: a peer's signature on a record naming another's key is invalid"""
    func = _pool_add("def one():\n    return 1\n")
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    rejection = bb.review_publish(func, 'reject', 'unsafe')

    # Eve, trusted too, files an approval in Alice's directory, naming Alice's key, signed with her own
    eve_key = _become(monkeypatch, tmp_path, 'eve')
    record = json.loads(rejection.read_text(encoding='utf-8'))
    record.update(verdict='approve', ack=sorted(bb.REVIEW_ACKS), comment='', created='2999-01-01T00:00:00Z',
                  reviewer={'name': 'eve', 'email': 'eve@example.org'})
    forged = rejection.parent / f"{bb.review_compute_id(record)}.json"
    forged.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(eve_key), '-n', bb.REVIEW_SIGNATURE_NAMESPACE,
                    str(forged)], check=True, capture_output=True)

    _become(monkeypatch, tmp_path, 'bob')
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    bb.command_trust_add('eve@example.org', str(eve_key) + '.pub')
    current = {(r['verdict'], r['status']) for r in bb.review_list_shared(func)}
    assert current == {('reject', 'trusted'), ('approve', 'invalid')}
    assert bb.review_approvals([func]) == set()

    # Nor can she borrow Alice's email with her own key: the pair is not in Bob's list
    record.update(public_key=' '.join(Path(str(eve_key) + '.pub').read_text().split()[:2]),
                  reviewer={'name': 'alice', 'email': 'alice@example.org'})
    hers = rejection.parent.parent / bb.helper_key_id(record['public_key']) / f"{bb.review_compute_id(record)}.json"
    hers.parent.mkdir()
    hers.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(eve_key), '-n', bb.REVIEW_SIGNATURE_NAMESPACE,
                    str(hers)], check=True, capture_output=True)
    assert ('approve', 'untrusted') in {(r['verdict'], r['status']) for r in bb.review_list_shared(func)}
    assert bb.review_approvals([func]) == set()


def test_review_checks_every_hash_before_it_tells_or_asks_anything(mock_bb_dir, monkeypatch, capsys):
    """An approved function swapped since, or a swapped check shown under its function, stops the review"""
    check = _pool_add("def check_one(f):\n    assert f() == 1\n")
    func = _pool_add("def one():\n    return 1\n", checks=[check])
    monkeypatch.setattr('builtins.input', lambda *_: 'y')

    # Everything approved, then the function swapped: not "already reviewed"
    bb.review_save_state({check, func})
    genuine = bb.storage_object_path(func).read_text(encoding='utf-8')
    _swap_body(func, "def _bb_v_0():\n    return 'swapped'")
    with pytest.raises(SystemExit) as exit_info:
        bb.command_review(func, '')
    captured = capsys.readouterr()
    assert exit_info.value.code == 1 and 'already reviewed' not in captured.out
    assert 'does not match its stored code' in captured.err and 'Nothing reviewed' in captured.err
    bb.storage_object_path(func).write_text(genuine, encoding='utf-8')
    bb.storage_cache_clear()

    # An approved check swapped: its function is not shown with it, and not approved
    bb.review_save_state({check})
    _swap_body(check, "def _bb_v_0(_bb_v_1):\n    assert True, 'swapped'")
    with pytest.raises(SystemExit) as exit_info:
        bb.command_review(func, '')
    captured = capsys.readouterr()
    assert exit_info.value.code == 1 and 'swapped' not in captured.out
    assert bb.review_load_state() == {check}
    with pytest.raises(ValueError, match='does not match its stored code'):
        bb.review_render(check, ['eng'])
    # Nor is it handed to a translator
    with pytest.raises(SystemExit):
        bb.translate_render_file(check, 'eng', 'fra')
    assert 'refusing to translate it' in capsys.readouterr().err


def test_review_signer_formats_refuse_what_allowed_signers_would_read_otherwise():
    """One signer, one line: no newline (trailing ones too), no extra field, no pattern or list of principals"""
    key = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ7+abc/def='
    assert bb.REVIEW_KEY_FORMAT.fullmatch(key) and bb.REVIEW_KEY_FORMAT.fullmatch('sk-ssh-ed25519@openssh.com AAAA')
    for bad in (key + '\n', '\n' + key, key + ' comment', key.replace(' ', '\t'), key.replace(' ', '  '),
                key + '\nvictim@example.org ' + key, 'ssh-ed25519', ''):
        assert not bb.REVIEW_KEY_FORMAT.fullmatch(bad), repr(bad)
    assert bb.REVIEW_EMAIL_FORMAT.fullmatch('alice+bb@example.org')
    for bad in ('alice@example.org\n', '\nalice@example.org', 'a b', '', '*', '*@example.org', 'a@x,b@y',
                '!alice@example.org', '#alice@example.org', '"alice"@example.org', 'a?ice@example.org'):
        assert not bb.REVIEW_EMAIL_FORMAT.fullmatch(bad), repr(bad)


@needs_ssh_keygen
def test_a_review_whose_email_ends_with_a_newline_is_invalid(mock_bb_dir, monkeypatch, tmp_path):
    """split() counted one token and let it through to the allowed_signers line"""
    func = _pool_add("def two():\n    return 2\n")
    key = _become(monkeypatch, tmp_path, 'mallory')
    record = {
        'schema_version': 3, 'function': func, 'verdict': 'approve', 'ack': sorted(bb.REVIEW_ACKS),
        'reviewer': {'name': 'mallory', 'email': 'mallory@example.org\n'},
        'public_key': bb.helper_signing_identity()[1],
        'created': '2026-11-24T00:00:00Z', 'comment': '', 'supersedes': [],
    }
    directory = bb.storage_get_review_directory() / func[:2] / func[2:] / bb.helper_key_id(record['public_key'])
    directory.mkdir(parents=True)
    path = directory / f"{bb.review_compute_id(record)}.json"
    path.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', str(key),
                    '-n', bb.REVIEW_SIGNATURE_NAMESPACE, str(path)], check=True, capture_output=True)
    assert [r['status'] for r in bb.review_list_shared(func)] == ['invalid']


def test_a_lineage_step_whose_from_loops_back_does_not_hang(mock_bb_dir):
    """Steps written by hand can name each other; walking back from one must end"""
    func = _pool_add("def one():\n    return 1\n")
    step = _only_step(func)
    record = dict(bb.lineage_index()['steps'][step])
    directory = bb.storage_get_bb_directory() / 'pool' / 'lineage'
    first, second, end = 'a' * 64, 'b' * 64, 'c' * 64
    # The end of the name's lineage comes from a step that comes from a step that comes from it
    for step_id, previous in ((first, second), (second, first), (end, first)):
        path = directory / step_id[:2] / f"{step_id[2:]}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dict(record, **{'from': previous, 'relation': 'document'})), encoding='utf-8')
    bb.storage_cache_clear()
    steps = bb.lineage_index()['steps']
    if first not in steps:
        pytest.skip("steps whose id is not their hash are not loaded: no loop can be built")
    import signal

    def too_long(*_):
        raise AssertionError("helper_previous_version did not return")
    signal.signal(signal.SIGALRM, too_long)
    signal.alarm(5)
    try:
        assert end in bb.lineage_current_steps('eng', 'one', func)
        assert bb.helper_previous_version(func, 'eng', 'one') is None
    finally:
        signal.alarm(0)


@needs_ssh_keygen
@pytest.mark.parametrize('error', [PermissionError(13, 'Permission denied'), OSError(8, 'Exec format error'),
                                   UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'invalid start byte')])
def test_a_signing_error_saves_unsigned_never_half_a_save(mock_bb_dir, monkeypatch, tmp_path, capsys, error):
    """Whatever ssh-keygen does, a save goes through: unsigned, naming no key"""
    _become(monkeypatch, tmp_path, 'alice')
    real_run = subprocess.run

    def run(command, *args, **kwargs):
        if command[:3] == ['ssh-keygen', '-Y', 'sign']:
            raise error
        return real_run(command, *args, **kwargs)
    monkeypatch.setattr(bb.subprocess, 'run', run)
    bb._SIGNATURE_WARNED.clear()

    func = _pool_add("def one():\n    return 1\n")
    assert 'could not sign what you save' in capsys.readouterr().err
    path = bb.storage_object_path(func)
    assert 'public_key' not in json.loads(path.read_text(encoding='utf-8'))
    assert not bb.signature_path(path).exists()
    assert bb.object_signature_status(func)[0] == 'unsigned'
    step = _only_step(func)
    assert 'public_key' not in bb.lineage_index()['steps'][step]
    assert bb.lineage_signature_status(step) == 'unsigned'
    assert not list(bb.storage_get_bb_directory().rglob('*.sig'))


def test_search_describe_asks_how_a_function_uses_the_pool_once(mock_bb_dir, monkeypatch, capsys):
    """search_uses reads the mapping of every name: once per function, not per field and per dependency"""
    first, second = _pool_add("def one():\n    return 1\n"), _pool_add("def two():\n    return 2\n")
    caller = _pool_add(f"import bb\none = bb.ref('{first}')\ntwo = bb.ref('{second}')\n\n\n"
                       "def both():\n    return one() + two()\n")
    calls = []
    real = bb.search_uses
    monkeypatch.setattr(bb, 'search_uses', lambda function: calls.append(function['hash']) or real(function))
    capsys.readouterr()
    bb.command_search(['both'], as_json=True)
    [found] = json.loads(capsys.readouterr().out)
    assert found['hash'] == caller
    # Once for what it shows, once for what is out of date and once for what is behind
    # (each memoized apart); it was once per field, and again for every dependency
    assert calls.count(caller) <= 3, calls


@needs_ssh_keygen
def test_a_signature_is_verified_by_ssh_keygen_once(mock_bb_dir, monkeypatch, tmp_path, capsys):
    """bb log says of every signed record whether its signature holds: one process each the first time, none after"""
    alice_key = _become(monkeypatch, tmp_path, 'alice')
    funcs = [_pool_add(f"def f{n}():\n    return {n}\n") for n in range(3)]
    verifications, reads = [], []
    real_run, real_parse = subprocess.run, bb.trust_parse

    def run(command, *args, **kwargs):
        if command[:3] == ['ssh-keygen', '-Y', 'verify']:
            verifications.append(command)
        return real_run(command, *args, **kwargs)
    monkeypatch.setattr(bb.subprocess, 'run', run)
    monkeypatch.setattr(bb, 'trust_parse', lambda path: reads.append(path) or real_parse(path))

    def log():
        verifications.clear(), reads.clear(), capsys.readouterr()
        bb.command_log()
        return capsys.readouterr().out

    first = log()
    assert first.count('Signed: trusted') == 6 and len(verifications) == 6  # three objects, three steps
    assert len(reads) == 1  # who you trust is read once, not once per record
    assert log() == first and verifications == []

    # What was verified is private state: nobody can commit "verified" next to a forgery
    assert bb.signature_verified_path().parent == bb.storage_get_state_directory()
    assert not list(bb.storage_get_bb_directory().rglob('verified*'))
    # ... and another process starts from the file
    bb._SIGNATURE_VERIFIED.clear()
    assert log() == first and verifications == []

    # A record altered since is another content: checked again, and found out
    path = bb.storage_object_path(funcs[0])
    data = json.loads(path.read_text(encoding='utf-8'))
    data['metadata']['name'] = 'mallory'
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    bb.storage_cache_clear()
    altered = log()
    assert altered.count('Signed: INVALID') == 1 and len(verifications) == 1
    log()
    assert len(verifications) == 1  # a failure is never remembered

    # Someone else verifies for themselves, and trusts whom they trust
    _become(monkeypatch, tmp_path, 'bob')
    theirs = log()
    assert theirs.count('Signed: untrusted') == 5 and len(verifications) == 6
    bb.command_trust_add('alice@example.org', str(alice_key) + '.pub')
    assert log().count('Signed: trusted') == 5 and len(verifications) == 1
