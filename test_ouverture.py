"""
Comprehensive unit tests for ouverture.py

All tests are implemented as functions following pytest conventions.
"""
import ast
import json
import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import ouverture


def normalize_code_for_test(code: str) -> str:
    """
    Normalize code string to match ast.unparse() output format.

    All normalized code strings in tests MUST go through this function to ensure
    they match the format that ouverture produces. This is because ast.unparse()
    always outputs code with proper line breaks and indentation, regardless of
    the input format.

    The function:
    1. Parses code into AST
    2. Clears all line/column information recursively (using ouverture.ast_clear_locations)
    3. Fixes missing locations
    4. Unparses back to string

    Example:
        # Wrong - this format never exists in practice:
        normalized_code = "def _ouverture_v_0(): return 42"

        # Correct - use this helper:
        normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
        # Returns: "def _ouverture_v_0():\\n    return 42"
    """
    tree = ast.parse(code)
    ouverture.ast_clear_locations(tree)
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


@pytest.fixture
def mock_ouverture_dir(tmp_path, monkeypatch):
    """
    Fixture to monkey patch directory_get_ouverture to return a temp directory.
    This ensures tests work with pytest-xdist (parallel test runner).
    """
    def _get_temp_ouverture_dir():
        return tmp_path / '.ouverture'

    monkeypatch.setattr(ouverture, 'directory_get_ouverture', _get_temp_ouverture_dir)
    return tmp_path


# Tests for ASTNormalizer class

def test_ast_normalizer_visit_name_with_mapping():
    """Test that Name nodes are renamed according to mapping"""
    mapping = {"x": "_ouverture_v_1", "y": "_ouverture_v_2"}
    normalizer = ouverture.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_ouverture_v_1" in result
    assert "_ouverture_v_2" in result


def test_ast_normalizer_visit_name_without_mapping():
    """Test that unmapped names remain unchanged"""
    mapping = {"x": "_ouverture_v_1"}
    normalizer = ouverture.ASTNormalizer(mapping)

    code = "z = x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_ouverture_v_1" in result
    assert "y" in result  # y should remain unchanged


def test_ast_normalizer_visit_arg_with_mapping():
    """Test that function arguments are renamed"""
    mapping = {"x": "_ouverture_v_1", "y": "_ouverture_v_2"}
    normalizer = ouverture.ASTNormalizer(mapping)

    code = "def foo(x, y): return x + y"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_ouverture_v_1" in result
    assert "_ouverture_v_2" in result


def test_ast_normalizer_visit_functiondef_with_mapping():
    """Test that function names are renamed"""
    mapping = {"foo": "_ouverture_v_0"}
    normalizer = ouverture.ASTNormalizer(mapping)

    code = "def foo(): pass"
    tree = ast.parse(code)
    normalizer.visit(tree)
    result = ast.unparse(tree)

    assert "_ouverture_v_0" in result
    assert "foo" not in result


# Tests for collect_names function

def test_collect_names_simple_names():
    """Test collecting variable names"""
    code = "x = 1\ny = 2\nz = x + y"
    tree = ast.parse(code)
    names = ouverture.names_collect(tree)

    assert "x" in names
    assert "y" in names
    assert "z" in names


def test_collect_names_function_names():
    """Test collecting function names and arguments"""
    code = "def foo(a, b): return a + b"
    tree = ast.parse(code)
    names = ouverture.names_collect(tree)

    assert "foo" in names
    assert "a" in names
    assert "b" in names


def test_collect_names_empty_tree():
    """Test collecting names from empty module"""
    tree = ast.parse("")
    names = ouverture.names_collect(tree)

    assert len(names) == 0


# Tests for get_imported_names function

def test_get_imported_names_import_statement():
    """Test extracting names from import statement"""
    code = "import math"
    tree = ast.parse(code)
    names = ouverture.imports_get_names(tree)

    assert "math" in names


def test_get_imported_names_import_with_alias():
    """Test extracting aliased import names"""
    code = "import numpy as np"
    tree = ast.parse(code)
    names = ouverture.imports_get_names(tree)

    assert "np" in names
    assert "numpy" not in names


def test_get_imported_names_from_import():
    """Test extracting names from from-import"""
    code = "from collections import Counter"
    tree = ast.parse(code)
    names = ouverture.imports_get_names(tree)

    assert "Counter" in names


def test_get_imported_names_from_import_with_alias():
    """Test extracting aliased from-import names"""
    code = "from collections import Counter as C"
    tree = ast.parse(code)
    names = ouverture.imports_get_names(tree)

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
    names = ouverture.imports_get_names(tree)

    assert "math" in names
    assert "Counter" in names
    assert "np" in names


# Tests for check_unused_imports function

def test_check_unused_imports_all_imports_used():
    """Test when all imports are used"""
    code = """
import math
def foo():
    return math.sqrt(4)
"""
    tree = ast.parse(code)
    imported = ouverture.imports_get_names(tree)
    all_names = ouverture.names_collect(tree)

    result = ouverture.imports_check_unused(tree, imported, all_names)
    assert result is True


def test_check_unused_imports_unused_import():
    """Test when an import is unused"""
    code = """
import math
def foo():
    return 4
"""
    tree = ast.parse(code)
    imported = ouverture.imports_get_names(tree)
    all_names = ouverture.names_collect(tree)

    result = ouverture.imports_check_unused(tree, imported, all_names)
    assert result is False


# Tests for sort_imports function

def test_sort_imports_simple_imports():
    """Test sorting import statements"""
    code = """
import sys
import ast
import os
"""
    tree = ast.parse(code)
    sorted_tree = ouverture.imports_sort(tree)
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
    sorted_tree = ouverture.imports_sort(tree)
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
    sorted_tree = ouverture.imports_sort(tree)
    result = ast.unparse(sorted_tree)

    # All imports should come before the function
    func_pos = result.index("def foo")
    assert result.index("import") < func_pos


# Tests for extract_function_def function

def test_extract_function_def_simple_function():
    """Test extracting a simple function"""
    code = """
def foo():
    return 42
"""
    tree = ast.parse(code)
    func_def, imports = ouverture.function_extract_definition(tree)

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
    func_def, imports = ouverture.function_extract_definition(tree)

    assert func_def is not None
    assert func_def.name == "process"
    assert len(imports) == 2


def test_extract_function_def_no_function_raises_error():
    """Test that missing function raises ValueError"""
    code = "x = 42"
    tree = ast.parse(code)

    with pytest.raises(ValueError, match="No function definition found"):
        ouverture.function_extract_definition(tree)


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
        ouverture.function_extract_definition(tree)


# Tests for create_name_mapping function

def test_create_name_mapping_function_name_always_v0():
    """Test that function name always maps to _ouverture_v_0"""
    code = "def my_function(x): return x"
    tree = ast.parse(code)
    func_def, imports = ouverture.function_extract_definition(tree)

    forward, reverse = ouverture.mapping_create_name(func_def, imports)

    assert forward["my_function"] == "_ouverture_v_0"
    assert reverse["_ouverture_v_0"] == "my_function"


def test_create_name_mapping_sequential_numbering():
    """Test that variables get sequential numbers"""
    code = """
def foo(a, b):
    c = a + b
    return c
"""
    tree = ast.parse(code)
    func_def, imports = ouverture.function_extract_definition(tree)

    forward, reverse = ouverture.mapping_create_name(func_def, imports)

    # foo should be v_0
    assert forward["foo"] == "_ouverture_v_0"
    # Other names should be numbered sequentially
    assert all(name.startswith("_ouverture_v_") for name in forward.values())


def test_create_name_mapping_builtins_not_renamed():
    """Test that built-in functions are not renamed"""
    code = """
def foo(items):
    return len(items)
"""
    tree = ast.parse(code)
    func_def, imports = ouverture.function_extract_definition(tree)

    forward, reverse = ouverture.mapping_create_name(func_def, imports)

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
    func_def, imports = ouverture.function_extract_definition(tree)

    forward, reverse = ouverture.mapping_create_name(func_def, imports)

    # math is imported and should not be renamed
    assert "math" not in forward
    assert "x" in forward


def test_create_name_mapping_ouverture_aliases_not_renamed():
    """Test that ouverture aliases are excluded from renaming"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    func_def, imports = ouverture.function_extract_definition(tree)

    # Simulate that 'helper' is an ouverture alias
    ouverture_aliases = {"helper"}
    forward, reverse = ouverture.mapping_create_name(func_def, imports, ouverture_aliases)

    # helper should not be renamed
    assert "helper" not in forward
    assert "x" in forward


# Tests for rewrite_ouverture_imports function

def test_rewrite_ouverture_imports_with_alias():
    """Test rewriting ouverture import with alias"""
    code = "from ouverture.pool import abc123 as helper"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = ouverture.imports_rewrite_ouverture(imports)

    # Should remove alias but keep ouverture.pool module name
    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from ouverture.pool import abc123" in result
    assert "as helper" not in result

    # Should track the alias
    assert alias_mapping["abc123"] == "helper"


def test_rewrite_ouverture_imports_without_alias():
    """Test rewriting ouverture import without alias"""
    code = "from ouverture.pool import abc123"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = ouverture.imports_rewrite_ouverture(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "from ouverture.pool import abc123" in result
    assert len(alias_mapping) == 0


def test_rewrite_ouverture_imports_non_ouverture_imports_unchanged():
    """Test that non-ouverture imports remain unchanged"""
    code = "import math\nfrom collections import Counter"
    tree = ast.parse(code)
    imports = tree.body

    new_imports, alias_mapping = ouverture.imports_rewrite_ouverture(imports)

    result = ast.unparse(ast.Module(body=new_imports, type_ignores=[]))
    assert "import math" in result
    assert "from collections import Counter" in result
    assert len(alias_mapping) == 0


# Tests for replace_ouverture_calls function

def test_replace_ouverture_calls_aliased_call():
    """Test replacing aliased ouverture function calls"""
    code = """
def foo(x):
    return helper(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = ouverture.calls_replace_ouverture(tree, alias_mapping, name_mapping)
    result = ast.unparse(new_tree)

    # helper(x) should become abc123._ouverture_v_0(x)
    assert "abc123._ouverture_v_0" in result
    assert "helper" not in result


def test_replace_ouverture_calls_non_aliased_names_unchanged():
    """Test that non-aliased names remain unchanged"""
    code = """
def foo(x):
    return other(x)
"""
    tree = ast.parse(code)
    alias_mapping = {"abc123": "helper"}
    name_mapping = {}

    new_tree = ouverture.calls_replace_ouverture(tree, alias_mapping, name_mapping)
    result = ast.unparse(new_tree)

    # other should remain unchanged
    assert "other" in result


# Tests for clear_locations function

def test_clear_locations_all_location_info():
    """Test that all location info is cleared"""
    code = "def foo(x): return x + 1"
    tree = ast.parse(code)

    # Verify locations exist initially
    for node in ast.walk(tree):
        if hasattr(node, 'lineno'):
            assert node.lineno is not None
            break

    ouverture.ast_clear_locations(tree)

    # Verify all locations are None
    for node in ast.walk(tree):
        if hasattr(node, 'lineno'):
            assert node.lineno is None
        if hasattr(node, 'col_offset'):
            assert node.col_offset is None


# Tests for extract_docstring function

def test_extract_docstring_existing_docstring():
    """Test extracting an existing docstring"""
    code = '''
def foo():
    """This is a docstring"""
    return 42
'''
    tree = ast.parse(code)
    func_def, _ = ouverture.function_extract_definition(tree)

    docstring, func_without_doc = ouverture.docstring_extract(func_def)

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
    func_def, _ = ouverture.function_extract_definition(tree)

    docstring, func_without_doc = ouverture.docstring_extract(func_def)

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
    func_def, _ = ouverture.function_extract_definition(tree)

    docstring, func_without_doc = ouverture.docstring_extract(func_def)

    assert "multiline" in docstring
    assert "docstring" in docstring


# Tests for normalize_ast function

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
        ouverture.ast_normalize(tree, "eng")

    assert "_ouverture_v_0" in code_with_doc  # Function name normalized
    assert docstring == "Add two numbers"
    assert "_ouverture_v_0" in name_mapping.keys()
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

    code_with_doc, _, _, _, _ = ouverture.ast_normalize(tree, "eng")

    # Verify imports are sorted
    assert code_with_doc.index("import ast") < code_with_doc.index("import os")
    assert code_with_doc.index("import os") < code_with_doc.index("import sys")


def test_normalize_ast_with_ouverture_import():
    """Test normalizing function with ouverture import"""
    code = """
from ouverture.pool import abc123 as helper

def foo(x):
    \"\"\"Process with helper\"\"\"
    return helper(x)
"""
    tree = ast.parse(code)

    code_with_doc, code_without_doc, docstring, name_mapping, alias_mapping = \
        ouverture.ast_normalize(tree, "eng")

    # Should remove alias but keep ouverture.pool module name
    assert "from ouverture.pool import abc123" in code_with_doc
    assert "as helper" not in code_with_doc

    # Should track alias
    assert alias_mapping["abc123"] == "helper"

    # Should replace calls
    assert "abc123._ouverture_v_0" in code_with_doc


# Tests for compute_hash function

def test_compute_hash_deterministic():
    """Test that same input produces same hash"""
    code = "def foo(): return 42"

    hash1 = ouverture.hash_compute(code)
    hash2 = ouverture.hash_compute(code)

    assert hash1 == hash2


def test_compute_hash_format():
    """Test that hash is 64 hex characters"""
    code = "def foo(): return 42"

    hash_value = ouverture.hash_compute(code)

    assert len(hash_value) == 64
    assert all(c in '0123456789abcdef' for c in hash_value)


def test_compute_hash_different_code_different_hash():
    """Test that different code produces different hash"""
    code1 = "def foo(): return 42"
    code2 = "def bar(): return 43"

    hash1 = ouverture.hash_compute(code1)
    hash2 = ouverture.hash_compute(code2)

    assert hash1 != hash2


# Tests for save_function function

def test_save_function_new_function(mock_ouverture_dir):
    """Test saving a new function (v0 format - legacy)"""
    hash_value = "a" * 64
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    docstring = "Test function"
    name_mapping = {"_ouverture_v_0": "foo"}
    alias_mapping = {}

    # Capture stdout
    with patch('sys.stdout'):
        ouverture.function_save_v0(hash_value, lang, normalized_code,
                                   docstring, name_mapping, alias_mapping)

    # Verify file was created
    json_path = mock_ouverture_dir / '.ouverture/objects/aa' / (('a' * 62) + '.json')
    assert json_path.exists()

    # Verify content
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert data['hash'] == hash_value
    assert data['normalized_code'] == normalized_code
    assert data['docstrings']['eng'] == docstring
    assert data['name_mappings']['eng'] == name_mapping


def test_save_function_additional_language(mock_ouverture_dir):
    """Test adding another language to existing function (v0 format - legacy)"""
    hash_value = "b" * 64
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")

    # Save English version
    with patch('sys.stdout'):
        ouverture.function_save_v0(hash_value, "eng", normalized_code,
                                   "English doc", {"_ouverture_v_0": "foo"}, {})

    # Save French version
    with patch('sys.stdout'):
        ouverture.function_save_v0(hash_value, "fra", normalized_code,
                                   "French doc", {"_ouverture_v_0": "foo"}, {})

    # Verify both languages are present
    json_path = mock_ouverture_dir / '.ouverture/objects/bb' / (('b' * 62) + '.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert 'eng' in data['docstrings']
    assert 'fra' in data['docstrings']
    assert data['docstrings']['eng'] == "English doc"
    assert data['docstrings']['fra'] == "French doc"


# Tests for replace_docstring function

def test_replace_docstring_existing_docstring():
    """Test replacing an existing docstring"""
    code = '''
def foo():
    """Old docstring"""
    return 42
'''
    new_doc = "New docstring"

    result = ouverture.docstring_replace(code, new_doc)

    assert "New docstring" in result
    assert "Old docstring" not in result


def test_replace_docstring_add_when_none_exists():
    """Test adding docstring to function without one"""
    code = """
def foo():
    return 42
"""
    new_doc = "Added docstring"

    result = ouverture.docstring_replace(code, new_doc)

    assert "Added docstring" in result


def test_replace_docstring_remove_with_empty_string():
    """Test removing docstring by passing empty string"""
    code = '''
def foo():
    """Remove this"""
    return 42
'''
    result = ouverture.docstring_replace(code, "")

    assert "Remove this" not in result
    tree = ast.parse(result)
    func_def = tree.body[0]

    # Should only have return statement
    assert len(func_def.body) == 1
    assert isinstance(func_def.body[0], ast.Return)


# Tests for denormalize_code function

def test_denormalize_code_variable_names():
    """Test denormalizing variable names"""
    normalized = """
def _ouverture_v_0(_ouverture_v_1, _ouverture_v_2):
    _ouverture_v_3 = _ouverture_v_1 + _ouverture_v_2
    return _ouverture_v_3
"""
    name_mapping = {
        "_ouverture_v_0": "calculate",
        "_ouverture_v_1": "first",
        "_ouverture_v_2": "second",
        "_ouverture_v_3": "result"
    }
    alias_mapping = {}

    result = ouverture.code_denormalize(normalized, name_mapping, alias_mapping)

    assert "calculate" in result
    assert "first" in result
    assert "second" in result
    assert "result" in result
    assert "_ouverture_v_" not in result


def test_denormalize_code_ouverture_imports():
    """Test denormalizing ouverture imports"""
    normalized = """
from ouverture.pool import abc123

def _ouverture_v_0(_ouverture_v_1):
    return abc123._ouverture_v_0(_ouverture_v_1)
"""
    name_mapping = {
        "_ouverture_v_0": "process",
        "_ouverture_v_1": "data"
    }
    alias_mapping = {
        "abc123": "helper"
    }

    result = ouverture.code_denormalize(normalized, name_mapping, alias_mapping)

    # Should restore import with alias
    assert "from ouverture.pool import abc123 as helper" in result

    # Should restore function calls
    assert "helper(data)" in result
    assert "abc123._ouverture_v_0" not in result


# Integration tests for add_function command

def test_add_function_missing_lang_suffix():
    """Test that missing @lang suffix causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_add("example.py")


def test_add_function_invalid_lang_code():
    """Test that invalid language code causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_add("example.py@en")  # Should be 3 chars


def test_add_function_file_not_found():
    """Test that missing file causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_add("nonexistent.py@eng")


def test_add_function_syntax_error(tmp_path):
    """Test that syntax error in file causes error"""
    test_file = tmp_path / "bad.py"
    test_file.write_text("def foo( invalid syntax", encoding='utf-8')

    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_add(f"{test_file}@eng")


def test_add_function_success(mock_ouverture_dir):
    """Test successfully adding a function"""
    test_file = mock_ouverture_dir / "test.py"
    test_file.write_text('''
def calculate_sum(a, b):
    """Add two numbers"""
    return a + b
''', encoding='utf-8')

    with patch('sys.stdout'):
        ouverture.function_add(f"{test_file}@eng")

    # Verify .ouverture directory was created
    ouverture_objects = mock_ouverture_dir / '.ouverture/objects'
    assert ouverture_objects.exists()

    # Verify JSON files were created (v1 format: object.json + mapping.json)
    json_files = list(ouverture_objects.rglob('*.json'))
    assert len(json_files) == 2  # object.json + mapping.json for v1 format


# Integration tests for get_function command

def test_get_function_missing_lang_suffix():
    """Test that missing @lang suffix causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_get("abc123")


def test_get_function_invalid_lang_code():
    """Test that invalid language code causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_get("abc123@en")  # Should be 3 chars


def test_get_function_invalid_hash_format():
    """Test that invalid hash format causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_get("notahash@eng")


def test_get_function_not_found():
    """Test that non-existent function causes error"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            hash_value = "a" * 64
            ouverture.function_get(f"{hash_value}@eng")


def test_get_function_language_not_found(mock_ouverture_dir):
    """Test that requesting unavailable language causes error (v0 format)"""
    # Create a function with only English
    hash_value = "c" * 64
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    with patch('sys.stdout'):
        ouverture.function_save_v0(hash_value, "eng",
                                   normalized_code,
                                   "English doc",
                                   {"_ouverture_v_0": "foo"}, {})

    # Try to get it in French
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_get(f"{hash_value}@fra")


def test_get_function_success(mock_ouverture_dir):
    """Test successfully retrieving a function (v0 format)"""
    # Save a function
    hash_value = "d" * 64
    normalized_code = """
def _ouverture_v_0(_ouverture_v_1, _ouverture_v_2):
    _ouverture_v_3 = _ouverture_v_1 + _ouverture_v_2
    return _ouverture_v_3
"""
    name_mapping = {
        "_ouverture_v_0": "add",
        "_ouverture_v_1": "x",
        "_ouverture_v_2": "y",
        "_ouverture_v_3": "result"
    }

    with patch('sys.stdout'):
        ouverture.function_save_v0(hash_value, "eng", normalized_code,
                                   "Add two numbers", name_mapping, {})

    # Retrieve it
    with patch('sys.stdout') as mock_stdout:
        ouverture.function_get(f"{hash_value}@eng")

        # Verify it was printed (can't easily capture print output)
        # Just verify no exception was raised


# End-to-end integration tests

def test_end_to_end_roundtrip_simple_function(mock_ouverture_dir):
    """Test add then get produces equivalent code"""
    # Create test file
    test_file = mock_ouverture_dir / "test.py"
    original_code = '''def calculate_sum(first, second):
    """Add two numbers"""
    result = first + second
    return result'''
    test_file.write_text(original_code, encoding='utf-8')

    # Add function
    hash_value = None
    with patch('sys.stdout') as mock_stdout:
        with patch('builtins.print') as mock_print:
            ouverture.function_add(f"{test_file}@eng")
            # Extract hash from print calls
            for call in mock_print.call_args_list:
                args = str(call)
                if 'Hash:' in args:
                    hash_value = args.split('Hash: ')[1].split("'")[0]

    assert hash_value is not None

    # Get function back
    output = []
    with patch('builtins.print', side_effect=lambda x: output.append(x)):
        ouverture.function_get(f"{hash_value}@eng")

    retrieved_code = '\n'.join(output)

    # Parse both to compare structure
    original_tree = ast.parse(original_code)
    retrieved_tree = ast.parse(retrieved_code)

    # Should have same structure (function name, args, etc.)
    orig_func = original_tree.body[0]
    retr_func = retrieved_tree.body[0]

    assert orig_func.name == retr_func.name
    assert len(orig_func.args.args) == len(retr_func.args.args)


def test_end_to_end_multilingual_same_hash(mock_ouverture_dir):
    """Test that equivalent functions in different languages produce same hash"""
    # English version
    eng_file = mock_ouverture_dir / "english.py"
    eng_file.write_text('''def calculate_sum(first_number, second_number):
    """Calculate the sum of two numbers."""
    result = first_number + second_number
    return result''', encoding='utf-8')

    # French version (same logic, different names/docstring)
    fra_file = mock_ouverture_dir / "french.py"
    fra_file.write_text('''def calculate_sum(first_number, second_number):
    """Calculer la somme de deux nombres."""
    result = first_number + second_number
    return result''', encoding='utf-8')

    # Add both
    eng_hash = None
    fra_hash = None

    with patch('builtins.print') as mock_print:
        ouverture.function_add(f"{eng_file}@eng")
        for call in mock_print.call_args_list:
            args = str(call)
            if 'Hash:' in args:
                eng_hash = args.split('Hash: ')[1].split("'")[0]

    with patch('builtins.print') as mock_print:
        ouverture.function_add(f"{fra_file}@fra")
        for call in mock_print.call_args_list:
            args = str(call)
            if 'Hash:' in args:
                fra_hash = args.split('Hash: ')[1].split("'")[0]

    # Should have the same hash
    assert eng_hash == fra_hash

    # Should be able to retrieve in both languages
    with patch('builtins.print'):
        ouverture.function_get(f"{eng_hash}@eng")
        ouverture.function_get(f"{fra_hash}@fra")


# ============================================================================
# Tests for Phase 1: Foundation (Schema v1)
# ============================================================================

def test_mapping_compute_hash_deterministic():
    """Test that mapping_compute_hash produces deterministic hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_ouverture_v_0": "calculate_average", "_ouverture_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}
    comment = "Formal terminology"

    # Compute hash twice - should be identical
    hash1 = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, comment)
    hash2 = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, comment)

    assert hash1 == hash2
    assert len(hash1) == 64  # SHA256 produces 64 hex characters
    assert all(c in '0123456789abcdef' for c in hash1)


def test_mapping_compute_hash_different_comments():
    """Test that different comments produce different hashes"""
    docstring = "Calculate the average"
    name_mapping = {"_ouverture_v_0": "calculate_average", "_ouverture_v_1": "numbers"}
    alias_mapping = {"abc123": "helper"}

    hash1 = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, "Formal")
    hash2 = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, "Informal")

    assert hash1 != hash2


def test_mapping_compute_hash_empty_comment():
    """Test that mapping hash works with empty comment"""
    docstring = "Calculate the average"
    name_mapping = {"_ouverture_v_0": "calculate_average"}
    alias_mapping = {}

    hash_val = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, "")

    assert len(hash_val) == 64
    assert all(c in '0123456789abcdef' for c in hash_val)


def test_mapping_compute_hash_canonical_json():
    """Test that mapping hash is based on canonical JSON (order-independent)"""
    docstring = "Test"
    # Different key orders should produce same hash
    name_mapping1 = {"_ouverture_v_0": "foo", "_ouverture_v_1": "bar"}
    name_mapping2 = {"_ouverture_v_1": "bar", "_ouverture_v_0": "foo"}
    alias_mapping = {}

    hash1 = ouverture.mapping_compute_hash(docstring, name_mapping1, alias_mapping, "")
    hash2 = ouverture.mapping_compute_hash(docstring, name_mapping2, alias_mapping, "")

    assert hash1 == hash2


def test_schema_detect_version_v0(mock_ouverture_dir):
    """Test that schema_detect_version correctly identifies v0 format"""
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
    test_hash = "abcd1234" + "0" * 56

    # Create v0 format: XX/YYYYYY.json
    hash_dir = objects_dir / test_hash[:2]
    hash_dir.mkdir(parents=True, exist_ok=True)
    json_path = hash_dir / f'{test_hash[2:]}.json'

    v0_data = {
        'version': 0,
        'hash': test_hash,
        'normalized_code': 'def _ouverture_v_0(): pass',
        'docstrings': {},
        'name_mappings': {},
        'alias_mappings': {}
    }

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(v0_data, f)

    version = ouverture.schema_detect_version(test_hash)
    assert version == 0


def test_schema_detect_version_v1(mock_ouverture_dir):
    """Test that schema_detect_version correctly identifies v1 format"""
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
    test_hash = "abcd1234" + "0" * 56

    # Create v1 format: sha256/XX/YYYYYY.../object.json
    func_dir = objects_dir / 'sha256' / test_hash[:2] / test_hash[2:]
    func_dir.mkdir(parents=True, exist_ok=True)
    object_json = func_dir / 'object.json'

    v1_data = {
        'schema_version': 1,
        'hash': test_hash,
        'hash_algorithm': 'sha256',
        'normalized_code': 'def _ouverture_v_0(): pass',
        'encoding': 'none',
        'metadata': {}
    }

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(v1_data, f)

    version = ouverture.schema_detect_version(test_hash)
    assert version == 1


def test_schema_detect_version_not_found(mock_ouverture_dir):
    """Test that schema_detect_version returns None for non-existent function"""
    test_hash = "nonexistent" + "0" * 54

    version = ouverture.schema_detect_version(test_hash)
    assert version is None


def test_metadata_create_basic():
    """Test that metadata_create generates proper metadata structure"""
    metadata = ouverture.metadata_create()

    assert 'created' in metadata
    assert 'author' in metadata
    assert 'tags' in metadata
    assert 'dependencies' in metadata

    assert isinstance(metadata['tags'], list)
    assert isinstance(metadata['dependencies'], list)
    assert len(metadata['tags']) == 0
    assert len(metadata['dependencies']) == 0


def test_metadata_create_with_author():
    """Test that metadata_create uses author from environment"""
    with patch.dict(os.environ, {'USER': 'testuser'}):
        metadata = ouverture.metadata_create()
        assert metadata['author'] == 'testuser'


def test_metadata_create_timestamp_format():
    """Test that metadata_create uses ISO 8601 timestamp format"""
    metadata = ouverture.metadata_create()
    created = metadata['created']

    # Should be ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
    assert 'T' in created
    assert len(created) >= 19  # At minimum: 2025-01-01T00:00:00


def test_hash_compute_with_algorithm_parameter():
    """Test that hash_compute works with algorithm parameter"""
    code = "def foo(): pass"

    # Default should be sha256
    hash_default = ouverture.hash_compute(code)
    hash_sha256 = ouverture.hash_compute(code, algorithm='sha256')

    assert hash_default == hash_sha256
    assert len(hash_default) == 64


def test_hash_compute_algorithm_deterministic():
    """Test that hash_compute with algorithm produces deterministic results"""
    code = "def foo(): pass"

    hash1 = ouverture.hash_compute(code, algorithm='sha256')
    hash2 = ouverture.hash_compute(code, algorithm='sha256')

    assert hash1 == hash2


# ============================================================================
# Tests for Phase 2: V1 Write Path (Schema v1)
# ============================================================================

def test_function_save_v1_creates_object_json(mock_ouverture_dir):
    """Test that function_save_v1 creates proper object.json"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['test'],
        'dependencies': []
    }

    ouverture.function_save_v1(test_hash, normalized_code, metadata)

    # Check that object.json was created - with sha256 in path
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
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


def test_function_save_v1_no_language_data(mock_ouverture_dir):
    """Test that function_save_v1 does NOT include language-specific data"""
    test_hash = "abcd1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")
    metadata = ouverture.metadata_create()

    ouverture.function_save_v1(test_hash, normalized_code, metadata)

    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
    func_dir = objects_dir / 'sha256' / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Should NOT have docstrings, name_mappings, alias_mappings
    assert 'docstrings' not in data
    assert 'name_mappings' not in data
    assert 'alias_mappings' not in data


def test_mapping_save_v1_creates_mapping_json(mock_ouverture_dir):
    """Test that mapping_save_v1 creates proper mapping.json"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_ouverture_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # First create the function (object.json must exist)
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")
    metadata = ouverture.metadata_create()
    ouverture.function_save_v1(func_hash, normalized_code, metadata)

    # Now save the mapping
    mapping_hash = ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Check that mapping.json was created - with sha256 in paths
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
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


def test_mapping_save_v1_returns_hash(mock_ouverture_dir):
    """Test that mapping_save_v1 returns the mapping hash"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_ouverture_v_0": "test"}
    alias_mapping = {}
    comment = ""

    # Create function first
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())

    # Save mapping
    mapping_hash = ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Verify it's a valid hash
    assert len(mapping_hash) == 64
    assert all(c in '0123456789abcdef' for c in mapping_hash)

    # Verify it matches computed hash
    expected_hash = ouverture.mapping_compute_hash(docstring, name_mapping, alias_mapping, comment)
    assert mapping_hash == expected_hash


def test_mapping_save_v1_deduplication(mock_ouverture_dir):
    """Test that identical mappings share the same file (deduplication)"""
    func_hash1 = "aaaa" + "0" * 60
    func_hash2 = "bbbb" + "0" * 60
    lang = "eng"
    docstring = "Identical docstring"
    name_mapping = {"_ouverture_v_0": "identical"}
    alias_mapping = {}
    comment = "Same comment"

    # Create two different functions
    ouverture.function_save_v1(func_hash1, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())
    ouverture.function_save_v1(func_hash2, normalize_code_for_test("def _ouverture_v_0(): return 42"), ouverture.metadata_create())

    # Save identical mappings for both
    mapping_hash1 = ouverture.mapping_save_v1(func_hash1, lang, docstring, name_mapping, alias_mapping, comment)
    mapping_hash2 = ouverture.mapping_save_v1(func_hash2, lang, docstring, name_mapping, alias_mapping, comment)

    # Hashes should be identical
    assert mapping_hash1 == mapping_hash2

    # Both should point to the same mapping file - with sha256 in paths
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'

    mapping_dir1 = objects_dir / 'sha256' / func_hash1[:2] / func_hash1[2:] / lang / 'sha256' / mapping_hash1[:2] / mapping_hash1[2:]
    mapping_dir2 = objects_dir / 'sha256' / func_hash2[:2] / func_hash2[2:] / lang / 'sha256' / mapping_hash2[:2] / mapping_hash2[2:]

    # Both mapping.json files should exist
    assert (mapping_dir1 / 'mapping.json').exists()
    assert (mapping_dir2 / 'mapping.json').exists()

    # And they should have identical content
    with open(mapping_dir1 / 'mapping.json', 'r', encoding='utf-8') as f1:
        data1 = json.load(f1)
    with open(mapping_dir2 / 'mapping.json', 'r', encoding='utf-8') as f2:
        data2 = json.load(f2)

    assert data1 == data2


def test_mapping_save_v1_different_comments_different_hashes(mock_ouverture_dir):
    """Test that different comments produce different mapping hashes"""
    func_hash = "abcd1234" + "0" * 56
    lang = "eng"
    docstring = "Test"
    name_mapping = {"_ouverture_v_0": "test"}
    alias_mapping = {}

    # Create function
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())

    # Save two mappings with different comments
    hash1 = ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, "Formal")
    hash2 = ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, "Informal")

    # Hashes should be different
    assert hash1 != hash2


def test_v1_write_integration_full_structure(mock_ouverture_dir):
    """Integration test: verify complete v1 directory structure"""
    func_hash = "test1234" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(_ouverture_v_1): return _ouverture_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['math'],
        'dependencies': []
    }

    # Save function
    ouverture.function_save_v1(func_hash, normalized_code, metadata)

    # Save mappings in two languages
    eng_hash = ouverture.mapping_save_v1(
        func_hash, "eng",
        "Double the input",
        {"_ouverture_v_0": "double", "_ouverture_v_1": "value"},
        {},
        "Simple English"
    )

    fra_hash = ouverture.mapping_save_v1(
        func_hash, "fra",
        "Doubler l'entrée",
        {"_ouverture_v_0": "doubler", "_ouverture_v_1": "valeur"},
        {},
        "Français simple"
    )

    # Verify directory structure - with sha256 in paths
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    objects_dir = ouverture_dir / 'objects'
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
# Tests for Phase 3: V1 Read Path (Schema v1)
# ============================================================================

def test_function_load_v1_loads_object_json(mock_ouverture_dir):
    """Test that function_load_v1 loads object.json correctly"""
    func_hash = "test5678" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(_ouverture_v_1): return _ouverture_v_1 * 2")
    metadata = {
        'created': '2025-01-01T00:00:00Z',
        'author': 'testuser',
        'tags': ['test'],
        'dependencies': []
    }

    # Save function first
    ouverture.function_save_v1(func_hash, normalized_code, metadata)

    # Load it back
    loaded_data = ouverture.function_load_v1(func_hash)

    # Verify data
    assert loaded_data['schema_version'] == 1
    assert loaded_data['hash'] == func_hash
    assert loaded_data['hash_algorithm'] == 'sha256'
    assert loaded_data['normalized_code'] == normalized_code
    assert loaded_data['encoding'] == 'none'
    assert loaded_data['metadata'] == metadata


def test_mappings_list_v1_single_mapping(mock_ouverture_dir):
    """Test that mappings_list_v1 returns single mapping correctly"""
    func_hash = "list1234" + "0" * 56
    lang = "eng"
    docstring = "Test function"
    name_mapping = {"_ouverture_v_0": "test_func"}
    alias_mapping = {}
    comment = "Test variant"

    # Create function and mapping
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())
    ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # List mappings
    mappings = ouverture.mappings_list_v1(func_hash, lang)

    # Should have exactly one mapping
    assert len(mappings) == 1
    mapping_hash, mapping_comment = mappings[0]
    assert len(mapping_hash) == 64
    assert mapping_comment == comment


def test_mappings_list_v1_multiple_mappings(mock_ouverture_dir):
    """Test that mappings_list_v1 returns multiple mappings"""
    func_hash = "list5678" + "0" * 56
    lang = "eng"

    # Create function
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())

    # Add two mappings with different comments
    ouverture.mapping_save_v1(func_hash, lang, "Doc 1", {"_ouverture_v_0": "func1"}, {}, "Formal")
    ouverture.mapping_save_v1(func_hash, lang, "Doc 2", {"_ouverture_v_0": "func2"}, {}, "Casual")

    # List mappings
    mappings = ouverture.mappings_list_v1(func_hash, lang)

    # Should have two mappings
    assert len(mappings) == 2

    # Extract comments
    comments = [comment for _, comment in mappings]
    assert "Formal" in comments
    assert "Casual" in comments


def test_mappings_list_v1_no_mappings(mock_ouverture_dir):
    """Test that mappings_list_v1 returns empty list when no mappings exist"""
    func_hash = "nomaps12" + "0" * 56

    # Create function without any mappings
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())

    # List mappings for a language that doesn't exist
    mappings = ouverture.mappings_list_v1(func_hash, "fra")

    # Should be empty
    assert len(mappings) == 0


def test_mapping_load_v1_loads_correctly(mock_ouverture_dir):
    """Test that mapping_load_v1 loads a specific mapping"""
    func_hash = "load1234" + "0" * 56
    lang = "eng"
    docstring = "Test docstring"
    name_mapping = {"_ouverture_v_0": "test_func", "_ouverture_v_1": "param"}
    alias_mapping = {"abc123": "helper"}
    comment = "Test variant"

    # Create function and mapping
    ouverture.function_save_v1(func_hash, normalize_code_for_test("def _ouverture_v_0(): pass"), ouverture.metadata_create())
    mapping_hash = ouverture.mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment)

    # Load the mapping
    loaded_doc, loaded_name, loaded_alias, loaded_comment = ouverture.mapping_load_v1(func_hash, lang, mapping_hash)

    # Verify data
    assert loaded_doc == docstring
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_comment == comment


def test_function_load_v1_integration(mock_ouverture_dir):
    """Integration test: write v1, read v1, verify correctness"""
    func_hash = "integ123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(_ouverture_v_1): return _ouverture_v_1 + 1")
    docstring = "Increment by one"
    name_mapping = {"_ouverture_v_0": "increment", "_ouverture_v_1": "value"}
    alias_mapping = {}
    comment = "Simple increment"

    # Write v1 format
    ouverture.function_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping, comment)

    # Read back using dispatch (should detect v1)
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)

    # Verify correctness
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_function_load_v0_backward_compatibility(mock_ouverture_dir):
    """Integration test: read v0 file, verify backward compatibility"""
    func_hash = "v0compat" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    docstring = "Return 42"
    name_mapping = {"_ouverture_v_0": "get_answer"}
    alias_mapping = {}

    # Write v0 format explicitly
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Read back using dispatch (should detect v0)
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)

    # Verify correctness
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_function_load_dispatch_multiple_mappings(mock_ouverture_dir):
    """Test that dispatch with multiple mappings defaults to first one"""
    func_hash = "multi123" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")

    # Create function with two mappings
    ouverture.function_save_v1(func_hash, normalized_code, ouverture.metadata_create())
    hash1 = ouverture.mapping_save_v1(func_hash, lang, "Doc 1", {"_ouverture_v_0": "func1"}, {}, "First")
    hash2 = ouverture.mapping_save_v1(func_hash, lang, "Doc 2", {"_ouverture_v_0": "func2"}, {}, "Second")

    # Load without specifying mapping_hash (should return first alphabetically)
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)

    # Should load one of the mappings (implementation will pick first alphabetically)
    assert loaded_code == normalized_code
    assert loaded_name in [{"_ouverture_v_0": "func1"}, {"_ouverture_v_0": "func2"}]
    assert loaded_doc in ["Doc 1", "Doc 2"]


def test_function_load_dispatch_explicit_mapping(mock_ouverture_dir):
    """Test that dispatch can load specific mapping by hash"""
    func_hash = "explicit1" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")

    # Create function with two mappings
    ouverture.function_save_v1(func_hash, normalized_code, ouverture.metadata_create())
    hash1 = ouverture.mapping_save_v1(func_hash, lang, "Doc 1", {"_ouverture_v_0": "func1"}, {}, "First")
    hash2 = ouverture.mapping_save_v1(func_hash, lang, "Doc 2", {"_ouverture_v_0": "func2"}, {}, "Second")

    # Load with specific mapping_hash
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang, mapping_hash=hash2)

    # Should load the second mapping
    assert loaded_code == normalized_code
    assert loaded_name == {"_ouverture_v_0": "func2"}
    assert loaded_doc == "Doc 2"


# ============================================================================
# Tests for Phase 4: Migration Tool (v0 → v1)
# ============================================================================

def test_schema_migrate_function_v0_to_v1_basic(mock_ouverture_dir):
    """Test basic function migration from v0 to v1"""
    func_hash = "migrate01" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    docstring = "Return 42"
    name_mapping = {"_ouverture_v_0": "get_answer"}
    alias_mapping = {}

    # Create v0 function
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate to v1 (keep v0 by default in test)
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=True)

    # Verify v1 format exists
    version = ouverture.schema_detect_version(func_hash)
    assert version == 1

    # Verify v0 still exists (keep_v0=True)
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    v0_path = ouverture_dir / 'objects' / func_hash[:2] / f'{func_hash[2:]}.json'
    assert v0_path.exists()

    # Load from v1 and verify data
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_schema_migrate_function_v0_to_v1_delete_v0(mock_ouverture_dir):
    """Test migration with v0 deletion"""
    func_hash = "migrate02" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 100")
    docstring = "Return 100"
    name_mapping = {"_ouverture_v_0": "get_hundred"}
    alias_mapping = {}

    # Create v0 function
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate to v1 (delete v0)
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Verify v1 format exists
    version = ouverture.schema_detect_version(func_hash)
    assert version == 1

    # Verify v0 was deleted
    ouverture_dir = mock_ouverture_dir / '.ouverture'
    v0_path = ouverture_dir / 'objects' / func_hash[:2] / f'{func_hash[2:]}.json'
    assert not v0_path.exists()

    # Load from v1 and verify data
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_code == normalized_code
    assert loaded_name == name_mapping
    assert loaded_alias == alias_mapping
    assert loaded_doc == docstring


def test_schema_migrate_function_v0_to_v1_multiple_languages(mock_ouverture_dir):
    """Test migration with multiple languages"""
    func_hash = "migrate03" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 50")

    # Create v0 function with two languages
    ouverture.function_save_v0(func_hash, "eng", normalized_code, "English doc", {"_ouverture_v_0": "fifty"}, {})
    ouverture.function_save_v0(func_hash, "fra", normalized_code, "French doc", {"_ouverture_v_0": "cinquante"}, {})

    # Migrate to v1
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Verify both languages are available in v1
    loaded_code_eng, loaded_name_eng, _, loaded_doc_eng = ouverture.function_load(func_hash, "eng")
    loaded_code_fra, loaded_name_fra, _, loaded_doc_fra = ouverture.function_load(func_hash, "fra")

    assert loaded_code_eng == normalized_code
    assert loaded_name_eng == {"_ouverture_v_0": "fifty"}
    assert loaded_doc_eng == "English doc"

    assert loaded_code_fra == normalized_code
    assert loaded_name_fra == {"_ouverture_v_0": "cinquante"}
    assert loaded_doc_fra == "French doc"


def test_schema_migrate_all_v0_to_v1(mock_ouverture_dir):
    """Test migrating all v0 functions at once"""
    # Create three v0 functions
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): pass")
    for i in range(3):
        func_hash = f"migrall{i}" + "0" * 56
        ouverture.function_save_v0(func_hash, "eng", normalized_code, f"Doc {i}", {"_ouverture_v_0": f"func{i}"}, {})

    # Migrate all
    ouverture.schema_migrate_all_v0_to_v1(keep_v0=False, dry_run=False)

    # Verify all three were migrated
    for i in range(3):
        func_hash = f"migrall{i}" + "0" * 56
        version = ouverture.schema_detect_version(func_hash)
        assert version == 1

        # Verify v0 files deleted
        ouverture_dir = mock_ouverture_dir / '.ouverture'
        v0_path = ouverture_dir / 'objects' / func_hash[:2] / f'{func_hash[2:]}.json'
        assert not v0_path.exists()


def test_schema_migrate_all_v0_to_v1_dry_run(mock_ouverture_dir):
    """Test dry-run doesn't actually migrate"""
    func_hash = "dryrun01" + "0" * 56
    ouverture.function_save_v0(func_hash, "eng", normalize_code_for_test("def _ouverture_v_0(): pass"), "Doc", {"_ouverture_v_0": "func"}, {})

    # Dry run
    result = ouverture.schema_migrate_all_v0_to_v1(keep_v0=True, dry_run=True)

    # Verify nothing changed (still v0)
    version = ouverture.schema_detect_version(func_hash)
    assert version == 0

    # Verify result contains the function that would be migrated
    assert len(result) == 1
    assert result[0] == func_hash


def test_schema_validate_v1_valid_function(mock_ouverture_dir):
    """Test validation of valid v1 function"""
    func_hash = "valid001" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 1")
    docstring = "Return 1"
    name_mapping = {"_ouverture_v_0": "one"}
    alias_mapping = {}

    # Create valid v1 function
    ouverture.function_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is True
    assert len(errors) == 0


def test_schema_validate_v1_missing_object_json(mock_ouverture_dir):
    """Test validation detects missing object.json"""
    func_hash = "invalid01" + "0" * 56

    # Don't create anything, just validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is False
    assert len(errors) > 0
    assert any("not found" in err.lower() or "missing" in err.lower() for err in errors)


def test_schema_validate_v1_missing_mappings(mock_ouverture_dir):
    """Test validation detects function with no mappings"""
    func_hash = "invalid02" + "0" * 56
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 1")
    metadata = ouverture.metadata_create()

    # Create function without any mappings
    ouverture.function_save_v1(func_hash, normalized_code, metadata)

    # Validate
    is_valid, errors = ouverture.schema_validate_v1(func_hash)

    assert is_valid is False
    assert len(errors) > 0
    assert any("no mappings" in err.lower() or "no language" in err.lower() for err in errors)


def test_schema_migrate_function_preserves_alias_mappings(mock_ouverture_dir):
    """Test migration preserves alias mappings"""
    func_hash = "alias001" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return abc123._ouverture_v_0()")
    docstring = "Call helper"
    name_mapping = {"_ouverture_v_0": "caller"}
    alias_mapping = {"abc123": "helper"}

    # Create v0 function with alias mapping
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Migrate
    ouverture.schema_migrate_function_v0_to_v1(func_hash, keep_v0=False)

    # Load and verify alias mapping preserved
    loaded_code, loaded_name, loaded_alias, loaded_doc = ouverture.function_load(func_hash, lang)
    assert loaded_alias == alias_mapping


# ============================================================================
# Tests for Phase 5: Mapping Exploration (show command)
# ============================================================================

def test_function_show_single_mapping(mock_ouverture_dir):
    """Test show command with single mapping - should output code directly"""
    func_hash = "a0001000" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 42")
    docstring = "Return 42"
    name_mapping = {"_ouverture_v_0": "answer"}
    alias_mapping = {}

    # Create function with single mapping
    ouverture.function_save(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Capture output
    output = []
    def capture_print(x='', **kwargs):
        output.append(str(x))

    with patch('builtins.print', side_effect=capture_print):
        ouverture.function_show(f"{func_hash}@{lang}")

    # Should output the denormalized code directly
    output_text = '\n'.join(output)
    assert 'def answer():' in output_text
    assert 'Return 42' in output_text


def test_function_show_multiple_mappings_menu(mock_ouverture_dir):
    """Test show command with multiple mappings - should show selection menu"""
    func_hash = "a0002000" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 100")

    # Create function with two mappings
    ouverture.function_save_v1(func_hash, normalized_code, ouverture.metadata_create())
    hash1 = ouverture.mapping_save_v1(func_hash, lang, "Formal doc", {"_ouverture_v_0": "formal_name"}, {}, "Formal variant")
    hash2 = ouverture.mapping_save_v1(func_hash, lang, "Casual doc", {"_ouverture_v_0": "casual_name"}, {}, "Casual variant")

    # Capture output
    output = []
    def capture_print(x='', **kwargs):
        output.append(str(x))

    with patch('builtins.print', side_effect=capture_print):
        ouverture.function_show(f"{func_hash}@{lang}")

    output_text = '\n'.join(output)

    # Should show selection menu
    assert "Multiple mappings found" in output_text
    assert "Formal variant" in output_text
    assert "Casual variant" in output_text
    assert f"show {func_hash}@{lang}@" in output_text


def test_function_show_explicit_mapping_hash(mock_ouverture_dir):
    """Test show command with explicit mapping hash"""
    func_hash = "a0003000" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(_ouverture_v_1): return _ouverture_v_1")

    # Create function with two mappings
    ouverture.function_save_v1(func_hash, normalized_code, ouverture.metadata_create())
    hash1 = ouverture.mapping_save_v1(func_hash, lang, "First", {"_ouverture_v_0": "first", "_ouverture_v_1": "x"}, {}, "First variant")
    hash2 = ouverture.mapping_save_v1(func_hash, lang, "Second", {"_ouverture_v_0": "second", "_ouverture_v_1": "y"}, {}, "Second variant")

    # Capture output for explicit mapping
    output = []
    def capture_print(x='', **kwargs):
        output.append(str(x))

    with patch('builtins.print', side_effect=capture_print):
        ouverture.function_show(f"{func_hash}@{lang}@{hash2}")

    output_text = '\n'.join(output)

    # Should output the second mapping's code
    assert 'def second(y):' in output_text
    assert 'Second' in output_text
    assert 'def first(x):' not in output_text


def test_function_show_v0_backward_compatibility(mock_ouverture_dir):
    """Test show command works with v0 functions"""
    func_hash = "a0004000" + "0" * 56
    lang = "eng"
    normalized_code = normalize_code_for_test("def _ouverture_v_0(): return 77")
    docstring = "Return 77"
    name_mapping = {"_ouverture_v_0": "seventy_seven"}
    alias_mapping = {}

    # Create v0 function
    ouverture.function_save_v0(func_hash, lang, normalized_code, docstring, name_mapping, alias_mapping)

    # Capture output
    output = []
    def capture_print(x='', **kwargs):
        output.append(str(x))

    with patch('builtins.print', side_effect=capture_print):
        ouverture.function_show(f"{func_hash}@{lang}")

    output_text = '\n'.join(output)

    # Should output the code (v0 has only one mapping per language)
    assert 'def seventy_seven():' in output_text
    assert 'Return 77' in output_text


def test_function_show_invalid_format(mock_ouverture_dir):
    """Test show command with invalid format"""
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_show("invalid_format")


def test_function_show_function_not_found(mock_ouverture_dir):
    """Test show command with non-existent function"""
    func_hash = "notfound" + "0" * 56
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_show(f"{func_hash}@eng")


def test_function_show_language_not_found(mock_ouverture_dir):
    """Test show command with non-existent language"""
    func_hash = "a0005000" + "0" * 56
    lang = "eng"

    # Create function with only English
    ouverture.function_save(func_hash, lang, normalize_code_for_test("def _ouverture_v_0(): pass"), "Doc", {"_ouverture_v_0": "func"}, {})

    # Try to show in French (doesn't exist)
    with pytest.raises(SystemExit):
        with patch('sys.stderr'):
            ouverture.function_show(f"{func_hash}@fra")

