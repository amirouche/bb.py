"""
Tests for async/await function support.

Unit tests for low-level async function normalization (AST aspects).
Integration tests for async function CLI commands.
"""
import ast

import pytest

import mobius
from tests.conftest import normalize_code_for_test


# =============================================================================
# Integration tests for async function CLI commands
# =============================================================================

def test_async_add_and_show(cli_runner, tmp_path):
    """Integration test: Add async function and show it"""
    test_file = tmp_path / "async.py"
    test_file.write_text('''async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
''')

    func_hash = cli_runner.add(str(test_file), 'eng')
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    assert result.returncode == 0
    assert 'async def fetch_data' in result.stdout
    assert 'url' in result.stdout


def test_async_add_and_get(cli_runner, tmp_path):
    """Integration test: Add async function and get it"""
    test_file = tmp_path / "async_get.py"
    test_file.write_text('''async def process_item(item):
    """Process an item"""
    result = await do_work(item)
    return result
''')

    func_hash = cli_runner.add(str(test_file), 'eng')
    result = cli_runner.run(['get', f'{func_hash}@eng'])

    assert result.returncode == 0
    assert 'async def process_item' in result.stdout
    assert 'item' in result.stdout


def test_async_multilingual_same_hash(cli_runner, tmp_path):
    """Integration test: Same async logic in multiple languages produces same hash"""
    eng_file = tmp_path / "eng_async.py"
    eng_file.write_text('''async def download(url):
    """Download from URL"""
    data = await fetch(url)
    return data
''')

    fra_file = tmp_path / "fra_async.py"
    fra_file.write_text('''async def download(url):
    """Télécharger depuis URL"""
    data = await fetch(url)
    return data
''')

    eng_hash = cli_runner.add(str(eng_file), 'eng')
    fra_hash = cli_runner.add(str(fra_file), 'fra')

    assert eng_hash == fra_hash


# =============================================================================
# Unit tests for async function normalization (low-level AST)
# =============================================================================

def test_normalize_simple_async_function():
    """Test normalizing a simple async function"""
    code = '''async def fetch_data():
    """Fetch data asynchronously"""
    return await some_api()
'''
    tree = ast.parse(code)

    func_def, imports = mobius.code_extract_definition(tree)

    assert isinstance(func_def, ast.AsyncFunctionDef)
    assert func_def.name == "fetch_data"


def test_normalize_async_function_with_parameters():
    """Test normalizing async function with parameters"""
    code = '''async def process_item(item, timeout):
    """Process an item with timeout"""
    result = await do_work(item)
    return result
'''
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    name_mapping, reverse_mapping = mobius.code_create_name_mapping(func_def, imports, {})

    assert name_mapping["process_item"] == "_mobius_v_0"
    assert "item" in name_mapping
    assert "timeout" in name_mapping
    assert "result" in name_mapping


def test_normalize_async_function_with_await_expressions():
    """Test normalizing async function with multiple await expressions"""
    code = '''async def complex_async(url, data):
    """Complex async operation"""
    connection = await connect(url)
    response = await connection.send(data)
    result = await response.json()
    return result
'''
    tree = ast.parse(code)
    func_def, imports = mobius.code_extract_definition(tree)

    name_mapping, reverse_mapping = mobius.code_create_name_mapping(func_def, imports, {})

    assert "_mobius_v_0" in reverse_mapping
    assert "url" in name_mapping
    assert "data" in name_mapping
    assert "connection" in name_mapping
    assert "response" in name_mapping
    assert "result" in name_mapping


def test_async_function_hash_determinism():
    """Test that same async logic produces same hash"""
    code_eng = '''async def fetch_user(user_id):
    """Fetch user by ID"""
    result = await get_from_db(user_id)
    return result
'''
    code_fra = '''async def fetch_user(user_id):
    """Récupérer utilisateur par ID"""
    result = await get_from_db(user_id)
    return result
'''

    tree_eng = ast.parse(code_eng)
    tree_fra = ast.parse(code_fra)

    _, normalized_eng_no_doc, _, _, _ = mobius.code_normalize(tree_eng, "eng")
    _, normalized_fra_no_doc, _, _, _ = mobius.code_normalize(tree_fra, "fra")

    hash_eng = mobius.hash_compute(normalized_eng_no_doc)
    hash_fra = mobius.hash_compute(normalized_fra_no_doc)

    assert hash_eng == hash_fra


def test_async_function_preserves_async_keyword():
    """Test that normalized code preserves async keyword"""
    code = '''async def my_async_func():
    """Do async stuff"""
    return await something()
'''
    tree = ast.parse(code)

    normalized_with_doc, normalized_without_doc, _, _, _ = mobius.code_normalize(tree, "eng")

    assert "async def _mobius_v_0" in normalized_with_doc
    assert "async def _mobius_v_0" in normalized_without_doc


def test_ast_normalizer_visit_async_function_def():
    """Test ASTNormalizer handles AsyncFunctionDef"""
    code = '''async def original_name():
    pass
'''
    tree = ast.parse(code)

    name_mapping = {"original_name": "_mobius_v_0"}
    normalizer = mobius.ASTNormalizer(name_mapping)
    transformed = normalizer.visit(tree)

    func_def = transformed.body[0]
    assert isinstance(func_def, ast.AsyncFunctionDef)
    assert func_def.name == "_mobius_v_0"


def test_names_collect_includes_async_function():
    """Test names_collect handles async functions"""
    code = '''async def async_func(param):
    local_var = 42
    return local_var
'''
    tree = ast.parse(code)

    names = mobius.code_collect_names(tree)

    assert "async_func" in names
    assert "param" in names
    assert "local_var" in names
