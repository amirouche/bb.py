"""
Integration tests for end-to-end workflows.

Grey-box style tests that exercise complete CLI workflows combining multiple commands.
"""
import ast

import pytest


# =============================================================================
# Integration tests for complete CLI workflows
# =============================================================================

def test_workflow_add_show_roundtrip(cli_runner, tmp_path):
    """Test add then show produces correct output"""
    test_file = tmp_path / "greet.py"
    test_file.write_text('''def greet(name):
    """Greet someone by name"""
    return f"Hello, {name}!"
''')

    # Add function
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Show function
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    assert result.returncode == 0
    assert 'def greet' in result.stdout
    assert 'name' in result.stdout
    assert 'Hello' in result.stdout


def test_workflow_add_get_roundtrip(cli_runner, tmp_path):
    """Test add then get produces equivalent code"""
    test_file = tmp_path / "calc.py"
    original_code = '''def calculate_sum(first, second):
    """Add two numbers"""
    result = first + second
    return result'''
    test_file.write_text(original_code)

    func_hash = cli_runner.add(str(test_file), 'eng')
    result = cli_runner.run(['get', f'{func_hash}@eng'])

    assert result.returncode == 0

    # Parse both to compare structure
    original_tree = ast.parse(original_code)
    retrieved_tree = ast.parse(result.stdout)

    orig_func = original_tree.body[0]
    retr_func = retrieved_tree.body[0]

    assert orig_func.name == retr_func.name
    assert len(orig_func.args.args) == len(retr_func.args.args)


def test_workflow_multilingual_same_hash(cli_runner, tmp_path):
    """Test equivalent functions in different languages produce same hash"""
    eng_file = tmp_path / "english.py"
    eng_file.write_text('''def calculate_sum(first, second):
    """Calculate the sum of two numbers."""
    result = first + second
    return result''')

    fra_file = tmp_path / "french.py"
    fra_file.write_text('''def calculate_sum(first, second):
    """Calculer la somme de deux nombres."""
    result = first + second
    return result''')

    eng_hash = cli_runner.add(str(eng_file), 'eng')
    fra_hash = cli_runner.add(str(fra_file), 'fra')

    # Should have the same hash
    assert eng_hash == fra_hash

    # Should be able to retrieve in both languages
    eng_result = cli_runner.run(['get', f'{eng_hash}@eng'])
    fra_result = cli_runner.run(['get', f'{fra_hash}@fra'])

    assert eng_result.returncode == 0
    assert fra_result.returncode == 0


def test_workflow_multilingual_get_different_languages(cli_runner, tmp_path):
    """Test get retrieves correct language version"""
    eng_file = tmp_path / "eng.py"
    eng_file.write_text('''def greet(name):
    """Greet someone in English"""
    return f"Hello, {name}!"
''')

    fra_file = tmp_path / "fra.py"
    fra_file.write_text('''def greet(name):
    """Saluer quelqu'un en français"""
    return f"Hello, {name}!"
''')

    eng_hash = cli_runner.add(str(eng_file), 'eng')
    cli_runner.add(str(fra_file), 'fra')

    # Get English version
    eng_result = cli_runner.run(['get', f'{eng_hash}@eng'])
    assert 'Greet someone in English' in eng_result.stdout

    # Get French version
    fra_result = cli_runner.run(['get', f'{eng_hash}@fra'])
    assert 'Saluer' in fra_result.stdout


def test_workflow_function_with_imports(cli_runner, tmp_path):
    """Test add and show with imported libraries"""
    test_file = tmp_path / "with_imports.py"
    test_file.write_text('''import math
from collections import Counter

def analyze(data):
    """Analyze data"""
    count = Counter(data)
    return math.sqrt(len(count))
''')

    func_hash = cli_runner.add(str(test_file), 'eng')
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    assert result.returncode == 0
    assert 'import math' in result.stdout
    assert 'from collections import Counter' in result.stdout
    assert 'def analyze' in result.stdout


def test_workflow_function_with_ouverture_import(cli_runner, tmp_path):
    """Test adding function that imports from ouverture pool"""
    # First, add a helper function
    helper_file = tmp_path / "helper.py"
    helper_file.write_text('''def helper(x):
    """A helper function"""
    return x * 2
''')

    helper_hash = cli_runner.add(str(helper_file), 'eng')

    # Now add a function that uses the helper
    main_file = tmp_path / "main.py"
    main_file.write_text(f'''from ouverture.pool import object_{helper_hash} as helper

def process(value):
    """Process a value using helper"""
    return helper(value) + 1
''')

    main_hash = cli_runner.add(str(main_file), 'eng')

    # Show should restore the import with alias
    result = cli_runner.run(['show', f'{main_hash}@eng'])

    assert result.returncode == 0
    assert 'from ouverture.pool import' in result.stdout
    assert 'as helper' in result.stdout
    assert 'def process' in result.stdout


def test_workflow_add_multiple_then_list(cli_runner, tmp_path):
    """Test adding multiple functions and listing them via log"""
    # Add three different functions
    for i, name in enumerate(['alpha', 'beta', 'gamma']):
        test_file = tmp_path / f"{name}.py"
        test_file.write_text(f'''def {name}():
    """Function {name}"""
    return {i}
''')
        cli_runner.add(str(test_file), 'eng')

    # Log should work (even if empty, shouldn't error)
    result = cli_runner.run(['log'])
    assert result.returncode == 0


def test_workflow_error_handling_invalid_file(cli_runner):
    """Test error handling for nonexistent file"""
    result = cli_runner.run(['add', '/nonexistent/file.py@eng'])
    assert result.returncode != 0


def test_workflow_error_handling_missing_language(cli_runner, tmp_path):
    """Test error handling for missing language suffix"""
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')

    result = cli_runner.run(['add', str(test_file)])
    assert result.returncode != 0
    assert 'Missing language suffix' in result.stderr


def test_workflow_error_handling_invalid_language(cli_runner, tmp_path):
    """Test error handling for invalid language code"""
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')

    result = cli_runner.run(['add', f'{test_file}@invalid'])
    assert result.returncode != 0
    assert 'Language code must be 3 characters' in result.stderr
