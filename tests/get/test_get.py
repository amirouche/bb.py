"""
Integration tests for 'ouverture.py get' command.

Grey-box style:
- Setup: Use 'add' command to create functions
- Test: Call 'get' command via CLI
- Assert: Check output contains expected code

Note: 'get' and 'show' are functionally equivalent for single-mapping functions.
"""


def test_get_returns_denormalized_code(cli_runner, tmp_path):
    """Test that get returns function with original names"""
    # Setup: Create and add a function
    test_file = tmp_path / "func.py"
    test_file.write_text('''def process(data):
    """Process data"""
    result = data * 2
    return result
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Get the function
    result = cli_runner.run(['get', f'{func_hash}@eng'])

    # Assert: Should show original function name
    assert result.returncode == 0
    assert 'def process(data):' in result.stdout
    assert 'result' in result.stdout


def test_get_preserves_imports(cli_runner, tmp_path):
    """Test that get preserves imports in output"""
    # Setup
    test_file = tmp_path / "with_imports.py"
    test_file.write_text('''import json
from pathlib import Path

def load_config(filepath):
    """Load config from JSON file"""
    path = Path(filepath)
    with path.open() as f:
        return json.load(f)
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test
    result = cli_runner.run(['get', f'{func_hash}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'import json' in result.stdout
    assert 'from pathlib import Path' in result.stdout


def test_get_multilingual_english(cli_runner, tmp_path):
    """Test get retrieves correct language version - English"""
    # Setup
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

    func_hash = cli_runner.add(str(eng_file), 'eng')
    cli_runner.add(str(fra_file), 'fra')

    # Test
    result = cli_runner.run(['get', f'{func_hash}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'Greet someone in English' in result.stdout


def test_get_multilingual_french(cli_runner, tmp_path):
    """Test get retrieves correct language version - French"""
    # Setup
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

    func_hash = cli_runner.add(str(eng_file), 'eng')
    cli_runner.add(str(fra_file), 'fra')

    # Test
    result = cli_runner.run(['get', f'{func_hash}@fra'])

    # Assert
    assert result.returncode == 0
    assert 'Saluer' in result.stdout


def test_get_missing_language_suffix_fails(cli_runner, tmp_path):
    """Test that get fails without language suffix"""
    # Setup
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test
    result = cli_runner.run(['get', func_hash])

    # Assert
    assert result.returncode != 0


def test_get_invalid_hash_fails(cli_runner):
    """Test that get fails with invalid hash format"""
    result = cli_runner.run(['get', 'not-a-valid-hash@eng'])

    assert result.returncode != 0


def test_get_nonexistent_function_fails(cli_runner):
    """Test that get fails for nonexistent function"""
    fake_hash = "f" * 64

    result = cli_runner.run(['get', f'{fake_hash}@eng'])

    assert result.returncode != 0


def test_get_nonexistent_language_fails(cli_runner, tmp_path):
    """Test that get fails when language doesn't exist"""
    # Setup: Add function in English only
    test_file = tmp_path / "eng_only.py"
    test_file.write_text('def foo(): pass')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Try to get in Spanish (doesn't exist)
    result = cli_runner.run(['get', f'{func_hash}@spa'])

    # Assert
    assert result.returncode != 0
