"""
Integration tests for 'ouverture.py add' command.

Grey-box style:
- Setup: Create test files
- Test: Call CLI command
- Assert: Check output and files
"""
import json
from pathlib import Path


def test_add_simple_function(cli_runner, tmp_path):
    """Test adding a simple function via CLI"""
    # Setup: Create a test file
    test_file = tmp_path / "simple.py"
    test_file.write_text('''def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
''')

    # Test: Run add command
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Check success and output
    assert result.returncode == 0
    assert 'Hash:' in result.stdout

    # Extract hash and verify file was created
    func_hash = result.stdout.split('Hash:')[1].strip().split()[0]
    assert len(func_hash) == 64

    # Verify object was stored
    objects_dir = cli_runner.pool_dir / 'sha256'
    assert objects_dir.exists()


def test_add_function_creates_v1_structure(cli_runner, tmp_path):
    """Test that add creates proper v1 directory structure"""
    # Setup
    test_file = tmp_path / "math_func.py"
    test_file.write_text('''def add_numbers(a, b):
    """Add two numbers"""
    return a + b
''')

    # Test
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Assert: Check v1 directory structure
    func_dir = cli_runner.pool_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    assert func_dir.exists()

    # Check object.json exists
    object_json = func_dir / 'object.json'
    assert object_json.exists()

    # Check language mapping exists
    eng_dir = func_dir / 'eng'
    assert eng_dir.exists()


def test_add_function_stores_normalized_code(cli_runner, tmp_path):
    """Test that add normalizes and stores code correctly"""
    # Setup
    test_file = tmp_path / "normalize.py"
    test_file.write_text('''def my_function(param):
    """Doc"""
    local = param * 2
    return local
''')

    # Test
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Assert: Check normalized code in object.json
    func_dir = cli_runner.pool_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    # Function should be renamed to _ouverture_v_0
    assert '_ouverture_v_0' in data['normalized_code']
    # Original function name should NOT appear
    assert 'my_function' not in data['normalized_code']


def test_add_same_logic_same_hash(cli_runner, tmp_path):
    """Test that identical logic produces identical hash regardless of names"""
    # Setup: Create two files with same logic, different names
    eng_file = tmp_path / "english.py"
    eng_file.write_text('''def calculate_sum(numbers):
    """Calculate the sum of numbers"""
    total = 0
    for num in numbers:
        total = total + num
    return total
''')

    fra_file = tmp_path / "french.py"
    fra_file.write_text('''def calculate_sum(numbers):
    """Calculer la somme des nombres"""
    total = 0
    for num in numbers:
        total = total + num
    return total
''')

    # Test: Add both
    eng_hash = cli_runner.add(str(eng_file), 'eng')
    fra_hash = cli_runner.add(str(fra_file), 'fra')

    # Assert: Same hash (logic is identical, only docstring differs)
    assert eng_hash == fra_hash


def test_add_multilingual_creates_mappings(cli_runner, tmp_path):
    """Test that adding same function in multiple languages creates language mappings"""
    # Setup
    eng_file = tmp_path / "eng.py"
    eng_file.write_text('''def double(value):
    """Double a value"""
    return value * 2
''')

    fra_file = tmp_path / "fra.py"
    fra_file.write_text('''def double(value):
    """Doubler une valeur"""
    return value * 2
''')

    # Test
    eng_hash = cli_runner.add(str(eng_file), 'eng')
    fra_hash = cli_runner.add(str(fra_file), 'fra')

    # Assert: Same hash, both language directories exist
    assert eng_hash == fra_hash

    func_dir = cli_runner.pool_dir / 'sha256' / eng_hash[:2] / eng_hash[2:]
    assert (func_dir / 'eng').exists()
    assert (func_dir / 'fra').exists()


def test_add_async_function(cli_runner, tmp_path):
    """Test adding an async function via CLI"""
    # Setup
    test_file = tmp_path / "async_func.py"
    test_file.write_text('''async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
''')

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'Hash:' in result.stdout


def test_add_missing_language_suffix_fails(cli_runner, tmp_path):
    """Test that add fails without language suffix"""
    # Setup
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')

    # Test: Run without @lang
    result = cli_runner.run(['add', str(test_file)])

    # Assert: Should fail
    assert result.returncode != 0
    assert 'Missing language suffix' in result.stderr


def test_add_invalid_language_code_fails(cli_runner, tmp_path):
    """Test that add fails with invalid language code"""
    # Setup
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')

    # Test: Run with invalid lang
    result = cli_runner.run(['add', f'{test_file}@invalid'])

    # Assert: Should fail
    assert result.returncode != 0
    assert 'Language code must be 3 characters' in result.stderr


def test_add_nonexistent_file_fails(cli_runner):
    """Test that add fails for nonexistent file"""
    # Test
    result = cli_runner.run(['add', '/nonexistent/file.py@eng'])

    # Assert
    assert result.returncode != 0


def test_add_function_with_imports(cli_runner, tmp_path):
    """Test adding function with stdlib imports"""
    # Setup
    test_file = tmp_path / "with_imports.py"
    test_file.write_text('''import math
from collections import Counter

def analyze(data):
    """Analyze data"""
    count = Counter(data)
    return math.sqrt(len(count))
''')

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode == 0

    # Verify imports are preserved in object.json
    func_hash = result.stdout.split('Hash:')[1].strip().split()[0]
    func_dir = cli_runner.pool_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    assert 'import math' in data['normalized_code']
    assert 'from collections import Counter' in data['normalized_code']


def test_add_syntax_error_fails(cli_runner, tmp_path):
    """Test that syntax error in file causes error"""
    # Setup
    test_file = tmp_path / "bad.py"
    test_file.write_text("def foo( invalid syntax")

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode != 0
