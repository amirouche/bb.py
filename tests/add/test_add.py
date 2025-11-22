"""
Integration tests for 'mobius.py add' command.

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
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
    assert func_dir.exists()


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
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
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
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    # Function should be renamed to _mobius_v_0
    assert '_mobius_v_0' in data['normalized_code']
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

    func_dir = cli_runner.pool_dir / eng_hash[:2] / eng_hash[2:]
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
    """Test that add fails with too short language code"""
    # Setup
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')

    # Test: Run with too short lang (must be 3-256 chars)
    result = cli_runner.run(['add', f'{test_file}@ab'])

    # Assert: Should fail
    assert result.returncode != 0
    assert 'Language code must be 3-256 characters' in result.stderr


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
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
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


def test_add_empty_file_fails(cli_runner, tmp_path):
    """Test that empty file causes error"""
    # Setup
    test_file = tmp_path / "empty.py"
    test_file.write_text("")

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode != 0
    assert 'No function definition' in result.stderr


def test_add_class_only_fails(cli_runner, tmp_path):
    """Test that file with only class (no function) causes error"""
    # Setup
    test_file = tmp_path / "classonly.py"
    test_file.write_text("class Foo:\n    pass\n")

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode != 0
    assert 'No function definition' in result.stderr


def test_add_function_without_docstring(cli_runner, tmp_path):
    """Test adding function without docstring works"""
    # Setup
    test_file = tmp_path / "nodoc.py"
    test_file.write_text("def double(x):\n    return x * 2\n")

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'Hash:' in result.stdout


def test_add_hash_stability(cli_runner, tmp_path):
    """Test that the same function produces identical hash on repeated adds"""
    # Setup
    test_file = tmp_path / "stable.py"
    test_file.write_text('''def compute(value):
    """Compute something"""
    return value * 3
''')

    # Test: Add twice
    hash1 = cli_runner.add(str(test_file), 'eng')
    hash2 = cli_runner.add(str(test_file), 'eng')

    # Assert: Identical hashes
    assert hash1 == hash2
    assert len(hash1) == 64


def test_add_missing_mobius_import_fails(cli_runner, tmp_path):
    """Test that add fails when mobius imports don't exist in pool"""
    # Setup: Create function that imports a non-existent mobius function
    fake_hash = 'a' * 64
    test_file = tmp_path / "with_missing_dep.py"
    test_file.write_text(f'''from mobius.pool import object_{fake_hash} as helper

def use_helper(x):
    """Use helper function"""
    return helper(x) + 1
''')

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Should fail with informative error
    assert result.returncode != 0
    assert 'do not exist in the local pool' in result.stderr
    assert 'helper' in result.stderr


def test_add_with_existing_mobius_import_succeeds(cli_runner, tmp_path):
    """Test that add succeeds when mobius imports exist in pool"""
    # Setup: First add a helper function
    helper_file = tmp_path / "helper.py"
    helper_file.write_text('''def helper(x):
    """Helper function"""
    return x * 2
''')
    helper_hash = cli_runner.add(str(helper_file), 'eng')

    # Create function that imports the helper
    test_file = tmp_path / "use_helper.py"
    test_file.write_text(f'''from mobius.pool import object_{helper_hash} as helper

def use_helper(x):
    """Use helper function"""
    return helper(x) + 1
''')

    # Test
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Should succeed
    assert result.returncode == 0
    assert 'Hash:' in result.stdout


def test_add_hash_determinism_with_example_files(cli_runner):
    """Test hash determinism using the real example files (grey-box CLI test).

    This verifies the core Mobius principle via CLI: same logic = same hash,
    regardless of variable names or human language. Uses the example files:
    - examples/example_simple.py (English)
    - examples/example_simple_french.py (French)

    Grey-box: We use CLI to add files, then verify both the output AND
    the internal storage structure.
    """
    # Setup: Locate the example files
    examples_dir = Path(__file__).parent.parent.parent / 'examples'
    english_file = examples_dir / 'example_simple.py'
    french_file = examples_dir / 'example_simple_french.py'

    # Verify example files exist
    assert english_file.exists(), f"Example file not found: {english_file}"
    assert french_file.exists(), f"Example file not found: {french_file}"

    # Test: Add both files via CLI
    eng_hash = cli_runner.add(str(english_file), 'eng')
    fra_hash = cli_runner.add(str(french_file), 'fra')

    # Assert 1: Same hash (core principle)
    assert eng_hash == fra_hash, (
        f"Hash mismatch! English and French examples should have identical hashes.\n"
        f"English hash: {eng_hash}\n"
        f"French hash: {fra_hash}"
    )

    # Assert 2: Hash format is valid
    assert len(eng_hash) == 64, "Hash should be 64 hex characters (SHA256)"

    # Grey-box assertions: Check internal storage structure
    func_dir = cli_runner.pool_dir / eng_hash[:2] / eng_hash[2:]

    # Assert 3: Single function directory exists (not two separate ones)
    assert func_dir.exists(), "Function directory should exist"

    # Assert 4: Both language mappings exist under same function
    assert (func_dir / 'eng').exists(), "English mapping directory should exist"
    assert (func_dir / 'fra').exists(), "French mapping directory should exist"

    # Assert 5: object.json exists with normalized code
    object_json = func_dir / 'object.json'
    assert object_json.exists(), "object.json should exist"

    with open(object_json, 'r') as f:
        data = json.load(f)

    # Assert 6: Normalized code uses _mobius_v_0 (not original function names)
    assert '_mobius_v_0' in data['normalized_code']
    assert 'calculate_sum' not in data['normalized_code']
    assert 'calculer_somme' not in data['normalized_code']
