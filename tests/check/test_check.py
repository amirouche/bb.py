"""
Integration tests for '@check' decorator and 'mobius.py check' command.

Grey-box style:
- Setup: Create test files with @check decorators
- Test: Call CLI commands
- Assert: Check output and files
"""
import json
from pathlib import Path


def test_add_function_with_check_decorator(cli_runner, tmp_path):
    """Test adding a function with @check decorator stores checks in metadata"""
    # Setup: First add a function to be tested
    target_file = tmp_path / "target.py"
    target_file.write_text('''def add_numbers(a, b):
    """Add two numbers"""
    return a + b
''')

    target_hash = cli_runner.add(str(target_file), 'eng')

    # Setup: Create a test file with @check decorator
    test_file = tmp_path / "test_add.py"
    test_file.write_text(f'''from mobius.pool import object_{target_hash} as add_numbers

@check(object_{target_hash})
def test_add():
    """Test add_numbers function"""
    result = add_numbers(2, 3)
    return result == 5
''')

    # Test: Run add command for the test function
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Check success
    assert result.returncode == 0
    assert 'Hash:' in result.stdout

    # Extract hash
    test_hash = result.stdout.split('Hash:')[1].strip().split()[0]

    # Verify metadata contains checks
    func_dir = cli_runner.pool_dir / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    assert 'metadata' in data
    assert 'checks' in data['metadata']
    assert target_hash in data['metadata']['checks']


def test_add_function_with_multiple_check_decorators(cli_runner, tmp_path):
    """Test adding a function with multiple @check decorators"""
    # Setup: Add two target functions
    target1_file = tmp_path / "target1.py"
    target1_file.write_text('''def add(a, b):
    """Add two numbers"""
    return a + b
''')

    target2_file = tmp_path / "target2.py"
    target2_file.write_text('''def subtract(a, b):
    """Subtract two numbers"""
    return a - b
''')

    target1_hash = cli_runner.add(str(target1_file), 'eng')
    target2_hash = cli_runner.add(str(target2_file), 'eng')

    # Setup: Create a test file with multiple @check decorators
    test_file = tmp_path / "test_math.py"
    test_file.write_text(f'''from mobius.pool import object_{target1_hash} as add
from mobius.pool import object_{target2_hash} as subtract

@check(object_{target1_hash})
@check(object_{target2_hash})
def test_math():
    """Test both add and subtract functions"""
    return add(5, 3) == 8 and subtract(5, 3) == 2
''')

    # Test: Run add command
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Check success
    assert result.returncode == 0

    # Extract hash
    test_hash = result.stdout.split('Hash:')[1].strip().split()[0]

    # Verify metadata contains both checks
    func_dir = cli_runner.pool_dir / test_hash[:2] / test_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    assert 'metadata' in data
    assert 'checks' in data['metadata']
    assert target1_hash in data['metadata']['checks']
    assert target2_hash in data['metadata']['checks']


def test_check_command_finds_tests(cli_runner, tmp_path):
    """Test that 'check' command finds tests for a function"""
    # Setup: Add a target function
    target_file = tmp_path / "target.py"
    target_file.write_text('''def multiply(a, b):
    """Multiply two numbers"""
    return a * b
''')

    target_hash = cli_runner.add(str(target_file), 'eng')

    # Setup: Create a test file with @check decorator
    test_file = tmp_path / "test_multiply.py"
    test_file.write_text(f'''from mobius.pool import object_{target_hash} as multiply

@check(object_{target_hash})
def test_multiply():
    """Test multiply function"""
    return multiply(3, 4) == 12
''')

    result = cli_runner.run(['add', f'{test_file}@eng'])
    test_hash = result.stdout.split('Hash:')[1].strip().split()[0]

    # Test: Run check command
    check_result = cli_runner.run(['check', target_hash])

    # Assert: Check that test function is found
    assert check_result.returncode == 0
    assert test_hash in check_result.stdout
    assert 'mobius.py run' in check_result.stdout


def test_check_command_no_tests_found(cli_runner, tmp_path):
    """Test that 'check' command reports when no tests are found"""
    # Setup: Add a function without any tests
    target_file = tmp_path / "orphan.py"
    target_file.write_text('''def orphan_function():
    """A function with no tests"""
    return 42
''')

    target_hash = cli_runner.add(str(target_file), 'eng')

    # Test: Run check command
    result = cli_runner.run(['check', target_hash])

    # Assert: Check no tests found
    assert result.returncode == 0
    assert 'No tests found' in result.stdout


def test_add_with_check_missing_target_fails(cli_runner, tmp_path):
    """Test that adding a function with @check fails if target doesn't exist"""
    # Setup: Create a test file with @check pointing to non-existent function
    fake_hash = 'a' * 64  # Non-existent hash
    test_file = tmp_path / "test_bad.py"
    test_file.write_text(f'''from mobius.pool import object_{fake_hash} as fake_func

@check(object_{fake_hash})
def test_fake():
    """Test fake function"""
    return fake_func() == 42
''')

    # Test: Run add command
    result = cli_runner.run(['add', f'{test_file}@eng'])

    # Assert: Should fail because target doesn't exist
    assert result.returncode != 0
    assert 'do not exist' in result.stderr.lower() or 'error' in result.stderr.lower()


def test_check_command_invalid_hash(cli_runner, tmp_path):
    """Test that 'check' command rejects invalid hash format"""
    # Test: Run check with invalid hash
    result = cli_runner.run(['check', 'invalid-hash'])

    # Assert: Should fail
    assert result.returncode != 0
    assert 'invalid' in result.stderr.lower()


def test_check_command_nonexistent_hash(cli_runner, tmp_path):
    """Test that 'check' command fails for non-existent function"""
    # Test: Run check with valid format but non-existent hash
    fake_hash = 'b' * 64
    result = cli_runner.run(['check', fake_hash])

    # Assert: Should fail
    assert result.returncode != 0
    assert 'not found' in result.stderr.lower()


def test_add_function_without_check_decorator(cli_runner, tmp_path):
    """Test that functions without @check don't have checks in metadata"""
    # Setup: Create a file without @check decorator
    test_file = tmp_path / "no_check.py"
    test_file.write_text('''def simple_function():
    """A simple function without checks"""
    return 42
''')

    # Test: Run add command
    result = cli_runner.run(['add', f'{test_file}@eng'])
    assert result.returncode == 0

    # Extract hash
    func_hash = result.stdout.split('Hash:')[1].strip().split()[0]

    # Verify metadata does NOT contain checks
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    with open(object_json, 'r') as f:
        data = json.load(f)

    assert 'metadata' in data
    # checks should not be present (or should be empty/None)
    assert 'checks' not in data['metadata'] or not data['metadata'].get('checks')
