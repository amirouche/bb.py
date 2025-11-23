"""
Integration tests for 'bb.py show' command.

Grey-box style:
- Setup: Use 'add' command to create functions
- Test: Call 'show' command via CLI
- Assert: Check output contains expected code
"""


def test_show_displays_denormalized_code(cli_runner, tmp_path):
    """Test that show displays function with original names restored"""
    # Setup: Create and add a function
    test_file = tmp_path / "greet.py"
    test_file.write_text('''def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Show the function
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    # Assert: Should show original function name
    assert result.returncode == 0
    assert 'def greet(name):' in result.stdout
    assert 'Hello' in result.stdout
    # Should NOT show normalized names
    assert '_bb_v_0' not in result.stdout


def test_show_displays_docstring(cli_runner, tmp_path):
    """Test that show includes the docstring"""
    # Setup
    test_file = tmp_path / "documented.py"
    test_file.write_text('''def calculate_average(numbers):
    """Calculate the average of a list of numbers.

    Args:
        numbers: List of numbers

    Returns:
        The arithmetic mean
    """
    total = sum(numbers)
    return total / len(numbers)
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'Calculate the average' in result.stdout
    assert 'arithmetic mean' in result.stdout


def test_show_async_function(cli_runner, tmp_path):
    """Test that show works with async functions"""
    # Setup
    test_file = tmp_path / "async.py"
    test_file.write_text('''async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'async def fetch_data' in result.stdout


def test_show_function_with_imports(cli_runner, tmp_path):
    """Test that show displays functions with preserved imports"""
    # Setup
    test_file = tmp_path / "with_math.py"
    test_file.write_text('''import math

def circle_area(radius):
    """Calculate area of a circle"""
    return math.pi * radius ** 2
''')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    # Assert
    assert result.returncode == 0
    assert 'import math' in result.stdout
    assert 'def circle_area(radius):' in result.stdout


def test_show_multilang_english(cli_runner, tmp_path):
    """Test showing function added in multiple languages - English version"""
    # Setup: Add same logic in two languages
    eng_file = tmp_path / "eng.py"
    eng_file.write_text('''def multiply(value, factor):
    """Multiply value by factor"""
    result = value * factor
    return result
''')
    fra_file = tmp_path / "fra.py"
    fra_file.write_text('''def multiply(value, factor):
    """Multiplier valeur par facteur"""
    result = value * factor
    return result
''')

    eng_hash = cli_runner.add(str(eng_file), 'eng')
    cli_runner.add(str(fra_file), 'fra')

    # Test: Show English version
    result = cli_runner.run(['show', f'{eng_hash}@eng'])

    # Assert: Should show English docstring
    assert result.returncode == 0
    assert 'Multiply value by factor' in result.stdout


def test_show_multilang_french(cli_runner, tmp_path):
    """Test showing function added in multiple languages - French version"""
    # Setup: Add same logic in two languages
    eng_file = tmp_path / "eng.py"
    eng_file.write_text('''def multiply(value, factor):
    """Multiply value by factor"""
    result = value * factor
    return result
''')
    fra_file = tmp_path / "fra.py"
    fra_file.write_text('''def multiply(value, factor):
    """Multiplier valeur par facteur"""
    result = value * factor
    return result
''')

    eng_hash = cli_runner.add(str(eng_file), 'eng')
    cli_runner.add(str(fra_file), 'fra')

    # Test: Show French version
    result = cli_runner.run(['show', f'{eng_hash}@fra'])

    # Assert: Should show French docstring
    assert result.returncode == 0
    assert 'Multiplier valeur par facteur' in result.stdout


def test_show_without_language_lists_languages(cli_runner, tmp_path):
    """Test that show without @lang lists available languages"""
    # Setup
    test_file = tmp_path / "test.py"
    test_file.write_text('def foo(): pass')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Run without @lang
    result = cli_runner.run(['show', func_hash])

    # Assert: Should list available languages
    assert result.returncode == 0
    assert 'Available languages' in result.stdout
    assert 'eng' in result.stdout
    assert '1 mapping(s)' in result.stdout


def test_show_nonexistent_function_fails(cli_runner):
    """Test that show fails for nonexistent function"""
    fake_hash = "0" * 64

    result = cli_runner.run(['show', f'{fake_hash}@eng'])

    assert result.returncode != 0


def test_show_nonexistent_language_fails(cli_runner, tmp_path):
    """Test that show fails when language doesn't exist for function"""
    # Setup: Add function in English only
    test_file = tmp_path / "eng_only.py"
    test_file.write_text('def foo(): pass')
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Test: Try to show in French (doesn't exist)
    result = cli_runner.run(['show', f'{func_hash}@fra'])

    # Assert: Should fail
    assert result.returncode != 0


def test_show_multiple_mappings_shows_menu(cli_runner, tmp_path):
    """Test that show displays selection menu when multiple mappings exist"""
    # Setup: Add same function twice with different comments
    test_file = tmp_path / "func.py"
    test_file.write_text('''def foo():
    """Test function"""
    return 42
''')

    # Add with two different comments (creates two mappings)
    result1 = cli_runner.run(['add', f'{test_file}@eng', '--comment', 'first version'])
    assert result1.returncode == 0
    result2 = cli_runner.run(['add', f'{test_file}@eng', '--comment', 'second version'])
    assert result2.returncode == 0

    # Extract hash
    func_hash = result1.stdout.split('Hash:')[1].strip().split()[0]

    # Test: Show function with multiple mappings
    result = cli_runner.run(['show', f'{func_hash}@eng'])

    # Assert: Should show menu with options
    assert result.returncode == 0
    assert 'Multiple mappings found' in result.stdout
    assert 'first version' in result.stdout
    assert 'second version' in result.stdout


def test_show_explicit_mapping_hash(cli_runner, tmp_path):
    """Test that show with explicit mapping hash displays correct version"""
    # Setup
    test_file = tmp_path / "func.py"
    test_file.write_text('''def foo():
    """Test function"""
    return 42
''')

    # Add with comment
    result1 = cli_runner.run(['add', f'{test_file}@eng', '--comment', 'target version'])
    func_hash = result1.stdout.split('Hash:')[1].strip().split()[0]
    mapping_hash = result1.stdout.split('Mapping hash:')[1].strip().split()[0]

    # Test: Show with explicit mapping hash
    result = cli_runner.run(['show', f'{func_hash}@eng@{mapping_hash}'])

    # Assert: Should show the code directly
    assert result.returncode == 0
    assert 'def foo():' in result.stdout
    assert 'Test function' in result.stdout


def test_show_invalid_hash_format_fails(cli_runner):
    """Test that show fails with invalid hash format"""
    result = cli_runner.run(['show', 'not-valid-hash@eng'])

    assert result.returncode != 0
    assert 'Invalid hash format' in result.stderr


def test_show_invalid_language_code_fails(cli_runner, tmp_path):
    """Test that show fails with too short language code"""
    test_file = tmp_path / "func.py"
    test_file.write_text('def foo(): pass')
    func_hash = cli_runner.add(str(test_file), 'eng')

    result = cli_runner.run(['show', f'{func_hash}@ab'])

    assert result.returncode != 0
    assert 'Language code must be 3-256 characters' in result.stderr
