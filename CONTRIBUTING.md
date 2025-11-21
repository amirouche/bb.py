# Contributing to Ouverture

Thank you for your interest in contributing to Ouverture! This project welcomes all kinds of contributions, and we're excited to have you here.

## Philosophy: Vibe Coding Encouraged

This project embraces **vibe coding** - a creative, exploratory approach to development where intuition and experimentation are valued alongside rigorous planning. We encourage you to:

- Explore ideas freely and propose creative solutions
- Experiment with new approaches to multilingual code sharing
- Follow your instincts when designing features
- Share incomplete ideas - they often spark the best discussions

For a comprehensive understanding of the project architecture, design decisions, and implementation details, please read **[CLAUDE.md](CLAUDE.md)**. This document is your guide to understanding how Ouverture works under the hood.

## All Contributions Are Welcome

While code contributions are wonderful, they're not the only way to help! Valuable contributions include:

- **Documentation**: Improve README, add tutorials, translate docs
- **Examples**: Create example functions in different languages (check out the `examples/` directory!)
- **Testing**: Write tests, report bugs, verify multilingual behavior
- **Ideas**: Propose features, suggest improvements, discuss use cases
- **Community**: Answer questions, help other contributors, share your experiences
- **Translations**: Add function translations in your native language
- **Outreach**: Write blog posts, give talks, spread the word

## Getting Started

### Examples Directory

The `examples/` directory contains sample functions demonstrating Ouverture's capabilities:

- `example_simple.py` - Basic function (English)
- `example_simple_french.py` - Same logic in French
- `example_simple_spanish.py` - Same logic in Spanish
- `example_with_import.py` - Function with standard library imports
- `example_with_ouverture.py` - Function calling other pool functions

These examples are great for:
- Understanding how multilingual functions work
- Testing your changes
- Creating new demonstrations
- Learning the normalization process

Try adding your own examples in different human languages!

### Quick Test

```bash
# Add an example function
python3 ouverture.py add examples/example_simple.py@eng

# Verify it was stored (default location: $HOME/.local/ouverture)
find $HOME/.local/ouverture/objects -name "*.json"

# Clean up
rm -rf $HOME/.local/ouverture
```

## Roadmap: Creative Coding for User Reach

Ouverture aims to become a valuable tool for the **creative coding community**, enabling artists and designers to share algorithms across language barriers. Our vision includes:

- **p5.js integration**: Bridge Python functions with p5.js sketches for web-based creative coding
- **Processing compatibility**: Enable Python Mode for Processing to use Ouverture functions
- **Creative coding testbed**: Build a gallery of generative art, visual algorithms, and interactive demos
- **Educational outreach**: Provide multilingual examples for teaching computational creativity
- **Community building**: Foster collaboration between creative coders from different linguistic backgrounds

By targeting creative coding communities (p5.js, Processing, openFrameworks, etc.), we can demonstrate Ouverture's value while reaching artists, educators, and students worldwide. If you're interested in creative coding, consider contributing examples that showcase visual algorithms, generative patterns, or interactive systems!

---

# Testing Guide

This section includes comprehensive information about testing Ouverture.

## Installation

To run the tests, you need to install pytest:

```bash
pip install -r requirements-dev.txt
```

Or install pytest directly:

```bash
pip install pytest pytest-cov
```

## Running Tests

### Run all tests:

```bash
pytest
```

### Run with verbose output:

```bash
pytest -v
```

### Run with coverage report:

```bash
pytest --cov=ouverture --cov-report=html
```

### Run specific test by name pattern:

```bash
pytest -k "ast_normalizer"
```

### Run specific test:

```bash
pytest test_ouverture.py::test_ast_normalizer_visit_name_with_mapping
```

## Test Structure

**IMPORTANT**: All tests are implemented as functions, not classes. This follows pytest best practices and keeps tests simple and focused.

The test suite includes the following test functions:

### Unit Tests

1. **ASTNormalizer tests** (`test_ast_normalizer_*`)
   - Variable name transformation
   - Function argument renaming
   - Function definition renaming

2. **collect_names tests** (`test_collect_names_*`)
   - Collecting variable names
   - Collecting function names and arguments

3. **get_imported_names tests** (`test_get_imported_names_*`)
   - Import statement parsing
   - Import with aliases
   - From-import statements

4. **check_unused_imports tests** (`test_check_unused_imports_*`)
   - Detecting used imports
   - Detecting unused imports

5. **sort_imports tests** (`test_sort_imports_*`)
   - Sorting import statements
   - Sorting from-import statements
   - Import ordering relative to code

6. **extract_function_def tests** (`test_extract_function_def_*`)
   - Extracting function definitions
   - Extracting imports
   - Error handling for missing/multiple functions

7. **create_name_mapping tests** (`test_create_name_mapping_*`)
   - Function name mapping to _ouverture_v_0
   - Sequential variable numbering
   - Built-in exclusion from renaming
   - Imported name exclusion
   - Ouverture alias exclusion

8. **rewrite_ouverture_imports tests** (`test_rewrite_ouverture_imports_*`)
   - Rewriting ouverture imports to couverture
   - Alias tracking and removal
   - Preservation of non-ouverture imports

9. **replace_ouverture_calls tests** (`test_replace_ouverture_calls_*`)
   - Replacing aliased function calls
   - Transformation to HASH._ouverture_v_0 format

10. **clear_locations tests** (`test_clear_locations_*`)
    - Clearing AST location information

11. **extract_docstring tests** (`test_extract_docstring_*`)
    - Extracting existing docstrings
    - Handling functions without docstrings
    - Multiline docstring support

12. **normalize_ast tests** (`test_normalize_ast_*`)
    - Simple function normalization
    - Import sorting during normalization
    - Ouverture import handling

13. **compute_hash tests** (`test_compute_hash_*`)
    - Deterministic hashing
    - Hash format validation
    - Different code producing different hashes

14. **save_function tests** (`test_save_function_*`)
    - Saving new functions
    - Adding additional languages to existing functions
    - JSON file structure validation

15. **replace_docstring tests** (`test_replace_docstring_*`)
    - Replacing existing docstrings
    - Adding docstrings to functions without them
    - Removing docstrings

16. **denormalize_code tests** (`test_denormalize_code_*`)
    - Variable name denormalization
    - Ouverture import restoration
    - Function call restoration

### Integration Tests

17. **add_function tests** (`test_add_function_*`)
    - Error handling for missing language suffix
    - Error handling for invalid language codes
    - Error handling for missing files
    - Error handling for syntax errors
    - Successful function addition

18. **get_function tests** (`test_get_function_*`)
    - Error handling for missing language suffix
    - Error handling for invalid language codes
    - Error handling for invalid hash format
    - Error handling for non-existent functions
    - Error handling for unavailable languages
    - Successful function retrieval

19. **end-to-end tests** (`test_end_to_end_*`)
    - Roundtrip testing (add then get)
    - Multilingual function hashing (same logic = same hash)

## Test Coverage

The test suite covers:

- **Core AST manipulation**: All AST transformation and normalization logic
- **Name mapping**: Function and variable name mapping/unmapping
- **Import handling**: Both standard and ouverture imports
- **Hash computation**: Deterministic hashing excluding docstrings
- **Storage**: Content-addressed storage in $HOME/.local/ouverture/objects/
- **CLI commands**: Both `add` and `get` commands with error cases
- **End-to-end workflows**: Complete add/get cycles and multilingual support

## Key Test Scenarios

### 1. AST Normalization
Tests verify that:
- Function names always map to `_ouverture_v_0`
- Variables get sequential indices
- Built-ins are never renamed
- Imports are never renamed
- Ouverture aliases are tracked and excluded from renaming

### 2. Multilingual Support
Tests verify that:
- Same logic in different languages produces same hash
- Docstrings are excluded from hash computation
- Language-specific names are preserved in mappings
- Roundtrip (add then get) preserves functionality

### 3. Import Handling
Tests verify that:
- Standard imports remain unchanged
- Ouverture imports are rewritten to couverture
- Aliases are tracked and restored correctly
- Function calls are transformed appropriately

### 4. Error Handling
Tests verify proper error messages for:
- Missing language suffixes
- Invalid language codes (must be 3 characters)
- Missing files
- Syntax errors
- Invalid hash formats
- Non-existent functions
- Unavailable languages

## Test Fixtures

The tests use `tmp_path` pytest fixture for:
- Creating temporary test files
- Creating temporary ouverture directories
- Ensuring test isolation (no pollution of actual ouverture pool in $HOME/.local/ouverture)

## Mocking

Tests use `unittest.mock.patch` to:
- Capture stdout/stderr output
- Prevent actual file operations during some tests
- Test error conditions without side effects

## Continuous Integration

The project includes a GitHub Actions workflow that automatically runs tests on every push and pull request.

**Workflow file**: `.github/workflows/test.yml`

**Features**:
- Tests on multiple Python versions (3.9, 3.10, 3.11, 3.12)
- Runs full test suite with verbose output
- Generates coverage report on Python 3.11
- Uploads coverage to Codecov (optional)
- Runs on pushes to `main` and `claude/*` branches
- Runs on all pull requests to `main`

The workflow automatically installs dependencies from `requirements-dev.txt` and runs pytest. All tests must pass before merging pull requests.

**Status**: Check the Actions tab on GitHub to see test results for recent commits and PRs.

## Contributing Tests

When adding new functionality to ouverture.py:

1. Write tests first (TDD approach)
2. Ensure all existing tests still pass
3. Aim for >90% code coverage
4. Include both unit tests and integration tests
5. Test error conditions, not just happy paths

## Common Issues

### Import Errors

If you get import errors, make sure ouverture.py is in the Python path:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest
```

### Temporary Directory Issues

Tests use temporary directories for isolation. If tests fail with path issues, ensure the tmp_path fixture is working correctly.

### Git Ignores

The following directories/files should be in .gitignore:
- `.ouverture/` - Local function pool (if used for testing; default is $HOME/.local/ouverture)
- `__pycache__/` - Python bytecode
- `.pytest_cache/` - Pytest cache
- `htmlcov/` - Coverage reports
- `.coverage` - Coverage data

## Performance

The full test suite should complete in under 10 seconds on modern hardware. If tests are slow:

1. Check for large ouverture pools in $HOME/.local/ouverture or local .ouverture directories
2. Ensure tmp_path fixtures are being used correctly
3. Consider using pytest-xdist for parallel execution:
   ```bash
   pip install pytest-xdist
   pytest -n auto
   ```

## Further Reading

- [pytest documentation](https://docs.pytest.org/)
- [pytest fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [Python AST documentation](https://docs.python.org/3/library/ast.html)

---

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Write descriptive docstrings
- Use the `type_name_verb_complement` naming convention (see CLAUDE.md)

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit your changes with clear messages
6. Push to your branch
7. Open a Pull Request

## Questions?

If you have questions or need help:
- Open an issue for discussion
- Check CLAUDE.md for technical details
- Reach out to the maintainers

Thank you for contributing to Ouverture!
