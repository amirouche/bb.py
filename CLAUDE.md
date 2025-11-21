# CLAUDE.md - AI Assistant Guide for Ouverture

## Project Overview

**Ouverture** is a function pool manager for Python that enables multilingual function sharing through AST normalization and content-addressed storage. It allows the same logical function written in different human languages (with different variable names, docstrings, etc.) to share the same hash and be stored together.

### Core Concept

Functions with identical logic but different naming (e.g., English vs French variable names) are normalized to a canonical form, hashed, and stored in a content-addressed pool. This enables:
- **Multilingual code sharing**: Same function logic across different human languages
- **Deterministic hashing**: Identical logic produces identical hashes regardless of naming
- **Compositional functions**: Functions can reference other functions from the pool

## Architecture

### Key Design Principles

1. **AST-based normalization**: Source code is parsed into an AST, normalized, then unparsed
2. **Hash on logic, not names**: Docstrings excluded from hash computation to enable multilingual support
3. **Bidirectional mapping**: Original names preserved for reconstruction in target language
4. **Content-addressed storage**: Functions stored by hash in `.ouverture/objects/XX/YYYYYY.json`

### Data Flow

```
Source Code (@lang)
    ↓
Parse to AST
    ↓
Extract docstring (language-specific)
    ↓
Normalize AST (rename vars, sort imports, rewrite ouverture imports)
    ↓
Compute hash (on code WITHOUT docstring)
    ↓
Store in .ouverture/objects/ with:
    - normalized_code (with docstring for display)
    - per-language mappings (name_mappings, alias_mappings, docstrings)
```

## Directory Structure

```
hello-claude/
├── ouverture.py              # Main CLI tool (600+ lines)
├── examples/                  # Example functions directory
│   ├── README.md              # Testing documentation
│   ├── example_simple.py          # English example
│   ├── example_simple_french.py   # French example (same logic)
│   ├── example_simple_spanish.py  # Spanish example (same logic)
│   ├── example_with_import.py     # Example with stdlib imports
│   └── example_with_ouverture.py  # Example calling other pool functions
├── README_TESTING.md          # Testing documentation
├── .gitignore                 # Ignores .ouverture/, __pycache__, etc.
└── .ouverture/                # Generated function pool (gitignored)
    └── objects/
        └── XX/                # First 2 chars of hash
            └── YYYYYY.json    # Remaining 62 chars + .json
```

## Key Components

### Core Classes

#### `ASTNormalizer` (lines 20-40)
- **Purpose**: Transform AST by renaming variables/functions according to mapping
- **Methods**:
  - `visit_Name()`: Rename variable references
  - `visit_arg()`: Rename function arguments
  - `visit_FunctionDef()`: Rename function definitions

### Core Functions

#### `normalize_ast(tree, lang)` (lines 272-318)
**Central normalization pipeline**
- Sorts imports lexicographically
- Extracts function definition and imports
- Extracts docstring separately
- Rewrites `from ouverture import X as Y` → `from ouverture import X` (removes alias)
- Creates name mappings (`original → _ouverture_v_X`)
- Returns: normalized code (with/without docstring), docstring, mappings

#### `create_name_mapping(function_def, imports, ouverture_aliases)` (lines 133-180)
**Generates bidirectional name mappings**
- Function name always gets `_ouverture_v_0`
- Variables/args get sequential indices: `_ouverture_v_1`, `_ouverture_v_2`, ...
- **Excluded from renaming**: Python builtins, imported names, ouverture aliases
- Returns: `(forward_mapping, reverse_mapping)`

#### `rewrite_ouverture_imports(imports)` (lines 183-213)
**Transforms ouverture imports for normalization**
- Rewrites: `from ouverture import HASH as alias` → `from ouverture import HASH` (removes alias)
- Tracks alias mappings for later denormalization
- Necessary because normalized code uses `HASH._ouverture_v_0(...)` instead of `alias(...)`

#### `replace_ouverture_calls(tree, alias_mapping, name_mapping)` (lines 216-235)
**Replaces aliased function calls with normalized form**
- Transforms: `alias(...)` → `HASH._ouverture_v_0(...)`
- Uses alias_mapping to determine which names are ouverture functions

#### `compute_hash(code)` (lines 321-323)
**Generates SHA256 hash**
- CRITICAL: Hash computed on code **WITHOUT docstring**
- Ensures same logic = same hash across languages

#### `save_function(hash_value, lang, ...)` (lines 326-362)
**Stores function in content-addressed pool**
- Path: `.ouverture/objects/XX/YYYYYY.json` (XX = first 2 chars of hash)
- Merges with existing data if hash already exists
- Stores per-language: docstrings, name_mappings, alias_mappings

#### `denormalize_code(normalized_code, name_mapping, alias_mapping)` (lines 364-429)
**Reconstructs original-looking code**
- Reverses variable renaming: `_ouverture_v_X → original_name`
- Rewrites imports: `from ouverture import X` → `from ouverture import X as alias` (restores alias)
- Transforms calls: `HASH._ouverture_v_0(...)` → `alias(...)`

### CLI Commands

#### `add` command
```bash
python3 ouverture.py add path/to/file.py@lang
```
- Parses file, normalizes AST, computes hash, saves to pool
- Language code must be 3 characters (ISO 639-3)

#### `get` command
```bash
python3 ouverture.py get HASH@lang
```
- Retrieves function from pool, denormalizes to target language
- Prints reconstructed code to stdout

## Development Conventions

### Python Code Style

1. **Type hints**: Used in function signatures (`Dict[str, str]`, `Set[str]`, etc.)
2. **Docstrings**: Required for all public functions
3. **Error handling**: Explicit error messages to stderr, exit with code 1
4. **AST manipulation**: Use `ast` module, never regex on source code
5. **Encoding**: Always use `encoding='utf-8'` for file I/O

### Naming Conventions

- **Classes**: PascalCase (`ASTNormalizer`)
- **Functions**: snake_case (`create_name_mapping`)
- **Constants**: UPPER_SNAKE_CASE (`PYTHON_BUILTINS`)
- **Normalized names**: `_ouverture_v_N` (N = 0, 1, 2, ...)

### Testing Conventions

- **Test framework**: pytest
- **Test structure**: All tests MUST be functions, not classes
- **Test naming**: Use descriptive names like `test_<component>_<behavior>` (e.g., `test_ast_normalizer_visit_name_with_mapping`)
- **Test file**: `test_ouverture.py` contains 50+ test functions
- **Documentation**: See `README_PYTEST.md` for comprehensive testing guide

### Important Invariants

1. **Function name always `_ouverture_v_0`**: First entry in name mapping
2. **Built-ins never renamed**: `len`, `sum`, `print`, etc. preserved
3. **Imported names never renamed**: `math`, `Counter`, etc. preserved
4. **Imports sorted**: Lexicographically by module name
5. **Hash on logic only**: Docstrings excluded from hash computation
6. **Language codes**: Always 3 characters (ISO 639-3: eng, fra, spa, etc.)
7. **Hash format**: 64 lowercase hex characters (SHA256)

## Testing Strategy

### Pytest Unit Tests

The project includes comprehensive pytest unit tests in `test_ouverture.py`. See `README_PYTEST.md` for detailed documentation.

**CRITICAL REQUIREMENT**: All pytest tests MUST be implemented as functions, not classes. Use descriptive function names like `test_ast_normalizer_visit_name_with_mapping()` instead of class-based organization.

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=ouverture --cov-report=html

# Run specific test
pytest test_ouverture.py::test_ast_normalizer_visit_name_with_mapping

# Run tests matching pattern
pytest -k "ast_normalizer"
```

The test suite covers:
- AST normalization and transformation
- Name mapping and unmapping
- Import handling (standard and ouverture)
- Hash computation and determinism
- Storage and retrieval functions
- CLI commands with error handling
- End-to-end integration tests
- Multilingual function support

### Running Examples

```bash
# Add examples to pool
python3 ouverture.py add examples/example_simple.py@eng
python3 ouverture.py add examples/example_simple_french.py@fra
python3 ouverture.py add examples/example_simple_spanish.py@spa

# Verify they share the same hash
find .ouverture/objects -name "*.json"

# Retrieve in different language
python3 ouverture.py get HASH@eng
python3 ouverture.py get HASH@fra
```

### Test Cases to Consider

1. **Simple function**: No imports, basic operations
2. **Function with stdlib imports**: Ensure imports preserved
3. **Function with ouverture imports**: Test alias rewriting
4. **Multilingual equivalents**: Verify same hash for same logic
5. **Error cases**: Missing @lang, invalid language code, file not found

### Verification Checklist

- [ ] Imports are sorted lexicographically
- [ ] Function renamed to `_ouverture_v_0`
- [ ] Variables renamed sequentially
- [ ] Built-ins NOT renamed (`sum`, `len`, `print`)
- [ ] Imports NOT renamed (`math`, `Counter`)
- [ ] Docstring stored separately per language
- [ ] Hash identical for same logic in different languages

## Git Workflow

### Branch Strategy

- `main`: Stable releases
- `claude/*`: AI-generated feature branches
- Pull requests required for merging to main

### Commit Message Style

Based on recent commits:
- Use imperative mood: "Add", "Extract", "Fix"
- Be specific: "Add 'ouverture get HASH@lang' command"
- Reference context: "Extract docstrings from hash computation for multilingual support"

### Ignored Files

- `.ouverture/`: Generated function pool (never commit)
- `__pycache__/`, `*.pyc`: Python bytecode
- `.venv/`, `.env`: Virtual environments and secrets

## Import Handling Rules

Understanding how imports are processed is critical to the normalization system.

### Import Categories

#### 1. Standard Library & External Package Imports
**Examples**: `import math`, `from collections import Counter`, `import numpy as np`

**Processing**:
- **Before storage**: Sorted lexicographically, **no renaming**
- **In storage**: Identical to original (e.g., `import math`)
- **From storage**: No transformation
- **Usage**: Names like `math`, `Counter`, `np` are **never renamed** to `_ouverture_v_X`

**Example:**
```python
# Original & Normalized (unchanged)
from collections import Counter
import math
```

#### 2. Ouverture Imports (Pool Functions)
**Examples**: `from ouverture import abc123def as helper`

**Processing**:

**Before storage (normalization)**:
```python
from ouverture import abc123def as helper
```
↓ becomes ↓
```python
from ouverture import abc123def
```
- Alias removed: `as helper` is dropped
- Alias tracked in `alias_mapping`: `{"abc123def": "helper"}`
- Function calls transformed: `helper(x)` → `abc123def._ouverture_v_0(x)`

**From storage (denormalization)**:
```python
from ouverture import abc123def
```
↓ becomes ↓
```python
from ouverture import abc123def as helper
```
- Language-specific alias restored: `as helper` (from `alias_mapping[lang]`)
- Function calls transformed back: `abc123def._ouverture_v_0(x)` → `helper(x)`

### Why This Design?

- **Standard imports** are universal (same across all languages)
- **Ouverture imports** have language-specific aliases:
  - English: `from ouverture import abc123 as helper`
  - French: `from ouverture import abc123 as assistant`
  - Spanish: `from ouverture import abc123 as ayudante`

All normalize to: `from ouverture import abc123`, ensuring identical hashes.

## Key Algorithms

### AST Normalization Algorithm

```
1. Parse source to AST
2. Sort imports lexicographically
3. Extract function definition
4. Extract docstring from function
5. Rewrite ouverture imports (remove aliases)
6. Create name mapping (excluding builtins, imports, ouverture aliases)
7. Replace ouverture calls (alias → HASH._ouverture_v_0)
8. Apply name normalization
9. Clear AST location info
10. Unparse to normalized code
```

### Hash Computation Strategy

```
CRITICAL: Hash excludes docstrings to enable multilingual support

1. Normalize AST twice: with and without docstring
2. Compute hash on version WITHOUT docstring
3. Store version WITH docstring for display
4. Result: Same logic = same hash, regardless of language
```

## Common Pitfalls for AI Assistants

1. **Don't modify hash computation**: Adding docstrings to hash breaks multilingual support
2. **Don't skip language suffix**: Commands require `@lang`, not optional
3. **Don't rename built-ins**: `PYTHON_BUILTINS` set must remain untouched
4. **Don't assume Python 3.8**: Code uses `ast.unparse()` (requires Python 3.9+)
5. **Don't break import sorting**: Lexicographic order is part of normalization
6. **Don't create duplicate mappings**: `_ouverture_v_0` is ALWAYS the function name

## Extension Points

### Adding New Features

1. **New commands**: Add to `argparse` subparsers in `main()` (lines 584-603)
2. **New normalizations**: Extend `ASTNormalizer` class
3. **New validations**: Add to `normalize_ast()` or command handlers
4. **New storage formats**: Modify `save_function()` and increment version field

### Future Considerations

- **Versioning**: JSON has `"version": 0` field for schema evolution
- **Type checking**: Consider adding mypy type checking
- **Testing framework**: Project uses pytest for automated testing (see `test_ouverture.py` and `README_PYTEST.md`)
- **Documentation generation**: Extract docstrings to generate docs
- **Package distribution**: Consider setuptools/pyproject.toml for PyPI

## File Path References

When referencing code locations, use this format:

- `ouverture.py:272` - normalize_ast function
- `ouverture.py:133` - create_name_mapping function
- `ouverture.py:20` - ASTNormalizer class
- `ouverture.py:321` - compute_hash function

## Performance Considerations

- **AST parsing**: O(n) in source code length
- **Name collection**: O(n) in AST nodes
- **Hash computation**: O(n) in normalized code length
- **File I/O**: Minimal - only read/write JSON once per operation

## Security Considerations

1. **Code injection**: Uses `ast.parse()`, not `eval()` - safe
2. **Path traversal**: No user-controlled file paths in storage
3. **Hash collisions**: SHA256 - astronomically unlikely
4. **Malicious imports**: Not validated - user responsibility

## Questions to Ask Before Making Changes

1. Does this change affect hash computation? (If yes, be very careful)
2. Does this break multilingual support? (Test with different languages)
3. Does this preserve built-in and imported name handling?
4. Does this maintain import sorting?
5. Is the JSON schema still backward compatible?
6. Are error messages helpful to users?

## Debugging Tips

1. **Inspect JSON**: `cat .ouverture/objects/XX/YYY.json | python3 -m json.tool`
2. **Check AST**: Use `ast.dump()` to inspect tree structure
3. **Compare hashes**: Same logic should produce same hash
4. **Verify mappings**: Check name_mappings in JSON for correctness
5. **Test round-trip**: `add` then `get` should produce equivalent code

## Summary

Ouverture is a carefully designed system for multilingual function sharing through AST normalization. The key insight is separating logic (hashed) from presentation (language-specific names/docstrings). When modifying the code:

- Preserve the invariants listed above
- Test with multiple languages
- Ensure hash computation remains deterministic
- Maintain backward compatibility with existing pool data

The codebase is self-contained (single file), well-structured (clear function boundaries), and follows Python best practices. The testing guide (README_TESTING.md) provides comprehensive examples for validation.
