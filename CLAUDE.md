# CLAUDE.md - AI Assistant Guide for Beyond Babel

## Project Overview

**Beyond Babel** is a function pool manager for Python that enables multilingual function sharing through AST normalization and content-addressed storage. It allows the same logical function written in different human languages (with different variable names, docstrings, etc.) to share the same hash and be stored together.

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
4. **Content-addressed storage**: Functions stored by hash in `$HOME/.local/bb/pool/xx/yy.../object.json` (configurable via `BB_DIRECTORY` environment variable)
5. **Single-file architecture**: All code resides in `bb.py` - no modularization into separate packages. This keeps the tool simple, self-contained, and easy to distribute as a single script.
6. **Object prefix for valid identifiers**: BB imports use `object_` prefix (e.g., `from bb.pool import object_abc123 as func`) to ensure valid Python identifiers since SHA256 hashes can start with digits (0-9)

### Storage Location Configuration

The bb function pool location is controlled by the `BB_DIRECTORY` environment variable:

- **Default**: `$HOME/.local/bb/` (follows XDG Base Directory specification)
- **Custom location**: Set `BB_DIRECTORY=/path/to/pool` to override

**Examples**:
```bash
# Use default location
python3 bb.py add example.py@eng

# Use custom location
export BB_DIRECTORY=/shared/pool
python3 bb.py add example.py@eng
```

### Data Flow

```
Source Code (@lang)
    ↓
Parse to AST
    ↓
Extract docstring (language-specific)
    ↓
Normalize AST (rename vars, sort imports, rewrite bb imports)
    ↓
Compute hash (on code WITHOUT docstring)
    ↓
Store in $HOME/.local/bb/pool/ (or $BB_DIRECTORY/pool/) with:
    - normalized_code (with docstring for display)
    - per-language mappings (name_mappings, alias_mappings, docstrings)
```

## Directory Structure

### Project Structure

```
bb.py/
├── bb.py                      # Main CLI tool
├── examples/                      # Example functions directory
│   ├── README.md                  # Examples documentation
│   ├── example_simple.py          # English example
│   ├── example_simple_french.py   # French example (same logic)
│   ├── example_simple_spanish.py  # Spanish example (same logic)
│   ├── example_with_import.py     # Example with stdlib imports
│   └── example_with_bb.py     # Example calling other pool functions
├── strategies/                    # Design documents
├── tests/                         # Test suite
└── .gitignore                     # Ignores __pycache__, etc.
```

### Function Pool Structure (v1)

```
$HOME/.local/bb/pool/          # Default location (or $BB_DIRECTORY/pool/)
└── ab/                            # First 2 chars of function hash
    └── c123def456.../             # Function directory (remaining hash chars)
        ├── object.json            # Core function data
        ├── eng/                   # Language directory
        │   └── xy/                # First 2 chars of mapping hash
        │       └── z789.../
        │           └── mapping.json
        └── fra/                   # Another language
            └── mn/
                └── opqr.../
                    └── mapping.json
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

#### `ast_normalize(tree, lang)` (lines 272-318)
**Central normalization pipeline**
- Sorts imports lexicographically
- Extracts function definition and imports
- Extracts docstring separately
- Rewrites `from bb.pool import X as Y` → `from bb.pool import X` (removes alias)
- Creates name mappings (`original → _bb_v_X`)
- Returns: normalized code (with/without docstring), docstring, mappings

#### `mapping_create_name(function_def, imports, bb_aliases)` (lines 133-180)
**Generates bidirectional name mappings**
- Function name always gets `_bb_v_0`
- Variables/args get sequential indices: `_bb_v_1`, `_bb_v_2`, ...
- **Excluded from renaming**: Python builtins, imported names, bb aliases
- Returns: `(forward_mapping, reverse_mapping)`

#### `imports_rewrite_bb(imports)` (lines 183-213)
**Transforms bb imports for normalization**
- Rewrites: `from bb.pool import HASH as alias` → `from bb.pool import HASH` (removes alias)
- Tracks alias mappings for later denormalization
- Necessary because normalized code uses `HASH._bb_v_0(...)` instead of `alias(...)`

#### `calls_replace_bb(tree, alias_mapping, name_mapping)` (lines 216-235)
**Replaces aliased function calls with normalized form**
- Transforms: `alias(...)` → `HASH._bb_v_0(...)`
- Uses alias_mapping to determine which names are bb functions

#### `hash_compute(code)` (lines 321-335)
**Generates SHA256 hash**
- CRITICAL: Hash computed on code **WITHOUT docstring**
- Ensures same logic = same hash across languages
- Returns 64-character hex output

#### `mapping_compute_hash(docstring, name_mapping, alias_mapping, comment='')` (lines 338-371)
**Computes content-addressed hash for language mappings** (Schema v1)
- Creates canonical JSON from mapping components (sorted keys, no whitespace)
- Includes comment field in hash to distinguish variants
- Enables deduplication: identical mappings share same hash/storage
- Returns: 64-character hex SHA256 hash

#### `schema_detect_version(func_hash)` (lines 374-406)
**Detects if a function exists in the pool**
- Checks filesystem for v1 format: `pool/XX/YYYYYY.../object.json`
- Returns: 1 if found, None if not found

#### `metadata_create()` (lines 409-435)
**Generates default metadata for functions** (Schema v1)
- ISO 8601 timestamp (`created` field)
- Author from environment (USER or USERNAME)
- Returns: Dictionary with metadata structure
- Used when saving functions to v1 format

#### `function_save_v1(hash_value, normalized_code, metadata)` (lines 495-532)
**Stores function in v1 format** (Schema v1)
- Creates function directory: `$BB_DIRECTORY/pool/XX/YYYYYY.../`
- Writes `object.json` with schema_version=1, metadata
- Does NOT store language-specific data (stored separately in mapping files)
- Clean separation: code in object.json, language variants in mapping.json files

#### `mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment='')` (lines 534-585)
**Stores language mapping in v1 format** (Schema v1)
- Creates mapping directory: `$BB_DIRECTORY/pool/XX/Y.../lang/ZZ/W.../`
- Writes `mapping.json` with docstring, name_mapping, alias_mapping, comment
- Content-addressed by mapping hash (enables deduplication)
- Identical mappings across functions share same file
- Returns: mapping hash for verification

#### `function_save(hash_value, lang, normalized_code, docstring, name_mapping, alias_mapping, comment='')` (lines 471-494)
**Main entry point for saving functions** (Schema v1)
- Wrapper that calls function_save_v1() + mapping_save_v1()
- Creates metadata automatically using metadata_create()
- Accepts optional comment parameter for mapping variant identification
- **This is the default save function** - all new code uses v1 format

#### `function_load_v1(hash_value)` (lines 2072-2100)
**Loads function from pool using schema v1**
- Reads object.json: `$BB_DIRECTORY/pool/XX/YYYYYY.../object.json`
- Returns: Dictionary with schema_version, hash, normalized_code, metadata
- Does NOT load language-specific data (use mapping functions for that)

#### `mappings_list_v1(func_hash, lang)` (lines 851-909)
**Lists all mapping variants for a language** (Schema v1)
- Scans language directory: `$BB_DIRECTORY/pool/XX/Y.../lang/`
- Returns: List of (mapping_hash, comment) tuples
- Used to discover available mapping variants
- Returns empty list if language doesn't exist

#### `mapping_load_v1(func_hash, lang, mapping_hash)` (lines 912-950)
**Loads specific language mapping** (Schema v1)
- Reads mapping.json: `$BB_DIRECTORY/pool/XX/Y.../lang/ZZ/W.../mapping.json`
- Returns: Tuple of (docstring, name_mapping, alias_mapping, comment)
- Content-addressed storage enables deduplication

#### `function_load(hash_value, lang, mapping_hash=None)` (lines 953-1011)
**Main entry point for loading functions**
- Calls function_load_v1() + mapping_load_v1()
- If multiple mappings exist and no mapping_hash specified, picks first alphabetically
- Returns: Tuple of (normalized_code, name_mapping, alias_mapping, docstring)
- **This is the default load function**

#### `function_show(hash_with_lang_and_mapping)` (lines 1014-1107)
**Show function with mapping exploration and selection**
- Supports three formats: `HASH@LANG`, `HASH@LANG@MAPPING_HASH`
- Single mapping: Outputs code directly to stdout
- Multiple mappings: Displays selection menu with copyable commands and comments
- Explicit mapping hash: Directly outputs specified mapping
- Uses function_load() + code_denormalize() to reconstruct original code
- **This is the recommended command** for exploring and viewing functions
- **Note**: CLI command `bb.py show HASH@lang[@mapping_hash]`

#### `code_denormalize(normalized_code, name_mapping, alias_mapping)` (lines 497-679)
**Reconstructs original-looking code**
- Reverses variable renaming: `_bb_v_X → original_name`
- Rewrites imports: `from bb.pool import X` → `from bb.pool import X as alias` (restores alias)
- Transforms calls: `HASH._bb_v_0(...)` → `alias(...)`

### Storage Schema

See [STORE.md](STORE.md) for the complete storage specification.

**Directory Structure:**
```
$BB_DIRECTORY/pool/            # Default: $HOME/.local/bb/pool/
  ab/                              # First 2 chars of function hash
    c123def456.../                 # Function directory (remaining hash chars)
      object.json                  # Core function data
      eng/                         # Language code directory
        xy/                        # First 2 chars of mapping hash
          z789.../                 # Mapping directory (remaining hash chars)
            mapping.json           # Language mapping (content-addressed)
      fra/                         # Another language
        mn/
          opqr.../
            mapping.json           # Another language/variant
```

**object.json**:
```json
{
  "schema_version": 1,
  "hash": "abc123...",
  "normalized_code": "def _bb_v_0(...):\n    ...",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "username"
  }
}
```

**mapping.json** (in lang/XX/YYY.../):
```json
{
  "docstring": "Calculate the average...",
  "name_mapping": {"_bb_v_0": "calculate_average", "_bb_v_1": "numbers"},
  "alias_mapping": {"abc123": "helper"},
  "comment": "Formal mathematical terminology"
}
```

Key features:
- Language identifiers up to 256 characters
- Multiple mappings per language via multiple mapping.json files
- Content-addressed mapping storage (deduplicated across functions)
- No duplication between object.json and mapping.json
- Extensible metadata (author, timestamp)

## Development Conventions

### Python Code Style

1. **Type hints**: Used in function signatures (`Dict[str, str]`, `Set[str]`, etc.)
2. **Docstrings**: Required for all public functions
3. **Error handling**: Explicit error messages to stderr, exit with code 1
4. **AST manipulation**: Use `ast` module, never regex on source code
5. **Encoding**: Always use `encoding='utf-8'` for file I/O

### Naming Conventions

- **Classes**: PascalCase (`ASTNormalizer`)
- **Functions**: snake_case following `type_name_verb_complement` pattern (`mapping_create_name`, `ast_normalize`, `function_save`)
- **Constants**: UPPER_SNAKE_CASE (`PYTHON_BUILTINS`)
- **Normalized names**: `_bb_v_N` (N = 0, 1, 2, ...)

#### Preferred Function Naming Pattern

**Note**: This is a project-specific convention that differs from traditional PEP 8 `verb_noun` patterns. Beyond Babel emphasizes a structured naming convention that puts the type being operated on first:

**Pattern**: `type_name_verb_complement`

**Structure**:
- `type_name`: The type/object being operated on (filepath, function, hash, ast, etc.)
- `verb`: The action being performed (open, get, save, compute, normalize, etc.)
- `complement` (optional): Additional context or output format

**Examples**:
- `filepath_open()` - Opens a filepath
- `function_get()` - Gets a function from the pool
- `function_dump_as_json()` - Dumps a function as JSON
- `hash_compute()` - Computes a hash
- `ast_normalize()` - Normalizes an AST
- `code_denormalize()` - Denormalizes code

**Rationale**: This convention groups related operations alphabetically and makes the primary subject clear at a glance, which is particularly useful in a function-centric architecture where functions are the primary unit of composition.

**Avoid type_name proliferation**: Too many distinct type_names hurts readability and discoverability. Prefer consolidating related concepts under a common prefix. Target type_names: `code_`, `command_`, `compile_`, `git_`, `helper_`, `storage_`, `hash_`.

**Special type_name `helper_`**: Use `helper_` prefix for utility functions that don't fit other categories (e.g., UI interactions, generic operations). Example: `helper_open_editor_for_message()`.

### Testing Conventions

- **Test framework**: pytest
- **Test structure**: All tests MUST be functions, not classes
- **Test naming**: Use descriptive names like `test_<component>_<behavior>` (e.g., `test_ast_normalizer_visit_name_with_mapping`)
- **Test file**: `test_bb.py` contains 50+ test functions
- **Normalized code strings**: All normalized code strings in tests MUST use the `normalize_code_for_test()` helper function. This ensures the code format matches `ast.unparse()` output (with proper line breaks and indentation).

**Example of normalize_code_for_test usage**:
```python
# Wrong - this format never exists in practice:
normalized_code = "def _bb_v_0(): return 42"

# Correct - use the helper function:
normalized_code = normalize_code_for_test("def _bb_v_0(): return 42")
# Returns: "def _bb_v_0():\n    return 42"
```

### Important Invariants

1. **Function name always `_bb_v_0`**: First entry in name mapping
2. **Built-ins never renamed**: `len`, `sum`, `print`, etc. preserved
3. **Imported names never renamed**: `math`, `Counter`, etc. preserved
4. **Imports sorted**: Lexicographically by module name
5. **Hash on logic only**: Docstrings excluded from hash computation
6. **Language codes**: Always 3 characters (ISO 639-3: eng, fra, spa, etc.)
7. **Hash format**: 64 lowercase hex characters (SHA256)

## Testing Strategy

### Philosophy: Grey-Box Integration First

Beyond Babel follows a **grey-box integration testing** approach as the primary testing strategy. Most tests exercise the CLI commands end-to-end while having knowledge of the internal storage format for assertions.

**Testing pyramid for Beyond Babel**:
1. **Integration tests (grey-box)** - Primary focus, organized by CLI command
2. **Unit tests** - Only for complex algorithms (AST normalization, hash computation, schema validation)

### Directory Structure

```
tests/
├── conftest.py              # Shared fixtures (CLIRunner, normalize_code_for_test)
├── integration/
│   └── test_workflows.py    # End-to-end workflows combining multiple commands
├── add/
│   └── test_add.py          # Tests for 'bb.py add' command
├── show/
│   └── test_show.py         # Tests for 'bb.py show' command
├── test_internals.py        # Unit tests for complex algorithms
└── test_storage.py          # Storage schema validation tests
```

### Grey-Box Integration Tests

Grey-box tests call CLI commands but verify internal state:

```python
def test_add_function_creates_v1_structure(cli_runner, tmp_path):
    """Test that add creates proper v1 directory structure"""
    # Setup: Create test file
    test_file = tmp_path / "math_func.py"
    test_file.write_text('''def add_numbers(a, b):
    """Add two numbers"""
    return a + b
''')

    # Test: Call CLI command
    func_hash = cli_runner.add(str(test_file), 'eng')

    # Assert: Check internal storage structure (grey-box)
    func_dir = cli_runner.pool_dir / func_hash[:2] / func_hash[2:]
    assert func_dir.exists()
    assert (func_dir / 'object.json').exists()
    assert (func_dir / 'eng').exists()
```

### Unit Tests for Complex Algorithms

Unit tests are reserved for low-level components where grey-box testing would be impractical:

- **AST normalization** (`ASTNormalizer` class, `ast_normalize` function)
- **Name mapping** (`mapping_create_name`, `mapping_compute_hash`)
- **Hash computation** (`hash_compute` with determinism guarantees)
- **Schema detection** (`schema_detect_version`)
- **Import handling** (`imports_rewrite_bb`, `calls_replace_bb`)

These tests live in `tests/test_internals.py`.

### Running Tests

```bash
# Run all tests
pytest

# Run integration tests only
pytest tests/integration/ tests/add/ tests/show/

# Run unit tests only
pytest tests/test_internals.py tests/test_storage.py

# Run with coverage
pytest --cov=bb --cov-report=html

# Run tests matching pattern
pytest -k "add"
```

### Test Conventions

**CRITICAL REQUIREMENT**: All pytest tests MUST be implemented as functions, not classes. Use descriptive function names like `test_add_function_creates_v1_structure()` instead of class-based organization.

**Normalized code strings**: All normalized code strings in tests MUST use the `normalize_code_for_test()` helper function to ensure the code format matches `ast.unparse()` output.

### Running Examples

```bash
# Add examples to pool
python3 bb.py add examples/example_simple.py@eng
python3 bb.py add examples/example_simple_french.py@fra
python3 bb.py add examples/example_simple_spanish.py@spa

# Verify they share the same hash
find ~/.local/bb/pool -name "object.json"  # or $BB_DIRECTORY/pool/

# Show in different language
python3 bb.py show HASH@eng
python3 bb.py show HASH@fra
```

### Verification Checklist

- [ ] Imports are sorted lexicographically
- [ ] Function renamed to `_bb_v_0`
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
- Be specific: "Add 'bb show HASH@lang' command"
- Reference context: "Extract docstrings from hash computation for multilingual support"

### Ignored Files

- `__pycache__/`, `*.pyc`: Python bytecode
- `.venv/`, `.env`: Virtual environments and secrets

**Note:** The function pool is now stored in `$HOME/.local/bb/` by default (configurable via `BB_DIRECTORY`), so it's no longer in the project directory and doesn't need to be in `.gitignore`.

## Chore

### TODO.md Maintenance

- **Format**: TODO.md should remain a bullet list with topic, type names, and intended feature - no more than one sentence per item
- **Cleanup**: Regularly remove implemented entries in atomic commits (separate from feature commits)

## Import Handling Rules

Understanding how imports are processed is critical to the normalization system.

### Import Categories

#### 1. Standard Library & External Package Imports
**Examples**: `import math`, `from collections import Counter`, `import numpy as np`

**Processing**:
- **Before storage**: Sorted lexicographically, **no renaming**
- **In storage**: Identical to original (e.g., `import math`)
- **From storage**: No transformation
- **Usage**: Names like `math`, `Counter`, `np` are **never renamed** to `_bb_v_X`

**Example:**
```python
# Original & Normalized (unchanged)
from collections import Counter
import math
```

#### 2. BB Imports (Pool Functions)
**Examples**: `from bb.pool import object_abc123def as helper`

**Important**: BB imports must use the `object_` prefix followed by the hash. This ensures valid Python identifiers since SHA256 hashes can start with digits (0-9), which would otherwise be invalid identifiers.

**Processing**:

**Before storage (normalization)**:
```python
from bb.pool import object_abc123def as helper
```
↓ becomes ↓
```python
from bb.pool import object_abc123def
```
- Alias removed: `as helper` is dropped
- Alias tracked in `alias_mapping`: `{"abc123def": "helper"}` (actual hash without prefix)
- Function calls transformed: `helper(x)` → `object_abc123def._bb_v_0(x)`

**From storage (denormalization)**:
```python
from bb.pool import object_abc123def
```
↓ becomes ↓
```python
from bb.pool import object_abc123def as helper
```
- Language-specific alias restored: `as helper` (from `alias_mapping[lang]`)
- Function calls transformed back: `object_abc123def._bb_v_0(x)` → `helper(x)`

### Why This Design?

- **Standard imports** are universal (same across all languages)
- **BB imports** have language-specific aliases:
  - English: `from bb.pool import object_abc123 as helper`
  - French: `from bb.pool import object_abc123 as assistant`
  - Spanish: `from bb.pool import object_abc123 as ayudante`

All normalize to: `from bb.pool import object_abc123`, ensuring identical hashes.

The `object_` prefix is required because SHA256 hashes can start with digits (0-9), which would make them invalid Python identifiers.

## Key Algorithms

### AST Normalization Algorithm

```
1. Parse source to AST
2. Sort imports lexicographically
3. Extract function definition
4. Extract docstring from function
5. Rewrite bb imports (remove aliases)
6. Create name mapping (excluding builtins, imports, bb aliases)
7. Replace bb calls (alias → HASH._bb_v_0)
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

### Public-Facing Hash Specification

**Public hashes in Beyond Babel refer to content-addressed identifiers that follow strict deterministic serialization rules to ensure global consistency.

#### Hash Computation Rules

1. **Canonical serialization**: Public hashes are computed from JSON-serialized objects with:
   - **Sorted keys**: `json.dumps(obj, sort_keys=True, ...)` ensures key order is deterministic
   - **Unicode preservation**: `ensure_ascii=False` maintains Unicode characters without escape sequences
   - **No indentation**: Compact format without whitespace (no `indent` parameter)
   - **Consistent encoding**: UTF-8 encoding for all serialized data

2. **Hash vs. filename distinction**:
   - The hash in a filename (e.g., `pool/ab/cdef123.../object.json` or `eng/xy/z789.../mapping.json`) identifies the **logical content**
   - It is NOT a hash of the physical file's bytes on disk
   - The stored JSON may include metadata, formatting, or additional fields not included in hash computation

3. **Intermediate representation hashing**:
   - The hash may be computed from a canonical intermediate JSON representation
   - This intermediate form may differ from what is actually written to disk
   - Example: Function hash computed from normalized code without docstring, but stored JSON includes docstring

#### Example

```python
# Canonical form for hashing (intermediate representation)
# Hash is computed on normalized code WITHOUT docstring
canonical_code = "def _bb_v_0(...):\n    ..."

# Compute hash from code
hash_value = hashlib.sha256(canonical_code.encode('utf-8')).hexdigest()

# Stored object.json includes additional metadata
stored = {
    "schema_version": 1,
    "hash": hash_value,
    "normalized_code": "def _bb_v_0(...):\n    \"\"\"Docstring...\"\"\"\n    ...",
    "metadata": {...}
}
# Hash of stored JSON ≠ hash_value (hash is of code only)
```

#### Implications

- **Reproducibility**: Any system can independently verify hashes by reconstructing the canonical form
- **Flexibility**: Storage format can evolve without breaking hash-based references
- **Integrity**: Hash identifies logical content, not storage artifacts

## Common Pitfalls for AI Assistants

1. **Don't modify hash computation**: Adding docstrings to hash breaks multilingual support
2. **Don't skip language suffix**: Commands require `@lang`, not optional
3. **Don't rename built-ins**: `PYTHON_BUILTINS` set must remain untouched
4. **Don't assume Python 3.8**: Code uses `ast.unparse()` (requires Python 3.9+)
5. **Don't break import sorting**: Lexicographic order is part of normalization
6. **Don't create duplicate mappings**: `_bb_v_0` is ALWAYS the function name

## Extension Points

### Adding New Features

1. **New commands**: Add to `argparse` subparsers in `main()` (lines 584-603)
2. **New normalizations**: Extend `ASTNormalizer` class
3. **New validations**: Add to `ast_normalize()` or command handlers
4. **New storage formats**: Modify `function_save()` and increment version field

### Future Considerations

- **Type checking**: Consider adding mypy type checking
- **Testing framework**: Project uses pytest for automated testing (see `test_bb.py`)
- **Documentation generation**: Extract docstrings to generate docs
- **Package distribution**: Consider setuptools/pyproject.toml for PyPI

## File Path References

When referencing code locations, use this format:

- `bb.py:272` - ast_normalize function
- `bb.py:133` - mapping_create_name function
- `bb.py:20` - ASTNormalizer class
- `bb.py:321` - hash_compute function

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

1. **Inspect JSON**: `cat ~/.local/bb/pool/XX/YYY.../object.json | python3 -m json.tool`
2. **Check AST**: Use `ast.dump()` to inspect tree structure
3. **Compare hashes**: Same logic should produce same hash
4. **Verify mappings**: Check name_mappings in JSON for correctness
5. **Test round-trip**: `add` then `show` should produce equivalent code

## Chore

Regular maintenance tasks for AI assistants working on this codebase:

- Regularly analyze type_name statistics and semantics
- Regularly update USAGE.md

## Summary

Beyond Babel is a carefully designed system for multilingual function sharing through AST normalization. The key insight is separating logic (hashed) from presentation (language-specific names/docstrings). When modifying the code:

- Preserve the invariants listed above
- Test with multiple languages
- Ensure hash computation remains deterministic
- Maintain backward compatibility with existing pool data

The codebase is self-contained (single file), well-structured (clear function boundaries), and follows Python best practices.
