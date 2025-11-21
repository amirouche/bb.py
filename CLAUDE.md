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
4. **Content-addressed storage**: Functions stored by hash in `$HOME/.local/ouverture/objects/XX/YYYYYY.json` (configurable via `OUVERTURE_DIRECTORY` environment variable)
5. **Single-file architecture**: All code resides in `ouverture.py` - no modularization into separate packages. This keeps the tool simple, self-contained, and easy to distribute as a single script.
6. **Native language debugging**: Tracebacks and debugger interactions show variable names in the original human language, not normalized forms

### Storage Location Configuration

The ouverture function pool location is controlled by the `OUVERTURE_DIRECTORY` environment variable:

- **Default**: `$HOME/.local/ouverture/` (follows XDG Base Directory specification)
- **Custom location**: Set `OUVERTURE_DIRECTORY=/path/to/pool` to override
- **Legacy behavior**: Set `OUVERTURE_DIRECTORY=.ouverture` to use project-local storage (pre-v1.0 behavior)

**Examples**:
```bash
# Use default location
python3 ouverture.py add example.py@eng

# Use custom location
export OUVERTURE_DIRECTORY=/shared/pool
python3 ouverture.py add example.py@eng

# Use project-local directory
export OUVERTURE_DIRECTORY=.ouverture
python3 ouverture.py add example.py@eng
```

**Migration note**: If you have existing `.ouverture/` directories in your projects, you can either:
1. Copy them to `$HOME/.local/ouverture/` to consolidate into a global pool
2. Set `OUVERTURE_DIRECTORY=.ouverture` to continue using project-local storage
3. Re-add your functions to the new default location

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
Store in $HOME/.local/ouverture/objects/ (or $OUVERTURE_DIRECTORY/objects/) with:
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
└── .gitignore                 # Ignores __pycache__, etc.

# Function pool (default location, configurable via OUVERTURE_DIRECTORY):
$HOME/.local/ouverture/
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

#### `ast_normalize(tree, lang)` (lines 272-318)
**Central normalization pipeline**
- Sorts imports lexicographically
- Extracts function definition and imports
- Extracts docstring separately
- Rewrites `from ouverture.pool import X as Y` → `from ouverture.pool import X` (removes alias)
- Creates name mappings (`original → _ouverture_v_X`)
- Returns: normalized code (with/without docstring), docstring, mappings

#### `mapping_create_name(function_def, imports, ouverture_aliases)` (lines 133-180)
**Generates bidirectional name mappings**
- Function name always gets `_ouverture_v_0`
- Variables/args get sequential indices: `_ouverture_v_1`, `_ouverture_v_2`, ...
- **Excluded from renaming**: Python builtins, imported names, ouverture aliases
- Returns: `(forward_mapping, reverse_mapping)`

#### `imports_rewrite_ouverture(imports)` (lines 183-213)
**Transforms ouverture imports for normalization**
- Rewrites: `from ouverture.pool import HASH as alias` → `from ouverture.pool import HASH` (removes alias)
- Tracks alias mappings for later denormalization
- Necessary because normalized code uses `HASH._ouverture_v_0(...)` instead of `alias(...)`

#### `calls_replace_ouverture(tree, alias_mapping, name_mapping)` (lines 216-235)
**Replaces aliased function calls with normalized form**
- Transforms: `alias(...)` → `HASH._ouverture_v_0(...)`
- Uses alias_mapping to determine which names are ouverture functions

#### `hash_compute(code)` (lines 321-323)
**Generates SHA256 hash**
- CRITICAL: Hash computed on code **WITHOUT docstring**
- Ensures same logic = same hash across languages

#### `function_save(hash_value, lang, ...)` (lines 326-362)
**Stores function in content-addressed pool**
- Path: `$OUVERTURE_DIRECTORY/objects/XX/YYYYYY.json` (default: `$HOME/.local/ouverture/`, XX = first 2 chars of hash)
- Merges with existing data if hash already exists
- Stores per-language: docstrings, name_mappings, alias_mappings

#### `code_denormalize(normalized_code, name_mapping, alias_mapping)` (lines 364-429)
**Reconstructs original-looking code**
- Reverses variable renaming: `_ouverture_v_X → original_name`
- Rewrites imports: `from ouverture.pool import X` → `from ouverture.pool import X as alias` (restores alias)
- Transforms calls: `HASH._ouverture_v_0(...)` → `alias(...)`

### Storage Schema

#### Current Schema (v0)

Functions are stored in `$OUVERTURE_DIRECTORY/objects/XX/YYYYYY.json` (default: `$HOME/.local/ouverture/objects/XX/YYYYYY.json`) where XX is the first 2 characters of the hash.

```json
{
  "version": 0,
  "hash": "abc123def456...",
  "normalized_code": "def _ouverture_v_0(...):\n    ...",
  "docstrings": {
    "eng": "Calculate the average...",
    "fra": "Calculer la moyenne..."
  },
  "name_mappings": {
    "eng": {"_ouverture_v_0": "calculate_average", "_ouverture_v_1": "numbers"},
    "fra": {"_ouverture_v_0": "calculer_moyenne", "_ouverture_v_1": "nombres"}
  },
  "alias_mappings": {
    "eng": {"abc123": "helper"},
    "fra": {"abc123": "assistant"}
  }
}
```

**Current limitations**:
- Language codes limited to 3 characters (ISO 639-3)
- Only one mapping per language
- Mappings stored inline (no deduplication)
- Limited extensibility

#### Future Schema (v1) - Planned

See `TODO.md` Priority 0 for the comprehensive redesign plan.

**Directory Structure:**
```
$OUVERTURE_DIRECTORY/objects/  # Default: $HOME/.local/ouverture/objects/
  ab/c123def456.../              # Function directory
    object.json                  # Core function data (no language data)
    eng/xy/z789.../mapping.json  # Language mapping (content-addressed)
    fra/mn/opqr.../mapping.json  # Another language/variant
```

**object.json** (minimal - no duplication):
```json
{
  "schema_version": 1,
  "hash": "abc123...",
  "hash_algorithm": "sha256",
  "normalized_code": "...",
  "encoding": "none",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "username",
    "tags": ["math", "statistics"],
    "dependencies": ["def456...", "ghi789..."]
  }
}
```

**mapping.json** (in lang-code/XX/YYY.../):
```json
{
  "docstring": "Calculate the average...",
  "name_mapping": {"_ouverture_v_0": "calculate_average", ...},
  "alias_mapping": {"abc123": "helper"}
}
```

Key improvements:
- Language identifiers up to 256 characters
- Multiple mappings per language via multiple mapping.json files
- Content-addressed mapping storage (deduplicated across functions)
- No duplication between object.json and mapping.json
- Extensible metadata (author, timestamp, tags, dependencies)
- Alternative hash algorithms support
- Optional compression

### CLI Commands

**Target CLI interface** (some commands not yet implemented):

#### Configuration and Identity
```bash
ouverture.py init                          # Initialize ouverture directory (default: $HOME/.local/ouverture/) and config
ouverture.py whoami username [USERNAME]    # Get/set username
ouverture.py whoami email [EMAIL]          # Get/set email
ouverture.py whoami public-key [URL]       # Get/set public key URL
ouverture.py whoami language [LANG...]     # Get/set preferred languages
```

#### Remote Repository Management
```bash
ouverture.py remote add NAME URL                    # Add HTTP/HTTPS remote
ouverture.py remote add NAME file:///path/to/db     # Add SQLite file remote
ouverture.py remote remove NAME                     # Remove remote
ouverture.py remote pull NAME                       # Fetch functions from remote
ouverture.py remote push NAME                       # Publish functions to remote
```

#### Function Operations
```bash
ouverture.py add FILENAME.py@LANG              # Add function to local pool
ouverture.py get HASH[@LANG] FILENAME.py       # Retrieve function and save to file (in specific language)
ouverture.py translate HASH@LANG LANG          # Add translation for existing function
ouverture.py review HASH                       # Recursively review function and dependencies (in user's languages)
ouverture.py run HASH@lang                     # Execute function interactively
ouverture.py run HASH@lang --debug             # Execute with debugger (native language variables)
```

#### Discovery
```bash
ouverture.py log [NAME | URL]                  # Show git-like commit log of pool/remote
ouverture.py search [NAME | URL] [QUERY...]    # Search and list functions by query
```

**Currently implemented**:
- `add` command: Parses file, normalizes AST, computes hash, saves to local pool
- `get` command: Retrieves function from local pool, denormalizes to target language

**Language codes**: Currently 3 characters (ISO 639-3: eng, fra, spa, etc.), future support for any string <256 chars

## Native Language Debugging

### Traceback Localization

When executing functions from the pool, exceptions will show variable names in the original human language rather than normalized forms.

**Example**:
```python
# Original French function
def calculer_moyenne(nombres):
    total = sum(nombres)
    return total / len(nombres)

# If executed and error occurs, traceback shows:
# NameError: name 'nombres' is not defined
# NOT: NameError: name '_ouverture_v_1' is not defined
```

**Implementation approach**:
- Intercept exceptions during execution
- Map `_ouverture_v_X` back to original names using stored mappings
- Rewrite traceback with native language variable names
- Preserve line numbers from original source
- Show both normalized and native versions for debugging

### Interactive Debugger Integration

When using `ouverture.py run HASH@lang --debug`:
- Variables displayed with native language names
- Can set breakpoints using original function/variable names
- Step through code with native language context
- Inspect values using familiar names (e.g., `print(nombres)` not `print(_ouverture_v_1)`)

**Integration with pdb**:
```python
# Debugging French function
(Pdb) l
  1  def calculer_moyenne(nombres):
  2      total = sum(nombres)
  3  ->  return total / len(nombres)

(Pdb) p nombres
[1, 2, 3, 4, 5]

(Pdb) p total
15
```

This makes debugging natural for developers working in their native language.

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
- **Normalized names**: `_ouverture_v_N` (N = 0, 1, 2, ...)

#### Preferred Function Naming Pattern

**Note**: This is a project-specific convention that differs from traditional PEP 8 `verb_noun` patterns. Ouverture emphasizes a structured naming convention that puts the type being operated on first:

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
find ~/.local/ouverture/objects -name "*.json"  # or $OUVERTURE_DIRECTORY/objects/

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

- `__pycache__/`, `*.pyc`: Python bytecode
- `.venv/`, `.env`: Virtual environments and secrets

**Note:** The function pool is now stored in `$HOME/.local/ouverture/` by default (configurable via `OUVERTURE_DIRECTORY`), so it's no longer in the project directory and doesn't need to be in `.gitignore`.

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
**Examples**: `from ouverture.pool import abc123def as helper`

**Processing**:

**Before storage (normalization)**:
```python
from ouverture.pool import abc123def as helper
```
↓ becomes ↓
```python
from ouverture.pool import abc123def
```
- Alias removed: `as helper` is dropped
- Alias tracked in `alias_mapping`: `{"abc123def": "helper"}`
- Function calls transformed: `helper(x)` → `abc123def._ouverture_v_0(x)`

**From storage (denormalization)**:
```python
from ouverture.pool import abc123def
```
↓ becomes ↓
```python
from ouverture.pool import abc123def as helper
```
- Language-specific alias restored: `as helper` (from `alias_mapping[lang]`)
- Function calls transformed back: `abc123def._ouverture_v_0(x)` → `helper(x)`

### Why This Design?

- **Standard imports** are universal (same across all languages)
- **Ouverture imports** have language-specific aliases:
  - English: `from ouverture.pool import abc123 as helper`
  - French: `from ouverture.pool import abc123 as assistant`
  - Spanish: `from ouverture.pool import abc123 as ayudante`

All normalize to: `from ouverture.pool import abc123`, ensuring identical hashes.

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
3. **New validations**: Add to `ast_normalize()` or command handlers
4. **New storage formats**: Modify `function_save()` and increment version field

### Future Considerations

- **Versioning**: JSON has `"version": 0` field for schema evolution
- **Type checking**: Consider adding mypy type checking
- **Testing framework**: Project uses pytest for automated testing (see `test_ouverture.py` and `README_PYTEST.md`)
- **Documentation generation**: Extract docstrings to generate docs
- **Package distribution**: Consider setuptools/pyproject.toml for PyPI

## File Path References

When referencing code locations, use this format:

- `ouverture.py:272` - ast_normalize function
- `ouverture.py:133` - mapping_create_name function
- `ouverture.py:20` - ASTNormalizer class
- `ouverture.py:321` - hash_compute function

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
