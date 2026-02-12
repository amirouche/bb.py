# CLAUDE.md - AI Assistant Guide for Beyond Babel

For practical development workflows (running tests, adding commands, writing transcripts), see **SKILLS.md**.

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

## Development Conventions

### Python Code Style

1. **Type hints**: Used in function signatures (`Dict[str, str]`, `Set[str]`, etc.)
2. **Docstrings**: Required for all public functions
3. **Error handling**: Explicit error messages to stderr, exit with code 1
4. **AST manipulation**: Use `ast` module, never regex on source code
5. **Encoding**: Always use `encoding='utf-8'` for file I/O

### Naming Conventions

- **Classes**: PascalCase (`ASTNormalizer`)
- **Functions**: snake_case following `type_name_verb_complement` pattern (see SKILLS.md for details and examples)
- **Constants**: UPPER_SNAKE_CASE (`PYTHON_BUILTINS`)
- **Normalized names**: `_bb_v_N` (N = 0, 1, 2, ...)

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
├── transcript/
│   ├── conftest.py          # Transcript fixtures (isolated_bb_dir, --transcript option)
│   ├── parser.py            # Transcript markdown parser
│   ├── runner.py            # Transcript workflow executor
│   ├── test_parser.py       # Parser unit tests
│   ├── test_runner.py       # Runner unit tests
│   └── test_transcripts.py  # Parameterized transcript tests
├── test_internals.py        # Unit tests for complex algorithms
└── test_storage.py          # Storage schema validation tests
```

### normalize_code_for_test

All `normalized_code` values in tests MUST use the `normalize_code_for_test()` helper function. This ensures the code format matches `ast.unparse()` output (with proper line breaks and indentation). This applies to:
  - Direct assignments like `normalized_code = ...`
  - JSON fixture data like `object.json` files created in tests
  - Any string comparison involving normalized code

```python
from tests.conftest import normalize_code_for_test

# Wrong - this format never exists in practice:
normalized_code = "def _bb_v_0(): return 42"

# Correct - use the helper function:
normalized_code = normalize_code_for_test("def _bb_v_0(): return 42")
# Returns: "def _bb_v_0():\n    return 42"
```

### Verification Checklist

- [ ] Imports are sorted lexicographically
- [ ] Function renamed to `_bb_v_0`
- [ ] Variables renamed sequentially
- [ ] Built-ins NOT renamed (`sum`, `len`, `print`)
- [ ] Imports NOT renamed (`math`, `Counter`)
- [ ] Docstring stored separately per language
- [ ] Hash identical for same logic in different languages

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

#### 2. BB Imports (Pool Functions)
**Examples**: `from bb.pool import object_abc123def as helper`

**Important**: BB imports must use the `object_` prefix followed by the hash. This ensures valid Python identifiers since SHA256 hashes can start with digits (0-9), which would otherwise be invalid identifiers.

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

Public hashes in Beyond Babel refer to content-addressed identifiers that follow strict deterministic serialization rules to ensure global consistency.

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

## Common Pitfalls for AI Assistants

1. **Don't modify hash computation**: Adding docstrings to hash breaks multilingual support
2. **Don't skip language suffix**: Commands require `@lang`, not optional
3. **Don't rename built-ins**: `PYTHON_BUILTINS` set must remain untouched
4. **Don't assume Python 3.8**: Code uses `ast.unparse()` (requires Python 3.9+)
5. **Don't break import sorting**: Lexicographic order is part of normalization
6. **Don't create duplicate mappings**: `_bb_v_0` is ALWAYS the function name

## Questions to Ask Before Making Changes

1. Does this change affect hash computation? (If yes, be very careful)
2. Does this break multilingual support? (Test with different languages)
3. Does this preserve built-in and imported name handling?
4. Does this maintain import sorting?
5. Is the JSON schema still backward compatible?
6. Are error messages helpful to users?

## Chore

### TODO.md Maintenance

- **Format**: TODO.md should remain a bullet list with topic, type names, and intended feature - no more than one sentence per item
- **Cleanup**: Regularly remove implemented entries in atomic commits (separate from feature commits)

### Regular Maintenance

- Regularly analyze type_name statistics and semantics

## Summary

Beyond Babel is a carefully designed system for multilingual function sharing through AST normalization. The key insight is separating logic (hashed) from presentation (language-specific names/docstrings). When modifying the code:

- Preserve the invariants listed above
- Test with multiple languages
- Ensure hash computation remains deterministic
- Maintain backward compatibility with existing pool data

The codebase is self-contained (single file), well-structured (clear function boundaries), and follows Python best practices.
