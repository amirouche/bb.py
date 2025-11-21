# Refactoring Ouverture: Code Organization Strategy

## Current State

**ouverture.py**: ~600 lines, single file containing:
- AST normalization (`ASTNormalizer` class)
- Name mapping creation and management
- Import rewriting (standard library and ouverture)
- Hash computation
- File I/O (save/load from `.ouverture/objects/`)
- Denormalization (reconstructing original code)
- CLI interface (argparse)
- Main entry point

**Strengths**:
- Self-contained, easy to distribute
- No dependencies beyond Python stdlib
- Simple mental model (one file to understand)

**Weaknesses**:
- Difficult to test individual components
- Mixed concerns (AST manipulation + I/O + CLI)
- Hard to extend without touching everything
- No clear API boundaries
- Difficult for contributors to navigate

## Refactoring Goals

1. **Modularity**: Separate concerns into focused modules
2. **Testability**: Each module independently testable
3. **Extensibility**: Easy to add features without modifying core
4. **Maintainability**: Clear boundaries and responsibilities
5. **Backward compatibility**: Existing `.ouverture/` pools must work
6. **Distribution**: Consider packaging for PyPI

## Proposed Structure

```
ouverture/
├── __init__.py                 # Public API exports
├── __main__.py                 # Entry point for python -m ouverture
├── ast_normalization.py        # AST transformation
├── name_mapping.py             # Variable name management
├── import_handling.py          # Import rewriting logic
├── hash_computation.py         # Hashing algorithms
├── storage.py                  # File I/O, pool management
├── denormalization.py          # Code reconstruction
├── cli.py                      # Command-line interface
├── config.py                   # Configuration, constants
└── exceptions.py               # Custom exception types

tests/
├── test_ast_normalization.py
├── test_name_mapping.py
├── test_import_handling.py
├── test_hash_computation.py
├── test_storage.py
├── test_denormalization.py
├── test_cli.py
└── test_integration.py

docs/
├── API.md                      # Python API documentation
├── INTERNALS.md                # Architecture guide
└── CONTRIBUTING.md             # Contributor guide

pyproject.toml                  # Modern Python packaging
README.md                       # Project overview
CLAUDE.md                       # AI assistant guide
FEATURES_LIMITATIONS.md         # What works and doesn't
```

## Module Breakdown

### 1. `ast_normalization.py`

**Responsibility**: Transform Python ASTs to canonical form

```python
from typing import Dict, Set
import ast

class ASTNormalizer(ast.NodeTransformer):
    """Transform AST by renaming identifiers according to mapping."""

    def __init__(self, name_mapping: Dict[str, str]):
        self.name_mapping = name_mapping

    def visit_Name(self, node: ast.Name) -> ast.Name:
        # Implementation...

    def visit_arg(self, node: ast.arg) -> ast.arg:
        # Implementation...

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        # Implementation...

def normalize_ast(
    tree: ast.AST,
    lang: str,
    exclude_names: Set[str]
) -> NormalizationResult:
    """
    Normalize an AST to canonical form.

    Returns:
        NormalizationResult with:
        - normalized_code: str
        - docstring: Optional[str]
        - name_mapping: Dict[str, str]
        - alias_mapping: Dict[str, str]
    """
    # Implementation...

class NormalizationResult:
    """Result of AST normalization."""
    normalized_code: str
    normalized_code_no_docstring: str
    docstring: Optional[str]
    name_mapping: Dict[str, str]
    alias_mapping: Dict[str, str]
```

**Key functions**:
- `normalize_ast()`: Main entry point
- `extract_function_def()`: Find function in module
- `sort_imports()`: Lexicographic ordering
- `clear_location_info()`: Remove AST metadata

### 2. `name_mapping.py`

**Responsibility**: Create and manage variable name mappings

```python
from typing import Dict, Set, Tuple
import ast

def create_name_mapping(
    function_def: ast.FunctionDef,
    imports: List[ast.Import],
    ouverture_aliases: Set[str]
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create bidirectional name mapping.

    Returns:
        (forward_mapping, reverse_mapping)
        forward: original_name -> _ouverture_v_N
        reverse: _ouverture_v_N -> original_name
    """
    # Implementation...

def collect_names(node: ast.AST) -> Set[str]:
    """Collect all names used in an AST node."""
    # Implementation...

def get_imported_names(imports: List[ast.Import]) -> Set[str]:
    """Extract names made available by imports."""
    # Implementation...

PYTHON_BUILTINS: Set[str] = {
    'abs', 'all', 'any', 'bin', 'bool', 'chr', 'dict',
    # ... complete list
}
```

### 3. `import_handling.py`

**Responsibility**: Rewrite and normalize imports

```python
from typing import Dict, List, Tuple
import ast

def rewrite_ouverture_imports(
    imports: List[ast.Import]
) -> Tuple[List[ast.Import], Dict[str, str]]:
    """
    Rewrite ouverture imports to canonical form.

    from ouverture import HASH as alias
    -> from ouverture import HASH

    Returns:
        (rewritten_imports, alias_mapping)
    """
    # Implementation...

def replace_ouverture_calls(
    tree: ast.AST,
    alias_mapping: Dict[str, str],
    name_mapping: Dict[str, str]
) -> ast.AST:
    """
    Replace aliased function calls with canonical form.

    alias(x) -> HASH._ouverture_v_0(x)
    """
    # Implementation...

def restore_ouverture_imports(
    imports: List[ast.Import],
    alias_mapping: Dict[str, str]
) -> List[ast.Import]:
    """
    Restore language-specific aliases to imports.

    from ouverture import HASH
    -> from ouverture import HASH as alias
    """
    # Implementation...
```

### 4. `hash_computation.py`

**Responsibility**: Compute deterministic hashes

```python
import hashlib

def compute_hash(code: str) -> str:
    """
    Compute SHA256 hash of normalized code.

    IMPORTANT: Code must NOT include docstring for
    multilingual support.
    """
    return hashlib.sha256(code.encode('utf-8')).hexdigest()

def verify_hash(code: str, expected_hash: str) -> bool:
    """Verify that code matches expected hash."""
    return compute_hash(code) == expected_hash

# Future: Support multiple hash algorithms
class HashAlgorithm(Enum):
    SHA256 = "sha256"
    BLAKE2B = "blake2b"

def compute_hash_with_algorithm(
    code: str,
    algorithm: HashAlgorithm = HashAlgorithm.SHA256
) -> str:
    """Compute hash using specified algorithm."""
    # Implementation...
```

### 5. `storage.py`

**Responsibility**: Manage function pool storage

```python
from pathlib import Path
from typing import Dict, Optional
import json

class FunctionPool:
    """Manage content-addressed function storage."""

    def __init__(self, pool_dir: Path = Path(".ouverture")):
        self.pool_dir = pool_dir
        self.objects_dir = pool_dir / "objects"

    def save_function(
        self,
        hash_value: str,
        lang: str,
        normalized_code: str,
        docstring: Optional[str],
        name_mapping: Dict[str, str],
        alias_mapping: Dict[str, str]
    ) -> None:
        """Save or update function in pool."""
        # Implementation...

    def load_function(
        self,
        hash_value: str,
        lang: str
    ) -> FunctionData:
        """Load function from pool."""
        # Implementation...

    def function_exists(self, hash_value: str) -> bool:
        """Check if function exists in pool."""
        # Implementation...

    def get_languages(self, hash_value: str) -> List[str]:
        """Get available languages for a function."""
        # Implementation...

    def _get_path(self, hash_value: str) -> Path:
        """Get storage path for hash."""
        prefix = hash_value[:2]
        suffix = hash_value[2:] + ".json"
        return self.objects_dir / prefix / suffix

class FunctionData:
    """Data structure for stored functions."""
    version: int
    hash: str
    normalized_code: str
    languages: Dict[str, LanguageData]

class LanguageData:
    """Language-specific function data."""
    docstring: Optional[str]
    name_mapping: Dict[str, str]
    alias_mapping: Dict[str, str]
```

### 6. `denormalization.py`

**Responsibility**: Reconstruct code in target language

```python
from typing import Dict
import ast

def denormalize_code(
    normalized_code: str,
    name_mapping: Dict[str, str],
    alias_mapping: Dict[str, str]
) -> str:
    """
    Reconstruct code with language-specific names.

    - Reverse variable renaming
    - Restore import aliases
    - Transform function calls
    """
    # Implementation...

class Denormalizer(ast.NodeTransformer):
    """Reverse the normalization process."""

    def __init__(
        self,
        reverse_mapping: Dict[str, str],
        alias_mapping: Dict[str, str]
    ):
        self.reverse_mapping = reverse_mapping
        self.alias_mapping = alias_mapping

    # Implementation...
```

### 7. `cli.py`

**Responsibility**: Command-line interface

```python
import argparse
from pathlib import Path
from .storage import FunctionPool
from .ast_normalization import normalize_ast
from .hash_computation import compute_hash
from .denormalization import denormalize_code

def create_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Ouverture: multilingual function pool"
    )
    subparsers = parser.add_subparsers(dest='command')

    # add command
    add_parser = subparsers.add_parser('add')
    add_parser.add_argument('file_and_lang', help='FILE@LANG')

    # get command
    get_parser = subparsers.add_parser('get')
    get_parser.add_argument('hash_and_lang', help='HASH@LANG')

    # list command (new!)
    list_parser = subparsers.add_parser('list')
    list_parser.add_argument('--hash', help='Show languages for hash')

    return parser

def cmd_add(file_path: Path, lang: str) -> None:
    """Add function to pool."""
    # Implementation using other modules...

def cmd_get(hash_value: str, lang: str) -> None:
    """Retrieve function from pool."""
    # Implementation using other modules...

def cmd_list(hash_value: Optional[str] = None) -> None:
    """List functions in pool."""
    # Implementation...

def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command == 'add':
        # Parse file@lang
        # Call cmd_add()
    elif args.command == 'get':
        # Parse hash@lang
        # Call cmd_get()
    elif args.command == 'list':
        # Call cmd_list()
    else:
        parser.print_help()
        return 1

    return 0
```

### 8. `config.py`

**Responsibility**: Configuration and constants

```python
from pathlib import Path
from typing import Set

# Pool configuration
DEFAULT_POOL_DIR = Path(".ouverture")
OBJECTS_SUBDIR = "objects"
HASH_PREFIX_LENGTH = 2

# Storage format
STORAGE_VERSION = 0

# Language codes
LANGUAGE_CODE_LENGTH = 3  # ISO 639-3

# Hash format
HASH_LENGTH = 64  # SHA256
HASH_ALGORITHM = "sha256"

# Python built-ins (complete set)
PYTHON_BUILTINS: Set[str] = {
    'abs', 'all', 'any', 'ascii', 'bin', 'bool', 'breakpoint',
    'bytearray', 'bytes', 'callable', 'chr', 'classmethod',
    # ... complete list
}

# Normalization
NORMALIZED_NAME_PREFIX = "_ouverture_v_"
FUNCTION_NAME_INDEX = 0  # Always _ouverture_v_0
```

### 9. `exceptions.py`

**Responsibility**: Custom exception types

```python
class OuvertureError(Exception):
    """Base exception for Ouverture errors."""
    pass

class NormalizationError(OuvertureError):
    """Error during AST normalization."""
    pass

class StorageError(OuvertureError):
    """Error accessing function pool."""
    pass

class HashError(OuvertureError):
    """Error computing or verifying hash."""
    pass

class LanguageError(OuvertureError):
    """Invalid language code."""
    pass

class FunctionNotFoundError(OuvertureError):
    """Function not found in pool."""
    pass

class MultipleDefinitionsError(NormalizationError):
    """Multiple function definitions in file."""
    pass
```

### 10. `__init__.py`

**Responsibility**: Public API exports

```python
"""
Ouverture: multilingual function pool
"""

__version__ = "0.1.0"

from .storage import FunctionPool, FunctionData
from .ast_normalization import normalize_ast, NormalizationResult
from .hash_computation import compute_hash
from .denormalization import denormalize_code
from .exceptions import (
    OuvertureError,
    NormalizationError,
    StorageError,
    FunctionNotFoundError,
)

# Public API
__all__ = [
    "FunctionPool",
    "FunctionData",
    "normalize_ast",
    "NormalizationResult",
    "compute_hash",
    "denormalize_code",
    "OuvertureError",
    "NormalizationError",
    "StorageError",
    "FunctionNotFoundError",
]

# Example usage:
# from ouverture import FunctionPool
# pool = FunctionPool()
# pool.save_function(...)
```

## Migration Strategy

### Phase 1: Extract Core Logic (Week 1)

**Goal**: Move logic into modules, keep `ouverture.py` as wrapper

1. Create `ouverture/` directory
2. Extract classes/functions to new modules
3. Import everything back in `ouverture.py` for compatibility
4. Run full test suite (should pass)

**Deliverable**: Modular structure, zero breaking changes

### Phase 2: Update Tests (Week 2)

**Goal**: Test modules independently

1. Update `test_ouverture.py` to import from modules
2. Add module-specific test files
3. Increase coverage for edge cases
4. Add integration tests

**Deliverable**: >90% test coverage, module-level tests

### Phase 3: Improve CLI (Week 3)

**Goal**: Better command-line interface

1. Add `list` command
2. Add `--version` flag
3. Improve error messages
4. Add `--verbose` mode

**Deliverable**: More user-friendly CLI

### Phase 4: Package for Distribution (Week 4)

**Goal**: Make Ouverture installable

1. Create `pyproject.toml`
2. Add entry point: `ouverture` command
3. Test installation: `pip install -e .`
4. Prepare for PyPI

**Deliverable**: `pip install ouverture` works

### Phase 5: Documentation (Week 5)

**Goal**: Clear API documentation

1. Add docstrings to all public functions
2. Create `docs/API.md`
3. Create `docs/INTERNALS.md`
4. Create `docs/CONTRIBUTING.md`

**Deliverable**: Complete documentation

### Phase 6: Deprecate Old Interface (Week 6)

**Goal**: Clean up `ouverture.py` legacy

1. Move CLI entry point to `ouverture/__main__.py`
2. Make `ouverture.py` a thin wrapper with deprecation warning
3. Update README with new import patterns
4. Plan removal in v0.2.0

**Deliverable**: Clean structure, migration path

## Testing Strategy

### Unit Tests

Each module has its own test file:

```python
# test_ast_normalization.py
def test_normalize_simple_function():
    code = """
def add(a, b):
    return a + b
"""
    result = normalize_ast(ast.parse(code), "eng", set())
    assert "_ouverture_v_0" in result.normalized_code

def test_normalize_preserves_builtins():
    code = """
def process(items):
    return sum(items)
"""
    result = normalize_ast(ast.parse(code), "eng", set())
    assert "sum" in result.normalized_code  # Not renamed
```

### Integration Tests

Test full workflows:

```python
# test_integration.py
def test_add_and_retrieve_roundtrip():
    """Test adding and retrieving preserves behavior."""
    pool = FunctionPool(tmp_path)

    # Add function
    original_code = load_file("examples/example_simple.py")
    hash_val = add_to_pool(pool, original_code, "eng")

    # Retrieve function
    retrieved = pool.load_function(hash_val, "eng")
    reconstructed = denormalize_code(
        retrieved.normalized_code,
        retrieved.languages["eng"].name_mapping,
        retrieved.languages["eng"].alias_mapping
    )

    # Verify behavior
    assert exec_and_test(reconstructed) == exec_and_test(original_code)

def test_multilingual_same_hash():
    """Test that equivalent functions in different languages share hash."""
    pool = FunctionPool(tmp_path)

    eng_code = load_file("examples/example_simple.py")
    fra_code = load_file("examples/example_simple_french.py")
    spa_code = load_file("examples/example_simple_spanish.py")

    hash_eng = add_to_pool(pool, eng_code, "eng")
    hash_fra = add_to_pool(pool, fra_code, "fra")
    hash_spa = add_to_pool(pool, spa_code, "spa")

    assert hash_eng == hash_fra == hash_spa
```

## API Design

### Programmatic Usage

Enable use as a library:

```python
from ouverture import FunctionPool, normalize_ast, compute_hash

# Create pool
pool = FunctionPool("/path/to/pool")

# Add function
with open("my_function.py") as f:
    code = f.read()

tree = ast.parse(code)
result = normalize_ast(tree, "eng", set())
hash_val = compute_hash(result.normalized_code_no_docstring)

pool.save_function(
    hash_val,
    "eng",
    result.normalized_code,
    result.docstring,
    result.name_mapping,
    result.alias_mapping
)

# Retrieve function
data = pool.load_function(hash_val, "fra")
reconstructed = denormalize_code(
    data.normalized_code,
    data.languages["fra"].name_mapping,
    data.languages["fra"].alias_mapping
)

print(reconstructed)
```

### Plugin System (Future)

Enable extensions:

```python
from ouverture.plugins import NormalizationPlugin

class SemanticNormalizationPlugin(NormalizationPlugin):
    """Add semantic equivalence detection."""

    def pre_normalize(self, tree: ast.AST) -> ast.AST:
        """Transform AST before normalization."""
        # Convert sum() to loop, or vice versa
        return tree

    def suggest_equivalents(self, hash_val: str) -> List[str]:
        """Suggest semantically equivalent functions."""
        # Run property testing
        return ["hash1", "hash2"]

# Register plugin
pool.register_plugin(SemanticNormalizationPlugin())
```

## Packaging Configuration

### `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "ouverture"
version = "0.1.0"
description = "Multilingual function pool for Python"
readme = "README.md"
requires-python = ">=3.9"
license = {text = "MIT"}
authors = [
    {name = "Ouverture Contributors"}
]
keywords = ["multilingual", "code-sharing", "ast", "normalization"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

[project.urls]
Homepage = "https://github.com/amirouche/ouverture.py"
Issues = "https://github.com/amirouche/ouverture.py/issues"

[project.scripts]
ouverture = "ouverture.cli:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = "test_*.py"
python_functions = "test_*"

[tool.coverage.run]
source = ["ouverture"]
omit = ["tests/*"]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
]

[tool.mypy]
python_version = "3.9"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true

[tool.ruff]
line-length = 88
target-version = "py39"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B"]
```

## Benefits of Refactoring

### For Users

- **Better error messages**: Module-specific exceptions
- **Programmatic access**: Use as library, not just CLI
- **Easier installation**: `pip install ouverture`
- **More features**: Plugins, list command, verbose mode

### For Contributors

- **Clear boundaries**: Know where to add features
- **Easier testing**: Test modules independently
- **Better documentation**: Each module documented separately
- **Lower barrier**: Smaller files, focused concerns

### For Maintainers

- **Easier debugging**: Isolate issues to specific modules
- **Safer refactoring**: Change one module at a time
- **Better code review**: Smaller, focused PRs
- **Type checking**: Add mypy gradually per module

## Risks and Mitigation

### Risk 1: Breaking Changes

**Mitigation**: Keep `ouverture.py` as compatibility wrapper during transition

### Risk 2: Increased Complexity

**Mitigation**: Clear documentation, simple module structure

### Risk 3: Testing Burden

**Mitigation**: Maintain integration tests, ensure coverage stays high

### Risk 4: Loss of "Single File" Simplicity

**Mitigation**: Keep total codebase small, document architecture clearly

## Alternatives Considered

### Alternative 1: Stay Single File

**Pros**: Simple to distribute, easy mental model
**Cons**: Hard to maintain, difficult to extend
**Verdict**: Not scalable as features grow

### Alternative 2: Minimal Split (3-4 modules)

**Pros**: Less overhead than full modularization
**Cons**: Still some mixed concerns
**Verdict**: Could work, but full split is cleaner

### Alternative 3: Microservices

**Pros**: Ultimate modularity
**Cons**: Way overkill for this project
**Verdict**: Rejected, unnecessary complexity

## Timeline

**Week 1-2**: Extract modules, maintain compatibility
**Week 3-4**: Update tests, improve CLI
**Week 5-6**: Documentation, packaging
**Week 7**: Release v0.1.0 with new structure
**Week 8+**: Deprecate old interface, plan v0.2.0

## Success Criteria

- ✅ All existing tests pass
- ✅ Test coverage >90%
- ✅ Can install with `pip install ouverture`
- ✅ Can import and use as library
- ✅ All public functions have docstrings
- ✅ CLI has `--help` for all commands
- ✅ Migration guide in documentation

## Conclusion

Refactoring Ouverture from a single 600-line file to a modular package will:
- Improve maintainability
- Enable programmatic use
- Make contribution easier
- Prepare for future features

The key is **incremental migration**: don't break existing functionality, provide clear migration path, maintain high test coverage throughout.

The goal: Ouverture as a **library** with a CLI, not just a script.
