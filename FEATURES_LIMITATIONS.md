# Ouverture: Features and Limitations

This document provides a comprehensive overview of what Ouverture currently supports and its known limitations.

## Core Capabilities

### ✅ What Ouverture CAN Do

#### 1. Function Storage and Retrieval

- **Single function per file**: Store and retrieve individual Python functions
- **Content-addressed storage**: Functions are stored by hash in `.ouverture/objects/XX/YYYYYY.json`
- **Deterministic hashing**: Same logic produces the same hash regardless of variable/function names
- **Multilingual variants**: Multiple language versions of the same function share one hash

Example:
```bash
python3 ouverture.py add examples/example_simple.py@eng
python3 ouverture.py get <HASH>@fra
```

#### 2. Import Support

**Standard library and external packages** are fully supported and preserved:
```python
import math
from collections import Counter
import numpy as np

def process_data(items):
    return Counter(items)
```

- Import names (`math`, `Counter`, `np`) are **never renamed**
- Imports are sorted lexicographically during normalization
- All standard library modules work as expected

**Ouverture imports** for compositional functions:
```python
from ouverture import abc123def as helper

def process_values(data):
    return helper(data) * 2
```

- Aliases are tracked per language
- Normalized form: `from couverture import abc123def` (note: known typo, should be `ouverture`)
- Function calls transformed: `helper(x)` → `abc123def._ouverture_v_0(x)`

#### 3. Language Features

**Decorators** are supported and preserved:
```python
@staticmethod
@my_decorator
def my_function(x):
    return x * 2
```

Decorators are part of the function's AST and are stored/retrieved with the function.

**Type hints** are supported and preserved as-is:
```python
def calculate_sum(a: int, b: int) -> int:
    return a + b
```

Note: Type hints are not normalized, so different type hint styles will produce different hashes.

**Nested scopes** within a function:
```python
def outer(x):
    def inner(y):
        return x + y
    return inner(10)
```

#### 4. Name Normalization

Ouverture normalizes:
- Function names → `_ouverture_v_0` (always)
- Variable names → `_ouverture_v_1`, `_ouverture_v_2`, etc. (alphabetically sorted)
- Function arguments → normalized sequentially

Ouverture **does NOT** normalize:
- Python built-ins (`len`, `sum`, `print`, `range`, etc.)
- Imported names (`math`, `Counter`, `pd`, etc.)
- Ouverture function aliases

#### 5. Multilingual Support

- **Docstrings**: Stored separately per language, excluded from hash computation
- **Variable names**: Language-specific names preserved in mappings
- **Reconstruction**: Code reconstructed in target language with original naming style

Example equivalence (same hash):
```python
# English
def calculate_average(numbers):
    """Calculate the average of a list of numbers"""
    return sum(numbers) / len(numbers)

# French
def calculer_moyenne(nombres):
    """Calcule la moyenne d'une liste de nombres"""
    return sum(nombres) / len(nombres)

# Spanish
def calcular_promedio(numeros):
    """Calcula el promedio de una lista de números"""
    return sum(numeros) / len(numeros)
```

All three produce identical hashes because the logic is identical.

---

## Limitations

### ❌ What Ouverture CANNOT Do

#### 1. No Class or Method Support

**Cannot store classes**:
```python
# ❌ NOT SUPPORTED
class MyClass:
    def method(self):
        return 42
```

The code only looks for `ast.FunctionDef`, not `ast.ClassDef`. Classes are rejected during parsing.

**Cannot store methods**:
```python
# ❌ NOT SUPPORTED
class Calculator:
    def add(self, a, b):
        return a + b
```

Methods are part of classes, which aren't supported. Only standalone functions work.

**Workaround**: Extract logic into standalone functions:
```python
# ✅ SUPPORTED
def calculator_add(a, b):
    """Add two numbers"""
    return a + b
```

#### 2. No Global Variable Storage

**Cannot store global variables**:
```python
# ❌ NOT SUPPORTED
CONSTANT = 42

def use_constant():
    return CONSTANT
```

Only function definitions are extracted. Module-level variables are ignored.

**Workaround**: Return values from functions:
```python
# ✅ SUPPORTED
def get_constant():
    """Return the constant value"""
    return 42

def use_constant():
    return get_constant()
```

#### 3. One Function Per File

**Cannot store multiple functions**:
```python
# ❌ NOT SUPPORTED
def helper():
    return 1

def main():
    return helper() + 2
```

The `extract_function_def()` function raises an error: "Only one function definition is allowed per file"

**Workaround**: Split into separate files or nest functions:
```python
# ✅ SUPPORTED (nested)
def main():
    def helper():
        return 1
    return helper() + 2
```

Or store `helper()` separately and import it via ouverture:
```python
# file1.py
def helper():
    return 1

# file2.py
from ouverture import <HASH_OF_HELPER> as helper

def main():
    return helper() + 2
```

#### 4. No Async Function Support

**Async functions not explicitly handled**:
```python
# ⚠️ UNTESTED / LIKELY UNSUPPORTED
async def fetch_data(url):
    return await http.get(url)
```

The code only checks for `ast.FunctionDef`, not `ast.AsyncFunctionDef`. Async functions may fail during normalization.

**Status**: Not tested, behavior undefined.

#### 5. No Lambda Support (Top-Level)

**Cannot store lambdas as top-level constructs**:
```python
# ❌ NOT SUPPORTED
my_func = lambda x: x * 2
```

Only `def` function definitions are recognized.

**Lambdas inside functions work fine**:
```python
# ✅ SUPPORTED
def process(items):
    return list(map(lambda x: x * 2, items))
```

#### 6. No Module-Level Executable Code

**Cannot have statements outside the function**:
```python
# ❌ NOT SUPPORTED
import math

print("Loading module...")  # ❌ Not allowed

def calculate():
    return math.sqrt(2)
```

Only imports and one function definition are allowed at module level.

#### 7. Syntactic Equivalence Only

**No semantic analysis**:
```python
# These are semantically equivalent but have DIFFERENT hashes:

# Version 1
def sum_list(items):
    return sum(items)

# Version 2
def sum_list(items):
    total = 0
    for item in items:
        total += item
    return total
```

Ouverture only detects syntactic equivalence (same AST structure), not semantic equivalence (same behavior).

#### 8. No Validation of Ouverture Imports

**Imports not verified to exist**:
```python
# No error at add time, even if hash doesn't exist:
from ouverture import nonexistent_hash as helper

def use_helper(x):
    return helper(x)
```

The function will be stored successfully, but will fail at runtime if the imported hash doesn't exist in the pool.

#### 9. Type Hints Not Normalized

**Different type annotations produce different hashes**:
```python
# These have DIFFERENT hashes:

def add(a: int, b: int) -> int:
    return a + b

def add(a, b):  # No type hints
    return a + b
```

Type hints are preserved as-is, not normalized, so they affect the hash.

#### 10. Python Version Requirements

**Requires Python 3.9+**:
- Uses `ast.unparse()` which was added in Python 3.9
- Won't work on Python 3.8 or earlier

#### 11. Known Bugs

**Import rewriting typo** (ouverture.py:205):
```python
# Bug: rewrites to 'couverture' instead of 'ouverture'
new_imp = ast.ImportFrom(
    module='couverture',  # ❌ Should be 'ouverture'
    names=new_names,
    level=0
)
```

This is a known issue. The normalized form uses `from couverture import ...` instead of `from ouverture import ...`

---

## Design Decisions (By Design, Not Bugs)

### Hash Excludes Docstrings

**Why**: To enable multilingual support. Same logic = same hash, regardless of documentation language.

```python
# These produce the SAME hash:
def add(a, b):
    """Add two numbers"""  # English docs
    return a + b

def add(a, b):
    """Additionner deux nombres"""  # French docs
    return a + b
```

### Imports Are Sorted

**Why**: To ensure deterministic hashing. Same imports in different order should produce the same hash.

```python
# These normalize to the same form:
import math
from collections import Counter

# vs.
from collections import Counter
import math
```

### Built-ins Never Renamed

**Why**: Built-in functions like `len()`, `sum()`, `print()` are universal and don't need normalization.

```python
def process(items):
    return sum(items)  # 'sum' stays 'sum', not '_ouverture_v_X'
```

### Function Name Always `_ouverture_v_0`

**Why**: Consistent canonical form. The actual function name is preserved in language-specific mappings.

---

## Future Considerations

### Potential Enhancements

1. **Class support**: Would require normalizing method names, class hierarchy, etc.
2. **Multiple functions per file**: Would require dependency tracking within the file
3. **Async function support**: Add `ast.AsyncFunctionDef` handling
4. **Semantic equivalence**: Detect functionally equivalent but syntactically different code
5. **Type hint normalization**: Optionally normalize type hints for consistent hashing
6. **Import validation**: Verify ouverture imports exist in the pool
7. **Cross-language support**: Extend beyond Python (JavaScript, Rust, etc.)
8. **Version migration**: Handle schema changes in stored JSON format

### Research Questions

1. **Composability**: How deep can function composition go before performance degrades?
2. **Cognitive impact**: Does coding in native language improve comprehension and reduce bugs?
3. **LLM training**: Could multilingual function pools improve LLM performance on non-English code?
4. **Scale**: How large can the function pool grow before search/retrieval becomes slow?

---

## Summary

| Category | Supported | Not Supported |
|----------|-----------|---------------|
| **Functions** | ✅ Single function per file<br>✅ Decorators<br>✅ Nested functions | ❌ Multiple functions per file<br>❌ Async functions<br>❌ Top-level lambdas |
| **Classes** | | ❌ Classes<br>❌ Methods<br>❌ Class variables |
| **Imports** | ✅ Standard library<br>✅ External packages<br>✅ Ouverture imports | ❌ Import validation |
| **Variables** | ✅ Local variables<br>✅ Function arguments | ❌ Global variables<br>❌ Module constants |
| **Features** | ✅ Type hints (preserved)<br>✅ Docstrings (per-language)<br>✅ Multilingual names | ❌ Type hint normalization<br>❌ Semantic analysis |
| **Code** | ✅ Single function + imports | ❌ Module-level statements |

---

## Testing Your Code

To check if your code is supported:

1. **Single function?** ✅ Only one `def` in the file (besides nested functions)
2. **Only imports and function?** ✅ No module-level executable code
3. **Standard imports?** ✅ All stdlib and package imports preserved
4. **No classes?** ✅ No `class` definitions
5. **No global variables?** ✅ Return values from functions instead

If all checks pass, your function should work with Ouverture!

---

## See Also

- `CLAUDE.md` - Detailed technical documentation for AI assistants
- `README.md` - Project overview and philosophy
- `README_TESTING.md` - Testing guide and examples
- `ouverture.py` - Main implementation (~600 lines)
