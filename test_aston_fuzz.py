#!/usr/bin/env python3
"""
Fuzz testing for aston.py round-trip correctness.

Tests that aston_read(aston_write(ast)) == ast for various Python code samples.
"""

import ast
import sys
import traceback
from pathlib import Path
from typing import List, Tuple

# Import from aston.py
from aston import aston_write, aston_read


# Test corpus: various Python language features
FUZZ_CORPUS = [
    # Empty module
    "",

    # Simple expressions
    "x = 1",
    "x = 1 + 2",
    "x = 1 + 2 * 3",

    # Function definitions
    "def f(): pass",
    "def f(x): return x",
    "def f(x, y=1): return x + y",
    "def f(*args): pass",
    "def f(**kwargs): pass",
    "def f(a, *args, **kwargs): pass",

    # Function with defaults and annotations
    "def f(x: int, y: str = 'hello') -> bool: return True",

    # Decorators
    "@decorator\ndef f(): pass",
    "@decorator1\n@decorator2\ndef f(): pass",

    # Classes
    "class C: pass",
    "class C:\n    def __init__(self): pass",
    "class C(Base): pass",
    "class C(Base1, Base2): pass",

    # Class with decorators
    "@dataclass\nclass C: pass",

    # Imports
    "import os",
    "import os, sys",
    "from os import path",
    "from os import path, environ",
    "from os import *",
    "import numpy as np",

    # Control flow
    "if x:\n    pass",
    "if x:\n    pass\nelse:\n    pass",
    "if x:\n    pass\nelif y:\n    pass\nelse:\n    pass",
    "for i in range(10):\n    pass",
    "while x:\n    pass",
    "try:\n    pass\nexcept:\n    pass",
    "try:\n    pass\nexcept Exception as e:\n    pass\nfinally:\n    pass",

    # List/dict/set comprehensions
    "[x for x in range(10)]",
    "[x for x in range(10) if x > 5]",
    "{x: x*2 for x in range(10)}",
    "{x for x in range(10)}",

    # Lambda
    "lambda x: x + 1",
    "lambda x, y: x + y",

    # Async/await
    "async def f():\n    await something()",
    "async def f():\n    async for x in items:\n        pass",
    "async def f():\n    async with context:\n        pass",

    # With statement
    "with open('file') as f:\n    pass",
    "with a as x, b as y:\n    pass",

    # Assignment variants
    "x, y = 1, 2",
    "x = y = z = 1",
    "x += 1",
    "x *= 2",

    # Walrus operator
    "if (x := 10) > 5:\n    pass",

    # Match statement (Python 3.10+)
    "match x:\n    case 1:\n        pass\n    case _:\n        pass",

    # F-strings
    "f'hello {x}'",
    "f'hello {x + 1}'",
    "f'hello {x:.2f}'",

    # Complex nested structure
    """
def outer(x):
    def inner(y):
        return x + y
    return inner

class MyClass:
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        self._value = v

@decorator
async def complex_func(a: int, *args, b: str = "default", **kwargs) -> List[Tuple]:
    async with context_manager() as cm:
        result = [x for x in range(10) if x > 5]
        await asyncio.gather(*tasks)
        return result
""",

    # Docstrings
    '"""Module docstring"""',
    'def f():\n    """Function docstring"""\n    pass',
    'class C:\n    """Class docstring"""\n    pass',

    # Multiple statements
    "x = 1\ny = 2\nz = x + y",

    # Global/nonlocal
    "def f():\n    global x\n    x = 1",
    "def outer():\n    x = 1\n    def inner():\n        nonlocal x\n        x = 2",

    # Assert/raise/delete
    "assert x > 0",
    "assert x > 0, 'x must be positive'",
    "raise ValueError('error')",
    "del x",

    # Yield/return
    "def gen():\n    yield 1\n    yield 2",
    "def f():\n    return 42",
    "def f():\n    return",
]


def test_code_sample(code: str, index: int) -> Tuple[bool, str]:
    """Test a single code sample for round-trip correctness.

    Returns:
        (success, error_message)
    """
    try:
        # Parse original
        tree = ast.parse(code)

        # Convert to ASTON and back
        _, tuples = aston_write(tree)
        reconstructed = aston_read(tuples)

        # Compare using ast.dump
        original_dump = ast.dump(tree)
        reconstructed_dump = ast.dump(reconstructed)

        if original_dump != reconstructed_dump:
            return False, f"AST mismatch:\nOriginal: {original_dump[:200]}...\nReconstructed: {reconstructed_dump[:200]}..."

        # Also verify code equivalence
        original_code = ast.unparse(tree)
        reconstructed_code = ast.unparse(reconstructed)

        if original_code != reconstructed_code:
            return False, f"Code mismatch:\nOriginal: {original_code[:200]}...\nReconstructed: {reconstructed_code[:200]}..."

        return True, ""

    except SyntaxError as e:
        # Some samples might be Python 3.10+ specific
        return True, f"Skipped (syntax not supported): {e}"
    except Exception as e:
        return False, f"Exception: {type(e).__name__}: {e}\n{traceback.format_exc()}"


def test_file(filepath: Path) -> Tuple[bool, str]:
    """Test a Python file for round-trip correctness."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            code = f.read()

        tree = ast.parse(code)
        _, tuples = aston_write(tree)
        reconstructed = aston_read(tuples)

        original_dump = ast.dump(tree)
        reconstructed_dump = ast.dump(reconstructed)

        if original_dump != reconstructed_dump:
            return False, f"AST mismatch in {filepath}"

        return True, ""

    except Exception as e:
        return False, f"Exception in {filepath}: {type(e).__name__}: {e}"


def main():
    """Run fuzz tests."""
    print("=" * 70)
    print("ASTON Fuzz Testing")
    print("=" * 70)

    total_tests = 0
    passed = 0
    failed = 0
    skipped = 0

    # Test corpus samples
    print(f"\n[1/2] Testing corpus ({len(FUZZ_CORPUS)} samples)...")
    for i, code in enumerate(FUZZ_CORPUS):
        total_tests += 1
        success, error = test_code_sample(code, i)

        if success and not error:
            passed += 1
            print(f"  ✓ Sample {i+1}/{len(FUZZ_CORPUS)}")
        elif success and "Skipped" in error:
            skipped += 1
            print(f"  ⊘ Sample {i+1}/{len(FUZZ_CORPUS)}: {error}")
        else:
            failed += 1
            print(f"  ✗ Sample {i+1}/{len(FUZZ_CORPUS)}: FAILED")
            print(f"    Code: {code[:100]}")
            print(f"    Error: {error}")

    # Test example files
    examples_dir = Path(__file__).parent / 'examples'
    if examples_dir.exists():
        example_files = list(examples_dir.glob('*.py'))
        print(f"\n[2/2] Testing example files ({len(example_files)} files)...")

        for filepath in example_files:
            total_tests += 1
            success, error = test_file(filepath)

            if success:
                passed += 1
                print(f"  ✓ {filepath.name}")
            else:
                failed += 1
                print(f"  ✗ {filepath.name}: FAILED")
                print(f"    Error: {error}")
    else:
        print(f"\n[2/2] Skipping example files (directory not found)")

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Total tests:  {total_tests}")
    print(f"Passed:       {passed} ({100*passed//total_tests if total_tests > 0 else 0}%)")
    if skipped > 0:
        print(f"Skipped:      {skipped}")
    print(f"Failed:       {failed}")

    if failed > 0:
        print("\n✗ FUZZ TEST FAILED")
        sys.exit(1)
    else:
        print("\n✓ ALL FUZZ TESTS PASSED")
        sys.exit(0)


if __name__ == '__main__':
    main()
