# Advanced Testing Strategies for Ouverture

## Overview

This document complements `README_TESTING.md` and `README_PYTEST.md` by focusing on:
- Advanced testing techniques
- Property-based testing
- Multilingual test coverage
- Performance testing
- Fuzzing and edge case discovery
- Integration testing strategies
- Continuous integration

## Current Test Coverage

**Existing test files**:
- `test_ouverture.py`: 50+ unit tests covering core functionality
- `README_TESTING.md`: Manual testing guide
- `README_PYTEST.md`: Pytest documentation

**Current coverage**: See `README_PYTEST.md` for details

**Gaps to address**:
1. Property-based testing for normalization
2. Multilingual equivalence testing
3. Performance benchmarks
4. Fuzzing for edge cases
5. Integration tests with real-world code
6. Regression test suite

## Property-Based Testing

### What is Property-Based Testing?

Instead of writing specific test cases, define **properties** that should hold for all inputs:

```python
# Traditional test
def test_sum_specific():
    assert sum_list([1, 2, 3]) == 6

# Property-based test
@given(lists(integers()))
def test_sum_property(numbers):
    result = sum_list(numbers)
    assert result == sum(numbers)  # Built-in sum as oracle
```

### Why Property-Based Testing for Ouverture?

Ouverture's core properties:
1. **Normalization idempotence**: Normalizing twice yields same result
2. **Multilingual equivalence**: Same logic → same hash, regardless of language
3. **Roundtrip preservation**: add → get → reconstruct preserves behavior
4. **Hash determinism**: Same input → same hash, always
5. **Denormalization inverse**: denormalize(normalize(code)) ≈ code

### Implementation with Hypothesis

Install Hypothesis:
```bash
pip install hypothesis
```

#### Property 1: Normalization Idempotence

```python
from hypothesis import given, strategies as st
import ast

@given(st.text(min_size=1, max_size=100))
def test_normalize_idempotent(code):
    """Normalizing normalized code should not change it."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return  # Invalid Python, skip

    try:
        result1 = normalize_ast(tree, "eng", set())
        tree2 = ast.parse(result1.normalized_code)
        result2 = normalize_ast(tree2, "eng", set())

        assert result1.normalized_code == result2.normalized_code
    except NormalizationError:
        pass  # Expected for invalid structures
```

#### Property 2: Multilingual Equivalence

```python
from hypothesis import given, strategies as st

def generate_function_code(var_names, func_name):
    """Generate Python function with given names."""
    params = ", ".join(var_names)
    body = " + ".join(var_names) if var_names else "0"
    return f"""
def {func_name}({params}):
    return {body}
"""

@given(
    st.lists(st.text(alphabet=st.characters(whitelist_categories=("Ll",)),
                     min_size=1, max_size=10),
            min_size=1, max_size=5),
    st.text(alphabet=st.characters(whitelist_categories=("Ll",)),
            min_size=1, max_size=10)
)
def test_multilingual_same_hash(var_names_eng, func_name_eng):
    """Functions with different names but same logic should have same hash."""
    # English version
    code_eng = generate_function_code(var_names_eng, func_name_eng)

    # French version (different names, same logic)
    var_names_fra = [name + "_fr" for name in var_names_eng]
    func_name_fra = func_name_eng + "_fr"
    code_fra = generate_function_code(var_names_fra, func_name_fra)

    try:
        tree_eng = ast.parse(code_eng)
        tree_fra = ast.parse(code_fra)

        result_eng = normalize_ast(tree_eng, "eng", set())
        result_fra = normalize_ast(tree_fra, "fra", set())

        hash_eng = compute_hash(result_eng.normalized_code_no_docstring)
        hash_fra = compute_hash(result_fra.normalized_code_no_docstring)

        assert hash_eng == hash_fra
    except Exception:
        pass  # Skip invalid code
```

#### Property 3: Roundtrip Preservation

```python
@given(st.text(min_size=1, max_size=200))
def test_roundtrip_preserves_behavior(code):
    """Adding then retrieving should preserve function behavior."""
    try:
        # Parse original
        tree = ast.parse(code)

        # Normalize
        result = normalize_ast(tree, "eng", set())
        hash_val = compute_hash(result.normalized_code_no_docstring)

        # Store
        pool = FunctionPool(tmp_path)
        pool.save_function(
            hash_val, "eng",
            result.normalized_code,
            result.docstring,
            result.name_mapping,
            result.alias_mapping
        )

        # Retrieve
        data = pool.load_function(hash_val, "eng")

        # Denormalize
        reconstructed = denormalize_code(
            data.normalized_code,
            data.languages["eng"].name_mapping,
            data.languages["eng"].alias_mapping
        )

        # Compare behavior (if function is executable)
        if is_executable(code):
            assert same_behavior(code, reconstructed)
    except Exception:
        pass  # Skip invalid code
```

#### Property 4: Hash Determinism

```python
@given(st.text(min_size=1, max_size=100))
def test_hash_determinism(code):
    """Same code should always produce same hash."""
    try:
        tree = ast.parse(code)
        result = normalize_ast(tree, "eng", set())

        hash1 = compute_hash(result.normalized_code_no_docstring)
        hash2 = compute_hash(result.normalized_code_no_docstring)
        hash3 = compute_hash(result.normalized_code_no_docstring)

        assert hash1 == hash2 == hash3
    except Exception:
        pass
```

## Multilingual Test Corpus

### Building a Test Corpus

Create parallel function implementations in multiple languages:

```
tests/corpus/
├── simple_functions/
│   ├── sum_list.eng.py
│   ├── sum_list.fra.py
│   ├── sum_list.spa.py
│   ├── sum_list.ara.py
│   └── sum_list.zho.py
├── with_imports/
│   ├── count_frequency.eng.py
│   ├── count_frequency.fra.py
│   └── count_frequency.spa.py
└── compositional/
    ├── process_data.eng.py
    ├── process_data.fra.py
    └── process_data.spa.py
```

### Corpus Test Generator

```python
import os
from pathlib import Path

def discover_test_corpus():
    """Discover parallel function implementations."""
    corpus_dir = Path("tests/corpus")
    groups = {}

    for file_path in corpus_dir.rglob("*.*.py"):
        # Parse filename: function_name.lang.py
        stem = file_path.stem
        parts = stem.rsplit(".", 1)
        if len(parts) == 2:
            func_name, lang = parts
            if func_name not in groups:
                groups[func_name] = {}
            groups[func_name][lang] = file_path

    return groups

def test_corpus_equivalence():
    """Test that parallel implementations have same hash."""
    corpus = discover_test_corpus()

    for func_name, implementations in corpus.items():
        if len(implementations) < 2:
            continue  # Need at least 2 languages

        hashes = {}
        for lang, file_path in implementations.items():
            code = file_path.read_text()
            tree = ast.parse(code)
            result = normalize_ast(tree, lang, set())
            hash_val = compute_hash(result.normalized_code_no_docstring)
            hashes[lang] = hash_val

        # All hashes should be identical
        unique_hashes = set(hashes.values())
        assert len(unique_hashes) == 1, (
            f"Function {func_name} has different hashes across languages:\n"
            + "\n".join(f"{lang}: {h}" for lang, h in hashes.items())
        )

def test_corpus_roundtrip():
    """Test that all corpus functions survive roundtrip."""
    corpus = discover_test_corpus()

    for func_name, implementations in corpus.items():
        for lang, file_path in implementations.items():
            original_code = file_path.read_text()

            # Add to pool
            tree = ast.parse(original_code)
            result = normalize_ast(tree, lang, set())
            hash_val = compute_hash(result.normalized_code_no_docstring)

            pool = FunctionPool(tmp_path)
            pool.save_function(
                hash_val, lang,
                result.normalized_code,
                result.docstring,
                result.name_mapping,
                result.alias_mapping
            )

            # Retrieve
            data = pool.load_function(hash_val, lang)
            reconstructed = denormalize_code(
                data.normalized_code,
                data.languages[lang].name_mapping,
                data.languages[lang].alias_mapping
            )

            # Test behavior preservation
            if has_test_cases(file_path):
                test_cases = load_test_cases(file_path)
                assert_same_behavior(original_code, reconstructed, test_cases)
```

## Fuzzing for Edge Cases

### AST Fuzzing

Generate random but valid Python ASTs:

```python
import random
import ast

def generate_random_function(depth=3):
    """Generate random Python function AST."""
    body = generate_random_statements(depth)

    func = ast.FunctionDef(
        name=f"func_{random.randint(0, 1000)}",
        args=ast.arguments(
            args=[ast.arg(arg=f"arg{i}") for i in range(random.randint(0, 3))],
            posonlyargs=[],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=[]
        ),
        body=body,
        decorator_list=[],
        returns=None
    )

    return ast.Module(body=[func], type_ignores=[])

def generate_random_statements(depth):
    """Generate random statement list."""
    if depth == 0:
        return [ast.Return(value=ast.Constant(value=42))]

    statements = []
    for _ in range(random.randint(1, 3)):
        stmt = random.choice([
            generate_assign(),
            generate_if(),
            generate_for(),
            generate_return()
        ])
        statements.append(stmt)

    if not any(isinstance(s, ast.Return) for s in statements):
        statements.append(ast.Return(value=ast.Constant(value=None)))

    return statements

# ... implement generate_* functions ...

def test_fuzz_normalization(iterations=1000):
    """Fuzz test normalization with random ASTs."""
    for i in range(iterations):
        func_ast = generate_random_function()

        try:
            # Should not crash
            result = normalize_ast(func_ast, "eng", set())
            hash_val = compute_hash(result.normalized_code_no_docstring)

            # Hash should be deterministic
            result2 = normalize_ast(func_ast, "eng", set())
            hash_val2 = compute_hash(result2.normalized_code_no_docstring)
            assert hash_val == hash_val2

        except NormalizationError:
            pass  # Expected for some invalid structures
        except Exception as e:
            # Unexpected error - save for debugging
            save_crash_case(func_ast, e, f"crash_{i}.py")
            raise
```

### Mutation Testing

Modify valid code slightly to find edge cases:

```python
def mutate_code(code):
    """Generate mutations of Python code."""
    tree = ast.parse(code)

    mutations = [
        add_random_variable(tree),
        remove_random_statement(tree),
        swap_statement_order(tree),
        change_operator(tree),
        add_nested_function(tree),
    ]

    return [ast.unparse(m) for m in mutations if m]

def test_mutation_stability():
    """Test that mutations produce different hashes (usually)."""
    original = """
def calculate(x, y):
    result = x + y
    return result
"""

    tree = ast.parse(original)
    result = normalize_ast(tree, "eng", set())
    original_hash = compute_hash(result.normalized_code_no_docstring)

    mutations = mutate_code(original)

    for mutated in mutations:
        try:
            mut_tree = ast.parse(mutated)
            mut_result = normalize_ast(mut_tree, "eng", set())
            mut_hash = compute_hash(mut_result.normalized_code_no_docstring)

            # Most mutations should change hash
            # (but not all - some might be semantic equivalents)
            if mutated != original:
                print(f"Mutation: {mutated}")
                print(f"Same hash: {mut_hash == original_hash}")

        except Exception:
            pass
```

## Performance Testing

### Benchmark Suite

```python
import time
from pathlib import Path

def benchmark_normalization():
    """Benchmark normalization performance."""
    test_files = [
        ("small", "examples/example_simple.py", 10000),
        ("medium", "tests/fixtures/medium_function.py", 1000),
        ("large", "tests/fixtures/large_function.py", 100),
    ]

    results = {}

    for name, file_path, iterations in test_files:
        code = Path(file_path).read_text()
        tree = ast.parse(code)

        start = time.perf_counter()
        for _ in range(iterations):
            result = normalize_ast(tree, "eng", set())
        end = time.perf_counter()

        avg_time = (end - start) / iterations
        results[name] = avg_time

    return results

def test_normalization_performance():
    """Ensure normalization is fast enough."""
    results = benchmark_normalization()

    # Performance requirements
    assert results["small"] < 0.001   # < 1ms for small functions
    assert results["medium"] < 0.01   # < 10ms for medium functions
    assert results["large"] < 0.1     # < 100ms for large functions

def benchmark_storage():
    """Benchmark storage performance."""
    pool = FunctionPool(tmp_path)

    # Prepare test data
    functions = []
    for i in range(100):
        code = f"""
def func_{i}(x):
    return x * {i}
"""
        tree = ast.parse(code)
        result = normalize_ast(tree, "eng", set())
        hash_val = compute_hash(result.normalized_code_no_docstring)
        functions.append((hash_val, result))

    # Benchmark saves
    start = time.perf_counter()
    for hash_val, result in functions:
        pool.save_function(
            hash_val, "eng",
            result.normalized_code,
            result.docstring,
            result.name_mapping,
            result.alias_mapping
        )
    save_time = time.perf_counter() - start

    # Benchmark loads
    start = time.perf_counter()
    for hash_val, _ in functions:
        pool.load_function(hash_val, "eng")
    load_time = time.perf_counter() - start

    print(f"Save: {save_time / len(functions) * 1000:.2f}ms per function")
    print(f"Load: {load_time / len(functions) * 1000:.2f}ms per function")

    # Performance requirements
    assert save_time / len(functions) < 0.01  # < 10ms per save
    assert load_time / len(functions) < 0.01  # < 10ms per load
```

### Scalability Testing

```python
def test_large_pool_performance():
    """Test performance with thousands of functions."""
    pool = FunctionPool(tmp_path)

    # Add 10,000 functions
    num_functions = 10_000
    hashes = []

    for i in range(num_functions):
        code = f"""
def func_{i}(x):
    return x * {i}
"""
        tree = ast.parse(code)
        result = normalize_ast(tree, "eng", set())
        hash_val = compute_hash(result.normalized_code_no_docstring)

        pool.save_function(
            hash_val, "eng",
            result.normalized_code,
            result.docstring,
            result.name_mapping,
            result.alias_mapping
        )
        hashes.append(hash_val)

    # Test retrieval performance doesn't degrade
    sample_indices = random.sample(range(num_functions), 100)

    start = time.perf_counter()
    for idx in sample_indices:
        pool.load_function(hashes[idx], "eng")
    end = time.perf_counter()

    avg_time = (end - start) / len(sample_indices)
    assert avg_time < 0.01  # < 10ms per retrieval even with 10k functions
```

## Regression Testing

### Snapshot Testing

Capture expected outputs for specific inputs:

```python
def test_normalization_snapshots():
    """Test normalization produces expected output."""
    snapshots = [
        {
            "name": "simple_function",
            "input": """
def add(a, b):
    return a + b
""",
            "expected_hash": "abc123...",
            "expected_normalized": """
def _ouverture_v_0(_ouverture_v_1, _ouverture_v_2):
    return _ouverture_v_1 + _ouverture_v_2
"""
        },
        # ... more snapshots
    ]

    for snapshot in snapshots:
        tree = ast.parse(snapshot["input"])
        result = normalize_ast(tree, "eng", set())
        hash_val = compute_hash(result.normalized_code_no_docstring)

        assert hash_val == snapshot["expected_hash"]
        assert result.normalized_code.strip() == snapshot["expected_normalized"].strip()
```

### Known Issues Tracking

```python
import pytest

@pytest.mark.xfail(reason="Known issue: couverture typo")
def test_ouverture_import_rewriting():
    """Test ouverture imports are rewritten correctly."""
    code = """
from ouverture import abc123 as helper

def use_helper(x):
    return helper(x)
"""
    tree = ast.parse(code)
    result = normalize_ast(tree, "eng", set())

    # Should contain 'from ouverture import'
    # Currently contains 'from couverture import' (bug)
    assert "from ouverture import" in result.normalized_code

@pytest.mark.skip(reason="Async functions not yet supported")
def test_async_function_normalization():
    """Test async function normalization."""
    code = """
async def fetch(url):
    return await http.get(url)
"""
    tree = ast.parse(code)
    result = normalize_ast(tree, "eng", set())
    # Should not crash and should normalize
    assert "_ouverture_v_0" in result.normalized_code
```

## Integration Testing

### Real-World Code Testing

Test with actual Python packages:

```python
def test_real_world_functions():
    """Test normalization on real-world Python code."""
    # Sample from popular libraries
    real_functions = [
        extract_function("requests", "requests.get"),
        extract_function("numpy", "numpy.mean"),
        extract_function("pandas", "DataFrame.head"),
    ]

    for func_code in real_functions:
        try:
            tree = ast.parse(func_code)
            result = normalize_ast(tree, "eng", set())
            hash_val = compute_hash(result.normalized_code_no_docstring)

            # Should not crash
            assert len(hash_val) == 64
            assert result.normalized_code

        except Exception as e:
            # Log failures for analysis
            print(f"Failed to normalize: {func_code[:50]}...")
            print(f"Error: {e}")
```

### CLI Integration Tests

Test the full CLI workflow:

```python
import subprocess

def test_cli_add_and_get():
    """Test CLI add and get commands."""
    # Add function
    result = subprocess.run(
        ["python3", "ouverture.py", "add", "examples/example_simple.py@eng"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0

    # Extract hash from output
    hash_val = extract_hash_from_output(result.stdout)

    # Get function
    result = subprocess.run(
        ["python3", "ouverture.py", "get", f"{hash_val}@eng"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "def" in result.stdout

def test_cli_error_handling():
    """Test CLI error messages."""
    # Missing @lang
    result = subprocess.run(
        ["python3", "ouverture.py", "add", "examples/example_simple.py"],
        capture_output=True,
        text=True
    )
    assert result.returncode != 0
    assert "must be in format" in result.stderr

    # Invalid file
    result = subprocess.run(
        ["python3", "ouverture.py", "add", "nonexistent.py@eng"],
        capture_output=True,
        text=True
    )
    assert result.returncode != 0
    assert "File not found" in result.stderr
```

## Continuous Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.9", "3.10", "3.11", "3.12"]

    steps:
    - uses: actions/checkout@v3

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pytest pytest-cov hypothesis

    - name: Run unit tests
      run: pytest tests/ -v

    - name: Run property tests
      run: pytest tests/property_tests.py --hypothesis-seed=0

    - name: Run integration tests
      run: pytest tests/integration/ -v

    - name: Run performance tests
      run: pytest tests/performance/ -v --benchmark

    - name: Generate coverage report
      run: pytest --cov=ouverture --cov-report=xml

    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: system
        pass_filenames: false
        always_run: true

      - id: mypy
        name: mypy
        entry: mypy
        language: system
        types: [python]

      - id: ruff
        name: ruff
        entry: ruff check
        language: system
        types: [python]
```

## Test Organization

```
tests/
├── unit/
│   ├── test_ast_normalization.py
│   ├── test_name_mapping.py
│   ├── test_import_handling.py
│   ├── test_hash_computation.py
│   ├── test_storage.py
│   └── test_denormalization.py
├── property/
│   ├── test_normalization_properties.py
│   ├── test_multilingual_properties.py
│   └── test_roundtrip_properties.py
├── integration/
│   ├── test_cli_integration.py
│   ├── test_real_world_code.py
│   └── test_multilingual_corpus.py
├── performance/
│   ├── test_normalization_performance.py
│   ├── test_storage_performance.py
│   └── test_scalability.py
├── fuzzing/
│   ├── test_ast_fuzzing.py
│   └── test_mutation_testing.py
├── regression/
│   ├── test_snapshots.py
│   └── test_known_issues.py
├── corpus/
│   ├── simple_functions/
│   ├── with_imports/
│   └── compositional/
└── fixtures/
    ├── small_function.py
    ├── medium_function.py
    └── large_function.py
```

## Summary

Advanced testing for Ouverture should include:

1. **Property-based testing**: Verify invariants hold for all inputs
2. **Multilingual corpus**: Test parallel implementations across languages
3. **Fuzzing**: Discover edge cases with random inputs
4. **Performance testing**: Ensure scalability and speed
5. **Regression testing**: Track known issues and expected outputs
6. **Integration testing**: Test full workflows and real-world code
7. **CI/CD**: Automate testing on every commit

These strategies complement the existing unit tests in `test_ouverture.py` and provide comprehensive coverage for a robust, production-ready system.
