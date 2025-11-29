#!/usr/bin/env python3
"""
Comprehensive ASTON Round-Trip Fuzzer

Combines multiple fuzzing strategies to validate ASTON serialization/deserialization:
1. Fixed corpus (example files)
2. Mutation-based fuzzing (imports, type hints, docstrings)
3. Generative fuzzing (random valid Python AST using tests/code/code.py)

All tests verify the round-trip invariant: ast == aston_read(aston_write(ast))
"""

import ast
import random
import sys
import traceback
from pathlib import Path
from typing import List, Tuple, Optional

# Import ASTON from bb.py
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from bb import aston_write, aston_read

# Import AST code generator
from tests.code.code import generate as generate_ast_code


# Mutation fuzzing constants and utilities
IMPORT_MODULES = [
    "os", "sys", "re", "json", "math", "random", "pathlib",
    "collections", "itertools", "functools", "typing", "datetime",
    "hashlib", "urllib", "abc",
]

IMPORT_ITEMS = {
    "os": ["path", "environ", "getcwd", "listdir"],
    "sys": ["argv", "exit", "stdout", "stderr"],
    "collections": ["Counter", "defaultdict", "OrderedDict", "namedtuple"],
    "typing": ["List", "Dict", "Tuple", "Optional", "Union"],
    "pathlib": ["Path", "PurePath"],
    "itertools": ["chain", "cycle", "repeat", "islice"],
    "functools": ["reduce", "partial", "lru_cache"],
    "datetime": ["datetime", "date", "time", "timedelta"],
}


def mutate_add_imports(code: str, rng: random.Random) -> str:
    """Add random import statements to code."""
    mutations = []
    num_imports = rng.randint(1, 5)

    for _ in range(num_imports):
        mutation_type = rng.choice(["import", "from_import", "from_import_as"])

        if mutation_type == "import":
            module = rng.choice(IMPORT_MODULES)
            mutations.append(f"import {module}")
        elif mutation_type == "from_import":
            module = rng.choice([m for m in IMPORT_MODULES if m in IMPORT_ITEMS])
            items = IMPORT_ITEMS[module]
            item = rng.choice(items)
            mutations.append(f"from {module} import {item}")
        elif mutation_type == "from_import_as":
            module = rng.choice([m for m in IMPORT_MODULES if m in IMPORT_ITEMS])
            items = IMPORT_ITEMS[module]
            item = rng.choice(items)
            alias = f"{item}_alias_{rng.randint(0, 999)}"
            mutations.append(f"from {module} import {item} as {alias}")

    import_block = "\n".join(mutations)
    return f"{import_block}\n\n{code}"


def mutate_code(code: str, seed: int) -> str:
    """Apply deterministic mutations to code based on seed."""
    rng = random.Random(seed)
    mutated = mutate_add_imports(code, rng)

    try:
        ast.parse(mutated)
        return mutated
    except SyntaxError:
        return code


class FuzzResult:
    """Result of a single fuzz test."""

    def __init__(self, success: bool, error: str = "", code: str = "", test_id: str = ""):
        self.success = success
        self.error = error
        self.code = code
        self.test_id = test_id


def test_round_trip(code: str, test_id: str) -> FuzzResult:
    """Test ASTON round-trip for a code sample.

    Returns:
        FuzzResult with success status and error details
    """
    try:
        # Parse original
        tree = ast.parse(code)

        # Convert to ASTON and back
        _, tuples = aston_write(tree)
        reconstructed = aston_read(tuples)

        # Compare using ast.dump (structural equivalence)
        original_dump = ast.dump(tree)
        reconstructed_dump = ast.dump(reconstructed)

        if original_dump != reconstructed_dump:
            error = f"AST structural mismatch"
            return FuzzResult(False, error, code, test_id)

        # Verify code equivalence
        original_code = ast.unparse(tree)
        reconstructed_code = ast.unparse(reconstructed)

        if original_code != reconstructed_code:
            error = f"Code mismatch:\nOriginal:\n{original_code[:200]}\nReconstructed:\n{reconstructed_code[:200]}"
            return FuzzResult(False, error, code, test_id)

        return FuzzResult(True, "", code, test_id)

    except SyntaxError as e:
        # Invalid Python - skip (shouldn't happen with generator)
        error = f"SyntaxError: {e}"
        return FuzzResult(False, error, code, test_id)
    except Exception as e:
        error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        return FuzzResult(False, error, code, test_id)


def save_failure(code: str, test_id: str, error: str) -> str:
    """Save failing code to /tmp and return filepath."""
    filename = f"/tmp/aston_fuzz_fail_{test_id}.py"

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"# Failure: {error}\n")
        f.write(f"# Test ID: {test_id}\n\n")
        f.write(code)

    return filename


class FuzzStrategy:
    """Base class for fuzzing strategies."""

    def __init__(self, name: str):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.failures = []

    def run(self):
        """Run the fuzzing strategy. Override in subclasses."""
        raise NotImplementedError

    def report(self):
        """Print summary report."""
        total = self.passed + self.failed + self.skipped
        print(f"\n{'=' * 70}")
        print(f"{self.name} - Summary")
        print(f"{'=' * 70}")
        print(f"Total:   {total}")
        print(f"Passed:  {self.passed} ({100 * self.passed // total if total > 0 else 0}%)")
        if self.skipped > 0:
            print(f"Skipped: {self.skipped}")
        print(f"Failed:  {self.failed}")

        if self.failures:
            print(f"\nFailures:")
            for failure in self.failures:
                print(f"  {failure['test_id']}: {failure['error'][:100]}")
                print(f"    File: {failure['filepath']}")
                print(f"    Reproduce: {failure['reproduce']}")


class CorpusFuzzStrategy(FuzzStrategy):
    """Test fixed corpus of example files."""

    def __init__(self):
        super().__init__("Corpus Fuzzing")
        self.examples_dir = Path(__file__).parent.parent.parent / 'examples'

    def run(self):
        """Test all example files."""
        if not self.examples_dir.exists():
            print(f"⚠ Examples directory not found: {self.examples_dir}")
            return

        example_files = sorted(self.examples_dir.glob('*.py'))
        print(f"\n[1/3] Testing corpus: {len(example_files)} example files")

        for filepath in example_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    code = f.read()

                test_id = f"corpus_{filepath.stem}"
                result = test_round_trip(code, test_id)

                if result.success:
                    self.passed += 1
                    print(f"  ✓ {filepath.name}")
                else:
                    self.failed += 1
                    saved_path = save_failure(code, test_id, result.error)
                    print(f"  ✗ {filepath.name}: FAILED")
                    print(f"    Error: {result.error[:100]}")

                    self.failures.append({
                        'test_id': test_id,
                        'error': result.error,
                        'filepath': saved_path,
                        'reproduce': f"python3 aston.py --test {saved_path}",
                    })

            except Exception as e:
                self.failed += 1
                print(f"  ✗ {filepath.name}: Exception: {e}")


class MutationFuzzStrategy(FuzzStrategy):
    """Test mutations of base corpus."""

    def __init__(self, num_mutations: int = 50):
        super().__init__("Mutation Fuzzing")
        self.num_mutations = num_mutations
        self.base_corpus = [
            "x = 1",
            "def f(): pass",
            "def f(x): return x",
            "def f(x, y): return x + y",
            "class C: pass",
            "for i in range(10): pass",
            "[x for x in range(10)]",
        ]

    def run(self):
        """Generate and test mutations."""
        total_tests = len(self.base_corpus) * self.num_mutations
        print(f"\n[2/3] Testing mutations: {len(self.base_corpus)} base × {self.num_mutations} mutations = {total_tests} tests")

        for base_idx, base_code in enumerate(self.base_corpus):
            base_passed = 0
            base_failed = 0

            for seed in range(self.num_mutations):
                mutated_code = mutate_code(base_code, seed)
                test_id = f"mutation_base{base_idx}_seed{seed}"

                result = test_round_trip(mutated_code, test_id)

                if result.success:
                    self.passed += 1
                    base_passed += 1
                else:
                    self.failed += 1
                    base_failed += 1
                    saved_path = save_failure(mutated_code, test_id, result.error)

                    self.failures.append({
                        'test_id': test_id,
                        'error': result.error,
                        'filepath': saved_path,
                        'reproduce': f"python3 tests/aston/fuzz.py --mutation --seed {seed}",
                    })

            if base_failed == 0:
                print(f"  ✓ Base {base_idx + 1}/{len(self.base_corpus)}: {base_passed} mutations passed")
            else:
                print(f"  ⚠ Base {base_idx + 1}/{len(self.base_corpus)}: {base_passed} passed, {base_failed} failed")


class GenerativeFuzzStrategy(FuzzStrategy):
    """Test randomly generated AST code."""

    def __init__(self, num_tests: int = 100, start_seed: int = 0):
        super().__init__("Generative Fuzzing")
        self.num_tests = num_tests
        self.start_seed = start_seed

    def run(self):
        """Generate and test random AST code."""
        print(f"\n[3/3] Testing generated code: {self.num_tests} tests (seeds {self.start_seed}-{self.start_seed + self.num_tests - 1})")

        for i in range(self.num_tests):
            seed = self.start_seed + i
            test_id = f"generated_seed{seed}"

            # Generate code
            code = generate_ast_code(seed=seed, energy=1000)

            if code is None:
                # Generator failed to produce valid code
                self.skipped += 1
                continue

            result = test_round_trip(code, test_id)

            if result.success:
                self.passed += 1
                if (i + 1) % 10 == 0:
                    print(f"  ✓ {i + 1}/{self.num_tests} tests passed")
            else:
                self.failed += 1
                saved_path = save_failure(code, test_id, result.error)
                print(f"  ✗ Seed {seed}: FAILED")
                print(f"    Error: {result.error[:100]}")

                self.failures.append({
                    'test_id': test_id,
                    'error': result.error,
                    'filepath': saved_path,
                    'reproduce': f"python3 tests/aston/fuzz.py --generative --seed {seed}",
                })

        if self.passed > 0:
            print(f"  ✓ Total: {self.passed}/{self.num_tests} passed")


def main():
    """Run comprehensive ASTON fuzzing."""
    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(description='Comprehensive ASTON round-trip fuzzer')
    parser.add_argument('--corpus', action='store_true', help='Run corpus fuzzing only')
    parser.add_argument('--mutation', action='store_true', help='Run mutation fuzzing only')
    parser.add_argument('--generative', action='store_true', help='Run generative fuzzing only')
    parser.add_argument('--seed', type=int, default=0, help='Starting seed for generative fuzzing')
    parser.add_argument('--mutations', type=int, default=50, help='Mutations per base (default: 50)')
    parser.add_argument('--tests', type=int, default=100, help='Number of generative tests (default: 100)')

    args = parser.parse_args()

    print("=" * 70)
    print("ASTON Comprehensive Fuzzing")
    print("=" * 70)

    strategies = []

    # Determine which strategies to run
    if args.corpus or not (args.mutation or args.generative):
        strategies.append(CorpusFuzzStrategy())

    if args.mutation or not (args.corpus or args.generative):
        strategies.append(MutationFuzzStrategy(num_mutations=args.mutations))

    if args.generative or not (args.corpus or args.mutation):
        strategies.append(GenerativeFuzzStrategy(num_tests=args.tests, start_seed=args.seed))

    # Run all strategies
    for strategy in strategies:
        strategy.run()
        strategy.report()

    # Overall summary
    total_passed = sum(s.passed for s in strategies)
    total_failed = sum(s.failed for s in strategies)
    total_skipped = sum(s.skipped for s in strategies)
    total = total_passed + total_failed + total_skipped

    print("\n" + "=" * 70)
    print("Overall Summary")
    print("=" * 70)
    print(f"Total tests:  {total}")
    print(f"Passed:       {total_passed} ({100 * total_passed // total if total > 0 else 0}%)")
    if total_skipped > 0:
        print(f"Skipped:      {total_skipped}")
    print(f"Failed:       {total_failed}")

    # List all failures
    all_failures = []
    for strategy in strategies:
        all_failures.extend(strategy.failures)

    if all_failures:
        print("\n" + "=" * 70)
        print("All Failures")
        print("=" * 70)
        for failure in all_failures:
            print(f"\n{failure['test_id']}:")
            print(f"  Error: {failure['error'][:150]}")
            print(f"  File:  {failure['filepath']}")
            print(f"  Test:  python3 aston.py --test {failure['filepath']}")
            print(f"  Repro: {failure['reproduce']}")

    if total_failed > 0:
        print("\n✗ FUZZ TEST FAILED")
        sys.exit(1)
    else:
        print("\n✓ ALL FUZZ TESTS PASSED")
        sys.exit(0)


if __name__ == '__main__':
    main()
