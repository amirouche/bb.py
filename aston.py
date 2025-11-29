#!/usr/bin/env python3
"""
aston.py - AST Object Notation converter

Converts Python source files to ASTON representation (tuples of content-addressed AST nodes).

Format: Each line is a JSON array representing a tuple (content_hash, key, index, value)
- content_hash: SHA256 hex digest of the canonical JSON representation
- key: Field name within the object
- index: Position in array (int) or None for scalar values
- value: Atomic data (None/str/int/float/bool) or hash reference (HC)
"""

import ast
import json
import sys

# Import ASTON functions from bb.py
from bb import aston_write, aston_read


def main():
    """Main CLI entry point."""
    test_mode = '--test' in sys.argv

    if test_mode:
        sys.argv.remove('--test')

    if len(sys.argv) != 2:
        print("Usage: aston.py [--test] <filepath>", file=sys.stderr)
        sys.exit(1)

    filepath = sys.argv[1]

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()
    except FileNotFoundError:
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error in {filepath}: {e}", file=sys.stderr)
        sys.exit(1)

    if test_mode:
        # Test round-trip: expected == aston_read(aston_write(expected))
        _, tuples = aston_write(tree)
        reconstructed = aston_read(tuples)

        # Compare using ast.dump
        original_dump = ast.dump(tree)
        reconstructed_dump = ast.dump(reconstructed)

        if original_dump == reconstructed_dump:
            print("✓ Round-trip test PASSED", file=sys.stderr)
            sys.exit(0)
        else:
            print("✗ Round-trip test FAILED", file=sys.stderr)
            print("\nOriginal AST:", file=sys.stderr)
            print(original_dump[:500], file=sys.stderr)
            print("\n...\n", file=sys.stderr)
            print("Reconstructed AST:", file=sys.stderr)
            print(reconstructed_dump[:500], file=sys.stderr)
            sys.exit(1)
    else:
        # Normal mode - output tuples as JSON lines
        _, tuples = aston_write(tree)
        for tup in tuples:
            print(json.dumps(tup, ensure_ascii=False))


if __name__ == '__main__':
    main()
