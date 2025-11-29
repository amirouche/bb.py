#!/usr/bin/env python3
"""
aston.py - AST Object Notation converter

Thin wrapper around 'bb.py aston' command for backward compatibility.

Converts Python source files to ASTON representation (tuples of content-addressed AST nodes).

Format: Each line is a JSON array representing a tuple (content_hash, key, index, value)
- content_hash: SHA256 hex digest of the canonical JSON representation
- key: Field name within the object
- index: Position in array (int) or None for scalar values
- value: Atomic data (None/str/int/float/bool) or hash reference (HC)
"""

import sys
from bb import command_aston


def main():
    """Main CLI entry point - wrapper around bb.py aston command."""
    test_mode = '--test' in sys.argv

    if test_mode:
        sys.argv.remove('--test')

    if len(sys.argv) != 2:
        print("Usage: aston.py [--test] <filepath>", file=sys.stderr)
        sys.exit(1)

    filepath = sys.argv[1]
    command_aston(filepath, test_mode=test_mode)


if __name__ == '__main__':
    main()
