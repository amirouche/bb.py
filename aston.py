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
import hashlib
import json
import sys
from typing import Any, List, Tuple


def aston_write(node: ast.AST) -> Tuple[str, List[Tuple]]:
    """Convert an AST node to ASTON tuples.

    Args:
        node: AST node to convert

    Returns:
        (content_hash, all_tuples) where:
        - content_hash: SHA256 hex digest of the canonical JSON representation
        - all_tuples: List of (content_hash, key, index, value) tuples for this node and all descendants
    """
    all_tuples = []
    obj = {'_type': node.__class__.__name__}

    # Process all fields and build obj for hashing
    field_data = {}

    for field, value in ast.iter_fields(node):
        if value is None:
            obj[field] = None
            field_data[field] = ('scalar', None)
        elif isinstance(value, (str, int, float, bool)):
            obj[field] = value
            field_data[field] = ('scalar', value)
        elif isinstance(value, list):
            obj[field] = []
            list_items = []
            for item in value:
                if isinstance(item, ast.AST):
                    child_hash, child_tuples = aston_write(item)
                    all_tuples.extend(child_tuples)
                    obj[field].append(child_hash)
                    list_items.append(child_hash)
                else:
                    obj[field].append(item)
                    list_items.append(item)
            # Mark empty lists explicitly
            if not list_items:
                field_data[field] = ('empty_list', None)
            else:
                field_data[field] = ('list', list_items)
        elif isinstance(value, ast.AST):
            child_hash, child_tuples = aston_write(value)
            all_tuples.extend(child_tuples)
            obj[field] = child_hash
            field_data[field] = ('scalar', child_hash)

    # Compute content hash from canonical JSON representation
    canonical = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    content_hash = hashlib.sha256(canonical.encode('utf-8')).hexdigest()

    # Create tuples for this node
    node_tuples = [(content_hash, '_type', None, node.__class__.__name__)]

    for field, (kind, data) in field_data.items():
        if kind == 'scalar':
            node_tuples.append((content_hash, field, None, data))
        elif kind == 'empty_list':
            # Use index -1 to mark empty list
            node_tuples.append((content_hash, field, -1, None))
        elif kind == 'list':
            for i, item_value in enumerate(data):
                node_tuples.append((content_hash, field, i, item_value))

    all_tuples.extend(node_tuples)
    return content_hash, all_tuples


def aston_read(tuples: List[Tuple]) -> ast.AST:
    """Reconstruct AST from ASTON tuples.

    Args:
        tuples: List of (content_hash, key, index, value) tuples

    Returns:
        Reconstructed AST node (root Module)
    """
    # Group tuples by content_hash
    objects = {}
    for content_hash, key, index, value in tuples:
        if content_hash not in objects:
            objects[content_hash] = {}

        if index is None:
            # Scalar field
            objects[content_hash][key] = value
        elif index == -1:
            # Empty list marker
            objects[content_hash][key] = []
        else:
            # Array field - collect items by index
            if key not in objects[content_hash]:
                objects[content_hash][key] = {}
            objects[content_hash][key][index] = value

    # Convert array dicts to sorted lists
    for hash_val, obj in objects.items():
        for key, value in list(obj.items()):
            if isinstance(value, dict) and value and all(isinstance(k, int) for k in value.keys()):
                # Convert {0: v0, 1: v1, ...} to [v0, v1, ...]
                max_index = max(value.keys())
                obj[key] = [value[i] for i in range(max_index + 1)]

    # Build AST nodes recursively
    ast_nodes = {}

    def build_ast(hash_val):
        if hash_val in ast_nodes:
            return ast_nodes[hash_val]

        obj = objects[hash_val]
        node_type = obj['_type']

        # Get the AST class
        ast_class = getattr(ast, node_type)

        # Build fields, resolving HC references
        fields = {}
        for key, value in obj.items():
            if key == '_type':
                continue

            if isinstance(value, str) and len(value) == 64 and value in objects:
                # HC reference - recursively build
                fields[key] = build_ast(value)
            elif isinstance(value, list):
                # Array - resolve any HC references
                resolved_list = []
                for item in value:
                    if isinstance(item, str) and len(item) == 64 and item in objects:
                        resolved_list.append(build_ast(item))
                    else:
                        resolved_list.append(item)
                fields[key] = resolved_list
            else:
                fields[key] = value

        # Create AST node
        node = ast_class(**fields)
        ast_nodes[hash_val] = node
        return node

    # Find root node (Module)
    root_hash = None
    for hash_val, obj in objects.items():
        if obj.get('_type') == 'Module':
            root_hash = hash_val
            break

    if root_hash is None:
        raise ValueError("No Module node found in tuples")

    return build_ast(root_hash)


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
