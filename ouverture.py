#!/usr/bin/env python3
"""
ouverture - A function pool manager for Python code
"""
import ast
import argparse
import builtins
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Dict, Set, Tuple, List


# Get all Python built-in names
PYTHON_BUILTINS = set(dir(builtins))


class ASTNormalizer(ast.NodeTransformer):
    """Normalizes an AST by renaming variables and functions"""

    def __init__(self, name_mapping: Dict[str, str]):
        self.name_mapping = name_mapping

    def visit_Name(self, node):
        if node.id in self.name_mapping:
            node.id = self.name_mapping[node.id]
        return node

    def visit_arg(self, node):
        if node.arg in self.name_mapping:
            node.arg = self.name_mapping[node.arg]
        return node

    def visit_FunctionDef(self, node):
        if node.name in self.name_mapping:
            node.name = self.name_mapping[node.name]
        self.generic_visit(node)
        return node


def collect_names(tree: ast.Module) -> Set[str]:
    """Collect all names (variables, functions) used in the AST"""
    names = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.FunctionDef):
            names.add(node.name)
            for arg in node.args.args:
                names.add(arg.arg)

    return names


def get_imported_names(tree: ast.Module) -> Set[str]:
    """Get all names that are imported"""
    imported = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.asname if alias.asname else alias.name)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported.add(alias.asname if alias.asname else alias.name)

    return imported


def check_unused_imports(tree: ast.Module, imported_names: Set[str], all_names: Set[str]) -> bool:
    """Check if all imports are used"""
    for name in imported_names:
        # Check if the imported name is used anywhere besides the import statement
        used = False
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            if isinstance(node, ast.Name) and node.id == name:
                used = True
                break
        if not used:
            return False
    return True


def sort_imports(tree: ast.Module) -> ast.Module:
    """Sort imports lexicographically"""
    imports = []
    other_nodes = []

    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(node)
        else:
            other_nodes.append(node)

    # Sort imports by their string representation
    def import_key(node):
        if isinstance(node, ast.Import):
            return ('import', tuple(sorted(alias.name for alias in node.names)))
        else:  # ImportFrom
            module = node.module if node.module else ''
            return ('from', module, tuple(sorted(alias.name for alias in node.names)))

    imports.sort(key=import_key)

    tree.body = imports + other_nodes
    return tree


def extract_function_def(tree: ast.Module) -> Tuple[ast.FunctionDef, List[ast.stmt]]:
    """Extract the function definition and import statements"""
    imports = []
    function_def = None

    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(node)
        elif isinstance(node, ast.FunctionDef):
            if function_def is not None:
                raise ValueError("Only one function definition is allowed per file")
            function_def = node

    if function_def is None:
        raise ValueError("No function definition found in file")

    return function_def, imports


def create_name_mapping(function_def: ast.FunctionDef, imports: List[ast.stmt],
                        ouverture_aliases: Set[str] = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create mapping from original names to normalized names.
    Returns (forward_mapping, reverse_mapping)
    Forward: original -> _ouverture_v_X
    Reverse: _ouverture_v_X -> original
    """
    if ouverture_aliases is None:
        ouverture_aliases = set()

    imported_names = set()
    for imp in imports:
        if isinstance(imp, ast.Import):
            for alias in imp.names:
                imported_names.add(alias.asname if alias.asname else alias.name)
        elif isinstance(imp, ast.ImportFrom):
            for alias in imp.names:
                imported_names.add(alias.asname if alias.asname else alias.name)

    forward_mapping = {}
    reverse_mapping = {}
    counter = 0

    # Function name is always _ouverture_v_0
    forward_mapping[function_def.name] = '_ouverture_v_0'
    reverse_mapping['_ouverture_v_0'] = function_def.name
    counter += 1

    # Collect all names in the function (excluding imported names, built-ins, and ouverture aliases)
    all_names = set()
    for node in ast.walk(function_def):
        if isinstance(node, ast.Name) and node.id not in imported_names and node.id not in PYTHON_BUILTINS and node.id not in ouverture_aliases:
            all_names.add(node.id)
        elif isinstance(node, ast.arg) and node.arg not in imported_names and node.arg not in PYTHON_BUILTINS and node.arg not in ouverture_aliases:
            all_names.add(node.arg)

    # Remove function name as it's already mapped
    all_names.discard(function_def.name)

    # Sort names for consistent mapping
    for name in sorted(all_names):
        normalized = f'_ouverture_v_{counter}'
        forward_mapping[name] = normalized
        reverse_mapping[normalized] = name
        counter += 1

    return forward_mapping, reverse_mapping


def rewrite_ouverture_imports(imports: List[ast.stmt]) -> Tuple[List[ast.stmt], Dict[str, str]]:
    """
    Rewrite imports from 'ouverture' to 'couverture' and track aliases.
    Returns (new_imports, alias_mapping)
    alias_mapping maps: imported_function_hash -> alias_in_lang
    """
    new_imports = []
    alias_mapping = {}

    for imp in imports:
        if isinstance(imp, ast.ImportFrom) and imp.module == 'ouverture':
            # Rewrite: from ouverture import c0ffeebad as kawa
            # To: from couverture import c0ffeebad
            new_names = []
            for alias in imp.names:
                # Track the alias mapping
                if alias.asname:
                    alias_mapping[alias.name] = alias.asname
                # Create new import without alias
                new_names.append(ast.alias(name=alias.name, asname=None))

            new_imp = ast.ImportFrom(
                module='couverture',
                names=new_names,
                level=0
            )
            new_imports.append(new_imp)
        else:
            new_imports.append(imp)

    return new_imports, alias_mapping


def replace_ouverture_calls(tree: ast.AST, alias_mapping: Dict[str, str], name_mapping: Dict[str, str]):
    """
    Replace calls to aliased ouverture functions.
    E.g., kawa(...) becomes c0ffeebad._ouverture_v_0(...)
    """
    class OuvertureCallReplacer(ast.NodeTransformer):
        def visit_Name(self, node):
            # If this name is an alias for an ouverture function
            for func_hash, alias in alias_mapping.items():
                if node.id == alias:
                    # Replace with c0ffeebad._ouverture_v_0
                    return ast.Attribute(
                        value=ast.Name(id=func_hash, ctx=ast.Load()),
                        attr='_ouverture_v_0',
                        ctx=node.ctx
                    )
            return node

    replacer = OuvertureCallReplacer()
    return replacer.visit(tree)


def clear_locations(tree: ast.AST):
    """Set all line and column information to None"""
    for node in ast.walk(tree):
        if hasattr(node, 'lineno'):
            node.lineno = None
        if hasattr(node, 'col_offset'):
            node.col_offset = None
        if hasattr(node, 'end_lineno'):
            node.end_lineno = None
        if hasattr(node, 'end_col_offset'):
            node.end_col_offset = None


def extract_docstring(function_def: ast.FunctionDef) -> Tuple[str, ast.FunctionDef]:
    """
    Extract docstring from function definition.
    Returns (docstring, function_without_docstring)
    """
    docstring = ast.get_docstring(function_def)

    # Create a copy of the function without the docstring
    import copy
    func_copy = copy.deepcopy(function_def)

    # Remove docstring if it exists (first statement is a string constant)
    if (func_copy.body and
        isinstance(func_copy.body[0], ast.Expr) and
        isinstance(func_copy.body[0].value, ast.Constant) and
        isinstance(func_copy.body[0].value.value, str)):
        func_copy.body = func_copy.body[1:]

    return docstring if docstring else "", func_copy


def normalize_ast(tree: ast.Module, lang: str) -> Tuple[str, str, str, Dict[str, str], Dict[str, str]]:
    """
    Normalize the AST according to ouverture rules.
    Returns (normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping)
    """
    # Sort imports
    tree = sort_imports(tree)

    # Extract function and imports
    function_def, imports = extract_function_def(tree)

    # Extract docstring from function
    docstring, function_without_docstring = extract_docstring(function_def)

    # Rewrite ouverture imports
    imports, alias_mapping = rewrite_ouverture_imports(imports)

    # Get the set of ouverture aliases (values in alias_mapping)
    ouverture_aliases = set(alias_mapping.values())

    # Create name mapping
    forward_mapping, reverse_mapping = create_name_mapping(function_def, imports, ouverture_aliases)

    # Create two modules: one with docstring (for display) and one without (for hashing)
    module_with_docstring = ast.Module(body=imports + [function_def], type_ignores=[])
    module_without_docstring = ast.Module(body=imports + [function_without_docstring], type_ignores=[])

    # Process both modules identically
    for module in [module_with_docstring, module_without_docstring]:
        # Replace ouverture calls with their normalized form
        module = replace_ouverture_calls(module, alias_mapping, forward_mapping)

        # Normalize names
        normalizer = ASTNormalizer(forward_mapping)
        normalizer.visit(module)

        # Clear locations
        clear_locations(module)

        # Fix missing locations
        ast.fix_missing_locations(module)

    # Unparse both versions
    normalized_code_with_docstring = ast.unparse(module_with_docstring)
    normalized_code_without_docstring = ast.unparse(module_without_docstring)

    return normalized_code_with_docstring, normalized_code_without_docstring, docstring, reverse_mapping, alias_mapping


def compute_hash(code: str) -> str:
    """Compute SHA256 hash of the code"""
    return hashlib.sha256(code.encode('utf-8')).hexdigest()


def save_function(hash_value: str, lang: str, normalized_code: str, docstring: str,
                  name_mapping: Dict[str, str], alias_mapping: Dict[str, str]):
    """Save the function to the ouverture objects directory"""
    # Create directory structure: .ouverture/objects/XX/
    objects_dir = Path('.ouverture/objects')
    hash_dir = objects_dir / hash_value[:2]
    hash_dir.mkdir(parents=True, exist_ok=True)

    # Create JSON file path
    json_path = hash_dir / f'{hash_value[2:]}.json'

    # Check if file exists and load existing data
    if json_path.exists():
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {
            'version': 0,
            'hash': hash_value,
            'normalized_code': normalized_code,
            'docstrings': {},
            'name_mappings': {},
            'alias_mappings': {}
        }

    # Add language-specific data
    data['docstrings'][lang] = docstring
    data['name_mappings'][lang] = name_mapping
    data['alias_mappings'][lang] = alias_mapping

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Function saved: {json_path}")
    print(f"Hash: {hash_value}")
    print(f"Language: {lang}")


def add_function(file_path_with_lang: str):
    """Add a function to the ouverture pool"""
    # Parse the path and language
    if '@' not in file_path_with_lang:
        print("Error: Missing language suffix. Use format: path/to/file.py@lang", file=sys.stderr)
        sys.exit(1)

    file_path, lang = file_path_with_lang.rsplit('@', 1)

    # Validate language code (should be 3 characters, ISO 639-3)
    if len(lang) != 3:
        print(f"Error: Language code must be 3 characters (ISO 639-3). Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    # Read and parse the file
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        print(f"Error: Failed to parse {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

    # Normalize the AST
    try:
        normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping = normalize_ast(tree, lang)
    except Exception as e:
        print(f"Error: Failed to normalize AST: {e}", file=sys.stderr)
        sys.exit(1)

    # Compute hash on code WITHOUT docstring (so same logic = same hash regardless of language)
    hash_value = compute_hash(normalized_code_without_docstring)

    # Save to JSON (store the version WITH docstring for display purposes)
    save_function(hash_value, lang, normalized_code_with_docstring, docstring, name_mapping, alias_mapping)


def main():
    parser = argparse.ArgumentParser(description='ouverture - Function pool manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a function to the pool')
    add_parser.add_argument('file', help='Path to Python file with @lang suffix (e.g., file.py@eng)')

    args = parser.parse_args()

    if args.command == 'add':
        add_function(args.file)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
