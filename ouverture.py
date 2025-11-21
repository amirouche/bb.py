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


def names_collect(tree: ast.Module) -> Set[str]:
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


def imports_get_names(tree: ast.Module) -> Set[str]:
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


def imports_check_unused(tree: ast.Module, imported_names: Set[str], all_names: Set[str]) -> bool:
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


def imports_sort(tree: ast.Module) -> ast.Module:
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


def function_extract_definition(tree: ast.Module) -> Tuple[ast.FunctionDef, List[ast.stmt]]:
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


def mapping_create_name(function_def: ast.FunctionDef, imports: List[ast.stmt],
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


def imports_rewrite_ouverture(imports: List[ast.stmt]) -> Tuple[List[ast.stmt], Dict[str, str]]:
    """
    Remove aliases from 'ouverture' imports and track them for later restoration.
    Returns (new_imports, alias_mapping)
    alias_mapping maps: imported_function_hash -> alias_in_lang
    """
    new_imports = []
    alias_mapping = {}

    for imp in imports:
        if isinstance(imp, ast.ImportFrom) and imp.module == 'ouverture.pool':
            # Rewrite: from ouverture.pool import c0ffeebad as kawa
            # To: from ouverture.pool import c0ffeebad
            new_names = []
            for alias in imp.names:
                # Track the alias mapping
                if alias.asname:
                    alias_mapping[alias.name] = alias.asname
                # Create new import without alias
                new_names.append(ast.alias(name=alias.name, asname=None))

            new_imp = ast.ImportFrom(
                module='ouverture.pool',
                names=new_names,
                level=0
            )
            new_imports.append(new_imp)
        else:
            new_imports.append(imp)

    return new_imports, alias_mapping


def calls_replace_ouverture(tree: ast.AST, alias_mapping: Dict[str, str], name_mapping: Dict[str, str]):
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


def ast_clear_locations(tree: ast.AST):
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


def docstring_extract(function_def: ast.FunctionDef) -> Tuple[str, ast.FunctionDef]:
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


def ast_normalize(tree: ast.Module, lang: str) -> Tuple[str, str, str, Dict[str, str], Dict[str, str]]:
    """
    Normalize the AST according to ouverture rules.
    Returns (normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping)
    """
    # Sort imports
    tree = imports_sort(tree)

    # Extract function and imports
    function_def, imports = function_extract_definition(tree)

    # Extract docstring from function
    docstring, function_without_docstring = docstring_extract(function_def)

    # Rewrite ouverture imports
    imports, alias_mapping = imports_rewrite_ouverture(imports)

    # Get the set of ouverture aliases (values in alias_mapping)
    ouverture_aliases = set(alias_mapping.values())

    # Create name mapping
    forward_mapping, reverse_mapping = mapping_create_name(function_def, imports, ouverture_aliases)

    # Create two modules: one with docstring (for display) and one without (for hashing)
    module_with_docstring = ast.Module(body=imports + [function_def], type_ignores=[])
    module_without_docstring = ast.Module(body=imports + [function_without_docstring], type_ignores=[])

    # Process both modules identically
    for module in [module_with_docstring, module_without_docstring]:
        # Replace ouverture calls with their normalized form
        module = calls_replace_ouverture(module, alias_mapping, forward_mapping)

        # Normalize names
        normalizer = ASTNormalizer(forward_mapping)
        normalizer.visit(module)

        # Clear locations
        ast_clear_locations(module)

        # Fix missing locations
        ast.fix_missing_locations(module)

    # Unparse both versions
    normalized_code_with_docstring = ast.unparse(module_with_docstring)
    normalized_code_without_docstring = ast.unparse(module_without_docstring)

    return normalized_code_with_docstring, normalized_code_without_docstring, docstring, reverse_mapping, alias_mapping


def hash_compute(code: str, algorithm: str = 'sha256') -> str:
    """
    Compute hash of the code using specified algorithm.

    Args:
        code: The code to hash
        algorithm: Hash algorithm to use (default: 'sha256')

    Returns:
        Hex string of the hash
    """
    if algorithm == 'sha256':
        return hashlib.sha256(code.encode('utf-8')).hexdigest()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def mapping_compute_hash(docstring: str, name_mapping: Dict[str, str],
                        alias_mapping: Dict[str, str], comment: str = "") -> str:
    """
    Compute content-addressed hash for a mapping.

    The hash is computed on the canonical JSON representation of:
    - docstring
    - name_mapping
    - alias_mapping
    - comment

    This enables deduplication: identical mappings share the same hash and storage.

    Args:
        docstring: The function docstring for this mapping
        name_mapping: Normalized name -> original name mapping
        alias_mapping: Ouverture function hash -> alias mapping
        comment: Optional comment explaining this mapping variant

    Returns:
        64-character hex hash (SHA256)
    """
    mapping_dict = {
        'docstring': docstring,
        'name_mapping': name_mapping,
        'alias_mapping': alias_mapping,
        'comment': comment
    }

    # Create canonical JSON (sorted keys, no whitespace)
    canonical_json = json.dumps(mapping_dict, sort_keys=True, ensure_ascii=False)

    # Compute hash
    return hash_compute(canonical_json)


def schema_detect_version(func_hash: str) -> int:
    """
    Detect the schema version of a stored function.

    Checks the filesystem to determine if a function is stored in v0 or v1 format:
    - v0: $OUVERTURE_DIRECTORY/objects/XX/YYYYYY.json
    - v1: $OUVERTURE_DIRECTORY/objects/sha256/XX/YYYYYY.../object.json

    Args:
        func_hash: The function hash to check

    Returns:
        0 for v0 format, 1 for v1 format, None if function not found
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Check for v1 format first (function directory with object.json)
    v1_func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    v1_object_json = v1_func_dir / 'object.json'

    if v1_object_json.exists():
        return 1

    # Check for v0 format (JSON file)
    v0_hash_dir = objects_dir / func_hash[:2]
    v0_json_path = v0_hash_dir / f'{func_hash[2:]}.json'

    if v0_json_path.exists():
        return 0

    # Function not found
    return None


def metadata_create() -> Dict[str, any]:
    """
    Create default metadata for a function.

    Generates metadata with:
    - created: ISO 8601 timestamp
    - author: Username from environment (USER or USERNAME)
    - tags: Empty list
    - dependencies: Empty list

    Returns:
        Dictionary with metadata fields
    """
    from datetime import datetime

    # Get author from environment
    author = os.environ.get('USER', os.environ.get('USERNAME', ''))

    # Get current timestamp in ISO 8601 format
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    return {
        'created': timestamp,
        'author': author,
        'tags': [],
        'dependencies': []
    }


def directory_get_ouverture() -> Path:
    """
    Get the ouverture directory from environment variable or default to '$HOME/.local/ouverture/'.
    Environment variable: OUVERTURE_DIRECTORY
    """
    env_dir = os.environ.get('OUVERTURE_DIRECTORY')
    if env_dir:
        return Path(env_dir)
    # Default to $HOME/.local/ouverture/
    home = os.environ.get('HOME', os.path.expanduser('~'))
    return Path(home) / '.local' / 'ouverture'


def function_save_v0(hash_value: str, lang: str, normalized_code: str, docstring: str,
                     name_mapping: Dict[str, str], alias_mapping: Dict[str, str]):
    """
    Save the function to the ouverture objects directory using schema v0.

    This function is kept for the migration tool and backward compatibility.
    New code should use function_save_v1() instead.
    """
    # Create directory structure: OUVERTURE_DIR/objects/XX/
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'
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


def function_save_v1(hash_value: str, normalized_code: str, metadata: Dict[str, any]):
    """
    Save function to ouverture directory using schema v1.

    Creates the function directory and object.json file:
    - Directory: $OUVERTURE_DIRECTORY/objects/sha256/XX/YYYYYY.../
    - File: $OUVERTURE_DIRECTORY/objects/sha256/XX/YYYYYY.../object.json

    Args:
        hash_value: Function hash (64-character hex)
        normalized_code: Normalized code with docstring
        metadata: Metadata dict (created, author, tags, dependencies)
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Create function directory: objects/sha256/XX/YYYYYY.../
    func_dir = objects_dir / 'sha256' / hash_value[:2] / hash_value[2:]
    func_dir.mkdir(parents=True, exist_ok=True)

    # Create object.json
    object_json = func_dir / 'object.json'

    data = {
        'schema_version': 1,
        'hash': hash_value,
        'hash_algorithm': 'sha256',
        'normalized_code': normalized_code,
        'encoding': 'none',
        'metadata': metadata
    }

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Function saved (v1): {object_json}")
    print(f"Hash: {hash_value}")


def mapping_save_v1(func_hash: str, lang: str, docstring: str,
                   name_mapping: Dict[str, str], alias_mapping: Dict[str, str],
                   comment: str = "") -> str:
    """
    Save language mapping to ouverture directory using schema v1.

    Creates the mapping directory and mapping.json file:
    - Directory: $OUVERTURE_DIRECTORY/objects/sha256/XX/Y.../lang/sha256/ZZ/W.../
    - File: $OUVERTURE_DIRECTORY/objects/sha256/XX/Y.../lang/sha256/ZZ/W.../mapping.json

    The mapping is content-addressed, enabling deduplication.

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: Ouverture function hash -> alias mapping
        comment: Optional comment explaining this mapping variant

    Returns:
        Mapping hash (64-character hex)
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Compute mapping hash
    mapping_hash = mapping_compute_hash(docstring, name_mapping, alias_mapping, comment)

    # Create mapping directory: objects/sha256/XX/Y.../lang/sha256/ZZ/W.../
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    mapping_dir = func_dir / lang / 'sha256' / mapping_hash[:2] / mapping_hash[2:]
    mapping_dir.mkdir(parents=True, exist_ok=True)

    # Create mapping.json
    mapping_json = mapping_dir / 'mapping.json'

    data = {
        'docstring': docstring,
        'name_mapping': name_mapping,
        'alias_mapping': alias_mapping,
        'comment': comment
    }

    with open(mapping_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Mapping saved (v1): {mapping_json}")
    print(f"Language: {lang}")
    print(f"Mapping hash: {mapping_hash}")

    return mapping_hash


def function_save(hash_value: str, lang: str, normalized_code: str, docstring: str,
                  name_mapping: Dict[str, str], alias_mapping: Dict[str, str], comment: str = ""):
    """
    Save function to ouverture directory using schema v1 (current default).

    This is the main entry point for saving functions. It uses schema v1 format.

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        normalized_code: Normalized code with docstring
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: Ouverture function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
    """
    # Create metadata
    metadata = metadata_create()

    # Save function (object.json)
    function_save_v1(hash_value, normalized_code, metadata)

    # Save mapping (mapping.json)
    mapping_save_v1(hash_value, lang, docstring, name_mapping, alias_mapping, comment)


def code_denormalize(normalized_code: str, name_mapping: Dict[str, str], alias_mapping: Dict[str, str]) -> str:
    """
    Denormalize code by applying reverse name mappings.
    name_mapping: maps normalized names (_ouverture_v_X) to original names
    alias_mapping: maps hash IDs to alias names (for ouverture imports)
    """
    tree = ast.parse(normalized_code)

    # Create reverse alias mapping: _ouverture_v_0 -> alias
    # We need to track which hashes should become which aliases
    hash_to_alias = {}
    for hash_id, alias in alias_mapping.items():
        hash_to_alias[hash_id] = alias

    class Denormalizer(ast.NodeTransformer):
        def visit_Name(self, node):
            # Replace normalized variable names
            if node.id in name_mapping:
                node.id = name_mapping[node.id]
            return node

        def visit_arg(self, node):
            # Replace normalized argument names
            if node.arg in name_mapping:
                node.arg = name_mapping[node.arg]
            return node

        def visit_FunctionDef(self, node):
            # Replace normalized function name
            if node.name in name_mapping:
                node.name = name_mapping[node.name]
            self.generic_visit(node)
            return node

        def visit_Attribute(self, node):
            # Replace c0ffeebad._ouverture_v_0(...) with alias(...)
            if (isinstance(node.value, ast.Name) and
                node.value.id in hash_to_alias and
                node.attr == '_ouverture_v_0'):
                # Return just the alias name
                return ast.Name(id=hash_to_alias[node.value.id], ctx=node.ctx)
            self.generic_visit(node)
            return node

        def visit_ImportFrom(self, node):
            # Add aliases back to 'from ouverture.pool import X'
            if node.module == 'ouverture.pool':
                node.module = 'ouverture.pool'
                # Add aliases back
                new_names = []
                for alias_node in node.names:
                    if alias_node.name in hash_to_alias:
                        # This hash should have an alias
                        new_names.append(ast.alias(
                            name=alias_node.name,
                            asname=hash_to_alias[alias_node.name]
                        ))
                    else:
                        new_names.append(alias_node)
                node.names = new_names
            return node

    denormalizer = Denormalizer()
    tree = denormalizer.visit(tree)

    return ast.unparse(tree)


def function_add(file_path_with_lang: str, comment: str = ""):
    """
    Add a function to the ouverture pool using schema v1.

    Args:
        file_path_with_lang: File path with language suffix (e.g., "file.py@eng")
        comment: Optional comment explaining this mapping variant
    """
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
        normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping = ast_normalize(tree, lang)
    except Exception as e:
        print(f"Error: Failed to normalize AST: {e}", file=sys.stderr)
        sys.exit(1)

    # Compute hash on code WITHOUT docstring (so same logic = same hash regardless of language)
    hash_value = hash_compute(normalized_code_without_docstring)

    # Save to v1 format (store the version WITH docstring for display purposes)
    function_save(hash_value, lang, normalized_code_with_docstring, docstring, name_mapping, alias_mapping, comment)


def docstring_replace(code: str, new_docstring: str) -> str:
    """
    Replace the docstring in a function with a new one.
    If new_docstring is empty, remove the docstring.
    """
    tree = ast.parse(code)

    # Find the function definition
    function_def = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            function_def = node
            break

    if function_def is None:
        return code

    # Check if there's an existing docstring
    has_docstring = (function_def.body and
                     isinstance(function_def.body[0], ast.Expr) and
                     isinstance(function_def.body[0].value, ast.Constant) and
                     isinstance(function_def.body[0].value.value, str))

    if new_docstring:
        # Create new docstring node
        new_docstring_node = ast.Expr(value=ast.Constant(value=new_docstring))

        if has_docstring:
            # Replace existing docstring
            function_def.body[0] = new_docstring_node
        else:
            # Insert new docstring at the beginning
            function_def.body.insert(0, new_docstring_node)
    else:
        # Remove docstring if it exists and new_docstring is empty
        if has_docstring:
            function_def.body = function_def.body[1:]

    return ast.unparse(tree)


def function_load_v0(hash_value: str, lang: str) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a function from the ouverture pool using schema v0.

    This function is kept for backward compatibility with v0 format.
    New code should use function_load() which dispatches to v0 or v1.

    Returns (normalized_code, name_mapping, alias_mapping, docstring)
    """
    # Build file path using configurable ouverture directory
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'
    hash_dir = objects_dir / hash_value[:2]
    json_path = hash_dir / f'{hash_value[2:]}.json'

    # Check if file exists
    if not json_path.exists():
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load the JSON data
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON file: {e}", file=sys.stderr)
        sys.exit(1)

    # Check if language exists in the data
    if lang not in data['name_mappings']:
        available_langs = ', '.join(data['name_mappings'].keys())
        print(f"Error: Language '{lang}' not found for this function.", file=sys.stderr)
        print(f"Available languages: {available_langs}", file=sys.stderr)
        sys.exit(1)

    # Get the normalized code and mappings for the requested language
    normalized_code = data['normalized_code']
    name_mapping = data['name_mappings'][lang]
    alias_mapping = data.get('alias_mappings', {}).get(lang, {})
    docstring = data.get('docstrings', {}).get(lang, '')

    return normalized_code, name_mapping, alias_mapping, docstring


def function_load_v1(hash_value: str) -> Dict[str, any]:
    """
    Load function from ouverture directory using schema v1.

    Loads only the object.json file (no language-specific data).

    Args:
        hash_value: Function hash (64-character hex)

    Returns:
        Dictionary with schema_version, hash, hash_algorithm, normalized_code, encoding, metadata
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Build path: objects/sha256/XX/YYYYYY.../object.json
    func_dir = objects_dir / 'sha256' / hash_value[:2] / hash_value[2:]
    object_json = func_dir / 'object.json'

    # Check if file exists
    if not object_json.exists():
        print(f"Error: Function not found (v1): {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load the JSON data
    try:
        with open(object_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse object.json: {e}", file=sys.stderr)
        sys.exit(1)

    return data


def mappings_list_v1(func_hash: str, lang: str) -> list:
    """
    List all mapping variants for a given function and language.

    Scans the language directory and returns all available mappings.

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")

    Returns:
        List of (mapping_hash, comment) tuples
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Build path: objects/sha256/XX/YYYYYY.../lang/
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    lang_dir = func_dir / lang

    # Check if language directory exists
    if not lang_dir.exists():
        return []

    # Scan for mapping directories: lang/sha256/ZZ/WWWW.../mapping.json
    mappings = []
    sha256_dir = lang_dir / 'sha256'
    if not sha256_dir.exists():
        return []

    # Iterate through hash prefix directories (ZZ/)
    for hash_prefix_dir in sha256_dir.iterdir():
        if not hash_prefix_dir.is_dir():
            continue

        # Iterate through full hash directories (WWWW.../)
        for mapping_hash_dir in hash_prefix_dir.iterdir():
            if not mapping_hash_dir.is_dir():
                continue

            # Check if mapping.json exists
            mapping_json = mapping_hash_dir / 'mapping.json'
            if not mapping_json.exists():
                continue

            # Reconstruct mapping hash from path
            mapping_hash = hash_prefix_dir.name + mapping_hash_dir.name

            # Load mapping to get comment
            try:
                with open(mapping_json, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                comment = mapping_data.get('comment', '')
                mappings.append((mapping_hash, comment))
            except (json.JSONDecodeError, IOError):
                # Skip invalid mapping files
                continue

    return mappings


def mapping_load_v1(func_hash: str, lang: str, mapping_hash: str) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a specific language mapping using schema v1.

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        mapping_hash: Mapping hash (64-character hex)

    Returns:
        Tuple of (docstring, name_mapping, alias_mapping, comment)
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Build path: objects/sha256/XX/Y.../lang/sha256/ZZ/W.../mapping.json
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    mapping_dir = func_dir / lang / 'sha256' / mapping_hash[:2] / mapping_hash[2:]
    mapping_json = mapping_dir / 'mapping.json'

    # Check if file exists
    if not mapping_json.exists():
        print(f"Error: Mapping not found: {func_hash}@{lang} (mapping hash: {mapping_hash})", file=sys.stderr)
        sys.exit(1)

    # Load the JSON data
    try:
        with open(mapping_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse mapping.json: {e}", file=sys.stderr)
        sys.exit(1)

    docstring = data.get('docstring', '')
    name_mapping = data.get('name_mapping', {})
    alias_mapping = data.get('alias_mapping', {})
    comment = data.get('comment', '')

    return docstring, name_mapping, alias_mapping, comment


def function_load(hash_value: str, lang: str, mapping_hash: str = None) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a function from the ouverture pool (dispatch to v0 or v1).

    Detects the schema version and routes to the appropriate loader.

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        mapping_hash: Optional mapping hash for v1 (64-character hex)

    Returns:
        Tuple of (normalized_code, name_mapping, alias_mapping, docstring)
    """
    # Detect schema version
    version = schema_detect_version(hash_value)

    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    if version == 0:
        # Load v0 format (backward compatibility)
        return function_load_v0(hash_value, lang)

    elif version == 1:
        # Load v1 format
        # Load object.json
        func_data = function_load_v1(hash_value)
        normalized_code = func_data['normalized_code']

        # Get available mappings
        mappings = mappings_list_v1(hash_value, lang)

        if len(mappings) == 0:
            print(f"Error: No mappings found for language '{lang}'", file=sys.stderr)
            sys.exit(1)

        # Determine which mapping to load
        if mapping_hash is not None:
            # Explicit mapping requested
            selected_hash = mapping_hash
        elif len(mappings) == 1:
            # Only one mapping available
            selected_hash, _ = mappings[0]
        else:
            # Multiple mappings available - pick first alphabetically for now
            # (Phase 5 will improve this with a selection menu)
            mappings_sorted = sorted(mappings, key=lambda x: x[0])
            selected_hash, _ = mappings_sorted[0]

        # Load the mapping
        docstring, name_mapping, alias_mapping, comment = mapping_load_v1(hash_value, lang, selected_hash)

        return normalized_code, name_mapping, alias_mapping, docstring

    else:
        print(f"Error: Unsupported schema version: {version}", file=sys.stderr)
        sys.exit(1)


def function_get(hash_with_lang: str):
    """Get a function from the ouverture pool"""
    # Parse the hash and language
    if '@' not in hash_with_lang:
        print("Error: Missing language suffix. Use format: HASH@lang", file=sys.stderr)
        sys.exit(1)

    hash_value, lang = hash_with_lang.rsplit('@', 1)

    # Validate language code (should be 3 characters, ISO 639-3)
    if len(lang) != 3:
        print(f"Error: Language code must be 3 characters (ISO 639-3). Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Validate hash format (should be 64 hex characters for SHA256)
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load function data from pool
    normalized_code, name_mapping, alias_mapping, docstring = function_load(hash_value, lang)

    # Replace the docstring with the language-specific one
    try:
        normalized_code = docstring_replace(normalized_code, docstring)
    except Exception as e:
        print(f"Error: Failed to replace docstring: {e}", file=sys.stderr)
        sys.exit(1)

    # Denormalize the code
    try:
        original_code = code_denormalize(normalized_code, name_mapping, alias_mapping)
    except Exception as e:
        print(f"Error: Failed to denormalize code: {e}", file=sys.stderr)
        sys.exit(1)

    # Print the code
    print(original_code)


def main():
    parser = argparse.ArgumentParser(description='ouverture - Function pool manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a function to the pool')
    add_parser.add_argument('file', help='Path to Python file with @lang suffix (e.g., file.py@eng)')
    add_parser.add_argument('--comment', default='', help='Optional comment explaining this mapping variant')

    # Get command
    get_parser = subparsers.add_parser('get', help='Get a function from the pool')
    get_parser.add_argument('hash', help='Function hash with @lang suffix (e.g., abc123...@eng)')

    args = parser.parse_args()

    if args.command == 'add':
        function_add(args.file, args.comment)
    elif args.command == 'get':
        function_get(args.hash)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
