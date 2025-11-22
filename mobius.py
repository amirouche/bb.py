#!/usr/bin/env python3
"""
mobius - A function pool manager for Python code
"""
import ast
import argparse
import builtins
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Set, Tuple, List, Union


# Get all Python built-in names
PYTHON_BUILTINS = set(dir(builtins))

# Prefix for mobius.pool imports to ensure valid Python identifiers
# SHA256 hashes can start with digits (0-9), which are invalid as Python identifiers
# By prefixing with "object_", we ensure all import names are valid
MOBIUS_IMPORT_PREFIX = "object_"


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

    def visit_AsyncFunctionDef(self, node):
        """Handle async function definitions the same as regular functions"""
        if node.name in self.name_mapping:
            node.name = self.name_mapping[node.name]
        self.generic_visit(node)
        return node


def code_collect_names(tree: ast.Module) -> Set[str]:
    """Collect all names (variables, functions) used in the AST"""
    names = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
            for arg in node.args.args:
                names.add(arg.arg)

    return names


def code_get_import_names(tree: ast.Module) -> Set[str]:
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


def code_check_unused_imports(tree: ast.Module, imported_names: Set[str], all_names: Set[str]) -> bool:
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


def code_sort_imports(tree: ast.Module) -> ast.Module:
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


def code_extract_definition(tree: ast.Module) -> Tuple[Union[ast.FunctionDef, ast.AsyncFunctionDef], List[ast.stmt]]:
    """Extract the function definition (sync or async) and import statements"""
    imports = []
    function_def = None

    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if function_def is not None:
                raise ValueError("Only one function definition is allowed per file")
            function_def = node

    if function_def is None:
        raise ValueError("No function definition found in file")

    return function_def, imports


def code_extract_check_decorators(function_def: Union[ast.FunctionDef, ast.AsyncFunctionDef]) -> List[str]:
    """
    Extract target function hashes from @check decorators.

    The @check decorator marks a function as a test for another function in the pool.
    Syntax: @check(object_HASH) where object_HASH is a mobius pool import.

    Args:
        function_def: The function definition AST node

    Returns:
        List of function hashes (without object_ prefix) that this function tests
    """
    checks = []

    for decorator in function_def.decorator_list:
        # Look for @check(object_HASH) pattern
        if isinstance(decorator, ast.Call):
            # Check if it's a call to 'check'
            if isinstance(decorator.func, ast.Name) and decorator.func.id == 'check':
                # Get the argument (should be a Name node like 'object_abc123...')
                if len(decorator.args) == 1 and isinstance(decorator.args[0], ast.Name):
                    arg_name = decorator.args[0].id
                    # Extract hash from object_HASH format
                    if arg_name.startswith(MOBIUS_IMPORT_PREFIX):
                        func_hash = arg_name[len(MOBIUS_IMPORT_PREFIX):]
                        checks.append(func_hash)

    return checks


def code_create_name_mapping(function_def: Union[ast.FunctionDef, ast.AsyncFunctionDef], imports: List[ast.stmt],
                        mobius_aliases: Set[str] = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create mapping from original names to normalized names.
    Returns (forward_mapping, reverse_mapping)
    Forward: original -> _mobius_v_X
    Reverse: _mobius_v_X -> original
    """
    if mobius_aliases is None:
        mobius_aliases = set()

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

    # Function name is always _mobius_v_0
    forward_mapping[function_def.name] = '_mobius_v_0'
    reverse_mapping['_mobius_v_0'] = function_def.name
    counter += 1

    # Collect all names in the function (excluding imported names, built-ins, and mobius aliases)
    # Use a set to track seen names and avoid duplicates
    seen_names = set()
    all_names = list()
    for node in ast.walk(function_def):
        if isinstance(node, ast.Name) and node.id not in imported_names and node.id not in PYTHON_BUILTINS and node.id not in mobius_aliases:
            if node.id not in seen_names:
                seen_names.add(node.id)
                all_names.append(node.id)
        elif isinstance(node, ast.arg) and node.arg not in imported_names and node.arg not in PYTHON_BUILTINS and node.arg not in mobius_aliases:
            if node.arg not in seen_names:
                seen_names.add(node.arg)
                all_names.append(node.arg)

    # XXX: all_names: do not sort, keep the order ast traversal
    # discovery.

    for name in all_names:
        normalized = f'_mobius_v_{counter}'
        forward_mapping[name] = normalized
        reverse_mapping[normalized] = name
        counter += 1

    return forward_mapping, reverse_mapping


def code_rewrite_mobius_imports(imports: List[ast.stmt]) -> Tuple[List[ast.stmt], Dict[str, str]]:
    """
    Remove aliases from 'mobius' imports and track them for later restoration.
    Returns (new_imports, alias_mapping)
    alias_mapping maps: actual_function_hash (without prefix) -> alias_in_lang

    Input format expected:
        from mobius.pool import object_c0ff33 as kawa

    Output:
        - import becomes: from mobius.pool import object_c0ff33
        - alias_mapping stores: {"c0ff33...": "kawa"} (actual hash without object_ prefix)
    """
    new_imports = []
    alias_mapping = {}

    for imp in imports:
        if isinstance(imp, ast.ImportFrom) and imp.module == 'mobius.pool':
            # Rewrite: from mobius.pool import object_c0ffeebad as kawa
            # To: from mobius.pool import object_c0ffeebad
            new_names = []
            for alias in imp.names:
                import_name = alias.name  # e.g., "object_c0ff33..."

                # Extract actual hash by stripping the prefix
                if import_name.startswith(MOBIUS_IMPORT_PREFIX):
                    actual_hash = import_name[len(MOBIUS_IMPORT_PREFIX):]
                else:
                    # Backward compatibility: no prefix (shouldn't happen in new code)
                    actual_hash = import_name

                # Track the alias mapping using actual hash
                if alias.asname:
                    alias_mapping[actual_hash] = alias.asname

                # Create new import without alias (but keep object_ prefix in import name)
                new_names.append(ast.alias(name=import_name, asname=None))

            new_imp = ast.ImportFrom(
                module='mobius.pool',
                names=new_names,
                level=0
            )
            new_imports.append(new_imp)
        else:
            new_imports.append(imp)

    return new_imports, alias_mapping


def code_replace_mobius_calls(tree: ast.AST, alias_mapping: Dict[str, str], name_mapping: Dict[str, str]):
    """
    Replace calls to aliased mobius functions.
    E.g., kawa(...) becomes object_c0ffeebad._mobius_v_0(...)

    alias_mapping maps actual hash (without prefix) -> alias name
    The replacement uses object_<hash> to match the import name.
    """
    class MobiusCallReplacer(ast.NodeTransformer):
        def visit_Name(self, node):
            # If this name is an alias for an mobius function
            for func_hash, alias in alias_mapping.items():
                if node.id == alias:
                    # Replace with object_c0ffeebad._mobius_v_0
                    # Use prefixed name to match the import statement
                    prefixed_name = MOBIUS_IMPORT_PREFIX + func_hash
                    return ast.Attribute(
                        value=ast.Name(id=prefixed_name, ctx=ast.Load()),
                        attr='_mobius_v_0',
                        ctx=node.ctx
                    )
            return node

    replacer = MobiusCallReplacer()
    return replacer.visit(tree)


def code_clear_locations(tree: ast.AST):
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


def code_extract_docstring(function_def: Union[ast.FunctionDef, ast.AsyncFunctionDef]) -> Tuple[str, Union[ast.FunctionDef, ast.AsyncFunctionDef]]:
    """
    Extract docstring from function definition (sync or async).
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


def code_normalize(tree: ast.Module, lang: str) -> Tuple[str, str, str, Dict[str, str], Dict[str, str]]:
    """
    Normalize the AST according to mobius rules.
    Returns (normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping)
    """
    # Sort imports
    tree = code_sort_imports(tree)

    # Extract function and imports
    function_def, imports = code_extract_definition(tree)

    # Extract docstring from function
    docstring, function_without_docstring = code_extract_docstring(function_def)

    # Rewrite mobius imports
    imports, alias_mapping = code_rewrite_mobius_imports(imports)

    # Get the set of mobius aliases (values in alias_mapping)
    mobius_aliases = set(alias_mapping.values())

    # Create name mapping
    forward_mapping, reverse_mapping = code_create_name_mapping(function_def, imports, mobius_aliases)

    # Create two modules: one with docstring (for display) and one without (for hashing)
    module_with_docstring = ast.Module(body=imports + [function_def], type_ignores=[])
    module_without_docstring = ast.Module(body=imports + [function_without_docstring], type_ignores=[])

    # Process both modules identically
    for module in [module_with_docstring, module_without_docstring]:
        # Replace mobius calls with their normalized form
        module = code_replace_mobius_calls(module, alias_mapping, forward_mapping)

        # Normalize names
        normalizer = ASTNormalizer(forward_mapping)
        normalizer.visit(module)

        # Clear locations
        code_clear_locations(module)

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


def code_compute_mapping_hash(docstring: str, name_mapping: Dict[str, str],
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
        alias_mapping: Mobius function hash -> alias mapping
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


def code_detect_schema(func_hash: str) -> int:
    """
    Detect the schema version of a stored function.

    Checks the filesystem to determine if a function is stored in v1 format:
    - v1: $MOBIUS_DIRECTORY/objects/sha256/XX/YYYYYY.../object.json

    Args:
        func_hash: The function hash to check

    Returns:
        1 for v1 format, None if function not found
    """
    pool_dir = storage_get_pool_directory()

    # Check for v1 format (function directory with object.json)
    v1_func_dir = pool_dir / func_hash[:2] / func_hash[2:]
    v1_object_json = v1_func_dir / 'object.json'

    if v1_object_json.exists():
        return 1

    # Function not found
    return None


def code_create_metadata(parent: str = None, checks: List[str] = None) -> Dict[str, any]:
    """
    Create default metadata for a function.

    Generates metadata with:
    - created: ISO 8601 timestamp
    - name: User's name from config
    - email: User's email from config
    - parent: Optional parent function hash (for lineage tracking)
    - checks: Optional list of function hashes this function tests

    Args:
        parent: Optional parent function hash (for fork lineage tracking)
        checks: Optional list of function hashes this function tests (from @check decorators)

    Returns:
        Dictionary with metadata fields
    """
    from datetime import datetime

    # Get name and email from config
    config = storage_read_config()
    name = config['user'].get('name', '')
    email = config['user'].get('email', '')

    # Get current timestamp in ISO 8601 format
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    metadata = {
        'created': timestamp,
        'name': name,
        'email': email
    }

    if parent:
        metadata['parent'] = parent

    if checks:
        metadata['checks'] = checks

    return metadata


def storage_get_mobius_directory() -> Path:
    """
    Get the mobius base directory from environment variable or default to '$HOME/.local/mobius/'.
    Environment variable: MOBIUS_DIRECTORY

    Directory structure:
        $MOBIUS_DIRECTORY/
        ├── pool/          # Pool directory (git repository for objects)
        │   └── sha256/    # Hash algorithm prefix
        │       └── XX/    # First 2 chars of hash
        └── config.json    # Configuration file
    """
    env_dir = os.environ.get('MOBIUS_DIRECTORY')
    if env_dir:
        return Path(env_dir)
    # Default to $HOME/.local/mobius/
    home = os.environ.get('HOME', os.path.expanduser('~'))
    return Path(home) / '.local' / 'mobius'


def storage_get_pool_directory() -> Path:
    """
    Get the pool directory (git repository) where objects are stored.
    Returns: $MOBIUS_DIRECTORY/pool/
    """
    return storage_get_mobius_directory() / 'pool'


def storage_get_config_path() -> Path:
    """
    Get the path to the config file.
    Config is stored in $MOBIUS_DIRECTORY/config.json
    Can be overridden with MOBIUS_CONFIG_PATH environment variable for testing.
    """
    config_override = os.environ.get('MOBIUS_CONFIG_PATH')
    if config_override:
        return Path(config_override)
    return storage_get_mobius_directory() / 'config.json'


def storage_read_config() -> Dict[str, any]:
    """
    Read the configuration file.
    Returns default config if file doesn't exist.
    """
    config_path = storage_get_config_path()

    if not config_path.exists():
        return {
            'user': {
                'name': '',
                'email': '',
                'public_key': '',
                'languages': []
            },
            'remotes': {}
        }

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error: Failed to read config file: {e}", file=sys.stderr)
        sys.exit(1)


def storage_write_config(config: Dict[str, any]):
    """
    Write the configuration file.
    """
    config_path = storage_get_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Error: Failed to write config file: {e}", file=sys.stderr)
        sys.exit(1)


def command_init():
    """
    Initialize mobius directory and config file.
    """
    mobius_dir = storage_get_mobius_directory()

    # Create pool directory (git repository for objects)
    pool_dir = storage_get_pool_directory()
    try:
        pool_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error: Failed to create pool directory: {e}", file=sys.stderr)
        sys.exit(1)

    # Create config file with defaults
    config_path = storage_get_config_path()
    if config_path.exists():
        print(f"Config file already exists: {config_path}")
    else:
        config = {
            'user': {
                'username': os.environ.get('USER', os.environ.get('USERNAME', '')),
                'email': '',
                'public_key': '',
                'languages': ['eng']
            },
            'remotes': {}
        }
        storage_write_config(config)
        print(f"Created config file: {config_path}")

    print(f"Initialized mobius directory: {mobius_dir}")


def command_whoami(subcommand: str, value: list = None):
    """
    Get or set user configuration.

    Args:
        subcommand: One of 'name', 'email', 'public-key', 'language'
        value: New value(s) to set (None to get current value)
    """
    config = storage_read_config()

    # Map CLI subcommand to config key
    key_map = {
        'name': 'name',
        'email': 'email',
        'public-key': 'public_key',
        'language': 'languages'
    }

    if subcommand not in key_map:
        print(f"Error: Unknown subcommand: {subcommand}", file=sys.stderr)
        print("Valid subcommands: name, email, public-key, language", file=sys.stderr)
        sys.exit(1)

    config_key = key_map[subcommand]

    # Get current value
    if value is None or len(value) == 0:
        current = config['user'][config_key]
        if isinstance(current, list):
            print(' '.join(current) if current else '')
        else:
            print(current if current else '')
    else:
        # Set new value
        if subcommand == 'language':
            # Languages is a list
            config['user'][config_key] = value
        else:
            # Other fields are strings (take first value)
            config['user'][config_key] = value[0]

        storage_write_config(config)

        if subcommand == 'language':
            print(f"Set {subcommand}: {' '.join(value)}")
        else:
            print(f"Set {subcommand}: {value[0]}")


def code_save_v1(hash_value: str, normalized_code: str, metadata: Dict[str, any]):
    """
    Save function to mobius directory using schema v1.

    Creates the function directory and object.json file:
    - Directory: $MOBIUS_DIRECTORY/pool/sha256/XX/YYYYYY.../
    - File: $MOBIUS_DIRECTORY/pool/sha256/XX/YYYYYY.../object.json

    Args:
        hash_value: Function hash (64-character hex)
        normalized_code: Normalized code with docstring
        metadata: Metadata dict (created, author)
    """
    pool_dir = storage_get_pool_directory()

    # Create function directory: pool/XX/YYYYYY.../
    func_dir = pool_dir / hash_value[:2] / hash_value[2:]
    func_dir.mkdir(parents=True, exist_ok=True)

    # Create object.json
    object_json = func_dir / 'object.json'

    data = {
        'schema_version': 1,
        'hash': hash_value,
        'normalized_code': normalized_code,
        'metadata': metadata
    }

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Hash: {hash_value}")


def mapping_save_v1(func_hash: str, lang: str, docstring: str,
                   name_mapping: Dict[str, str], alias_mapping: Dict[str, str],
                   comment: str = "") -> str:
    """
    Save language mapping to mobius directory using schema v1.

    Creates the mapping directory and mapping.json file:
    - Directory: $MOBIUS_DIRECTORY/objects/sha256/XX/Y.../lang/sha256/ZZ/W.../
    - File: $MOBIUS_DIRECTORY/objects/sha256/XX/Y.../lang/sha256/ZZ/W.../mapping.json

    The mapping is content-addressed, enabling deduplication.

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: Mobius function hash -> alias mapping
        comment: Optional comment explaining this mapping variant

    Returns:
        Mapping hash (64-character hex)
    """
    pool_dir = storage_get_pool_directory()

    # Compute mapping hash
    mapping_hash = code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment)

    # Create mapping directory: pool/XX/Y.../lang/ZZ/W.../
    func_dir = pool_dir / func_hash[:2] / func_hash[2:]
    mapping_dir = func_dir / lang / mapping_hash[:2] / mapping_hash[2:]
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

    print(f"Mapping hash: {mapping_hash}")

    return mapping_hash


def code_save(hash_value: str, lang: str, normalized_code: str, docstring: str,
                  name_mapping: Dict[str, str], alias_mapping: Dict[str, str], comment: str = "",
                  parent: str = None, checks: List[str] = None):
    """
    Save function to mobius directory using schema v1 (current default).

    This is the main entry point for saving functions. It uses schema v1 format.

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        normalized_code: Normalized code with docstring
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: Mobius function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        parent: Optional parent function hash (for fork lineage tracking)
        checks: Optional list of function hashes this function tests (from @check decorators)
    """
    # Create metadata (with optional parent for lineage and checks)
    metadata = code_create_metadata(parent=parent, checks=checks)

    # Save function (object.json)
    code_save_v1(hash_value, normalized_code, metadata)

    # Save mapping (mapping.json)
    mapping_save_v1(hash_value, lang, docstring, name_mapping, alias_mapping, comment)


def code_denormalize(normalized_code: str, name_mapping: Dict[str, str], alias_mapping: Dict[str, str]) -> str:
    """
    Denormalize code by applying reverse name mappings.
    name_mapping: maps normalized names (_mobius_v_X) to original names
    alias_mapping: maps actual hash IDs (without object_ prefix) to alias names

    Normalized code uses object_<hash> in imports and attributes.
    This function restores the original aliases.
    """
    tree = ast.parse(normalized_code)

    # Create reverse alias mapping: actual_hash -> alias
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

        def visit_AsyncFunctionDef(self, node):
            # Replace normalized async function name
            if node.name in name_mapping:
                node.name = name_mapping[node.name]
            self.generic_visit(node)
            return node

        def visit_Attribute(self, node):
            # Replace object_c0ffeebad._mobius_v_0(...) with alias(...)
            if (isinstance(node.value, ast.Name) and
                node.attr == '_mobius_v_0'):
                prefixed_name = node.value.id
                # Strip object_ prefix to get actual hash
                if prefixed_name.startswith(MOBIUS_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(MOBIUS_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name  # Backward compatibility

                if actual_hash in hash_to_alias:
                    # Return just the alias name
                    return ast.Name(id=hash_to_alias[actual_hash], ctx=node.ctx)
            self.generic_visit(node)
            return node

        def visit_ImportFrom(self, node):
            # Add aliases back to 'from mobius.pool import object_X'
            if node.module == 'mobius.pool':
                node.module = 'mobius.pool'
                # Add aliases back
                new_names = []
                for alias_node in node.names:
                    import_name = alias_node.name  # e.g., "object_c0ff33..."

                    # Strip object_ prefix to get actual hash
                    if import_name.startswith(MOBIUS_IMPORT_PREFIX):
                        actual_hash = import_name[len(MOBIUS_IMPORT_PREFIX):]
                    else:
                        actual_hash = import_name  # Backward compatibility

                    if actual_hash in hash_to_alias:
                        # This hash should have an alias
                        # Keep object_ prefix in import name
                        new_names.append(ast.alias(
                            name=import_name,
                            asname=hash_to_alias[actual_hash]
                        ))
                    else:
                        new_names.append(alias_node)
                node.names = new_names
            return node

    denormalizer = Denormalizer()
    tree = denormalizer.visit(tree)

    return ast.unparse(tree)


# =============================================================================
# Git Remote Functions
# =============================================================================

def git_run(args: List[str], cwd: str = None, timeout: int = 60) -> subprocess.CompletedProcess:
    """
    Execute a git command via subprocess.run().

    Args:
        args: List of arguments to pass to git (without 'git' prefix)
        cwd: Working directory for the command
        timeout: Timeout in seconds (default 60)

    Returns:
        subprocess.CompletedProcess with stdout/stderr captured

    Raises:
        subprocess.CalledProcessError: If git command fails
        subprocess.TimeoutExpired: If command times out
    """
    cmd = ['git'] + args
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout
    )
    return result


def git_url_parse(url: str) -> Dict[str, str]:
    """
    Parse a Git URL into components.

    Supports formats:
    - git@host:user/repo.git (SSH)
    - git+https://host/user/repo.git (HTTPS)
    - git+file:///path/to/repo (Local file)

    Args:
        url: Git URL to parse

    Returns:
        Dictionary with keys: protocol, host, path, original_url
        protocol: 'ssh', 'https', or 'file'
    """
    result = {'original_url': url}

    if url.startswith('git@'):
        # SSH format: git@host:user/repo.git
        result['protocol'] = 'ssh'
        # Split on : to get host and path
        parts = url[4:].split(':', 1)
        result['host'] = parts[0]
        result['path'] = parts[1] if len(parts) > 1 else ''
        result['git_url'] = url  # Already in git format

    elif url.startswith('git+https://'):
        # HTTPS format: git+https://host/path/repo.git
        result['protocol'] = 'https'
        # Remove git+ prefix for actual git URL
        actual_url = url[4:]  # Remove 'git+' prefix
        from urllib.parse import urlparse
        parsed = urlparse(actual_url)
        result['host'] = parsed.netloc
        result['path'] = parsed.path.lstrip('/')
        result['git_url'] = actual_url

    elif url.startswith('git+file://'):
        # Local file format: git+file:///path/to/repo
        result['protocol'] = 'file'
        result['host'] = ''
        result['path'] = url[11:]  # Remove 'git+file://' prefix
        result['git_url'] = 'file://' + result['path']

    else:
        raise ValueError(f"Unsupported Git URL format: {url}")

    return result


def git_detect_remote_type(url: str) -> str:
    """
    Detect the type of remote from URL.

    Args:
        url: Remote URL

    Returns:
        Remote type: 'file', 'git-ssh', 'git-https', 'git-file', 'http', 'https'
    """
    if url.startswith('file://'):
        return 'file'
    elif url.startswith('git@'):
        return 'git-ssh'
    elif url.startswith('git+https://'):
        return 'git-https'
    elif url.startswith('git+file://'):
        return 'git-file'
    elif url.startswith('https://'):
        return 'https'
    elif url.startswith('http://'):
        return 'http'
    else:
        return 'unknown'


def git_cache_path(remote_name: str) -> Path:
    """
    Get the cache path for a Git remote repository.

    Args:
        remote_name: Name of the remote

    Returns:
        Path to the cached repository directory
    """
    mobius_dir = storage_get_mobius_directory()
    return mobius_dir / 'cache' / 'git' / remote_name


def git_clone_or_fetch(git_url: str, local_path: Path) -> bool:
    """
    Clone a Git repository if it doesn't exist, or fetch if it does.

    Args:
        git_url: Git URL (SSH, HTTPS, or file://)
        local_path: Local path to clone/fetch to

    Returns:
        True if successful, False otherwise
    """
    if local_path.exists() and (local_path / '.git').exists():
        # Repository exists, fetch updates
        result = git_run(['fetch', 'origin'], cwd=str(local_path))
        if result.returncode != 0:
            print(f"Warning: git fetch failed: {result.stderr}", file=sys.stderr)
            return False

        # Pull changes (fast-forward only)
        result = git_run(['pull', '--ff-only', 'origin', 'main'], cwd=str(local_path))
        if result.returncode != 0:
            # Try 'master' branch if 'main' fails
            result = git_run(['pull', '--ff-only', 'origin', 'master'], cwd=str(local_path))
            if result.returncode != 0:
                print(f"Warning: git pull failed: {result.stderr}", file=sys.stderr)
                return False
        return True
    else:
        # Clone repository
        local_path.parent.mkdir(parents=True, exist_ok=True)
        result = git_run(['clone', git_url, str(local_path)])
        if result.returncode != 0:
            print(f"Error: git clone failed: {result.stderr}", file=sys.stderr)
            return False
        return True


def git_commit_and_push(local_path: Path, message: str) -> bool:
    """
    Stage all changes, commit, and push to remote.

    Args:
        local_path: Path to the Git repository
        message: Commit message

    Returns:
        True if successful, False otherwise
    """
    cwd = str(local_path)

    # Stage all changes
    result = git_run(['add', '.'], cwd=cwd)
    if result.returncode != 0:
        print(f"Error: git add failed: {result.stderr}", file=sys.stderr)
        return False

    # Check if there are changes to commit
    result = git_run(['diff', '--cached', '--quiet'], cwd=cwd)
    if result.returncode == 0:
        # No changes to commit
        print("No changes to commit")
        return True

    # Commit changes
    result = git_run(['commit', '-m', message], cwd=cwd)
    if result.returncode != 0:
        print(f"Error: git commit failed: {result.stderr}", file=sys.stderr)
        return False

    # Push to remote
    result = git_run(['push', 'origin', 'HEAD'], cwd=cwd)
    if result.returncode != 0:
        print(f"Error: git push failed: {result.stderr}", file=sys.stderr)
        return False

    return True


def command_remote_add(name: str, url: str):
    """
    Add a remote repository.

    Args:
        name: Remote name
        url: Remote URL. Supported formats:
            - file:///path/to/pool (direct file copy)
            - git@host:user/repo.git (Git SSH)
            - git+https://host/user/repo.git (Git HTTPS)
            - git+file:///path/to/repo (Local Git repository)
    """
    config = storage_read_config()

    if name in config['remotes']:
        print(f"Error: Remote '{name}' already exists", file=sys.stderr)
        sys.exit(1)

    # Detect remote type
    remote_type = git_detect_remote_type(url)

    if remote_type == 'unknown':
        print(f"Error: Invalid URL format: {url}", file=sys.stderr)
        print("Supported formats:", file=sys.stderr)
        print("  file:///path/to/pool          - Direct file copy", file=sys.stderr)
        print("  git@host:user/repo.git        - Git SSH", file=sys.stderr)
        print("  git+https://host/user/repo    - Git HTTPS", file=sys.stderr)
        print("  git+file:///path/to/repo      - Local Git repository", file=sys.stderr)
        sys.exit(1)

    config['remotes'][name] = {
        'url': url,
        'type': remote_type
    }

    storage_write_config(config)
    print(f"Added remote '{name}': {url} (type: {remote_type})")


def command_remote_remove(name: str):
    """
    Remove a remote repository.

    Args:
        name: Remote name to remove
    """
    config = storage_read_config()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    del config['remotes'][name]
    storage_write_config(config)
    print(f"Removed remote '{name}'")


def command_remote_list():
    """
    List all configured remotes.
    """
    config = storage_read_config()

    if not config['remotes']:
        print("No remotes configured")
        return

    print("Configured remotes:")
    for name, remote in config['remotes'].items():
        print(f"  {name}: {remote['url']}")


def command_remote_pull(name: str):
    """
    Fetch functions from a remote repository.

    Args:
        name: Remote name to pull from
    """
    import shutil

    config = storage_read_config()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    remote = config['remotes'][name]
    url = remote['url']
    remote_type = remote.get('type', git_detect_remote_type(url))

    print(f"Pulling from remote '{name}': {url}")
    print()

    if remote_type == 'file':
        # Local file system remote (direct copy)
        remote_path = Path(url[7:])  # Remove file:// prefix

        if not remote_path.exists():
            print(f"Error: Remote path does not exist: {remote_path}", file=sys.stderr)
            sys.exit(1)

        # Copy functions from remote to local pool
        local_pool = storage_get_pool_directory()
        local_objects = local_pool
        remote_objects = remote_path

        if not remote_objects.exists():
            print(f"Error: Remote objects directory not found: {remote_objects}", file=sys.stderr)
            sys.exit(1)

        # Copy all objects
        pulled_count = 0
        for item in remote_objects.rglob('*.json'):
            rel_path = item.relative_to(remote_objects)
            local_item = local_objects / rel_path

            if not local_item.exists():
                local_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, local_item)
                pulled_count += 1

        print(f"Pulled {pulled_count} new functions from '{name}'")

    elif remote_type in ('git-ssh', 'git-https', 'git-file'):
        # Git remote - clone/fetch then copy objects
        parsed = git_url_parse(url)
        cache_path = git_cache_path(name)

        print(f"Syncing Git repository to {cache_path}...")
        if not git_clone_or_fetch(parsed['git_url'], cache_path):
            print("Error: Failed to sync Git repository", file=sys.stderr)
            sys.exit(1)

        # Copy functions from cached repository to local pool
        local_pool = storage_get_pool_directory()
        local_objects = local_pool
        remote_objects = cache_path

        if not remote_objects.exists():
            print(f"Warning: No objects directory in remote repository")
            print("Pulled 0 new functions from '{name}'")
            return

        # Copy all objects
        pulled_count = 0
        for item in remote_objects.rglob('*.json'):
            rel_path = item.relative_to(remote_objects)
            local_item = local_objects / rel_path

            if not local_item.exists():
                local_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, local_item)
                pulled_count += 1

        print(f"Pulled {pulled_count} new functions from '{name}'")

    else:
        # HTTP/HTTPS remote (not Git)
        print(f"Error: Remote type '{remote_type}' not yet implemented", file=sys.stderr)
        sys.exit(1)


def command_remote_push(name: str):
    """
    Publish functions to a remote repository.

    Args:
        name: Remote name to push to
    """
    import shutil

    config = storage_read_config()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    remote = config['remotes'][name]
    url = remote['url']
    remote_type = remote.get('type', git_detect_remote_type(url))

    print(f"Pushing to remote '{name}': {url}")
    print()

    if remote_type == 'file':
        # Local file system remote (direct copy)
        remote_path = Path(url[7:])  # Remove file:// prefix

        # Create remote directory if it doesn't exist
        remote_path.mkdir(parents=True, exist_ok=True)

        # Copy functions from local pool to remote
        local_pool = storage_get_pool_directory()
        local_objects = local_pool
        remote_objects = remote_path

        if not local_objects.exists():
            print("Error: Local objects directory not found", file=sys.stderr)
            sys.exit(1)

        # Copy all objects
        pushed_count = 0
        for item in local_objects.rglob('*.json'):
            rel_path = item.relative_to(local_objects)
            remote_item = remote_objects / rel_path

            if not remote_item.exists():
                remote_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, remote_item)
                pushed_count += 1

        print(f"Pushed {pushed_count} new functions to '{name}'")

    elif remote_type in ('git-ssh', 'git-https', 'git-file'):
        # Git remote - sync repo, copy objects, commit and push
        parsed = git_url_parse(url)
        cache_path = git_cache_path(name)

        # First, ensure we have the latest from remote
        print(f"Syncing Git repository from {parsed['git_url']}...")
        if not git_clone_or_fetch(parsed['git_url'], cache_path):
            print("Error: Failed to sync Git repository", file=sys.stderr)
            sys.exit(1)

        # Copy functions from local pool to cached repository
        local_pool = storage_get_pool_directory()
        local_objects = local_pool
        remote_objects = cache_path

        if not local_objects.exists():
            print("Error: Local objects directory not found", file=sys.stderr)
            sys.exit(1)

        # Copy all new objects to cache
        pushed_count = 0
        for item in local_objects.rglob('*.json'):
            rel_path = item.relative_to(local_objects)
            remote_item = remote_objects / rel_path

            if not remote_item.exists():
                remote_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, remote_item)
                pushed_count += 1

        if pushed_count == 0:
            print("No new functions to push")
            return

        # Commit and push changes
        print(f"Committing {pushed_count} new functions...")
        commit_msg = f"Add {pushed_count} function(s) from mobius push"
        if not git_commit_and_push(cache_path, commit_msg):
            print("Error: Failed to commit and push changes", file=sys.stderr)
            sys.exit(1)

        print(f"Pushed {pushed_count} new functions to '{name}'")

    else:
        # HTTP/HTTPS remote (not Git)
        print(f"Error: Remote type '{remote_type}' not yet implemented", file=sys.stderr)
        sys.exit(1)


def code_extract_dependencies(normalized_code: str) -> List[str]:
    """
    Extract mobius dependencies from normalized code.

    Returns:
        List of actual function hashes (without object_ prefix) that this function depends on
    """
    dependencies = []
    tree = ast.parse(normalized_code)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'mobius.pool':
            for alias in node.names:
                import_name = alias.name  # e.g., "object_c0ff33..."
                # Strip object_ prefix to get actual hash
                if import_name.startswith(MOBIUS_IMPORT_PREFIX):
                    actual_hash = import_name[len(MOBIUS_IMPORT_PREFIX):]
                else:
                    actual_hash = import_name  # Backward compatibility
                dependencies.append(actual_hash)

    return dependencies


def code_resolve_dependencies(func_hash: str) -> List[str]:
    """
    Resolve all dependencies transitively and return them in topological order.

    The function itself is included at the end of the list.
    Dependencies are listed before the functions that depend on them.

    Args:
        func_hash: Function hash to resolve dependencies for

    Returns:
        List of function hashes in topological order (dependencies first, target last)
    """
    resolved = []
    visited = set()

    def visit(hash_value: str):
        if hash_value in visited:
            return
        visited.add(hash_value)

        # Detect version and load function data
        version = code_detect_schema(hash_value)
        if version is None:
            raise ValueError(f"Function not found: {hash_value}")

        # Load function to get its code (v1 only)
        func_data = code_load_v1(hash_value)
        normalized_code = func_data['normalized_code']

        # Extract and visit dependencies first
        deps = code_extract_dependencies(normalized_code)
        for dep in deps:
            visit(dep)

        # Add this function after its dependencies
        resolved.append(hash_value)

    visit(func_hash)
    return resolved


def code_bundle_dependencies(hashes: List[str], output_dir: Path) -> Path:
    """
    Bundle function files to an output directory.

    Copies all function files maintaining the v1 directory structure.

    Args:
        hashes: List of function hashes to bundle
        output_dir: Directory to copy files to

    Returns:
        Path to the output directory
    """
    import shutil

    pool_dir = storage_get_pool_directory()
    output_dir = Path(output_dir)
    output_objects = output_dir
    output_objects.mkdir(parents=True, exist_ok=True)

    for func_hash in hashes:
        version = code_detect_schema(func_hash)
        if version is None:
            raise ValueError(f"Function not found: {func_hash}")

        # Copy entire function directory (v1 only)
        src_dir = pool_dir / func_hash[:2] / func_hash[2:]
        dst_dir = output_objects / func_hash[:2] / func_hash[2:]
        if src_dir.exists():
            shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)

    return output_dir


def review_load_state() -> set:
    """
    Load the set of previously reviewed function hashes.

    Returns:
        Set of reviewed function hashes
    """
    mobius_dir = storage_get_mobius_directory()
    state_file = mobius_dir / 'review_state.json'

    if not state_file.exists():
        return set()

    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get('reviewed', []))
    except (json.JSONDecodeError, OSError):
        return set()


def review_save_state(reviewed: set):
    """
    Save the set of reviewed function hashes.

    Args:
        reviewed: Set of reviewed function hashes
    """
    mobius_dir = storage_get_mobius_directory()
    mobius_dir.mkdir(parents=True, exist_ok=True)

    state_file = mobius_dir / 'review_state.json'
    data = {'reviewed': list(reviewed)}

    with open(state_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def command_review(hash_value: str):
    """
    Interactively review a function and its dependencies.

    Reviews functions one at a time starting from lowest-level dependencies.
    Requires explicit acknowledgment for security/correctness.
    Remembers reviewed functions across invocations.

    Args:
        hash_value: Function hash to review
    """
    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Get user's preferred languages
    config = storage_read_config()
    preferred_langs = config['user'].get('languages', ['eng'])

    if not preferred_langs:
        preferred_langs = ['eng']

    # Load previously reviewed functions
    reviewed = review_load_state()

    # Resolve all dependencies (returns list with dependencies first, main function last)
    try:
        all_deps = code_resolve_dependencies(hash_value)
    except ValueError as e:
        print(f"Error resolving dependencies: {e}", file=sys.stderr)
        sys.exit(1)

    # Filter out already reviewed functions
    to_review = [h for h in all_deps if h not in reviewed]

    if not to_review:
        print("All functions in this dependency tree have already been reviewed.")
        print(f"Total reviewed: {len(reviewed)} function(s)")
        print("\nTo reset review state, delete: ~/.local/mobius/review_state.json")
        return

    print("Interactive Function Review")
    print("=" * 80)
    print(f"Functions to review: {len(to_review)}")
    print(f"Already reviewed: {len(all_deps) - len(to_review)}")
    print()
    print("For each function, review the code for security and correctness.")
    print("Press 'y' to approve, 'n' to skip, 'q' to quit.")
    print("=" * 80)
    print()

    for i, current_hash in enumerate(to_review):
        # Detect schema version
        version = code_detect_schema(current_hash)
        if version is None:
            print(f"Warning: Function {current_hash} not found in local pool", file=sys.stderr)
            continue

        # Try to load in user's preferred languages
        loaded = False
        for lang in preferred_langs:
            try:
                normalized_code, name_mapping, alias_mapping, docstring = code_load(current_hash, lang)
                func_name = name_mapping.get('_mobius_v_0', 'unknown')

                # Show function header
                print(f"[{i+1}/{len(to_review)}] Function: {func_name} ({lang})")
                print(f"Hash: {current_hash}")
                print("-" * 80)

                # Denormalize and show code
                normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
                original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)
                print(original_code)
                print("-" * 80)

                # Extract dependencies
                deps = code_extract_dependencies(normalized_code)
                if deps:
                    print(f"Dependencies: {len(deps)}")
                    for dep in deps:
                        status = "✓" if dep in reviewed else "pending"
                        print(f"  - {dep[:12]}... [{status}]")
                else:
                    print("Dependencies: None")

                print()
                loaded = True
                break
            except SystemExit:
                # Language not available, try next one
                continue

        if not loaded:
            print(f"Warning: Function {current_hash} not available in any preferred language", file=sys.stderr)
            print(f"Preferred languages: {', '.join(preferred_langs)}", file=sys.stderr)
            print()
            continue

        # Interactive prompt
        while True:
            try:
                response = input("Approve this function? [y/n/q]: ").strip().lower()
            except EOFError:
                print("\nNon-interactive mode - skipping remaining reviews.")
                review_save_state(reviewed)
                return

            if response == 'y':
                reviewed.add(current_hash)
                review_save_state(reviewed)
                print(f"✓ Function approved and saved to review state.\n")
                break
            elif response == 'n':
                print(f"✗ Function skipped.\n")
                break
            elif response == 'q':
                print(f"\nReview paused. Progress saved ({len(reviewed)} functions reviewed).")
                print("Run the command again to continue from where you left off.")
                return
            else:
                print("Invalid input. Please enter 'y', 'n', or 'q'.")

        print("=" * 80)
        print()

    print("Review complete!")
    print(f"Total reviewed: {len(reviewed)} function(s)")


def command_log():
    """
    Show a git-like commit log of the function pool.

    Lists all functions with metadata (timestamp, author, hash).
    """
    pool_dir = storage_get_pool_directory()

    if not pool_dir.exists():
        print("No functions in pool")
        return

    functions = []

    # Scan for v1 functions (pool/XX/YYY.../object.json)
    if pool_dir.exists():
        for hash_prefix_dir in pool_dir.iterdir():
            if not hash_prefix_dir.is_dir():
                continue

            for func_dir in hash_prefix_dir.iterdir():
                if not func_dir.is_dir():
                    continue

                object_json = func_dir / 'object.json'
                if not object_json.exists():
                    continue

                # Load function metadata
                try:
                    with open(object_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    func_hash = data['hash']
                    metadata = data.get('metadata', {})
                    created = metadata.get('created', 'unknown')
                    author = metadata.get('author', 'unknown')

                    # Get available languages
                    langs = []
                    for item in func_dir.iterdir():
                        if item.is_dir() and len(item.name) == 3:
                            langs.append(item.name)

                    functions.append({
                        'hash': func_hash,
                        'created': created,
                        'author': author,
                        'langs': sorted(langs)
                    })
                except (IOError, json.JSONDecodeError):
                    continue

    # Sort by created timestamp (newest first)
    functions.sort(key=lambda x: x['created'], reverse=True)

    # Display log
    print(f"Function Pool Log ({len(functions)} functions)")
    print("=" * 80)
    print()

    for func in functions:
        langs_str = ', '.join(func['langs']) if func['langs'] else 'none'
        print(f"Hash: {func['hash']}")
        print(f"Date: {func['created']}")
        print(f"Author: {func['author']}")
        print(f"Languages: {langs_str}")
        print()


def command_search(query: List[str]):
    """
    Search and list functions by query.

    Searches in function names, docstrings, and code content.

    Args:
        query: List of search terms
    """
    if not query:
        print("Error: No search query provided", file=sys.stderr)
        sys.exit(1)

    search_terms = [term.lower() for term in query]

    pool_dir = storage_get_pool_directory()

    if not pool_dir.exists():
        print("No functions in pool")
        return

    results = []

    # Scan for v1 functions
    if pool_dir.exists():
        for hash_prefix_dir in pool_dir.iterdir():
            if not hash_prefix_dir.is_dir():
                continue

            for func_dir in hash_prefix_dir.iterdir():
                if not func_dir.is_dir():
                    continue

                object_json = func_dir / 'object.json'
                if not object_json.exists():
                    continue

                # Load function
                try:
                    with open(object_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    func_hash = data['hash']
                    normalized_code = data['normalized_code']

                    # Search in code
                    code_lower = normalized_code.lower()
                    if any(term in code_lower for term in search_terms):
                        # Get available languages and load first available
                        for lang_dir in func_dir.iterdir():
                            if lang_dir.is_dir() and len(lang_dir.name) == 3:
                                lang = lang_dir.name
                                try:
                                    _, name_mapping, _, docstring = code_load(func_hash, lang)
                                    func_name = name_mapping.get('_mobius_v_0', 'unknown')

                                    # Check if search term in function name or docstring
                                    match_in = []
                                    if any(term in func_name.lower() for term in search_terms):
                                        match_in.append('name')
                                    if any(term in docstring.lower() for term in search_terms):
                                        match_in.append('docstring')
                                    if not match_in:
                                        match_in.append('code')

                                    results.append({
                                        'hash': func_hash,
                                        'name': func_name,
                                        'lang': lang,
                                        'docstring': docstring[:100],  # First 100 chars
                                        'match_in': match_in
                                    })
                                    break
                                except SystemExit:
                                    continue
                except (IOError, json.JSONDecodeError):
                    continue

    # Display results
    print(f"Search Results ({len(results)} matches for: {' '.join(query)})")
    print("=" * 80)
    print()

    if not results:
        print("No matches found")
        return

    for result in results:
        match_str = ', '.join(result['match_in'])
        print(f"Name: {result['name']} ({result['lang']})")
        print(f"Hash: {result['hash']}")
        print(f"Match: {match_str}")
        if result['docstring']:
            print(f"Description: {result['docstring']}...")
        print(f"View: mobius.py show {result['hash']}@{result['lang']}")
        print()


def code_strip_mobius_imports(code: str) -> str:
    """
    Strip mobius.pool import statements from code.

    These imports are handled separately by loading dependencies into the namespace.

    Args:
        code: Source code with possible mobius.pool imports

    Returns:
        Code with mobius.pool imports removed
    """
    tree = ast.parse(code)

    # Filter out mobius.pool imports
    new_body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'mobius.pool':
            continue
        new_body.append(node)

    tree.body = new_body
    return ast.unparse(tree)


def code_load_dependencies_recursive(func_hash: str, lang: str, namespace: dict, loaded: set = None):
    """
    Recursively load a function and all its dependencies into a namespace.

    Creates a module-like object for each dependency that can be accessed as:
    object_HASH._mobius_v_0(args) -> alias(args)

    Args:
        func_hash: Actual function hash (without object_ prefix) to load
        lang: Language code
        namespace: Dictionary to populate with loaded functions
        loaded: Set of already loaded hashes (to avoid cycles)

    Returns:
        The function object
    """
    if loaded is None:
        loaded = set()

    if func_hash in loaded:
        # Already loaded, return the existing module (stored with prefix)
        prefixed_name = MOBIUS_IMPORT_PREFIX + func_hash
        return namespace.get(prefixed_name)

    loaded.add(func_hash)

    # Load the function
    try:
        normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, lang)
    except SystemExit:
        print(f"Error: Could not load dependency {func_hash}@{lang}", file=sys.stderr)
        sys.exit(1)

    # First, recursively load all dependencies (deps are actual hashes without prefix)
    deps = code_extract_dependencies(normalized_code)
    for dep_hash in deps:
        code_load_dependencies_recursive(dep_hash, lang, namespace, loaded)

    # Denormalize the code
    normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
    original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

    # Strip mobius imports (dependencies are already in namespace)
    executable_code = code_strip_mobius_imports(original_code)

    # For each alias in alias_mapping, add the dependency function to namespace with that name
    # alias_mapping maps actual_hash -> alias
    for dep_hash, alias in alias_mapping.items():
        prefixed_dep_name = MOBIUS_IMPORT_PREFIX + dep_hash
        if prefixed_dep_name in namespace:
            # The dependency's function is already loaded, make alias point to it
            dep_module = namespace[prefixed_dep_name]
            if hasattr(dep_module, '_mobius_v_0'):
                namespace[alias] = dep_module._mobius_v_0

    # Execute the code in the namespace (dependencies are already loaded)
    try:
        exec(executable_code, namespace)
    except Exception as e:
        print(f"Error executing dependency {func_hash}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Get function name and create a module-like object for this hash
    func_name = name_mapping.get('_mobius_v_0', 'unknown')

    if func_name in namespace:
        # Create a simple namespace object that has _mobius_v_0 attribute
        # Store it under the prefixed name (object_<hash>) for lookup
        class MobiusModule:
            pass

        module = MobiusModule()
        module._mobius_v_0 = namespace[func_name]
        prefixed_name = MOBIUS_IMPORT_PREFIX + func_hash
        namespace[prefixed_name] = module

    return namespace.get(func_name)


def storage_list_languages(func_hash: str) -> list:
    """
    List available languages for a function hash.

    Args:
        func_hash: Function hash (64 hex characters)

    Returns:
        List of language codes available for this function
    """
    pool_dir = storage_get_pool_directory()
    func_dir = pool_dir / func_hash[:2] / func_hash[2:]

    if not func_dir.exists():
        return []

    languages = []
    for item in func_dir.iterdir():
        if item.is_dir() and len(item.name) >= 3 and len(item.name) <= 256:
            languages.append(item.name)

    return sorted(languages)


def command_run(hash_with_lang: str, debug: bool = False, func_args: list = None):
    """
    Execute a function from the pool interactively.

    Args:
        hash_with_lang: Function hash with optional language (e.g., "abc123..." or "abc123...@eng")
                       Language is required when --debug is set, optional otherwise.
        debug: If True, run with debugger (pdb)
        func_args: Arguments to pass to the function (after --)
    """
    if func_args is None:
        func_args = []

    # Parse hash and optional language
    if '@' in hash_with_lang:
        hash_value, lang = hash_with_lang.rsplit('@', 1)
        # Validate language code
        if len(lang) < 3 or len(lang) > 256:
            print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
            sys.exit(1)
    else:
        hash_value = hash_with_lang
        lang = None

    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # If no language provided, find first available
    if lang is None:
        if debug:
            print("Error: Language suffix required when using --debug. Use format: HASH@lang", file=sys.stderr)
            sys.exit(1)

        available_langs = storage_list_languages(hash_value)
        if not available_langs:
            print(f"Error: No language mappings found for function {hash_value}", file=sys.stderr)
            sys.exit(1)
        lang = available_langs[0]  # Use first available language

    # Load function from pool
    try:
        normalized_code, name_mapping, alias_mapping, docstring = code_load(hash_value, lang)
    except SystemExit:
        print(f"Error: Could not load function {hash_value}@{lang}", file=sys.stderr)
        sys.exit(1)

    # Get function name from mapping
    func_name = name_mapping.get('_mobius_v_0', 'unknown_function')

    # Create execution namespace
    namespace = {}

    # First, load all dependencies recursively
    deps = code_extract_dependencies(normalized_code)
    if deps:
        print(f"Loading {len(deps)} dependencies...")
        for dep_hash in deps:
            code_load_dependencies_recursive(dep_hash, lang, namespace, set())
        print()

    # Denormalize to original language
    normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
    original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

    print(f"Running function: {func_name} ({lang})")
    print("=" * 60)
    print(original_code)
    print("=" * 60)
    print()

    # Strip mobius imports (dependencies are already in namespace)
    executable_code = code_strip_mobius_imports(original_code)

    # For each alias in alias_mapping, add the dependency function to namespace with that name
    # alias_mapping maps actual_hash (without prefix) -> alias
    for dep_hash, alias in alias_mapping.items():
        prefixed_dep_name = MOBIUS_IMPORT_PREFIX + dep_hash
        if prefixed_dep_name in namespace:
            # The dependency's function is already loaded, make alias point to it
            dep_module = namespace[prefixed_dep_name]
            if hasattr(dep_module, '_mobius_v_0'):
                namespace[alias] = dep_module._mobius_v_0

    # Execute the code
    try:
        exec(executable_code, namespace)
    except Exception as e:
        print(f"Error executing function: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Get the function object
    if func_name not in namespace:
        print(f"Error: Function '{func_name}' not found in namespace", file=sys.stderr)
        sys.exit(1)

    func = namespace[func_name]

    # If arguments were provided, execute the function directly
    if func_args:
        # Parse arguments - try to convert to appropriate types
        parsed_args = []
        for arg in func_args:
            # Try int
            try:
                parsed_args.append(int(arg))
                continue
            except ValueError:
                pass
            # Try float
            try:
                parsed_args.append(float(arg))
                continue
            except ValueError:
                pass
            # Keep as string
            parsed_args.append(arg)

        print(f"Calling: {func_name}({', '.join(repr(a) for a in parsed_args)})")
        print()
        try:
            result = func(*parsed_args)
            print(f"Result: {result}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    elif debug:
        # Run with debugger
        import pdb
        print("Starting debugger...")
        print(f"The function '{func_name}' is available in the namespace.")
        print(f"Call it with: {func_name}(...)")
        print()
        pdb.set_trace()
    else:
        # Interactive mode
        print(f"Function '{func_name}' is loaded and ready to use.")
        print(f"Call it with: {func_name}(...)")
        print()

        # Start interactive Python shell with the function available
        import code
        code.interact(local=namespace, banner="")


def command_translate(hash_with_lang: str, target_lang: str):
    """
    Add a translation for an existing function.

    This command helps translate a function from one language to another
    by prompting for new variable names and docstring.

    Args:
        hash_with_lang: Function hash with source language (e.g., "abc123...@eng")
        target_lang: Target language code (e.g., "fra", "spa")
    """
    # Parse hash and source language
    if '@' not in hash_with_lang:
        print("Error: Missing language suffix. Use format: HASH@source_lang", file=sys.stderr)
        sys.exit(1)

    hash_value, source_lang = hash_with_lang.rsplit('@', 1)

    # Validate language codes
    if len(source_lang) < 3 or len(source_lang) > 256:
        print(f"Error: Source language code must be 3-256 characters. Got: {source_lang}", file=sys.stderr)
        sys.exit(1)

    if len(target_lang) < 3 or len(target_lang) > 256:
        print(f"Error: Target language code must be 3-256 characters. Got: {target_lang}", file=sys.stderr)
        sys.exit(1)

    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load source function
    try:
        normalized_code, name_mapping_source, alias_mapping_source, docstring_source = code_load(hash_value, source_lang)
    except SystemExit:
        print(f"Error: Could not load function {hash_value}@{source_lang}", file=sys.stderr)
        sys.exit(1)

    # Show source code for reference
    print(f"Source function ({source_lang}):")
    print("=" * 60)
    normalized_code_with_doc = code_replace_docstring(normalized_code, docstring_source)
    source_code = code_denormalize(normalized_code_with_doc, name_mapping_source, alias_mapping_source)
    print(source_code)
    print("=" * 60)
    print()

    # Create target name mapping by prompting user
    print(f"Creating translation to {target_lang}...")
    print()

    name_mapping_target = {}

    # Translate each name
    for norm_name, orig_name in sorted(name_mapping_source.items()):
        while True:
            target_name = input(f"Translate '{orig_name}' ({norm_name}): ").strip()
            if target_name:
                name_mapping_target[norm_name] = target_name
                break
            else:
                print("  Name cannot be empty. Please try again.")

    # Translate docstring
    print()
    print(f"Source docstring:\n{docstring_source}")
    print()
    target_docstring = input(f"Target docstring ({target_lang}): ").strip()

    # Use the same alias mapping (hash-based imports are language-independent)
    alias_mapping_target = alias_mapping_source

    # Optionally add a comment
    comment = input("Optional comment for this translation (press Enter to skip): ").strip()

    # Save the translation
    mapping_hash = mapping_save_v1(hash_value, target_lang, target_docstring, name_mapping_target, alias_mapping_target, comment)

    print(f"Mapping hash: {mapping_hash}")
    print()
    print(f"Translation saved successfully!")
    print(f"View with: mobius.py show {hash_value}@{target_lang}")


def code_add(file_path_with_lang: str, comment: str = ""):
    """
    Add a function to the mobius pool using schema v1.

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
    if len(lang) < 3 or len(lang) > 256:
        print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
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

    # Extract function definition to get @check decorators before normalization
    try:
        function_def, _ = code_extract_definition(tree)
        checks = code_extract_check_decorators(function_def)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Normalize the AST
    try:
        normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping = code_normalize(tree, lang)
    except Exception as e:
        print(f"Error: Failed to normalize AST: {e}", file=sys.stderr)
        sys.exit(1)

    # Verify all mobius imports resolve to objects in the local pool
    pool_dir = storage_get_pool_directory()

    if alias_mapping:
        missing_deps = []
        for dep_hash, alias in alias_mapping.items():
            dep_dir = pool_dir / dep_hash[:2] / dep_hash[2:]
            if not (dep_dir / 'object.json').exists():
                missing_deps.append((dep_hash, alias))

        if missing_deps:
            print("Error: The following mobius imports do not exist in the local pool:", file=sys.stderr)
            for dep_hash, alias in missing_deps:
                print(f"  - {alias} (hash: {dep_hash[:12]}...)", file=sys.stderr)
            print("\nPlease add these functions to the pool first, or pull them from a remote.", file=sys.stderr)
            sys.exit(1)

    # Verify all @check target hashes exist in the local pool
    if checks:
        missing_checks = []
        for check_hash in checks:
            check_dir = pool_dir / check_hash[:2] / check_hash[2:]
            if not (check_dir / 'object.json').exists():
                missing_checks.append(check_hash)

        if missing_checks:
            print("Error: The following @check target functions do not exist in the local pool:", file=sys.stderr)
            for check_hash in missing_checks:
                print(f"  - {check_hash[:12]}...", file=sys.stderr)
            print("\nPlease add these functions to the pool first, or pull them from a remote.", file=sys.stderr)
            sys.exit(1)

    # Compute hash on code WITHOUT docstring (so same logic = same hash regardless of language)
    hash_value = hash_compute(normalized_code_without_docstring)

    # Save to v1 format (store the version WITH docstring for display purposes)
    code_save(hash_value, lang, normalized_code_with_docstring, docstring, name_mapping, alias_mapping, comment, checks=checks)


def code_replace_docstring(code: str, new_docstring: str) -> str:
    """
    Replace the docstring in a function with a new one.
    If new_docstring is empty, remove the docstring.
    """
    tree = ast.parse(code)

    # Find the function definition (sync or async)
    function_def = None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
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


def code_load_v1(hash_value: str) -> Dict[str, any]:
    """
    Load function from mobius directory using schema v1.

    Loads only the object.json file (no language-specific data).

    Args:
        hash_value: Function hash (64-character hex)

    Returns:
        Dictionary with schema_version, hash, normalized_code, metadata
    """
    pool_dir = storage_get_pool_directory()

    # Build path: pool/XX/YYYYYY.../object.json
    func_dir = pool_dir / hash_value[:2] / hash_value[2:]
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
    pool_dir = storage_get_pool_directory()

    # Build path: pool/XX/YYYYYY.../lang/
    func_dir = pool_dir / func_hash[:2] / func_hash[2:]
    lang_dir = func_dir / lang

    # Check if language directory exists
    if not lang_dir.exists():
        return []

    # Scan for mapping directories: lang/ZZ/WWWW.../mapping.json
    mappings = []

    # Iterate through hash prefix directories (ZZ/)
    for hash_prefix_dir in lang_dir.iterdir():
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
    pool_dir = storage_get_pool_directory()

    # Build path: pool/XX/Y.../lang/ZZ/W.../mapping.json
    func_dir = pool_dir / func_hash[:2] / func_hash[2:]
    mapping_dir = func_dir / lang / mapping_hash[:2] / mapping_hash[2:]
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


def code_load(hash_value: str, lang: str, mapping_hash: str = None) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a function from the mobius pool (v1 format only).

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        mapping_hash: Optional mapping hash (64-character hex)

    Returns:
        Tuple of (normalized_code, name_mapping, alias_mapping, docstring)
    """
    # Detect schema version
    version = code_detect_schema(hash_value)

    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load v1 format
    # Load object.json
    func_data = code_load_v1(hash_value)
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


def code_show(hash_with_lang_and_mapping: str):
    """
    Show a function from the mobius pool with mapping selection support.

    Supports three formats:
    - HASH@LANG: Show single mapping, or menu if multiple exist
    - HASH@LANG@MAPPING_HASH: Show specific mapping

    Args:
        hash_with_lang_and_mapping: Function identifier in format HASH@LANG[@MAPPING_HASH]
    """
    # Parse the format
    if '@' not in hash_with_lang_and_mapping:
        print("Error: Missing language suffix. Use format: HASH@lang[@mapping_hash]", file=sys.stderr)
        sys.exit(1)

    parts = hash_with_lang_and_mapping.split('@')
    if len(parts) < 2:
        print("Error: Invalid format. Use format: HASH@lang[@mapping_hash]", file=sys.stderr)
        sys.exit(1)

    hash_value = parts[0]
    lang = parts[1]
    mapping_hash = parts[2] if len(parts) > 2 else None

    # Validate hash format (should be 64 hex characters for SHA256)
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Validate language code (should be 3 characters, ISO 639-3)
    if len(lang) < 3 or len(lang) > 256:
        print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Detect schema version
    version = code_detect_schema(hash_value)
    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Get available mappings for the language
    mappings = mappings_list_v1(hash_value, lang)

    if len(mappings) == 0:
        print(f"Error: No mappings found for language '{lang}'", file=sys.stderr)
        sys.exit(1)

    # Determine which mapping to show
    if mapping_hash is not None:
        # Explicit mapping hash provided
        selected_hash = mapping_hash
    elif len(mappings) == 1:
        # Only one mapping - show it directly
        selected_hash, _ = mappings[0]
    else:
        # Multiple mappings - show selection menu
        print(f"Multiple mappings found for '{lang}'. Please choose one:\n")
        for m_hash, comment in sorted(mappings):
            comment_suffix = f"  # {comment}" if comment else ""
            print(f"mobius.py show {hash_value}@{lang}@{m_hash}{comment_suffix}")
        return

    # Load the selected mapping
    normalized_code, name_mapping, alias_mapping, docstring = code_load(hash_value, lang, mapping_hash=selected_hash)

    # Replace docstring and denormalize
    try:
        normalized_code = code_replace_docstring(normalized_code, docstring)
        original_code = code_denormalize(normalized_code, name_mapping, alias_mapping)
    except Exception as e:
        print(f"Error: Failed to denormalize code: {e}", file=sys.stderr)
        sys.exit(1)

    # Print the code
    print(original_code)


def code_get(hash_with_lang: str):
    """Get a function from the mobius pool (backward compatible with show command)"""
    # Deprecation warning
    print("Warning: 'get' is deprecated. Use 'show' instead for better mapping support.", file=sys.stderr)

    # Parse the hash and language
    if '@' not in hash_with_lang:
        print("Error: Missing language suffix. Use format: HASH@lang", file=sys.stderr)
        sys.exit(1)

    hash_value, lang = hash_with_lang.rsplit('@', 1)

    # Validate language code (should be 3 characters, ISO 639-3)
    if len(lang) < 3 or len(lang) > 256:
        print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Validate hash format (should be 64 hex characters for SHA256)
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load function data from pool
    normalized_code, name_mapping, alias_mapping, docstring = code_load(hash_value, lang)

    # Replace the docstring with the language-specific one
    try:
        normalized_code = code_replace_docstring(normalized_code, docstring)
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


def schema_validate_v1(func_hash: str) -> tuple:
    """
    Validate a v1 function.

    Checks:
    - object.json exists and is valid
    - At least one language mapping exists
    - All mapping files are valid JSON

    Args:
        func_hash: Function hash (64-character hex)

    Returns:
        Tuple of (is_valid, errors) where errors is a list of error messages
    """
    errors = []
    pool_dir = storage_get_pool_directory()

    # Check object.json exists
    func_dir = pool_dir / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    if not object_json.exists():
        errors.append(f"object.json not found for function {func_hash}")
        return False, errors

    # Validate object.json structure
    try:
        with open(object_json, 'r', encoding='utf-8') as f:
            func_data = json.load(f)

        # Check required fields
        required_fields = ['schema_version', 'hash', 'normalized_code', 'metadata']
        for field in required_fields:
            if field not in func_data:
                errors.append(f"Missing required field in object.json: {field}")

        # Verify schema version
        if func_data.get('schema_version') != 1:
            errors.append(f"Invalid schema version: {func_data.get('schema_version')}")

    except (IOError, json.JSONDecodeError) as e:
        errors.append(f"Failed to parse object.json: {e}")
        return False, errors

    # Check that at least one language mapping exists
    if not func_dir.exists():
        errors.append(f"Function directory does not exist: {func_dir}")
        return False, errors

    # Count language directories
    lang_count = 0
    for item in func_dir.iterdir():
        if item.is_dir() and not item.name.endswith('.json'):
            lang_count += 1

    if lang_count == 0:
        errors.append("No language mappings found (no language directories)")

    # If we have errors, return False
    if errors:
        return False, errors

    return True, []


def schema_validate_directory() -> tuple:
    """
    Validate the entire mobius directory structure.

    Checks:
    - Config file exists and is valid JSON
    - Pool directory exists
    - All functions in pool are valid
    - All dependencies are resolvable

    Returns:
        Tuple of (is_valid, errors, stats) where:
        - is_valid: bool indicating overall validity
        - errors: list of error messages
        - stats: dict with validation statistics
    """
    errors = []
    stats = {
        'functions_total': 0,
        'functions_valid': 0,
        'functions_invalid': 0,
        'languages_total': set(),
        'dependencies_missing': []
    }

    mobius_dir = storage_get_mobius_directory()

    # Check if mobius directory exists
    if not mobius_dir.exists():
        errors.append(f"Mobius directory does not exist: {mobius_dir}")
        return False, errors, stats

    # Validate config file
    config_path = mobius_dir / 'config.json'
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            if not isinstance(config, dict):
                errors.append("Config file is not a valid JSON object")
        except json.JSONDecodeError as e:
            errors.append(f"Config file is invalid JSON: {e}")
    else:
        # Config is optional - just note it
        pass

    # Validate pool directory
    pool_dir = storage_get_pool_directory()
    if not pool_dir.exists():
        errors.append(f"Pool directory does not exist: {pool_dir}")
        return False, errors, stats

    # Collect all function hashes and validate each
    all_hashes = set()
    for prefix_dir in pool_dir.iterdir():
        if not prefix_dir.is_dir() or len(prefix_dir.name) != 2:
            continue

        for func_dir in prefix_dir.iterdir():
            if not func_dir.is_dir():
                continue

            # Reconstruct hash
            func_hash = prefix_dir.name + func_dir.name

            # Skip if not a valid hash format
            if len(func_hash) != 64:
                continue
            if not all(c in '0123456789abcdef' for c in func_hash.lower()):
                continue

            all_hashes.add(func_hash)
            stats['functions_total'] += 1

            # Validate individual function
            is_valid, func_errors = schema_validate_v1(func_hash)
            if is_valid:
                stats['functions_valid'] += 1

                # Check for available languages
                for item in func_dir.iterdir():
                    if item.is_dir() and not item.name.startswith('.'):
                        stats['languages_total'].add(item.name)
            else:
                stats['functions_invalid'] += 1
                for err in func_errors:
                    errors.append(f"[{func_hash[:12]}...] {err}")

    # Verify all dependencies are resolvable (only for valid functions)
    for func_hash in all_hashes:
        # Skip if this function had validation errors
        is_valid, _ = schema_validate_v1(func_hash)
        if not is_valid:
            continue

        try:
            func_data = code_load_v1(func_hash)
            normalized_code = func_data['normalized_code']
            deps = code_extract_dependencies(normalized_code)

            for dep in deps:
                if dep not in all_hashes:
                    stats['dependencies_missing'].append((func_hash, dep))
                    errors.append(f"[{func_hash[:12]}...] Missing dependency: {dep[:12]}...")
        except (Exception, SystemExit):
            # Already reported in individual validation
            pass

    # Convert set to count for stats
    stats['languages_total'] = len(stats['languages_total'])

    is_valid = len(errors) == 0
    return is_valid, errors, stats


def command_caller(hash_value: str):
    """
    Find all functions that depend on the given function.

    Scans all functions in the pool and prints `mobius.py show CALLER_HASH`
    for each function that imports the given hash.

    Args:
        hash_value: Function hash (64-character hex) to find callers of
    """
    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Check if function exists
    version = code_detect_schema(hash_value)
    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    pool_dir = storage_get_pool_directory()

    if not pool_dir.exists():
        return

    callers = []

    # Scan for v1 functions (pool/XX/YYY.../object.json)
    if pool_dir.exists():
        for hash_prefix_dir in pool_dir.iterdir():
            if not hash_prefix_dir.is_dir():
                continue

            for func_dir in hash_prefix_dir.iterdir():
                if not func_dir.is_dir():
                    continue

                object_json = func_dir / 'object.json'
                if not object_json.exists():
                    continue

                try:
                    with open(object_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    func_hash = data['hash']
                    normalized_code = data['normalized_code']

                    # Check if this function depends on the target hash
                    deps = code_extract_dependencies(normalized_code)
                    if hash_value in deps:
                        callers.append(func_hash)
                except (IOError, json.JSONDecodeError):
                    continue

    # Print results
    for caller_hash in sorted(callers):
        print(f"mobius.py show {caller_hash}")


def command_check(hash_value: str):
    """
    Find and run all tests for the given function.

    Scans all functions in the pool looking for functions that have
    the given hash in their metadata.checks list. Prints `mobius.py run TEST_HASH`
    for each test function found, or runs them directly.

    Args:
        hash_value: Function hash (64-character hex) to find tests for
    """
    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Check if function exists
    version = code_detect_schema(hash_value)
    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    pool_dir = storage_get_pool_directory()

    if not pool_dir.exists():
        print("No tests found.")
        return

    tests = []

    # Scan for v1 functions (pool/XX/YYY.../object.json)
    for hash_prefix_dir in pool_dir.iterdir():
        if not hash_prefix_dir.is_dir():
            continue

        for func_dir in hash_prefix_dir.iterdir():
            if not func_dir.is_dir():
                continue

            object_json = func_dir / 'object.json'
            if not object_json.exists():
                continue

            try:
                with open(object_json, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                func_hash = data['hash']
                metadata = data.get('metadata', {})
                checks = metadata.get('checks', [])

                # Check if this function tests the target hash
                if hash_value in checks:
                    tests.append(func_hash)
            except (IOError, json.JSONDecodeError):
                continue

    if not tests:
        print("No tests found.")
        return

    # Print results
    for test_hash in sorted(tests):
        print(f"mobius.py run {test_hash}")


def command_refactor(what_hash: str, from_hash: str, to_hash: str):
    """
    Replace a dependency hash with another in a function.

    Creates a new function where all references to from_hash are replaced
    with to_hash. Copies all language mappings, updating alias_mappings
    to use the new hash key.

    Args:
        what_hash: Function hash to modify (64-character hex)
        from_hash: Dependency hash to replace (64-character hex)
        to_hash: New dependency hash (64-character hex)
    """
    # Validate hash formats
    for name, h in [('what', what_hash), ('from', from_hash), ('to', to_hash)]:
        if len(h) != 64 or not all(c in '0123456789abcdef' for c in h.lower()):
            print(f"Error: Invalid {name} hash format. Expected 64 hex characters. Got: {h}", file=sys.stderr)
            sys.exit(1)

    # Check if what function exists
    what_version = code_detect_schema(what_hash)
    if what_version is None:
        print(f"Error: Function not found: {what_hash}", file=sys.stderr)
        sys.exit(1)

    # Check if to function exists
    to_version = code_detect_schema(to_hash)
    if to_version is None:
        print(f"Error: Target function not found: {to_hash}", file=sys.stderr)
        sys.exit(1)

    # Load the function's normalized code (v1 only)
    func_data = code_load_v1(what_hash)
    normalized_code = func_data['normalized_code']
    # Get all languages from v1 directory structure
    pool_dir = storage_get_pool_directory()
    func_dir = pool_dir / 'sha256' / what_hash[:2] / what_hash[2:]
    languages = []
    for item in func_dir.iterdir():
        if item.is_dir() and len(item.name) == 3:
            languages.append(item.name)

    # Check that the function actually depends on from_hash
    deps = code_extract_dependencies(normalized_code)
    if from_hash not in deps:
        print(f"Error: Function {what_hash} does not depend on {from_hash}", file=sys.stderr)
        sys.exit(1)

    # Replace the dependency in the code
    tree = ast.parse(normalized_code)

    class DependencyReplacer(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            if node.module == 'mobius.pool':
                new_names = []
                for alias in node.names:
                    import_name = alias.name
                    # Check if this is the from_hash import
                    if import_name.startswith(MOBIUS_IMPORT_PREFIX):
                        actual_hash = import_name[len(MOBIUS_IMPORT_PREFIX):]
                    else:
                        actual_hash = import_name

                    if actual_hash == from_hash:
                        # Replace with to_hash
                        new_name = MOBIUS_IMPORT_PREFIX + to_hash
                        new_names.append(ast.alias(name=new_name, asname=alias.asname))
                    else:
                        new_names.append(alias)
                node.names = new_names
            return node

        def visit_Attribute(self, node):
            # Transform object_from_hash._mobius_v_0 -> object_to_hash._mobius_v_0
            if (isinstance(node.value, ast.Name) and
                node.attr == '_mobius_v_0'):
                prefixed_name = node.value.id
                if prefixed_name.startswith(MOBIUS_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(MOBIUS_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name

                if actual_hash == from_hash:
                    node.value.id = MOBIUS_IMPORT_PREFIX + to_hash
            self.generic_visit(node)
            return node

    replacer = DependencyReplacer()
    tree = replacer.visit(tree)
    ast.fix_missing_locations(tree)

    new_normalized_code = ast.unparse(tree)

    # Compute new hash (hash is computed on code without docstring)
    # First, extract the code without docstring
    new_tree = ast.parse(new_normalized_code)
    for node in new_tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            _, func_without_docstring = code_extract_docstring(node)
            # Rebuild module without docstring for hashing
            imports = [n for n in new_tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
            module_without_docstring = ast.Module(body=imports + [func_without_docstring], type_ignores=[])
            ast.fix_missing_locations(module_without_docstring)
            code_without_docstring = ast.unparse(module_without_docstring)
            break

    new_hash = hash_compute(code_without_docstring)

    # Create metadata for the new function
    metadata = code_create_metadata()

    # Save the new function (object.json)
    code_save_v1(new_hash, new_normalized_code, metadata)

    # Copy all language mappings from what_hash to new_hash (v1 only)
    for lang in languages:
        mappings = mappings_list_v1(what_hash, lang)
        for mapping_hash, comment in mappings:
            docstring, name_mapping, alias_mapping, comment = mapping_load_v1(what_hash, lang, mapping_hash)

            # Update alias_mapping: replace from_hash key with to_hash
            new_alias_mapping = {}
            for dep_hash, alias in alias_mapping.items():
                if dep_hash == from_hash:
                    new_alias_mapping[to_hash] = alias
                else:
                    new_alias_mapping[dep_hash] = alias

            mapping_save_v1(new_hash, lang, docstring, name_mapping, new_alias_mapping, comment)

    # Print the result command
    print(f"mobius.py show {new_hash}")


# =============================================================================
# Compilation Functions
# =============================================================================

def compile_generate_config(func_hash: str, lang: str, output_name: str, inline_mode: bool = False) -> str:
    """
    Generate PyOxidizer configuration for compiling a function.

    Args:
        func_hash: Function hash to compile
        lang: Language for the function
        output_name: Name for the output executable
        inline_mode: If True, use inline code instead of runtime

    Returns:
        PyOxidizer configuration as a string
    """
    if inline_mode:
        # Simpler config for inline mode - just run __main__.py
        return f'''# PyOxidizer configuration for {output_name}
# Generated by mobius.py compile (inline mode)

def compile_create_dist():
    return default_python_distribution()

def compile_create_exe(dist):
    python_config = dist.make_python_interpreter_config()
    python_config.run_module = "__main__"

    exe = dist.to_python_executable(
        name="{output_name}",
        config=python_config,
    )

    exe.add_python_resource(PythonModuleSource(
        name="__main__",
        source=open("__main__.py").read(),
    ))

    return exe

def compile_embed_resources(exe):
    return exe.to_embedded_resources()

def compile_install(exe):
    files = FileManifest()
    files.add_python_resource(".", exe)
    return files

register_target("dist", compile_create_dist)
register_target("exe", compile_create_exe, depends=["dist"])
register_target("resources", compile_embed_resources, depends=["exe"], default_build_script=True)
register_target("install", compile_install, depends=["exe"], default=True)

resolve_targets()
'''
    else:
        # Debug mode: use runtime for dynamic loading
        return f'''# PyOxidizer configuration for {output_name}
# Generated by mobius.py compile (debug mode)

def compile_create_dist():
    return default_python_distribution()

def compile_create_exe(dist):
    python_config = dist.make_python_interpreter_config()
    python_config.run_command = """
import sys
sys.path.insert(0, '.')

# Import mobius runtime
from mobius_runtime import code_execute

# Execute the function
result = code_execute('{func_hash}', '{lang}', sys.argv[1:])
if result is not None:
    print(result)
"""

    exe = dist.to_python_executable(
        name="{output_name}",
        config=python_config,
    )

    exe.read_package_root(
        path=".",
        packages=["mobius_runtime"],
    )

    return exe

def compile_embed_resources(exe):
    return exe.to_embedded_resources()

def compile_install(exe):
    files = FileManifest()
    files.add_python_resource(".", exe)
    return files

register_target("dist", compile_create_dist)
register_target("exe", compile_create_exe, depends=["dist"])
register_target("resources", compile_embed_resources, depends=["exe"], default_build_script=True)
register_target("install", compile_install, depends=["exe"], default=True)

resolve_targets()
'''


def compile_generate_runtime(func_hash: str, lang: str, output_dir: Path) -> Path:
    """
    Generate the mobius runtime module for the compiled executable.

    Args:
        func_hash: Function hash to compile
        lang: Language for the function
        output_dir: Directory to write runtime to

    Returns:
        Path to the generated runtime module
    """
    runtime_dir = output_dir / 'mobius_runtime'
    runtime_dir.mkdir(parents=True, exist_ok=True)

    # Copy the necessary parts of mobius for runtime
    runtime_code = '''"""
Mobius Runtime - Minimal runtime for compiled functions
"""
import ast
import json
import os
import sys
from pathlib import Path


MOBIUS_IMPORT_PREFIX = "object_"


def storage_get_bundle_directory() -> Path:
    """Get the bundled objects directory."""
    # When compiled, objects are bundled alongside the executable
    exe_dir = Path(sys.executable).parent
    return exe_dir / 'bundle'


def code_load_v1(hash_value: str):
    """Load function from v1 format."""
    bundle_dir = storage_get_bundle_directory()
    func_dir = bundle_dir / 'sha256' / hash_value[:2] / hash_value[2:]
    object_path = func_dir / 'object.json'

    if not object_path.exists():
        raise FileNotFoundError(f"Function not found: {hash_value}")

    with open(object_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def mapping_load_v1(func_hash: str, lang: str, mapping_hash: str):
    """Load mapping from v1 format."""
    bundle_dir = storage_get_bundle_directory()
    mapping_path = (bundle_dir / 'sha256' / func_hash[:2] / func_hash[2:] /
                   lang / 'sha256' / mapping_hash[:2] / mapping_hash[2:] / 'mapping.json')

    with open(mapping_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return (
        data.get('docstring', ''),
        data.get('name_mapping', {}),
        data.get('alias_mapping', {}),
        data.get('comment', '')
    )


def mappings_list_v1(func_hash: str, lang: str):
    """List mappings for a function in a language."""
    bundle_dir = storage_get_bundle_directory()
    lang_dir = bundle_dir / 'sha256' / func_hash[:2] / func_hash[2:] / lang / 'sha256'

    if not lang_dir.exists():
        return []

    mappings = []
    for hash_dir in lang_dir.iterdir():
        if hash_dir.is_dir():
            for mapping_dir in hash_dir.iterdir():
                if mapping_dir.is_dir():
                    mapping_path = mapping_dir / 'mapping.json'
                    if mapping_path.exists():
                        with open(mapping_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        mapping_hash = hash_dir.name + mapping_dir.name
                        mappings.append((mapping_hash, data.get('comment', '')))

    return mappings


def code_load(hash_value: str, lang: str, mapping_hash: str = None):
    """Load function with language mapping."""
    func_data = code_load_v1(hash_value)
    normalized_code = func_data['normalized_code']

    mappings = mappings_list_v1(hash_value, lang)
    if not mappings:
        raise ValueError(f"No mapping found for language: {lang}")

    if mapping_hash:
        selected_hash = mapping_hash
    else:
        selected_hash = mappings[0][0]

    docstring, name_mapping, alias_mapping, comment = mapping_load_v1(hash_value, lang, selected_hash)

    return normalized_code, name_mapping, alias_mapping, docstring


def code_denormalize(normalized_code: str, name_mapping: dict, alias_mapping: dict) -> str:
    """Denormalize code by applying reverse name mappings."""
    tree = ast.parse(normalized_code)

    hash_to_alias = dict(alias_mapping)

    class Denormalizer(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id in name_mapping:
                node.id = name_mapping[node.id]
            return node

        def visit_arg(self, node):
            if node.arg in name_mapping:
                node.arg = name_mapping[node.arg]
            return node

        def visit_FunctionDef(self, node):
            if node.name in name_mapping:
                node.name = name_mapping[node.name]
            self.generic_visit(node)
            return node

        def visit_AsyncFunctionDef(self, node):
            if node.name in name_mapping:
                node.name = name_mapping[node.name]
            self.generic_visit(node)
            return node

    denormalizer = Denormalizer()
    tree = denormalizer.visit(tree)

    return ast.unparse(tree)


def code_execute(func_hash: str, lang: str, args: list):
    """Execute a function from the bundle."""
    normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, lang)
    denormalized_code = code_denormalize(normalized_code, name_mapping, alias_mapping)

    # Execute the function
    namespace = {}
    exec(denormalized_code, namespace)

    # Find the function in namespace
    func_name = name_mapping.get('_mobius_v_0', '_mobius_v_0')
    func = namespace.get(func_name)

    if func is None:
        raise ValueError(f"Function {func_name} not found in code")

    # Call the function with provided arguments
    if args:
        # Try to convert arguments to appropriate types
        converted_args = []
        for arg in args:
            try:
                # Try int first
                converted_args.append(int(arg))
            except ValueError:
                try:
                    # Try float
                    converted_args.append(float(arg))
                except ValueError:
                    # Keep as string
                    converted_args.append(arg)
        return func(*converted_args)
    else:
        return func()
'''

    # Write __init__.py
    init_path = runtime_dir / '__init__.py'
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write(runtime_code)

    return runtime_dir


def compile_generate_python(func_hash: str, lang: str = None, debug_mode: bool = False) -> str:
    """
    Generate a single Python file that includes all dependencies.

    Args:
        func_hash: Function hash to compile
        lang: Language for the function (required if debug_mode=True)
        debug_mode: If True, use human-readable names (requires lang and all translations)

    Returns:
        Python source code as a string

    Raises:
        ValueError: If debug_mode and lang is None or any dependency is missing the requested language
    """
    if debug_mode and lang is None:
        raise ValueError("debug_mode requires lang parameter")
    # Resolve all dependencies
    deps = code_resolve_dependencies(func_hash)

    if debug_mode:
        # Debug mode: check that all dependencies have the requested language available
        missing_lang = []
        for dep_hash in deps:
            mappings = mappings_list_v1(dep_hash, lang)
            if not mappings:
                available_langs = storage_list_languages(dep_hash)
                missing_lang.append((dep_hash, available_langs))

        if missing_lang:
            error_lines = [f"The following functions are not available in '{lang}':"]
            for dep_hash, available in missing_lang:
                if available:
                    error_lines.append(f"  - {dep_hash[:16]}... (available: {', '.join(available)})")
                else:
                    error_lines.append(f"  - {dep_hash[:16]}... (no languages available)")
            error_lines.append("")
            error_lines.append("Please add translations for these functions first:")
            for dep_hash, available in missing_lang:
                if available:
                    error_lines.append(f"  python3 mobius.py translate {dep_hash}@{available[0]} {lang}")
            raise ValueError("\n".join(error_lines))

    # Load all functions
    functions = []
    # Create a mapping from hash to unique function name for normal mode
    hash_to_func_name = {}
    for dep_hash in deps:
        hash_to_func_name[dep_hash] = f'_mobius_{dep_hash[:8]}'

    for dep_hash in deps:
        func_data = code_load_v1(dep_hash)
        normalized_code = func_data['normalized_code']

        if debug_mode:
            # Debug mode: denormalize to human-readable names
            mappings = mappings_list_v1(dep_hash, lang)
            mapping_hash = mappings[0][0]
            docstring, name_mapping, alias_mapping, _ = mapping_load_v1(dep_hash, lang, mapping_hash)

            # Denormalize the code
            normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
            code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)
            func_name = name_mapping.get('_mobius_v_0', '_mobius_v_0')
        else:
            # Normal mode: use normalized code with unique function names
            code = normalized_code
            func_name = hash_to_func_name[dep_hash]

            # Rename the function from _mobius_v_0 to unique name
            code = code.replace('def _mobius_v_0(', f'def {func_name}(', 1)

            # Replace calls to other mobius functions with their unique names
            for other_hash, other_name in hash_to_func_name.items():
                # Replace object_HASH._mobius_v_0 with the unique function name
                code = code.replace(f'object_{other_hash}._mobius_v_0', other_name)

        # Strip mobius imports - dependencies will be included inline
        code = code_strip_mobius_imports(code)

        functions.append({
            'hash': dep_hash,
            'code': code,
            'func_name': func_name,
        })

    # Build the Python file
    lines = ['#!/usr/bin/env python3', '"""', f'Compiled mobius function: {func_hash}', '"""', '']

    # Collect all standard imports from all functions
    all_imports = set()
    for func in functions:
        tree = ast.parse(func['code'])
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                all_imports.add(ast.unparse(node))

    # Add imports at the top
    for imp in sorted(all_imports):
        lines.append(imp)
    if all_imports:
        lines.append('')

    # Add each function (without imports)
    for func in functions:
        # Parse and extract just the function definition
        tree = ast.parse(func['code'])
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                lines.append('')
                lines.append(ast.unparse(node))
                lines.append('')

    # Add main entry point
    main_func = functions[-1]  # The last one is the main function (root of dependency tree)
    lines.append('')
    lines.append('if __name__ == "__main__":')
    lines.append('    import sys')
    lines.append(f'    # Entry point: {main_func["func_name"]}')
    lines.append('    if len(sys.argv) > 1:')
    lines.append('        args = []')
    lines.append('        for arg in sys.argv[1:]:')
    lines.append('            try:')
    lines.append('                args.append(int(arg))')
    lines.append('            except ValueError:')
    lines.append('                try:')
    lines.append('                    args.append(float(arg))')
    lines.append('                except ValueError:')
    lines.append('                    args.append(arg)')
    lines.append(f'        result = {main_func["func_name"]}(*args)')
    lines.append('        print(result)')
    lines.append('    else:')
    lines.append(f'        print("Usage: python {{sys.argv[0]}} [args...]")')
    lines.append(f'        print("Available function: {main_func["func_name"]}")')
    lines.append('')

    return '\n'.join(lines)


def command_compile(hash_with_lang: str, python_mode: bool = False, debug_mode: bool = False):
    """
    Compile a function into a standalone executable or Python file.

    Args:
        hash_with_lang: Function hash, optionally with language suffix (HASH or HASH@lang)
        python_mode: If True, generate a single Python file instead of native executable
        debug_mode: If True, use human-readable names (requires @lang and all translations)
    """
    import shutil
    import platform

    # Parse the hash and optional language
    if '@' in hash_with_lang:
        func_hash, lang = hash_with_lang.rsplit('@', 1)
        # Validate language code
        if len(lang) < 3 or len(lang) > 256:
            print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
            sys.exit(1)
    else:
        func_hash = hash_with_lang
        lang = None

    # Debug mode requires language
    if debug_mode and lang is None:
        print("Error: --debug requires language suffix. Use format: HASH@lang", file=sys.stderr)
        sys.exit(1)

    # Validate hash format
    if len(func_hash) != 64 or not all(c in '0123456789abcdef' for c in func_hash.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {func_hash}", file=sys.stderr)
        sys.exit(1)

    # Check if function exists
    version = code_detect_schema(func_hash)
    if version is None:
        print(f"Error: Function not found: {func_hash}", file=sys.stderr)
        sys.exit(1)

    if lang:
        print(f"Compiling function {func_hash[:8]}...@{lang}")
    else:
        print(f"Compiling function {func_hash[:8]}...")

    # Resolve dependencies
    print("Resolving dependencies...")
    try:
        deps = code_resolve_dependencies(func_hash)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Found {len(deps)} function(s) to bundle")

    if python_mode:
        # Generate single Python file
        print("Generating Python file...")
        output_path = Path('main.py')

        try:
            python_code = compile_generate_python(func_hash, lang, debug_mode=debug_mode)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(python_code)
            print(f"Python file created: {output_path}")
            print(f"Run with: python3 {output_path} [args...]")
        except Exception as e:
            print(f"Error generating Python file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Native executable mode - use PyOxidizer
        # Determine default output name
        if platform.system() == 'Windows':
            output_name = 'a.out.exe'
        else:
            output_name = 'a.out'

        # Check if PyOxidizer is available
        result = subprocess.run(['pyoxidizer', '--version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("Error: PyOxidizer not found. Please install it first:", file=sys.stderr)
            print("  pip install pyoxidizer", file=sys.stderr)
            print("  or: cargo install pyoxidizer", file=sys.stderr)
            print("\nAlternatively, use --python flag to generate a Python file.", file=sys.stderr)
            sys.exit(1)

        # Create build directory
        build_dir = Path(f'.mobius_build_{func_hash[:8]}')
        build_dir.mkdir(parents=True, exist_ok=True)

        try:
            if debug_mode:
                # Debug mode: Bundle pool structure and use runtime for dynamic loading
                print("Bundling functions...")
                bundle_dir = build_dir / 'bundle'
                code_bundle_dependencies(deps, bundle_dir)

                # Generate runtime
                print("Generating runtime...")
                compile_generate_runtime(func_hash, lang, build_dir)
            else:
                # Normal mode: Generate inline Python code (simpler, no runtime needed)
                print("Generating inline Python code...")
                python_code = compile_generate_python(func_hash, lang, debug_mode=False)
                main_path = build_dir / '__main__.py'
                with open(main_path, 'w', encoding='utf-8') as f:
                    f.write(python_code)

            # Generate PyOxidizer config
            print("Generating PyOxidizer configuration...")
            config = compile_generate_config(func_hash, lang, output_name, inline_mode=not debug_mode)

            config_path = build_dir / 'pyoxidizer.bzl'
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(config)

            # Build with PyOxidizer
            print("Building executable...")
            result = subprocess.run(
                ['pyoxidizer', 'build', '--release'],
                cwd=str(build_dir),
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                print(f"Error: PyOxidizer build failed:", file=sys.stderr)
                print(result.stderr, file=sys.stderr)
                sys.exit(1)

            # Find the built executable
            build_output = build_dir / 'build'
            exe_found = False
            for exe in build_output.rglob(output_name):
                if exe.is_file():
                    final_path = Path(output_name)
                    shutil.copy2(exe, final_path)
                    exe_found = True
                    print(f"Executable created: {final_path}")
                    break

            if not exe_found:
                print("Warning: Could not find built executable")
                print(f"Build output is in: {build_dir}")

        finally:
            # Optionally clean up build directory
            pass  # Keep for debugging; user can delete manually

    print("Compilation complete!")


def command_fork(hash_with_lang: str):
    """
    Fork a function to create a modified version with parent lineage tracking.

    Opens the function in an editor, allows modifications, and saves as a new
    function with the original hash recorded as parent in metadata.

    Args:
        hash_with_lang: Function hash with language suffix (HASH@lang)
    """
    import tempfile
    import subprocess

    # Parse the hash and language
    if '@' not in hash_with_lang:
        print("Error: Missing language suffix. Use format: HASH@lang", file=sys.stderr)
        sys.exit(1)

    func_hash, lang = hash_with_lang.rsplit('@', 1)

    # Validate hash format
    if len(func_hash) != 64 or not all(c in '0123456789abcdef' for c in func_hash.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {func_hash}", file=sys.stderr)
        sys.exit(1)

    # Validate language code
    if len(lang) < 3 or len(lang) > 256:
        print(f"Error: Language code must be 3-256 characters. Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Check if function exists
    version = code_detect_schema(func_hash)
    if version is None:
        print(f"Error: Function not found: {func_hash}", file=sys.stderr)
        sys.exit(1)

    # Load the function
    try:
        normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, lang)
    except SystemExit:
        print(f"Error: Could not load function {func_hash}@{lang}", file=sys.stderr)
        sys.exit(1)

    # Denormalize for editing
    normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
    original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

    # Get editor from environment
    editor = os.environ.get('EDITOR', os.environ.get('VISUAL', 'nano'))

    # Create a temporary file with the code
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
        f.write(original_code)
        temp_path = f.name

    try:
        # Open in editor
        print(f"Opening {func_hash[:8]}...@{lang} in {editor}...")
        print("Make your changes and save the file to create a fork.")
        print()

        result = subprocess.run([editor, temp_path])
        if result.returncode != 0:
            print(f"Error: Editor exited with code {result.returncode}", file=sys.stderr)
            sys.exit(1)

        # Read the edited code
        with open(temp_path, 'r', encoding='utf-8') as f:
            edited_code = f.read()

        # Check if code was modified
        if edited_code.strip() == original_code.strip():
            print("No changes detected. Fork cancelled.")
            sys.exit(0)

        # Parse the edited code
        try:
            tree = ast.parse(edited_code)
        except SyntaxError as e:
            print(f"Error: Syntax error in edited code: {e}", file=sys.stderr)
            sys.exit(1)

        # Normalize the new code
        try:
            new_normalized_with_doc, new_normalized_without_doc, new_docstring, new_name_mapping, new_alias_mapping = code_normalize(tree, lang)
        except Exception as e:
            print(f"Error: Failed to normalize code: {e}", file=sys.stderr)
            sys.exit(1)

        # Compute new hash
        new_hash = hash_compute(new_normalized_without_doc)

        # Check if hash is the same (logic unchanged despite surface changes)
        if new_hash == func_hash:
            print("Warning: The logic hasn't changed (same hash). This will add a new mapping to the existing function.")
            confirm = input("Continue? [y/N]: ").strip().lower()
            if confirm != 'y':
                print("Fork cancelled.")
                sys.exit(0)
            # Save as new mapping with parent reference in comment
            comment = f"Forked from {func_hash[:8]}...@{lang}"
            mapping_save_v1(new_hash, lang, new_docstring, new_name_mapping, new_alias_mapping, comment)
            print(f"\nNew mapping saved to existing function: {new_hash}@{lang}")
        else:
            # Save as new function with parent lineage
            code_save(new_hash, lang, new_normalized_with_doc, new_docstring, new_name_mapping, new_alias_mapping,
                      comment=f"Forked from {func_hash[:8]}...", parent=func_hash)
            print(f"\nForked function saved: {new_hash}")
            print(f"Parent: {func_hash}")

        print(f"View with: mobius.py show {new_hash}@{lang}")

    finally:
        # Clean up temp file
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description='mobius - Function pool manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize mobius directory and config')

    # Whoami command
    whoami_parser = subparsers.add_parser('whoami', help='Get or set user configuration')
    whoami_parser.add_argument('subcommand', choices=['name', 'email', 'public-key', 'language'],
                               help='Configuration field to get/set')
    whoami_parser.add_argument('value', nargs='*', help='New value(s) to set (omit to get current value)')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a function to the pool')
    add_parser.add_argument('file', help='Path to Python file with @lang suffix (e.g., file.py@eng)')
    add_parser.add_argument('--comment', default='', help='Optional comment explaining this mapping variant')

    # Get command (backward compatibility)
    get_parser = subparsers.add_parser('get', help='Get a function from the pool')
    get_parser.add_argument('hash', help='Function hash with @lang suffix (e.g., abc123...@eng)')

    # Show command (improved version of get with mapping selection)
    show_parser = subparsers.add_parser('show', help='Show a function with mapping selection support')
    show_parser.add_argument('hash', help='Function hash with @lang[@mapping_hash] (e.g., abc123...@eng or abc123...@eng@xyz789...)')

    # Translate command
    translate_parser = subparsers.add_parser('translate', help='Add translation for existing function')
    translate_parser.add_argument('hash', help='Function hash with source language (e.g., abc123...@eng)')
    translate_parser.add_argument('target_lang', help='Target language code (e.g., fra, spa)')

    # Run command
    run_parser = subparsers.add_parser('run', help='Execute function interactively')
    run_parser.add_argument('hash', help='Function hash with language (e.g., abc123...@eng)')
    run_parser.add_argument('--debug', action='store_true', help='Run with debugger (pdb)')
    run_parser.add_argument('func_args', nargs='*', help='Arguments to pass to function (after --)')

    # Review command
    review_parser = subparsers.add_parser('review', help='Recursively review function and dependencies')
    review_parser.add_argument('hash', help='Function hash to review')

    # Log command
    log_parser = subparsers.add_parser('log', help='Show git-like commit log of pool')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search and list functions by query')
    search_parser.add_argument('query', nargs='+', help='Search terms')

    # Remote command
    remote_parser = subparsers.add_parser('remote', help='Manage remote repositories')
    remote_subparsers = remote_parser.add_subparsers(dest='remote_command', help='Remote subcommands')

    # Remote add
    remote_add_parser = remote_subparsers.add_parser('add', help='Add remote repository')
    remote_add_parser.add_argument('name', help='Remote name')
    remote_add_parser.add_argument('url', help='Remote URL (http://, https://, or file://)')

    # Remote remove
    remote_remove_parser = remote_subparsers.add_parser('remove', help='Remove remote repository')
    remote_remove_parser.add_argument('name', help='Remote name to remove')

    # Remote list
    remote_list_parser = remote_subparsers.add_parser('list', help='List configured remotes')

    # Remote pull
    remote_pull_parser = remote_subparsers.add_parser('pull', help='Fetch functions from remote')
    remote_pull_parser.add_argument('name', help='Remote name to pull from')

    # Remote push
    remote_push_parser = remote_subparsers.add_parser('push', help='Publish functions to remote')
    remote_push_parser.add_argument('name', help='Remote name to push to')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate function or entire mobius directory')
    validate_parser.add_argument('hash', nargs='?', help='Function hash to validate (omit for whole directory)')
    validate_parser.add_argument('--all', '-a', action='store_true',
                                 help='Validate entire mobius directory including pool and config')

    # Caller command
    caller_parser = subparsers.add_parser('caller', help='Find functions that depend on a given function')
    caller_parser.add_argument('hash', help='Function hash to find callers of')

    # Check command
    check_parser = subparsers.add_parser('check', help='Find and run tests for a function')
    check_parser.add_argument('hash', help='Function hash to find tests for')

    # Refactor command
    refactor_parser = subparsers.add_parser('refactor', help='Replace a dependency in a function')
    refactor_parser.add_argument('what', help='Function hash to modify')
    refactor_parser.add_argument('from_hash', metavar='from', help='Dependency hash to replace')
    refactor_parser.add_argument('to_hash', metavar='to', help='New dependency hash')

    # Compile command
    compile_parser = subparsers.add_parser('compile', help='Compile function to standalone executable')
    compile_parser.add_argument('hash', help='Function hash (HASH or HASH@lang). @lang required with --debug')
    compile_parser.add_argument('--python', action='store_true',
                                help='Produce a single Python file instead of native executable (default output: main.py)')
    compile_parser.add_argument('--debug', action='store_true',
                                help='Use human-readable names (requires HASH@lang and all translations)')

    # Fork command
    fork_parser = subparsers.add_parser('fork', help='Fork a function to create a modified version with lineage tracking')
    fork_parser.add_argument('hash', help='Function hash with language (e.g., abc123...@eng)')

    args = parser.parse_args()

    if args.command == 'init':
        command_init()
    elif args.command == 'whoami':
        command_whoami(args.subcommand, args.value)
    elif args.command == 'add':
        code_add(args.file, args.comment)
    elif args.command == 'get':
        code_get(args.hash)
    elif args.command == 'show':
        code_show(args.hash)
    elif args.command == 'translate':
        command_translate(args.hash, args.target_lang)
    elif args.command == 'run':
        command_run(args.hash, debug=args.debug, func_args=args.func_args)
    elif args.command == 'review':
        command_review(args.hash)
    elif args.command == 'log':
        command_log()
    elif args.command == 'search':
        command_search(args.query)
    elif args.command == 'remote':
        if args.remote_command == 'add':
            command_remote_add(args.name, args.url)
        elif args.remote_command == 'remove':
            command_remote_remove(args.name)
        elif args.remote_command == 'list':
            command_remote_list()
        elif args.remote_command == 'pull':
            command_remote_pull(args.name)
        elif args.remote_command == 'push':
            command_remote_push(args.name)
        else:
            remote_parser.print_help()
    elif args.command == 'validate':
        if args.all or not args.hash:
            # Validate entire directory
            is_valid, errors, stats = schema_validate_directory()
            print("Mobius Directory Validation")
            print("=" * 60)
            print(f"Functions total:   {stats['functions_total']}")
            print(f"Functions valid:   {stats['functions_valid']}")
            print(f"Functions invalid: {stats['functions_invalid']}")
            print(f"Languages found:   {stats['languages_total']}")
            print(f"Missing deps:      {len(stats['dependencies_missing'])}")
            print()

            if is_valid:
                print("✓ Mobius directory is valid")
            else:
                print("✗ Validation errors found:", file=sys.stderr)
                for error in errors[:20]:  # Limit to first 20 errors
                    print(f"  - {error}", file=sys.stderr)
                if len(errors) > 20:
                    print(f"  ... and {len(errors) - 20} more errors", file=sys.stderr)
                sys.exit(1)
        else:
            # Validate single function
            is_valid, errors = schema_validate_v1(args.hash)
            if is_valid:
                print(f"✓ Function {args.hash} is valid")
            else:
                print(f"✗ Function {args.hash} is invalid:", file=sys.stderr)
                for error in errors:
                    print(f"  - {error}", file=sys.stderr)
                sys.exit(1)
    elif args.command == 'caller':
        command_caller(args.hash)
    elif args.command == 'check':
        command_check(args.hash)
    elif args.command == 'refactor':
        command_refactor(args.what, args.from_hash, args.to_hash)
    elif args.command == 'compile':
        command_compile(args.hash, python_mode=args.python, debug_mode=args.debug)
    elif args.command == 'fork':
        command_fork(args.hash)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
