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

# Prefix for ouverture.pool imports to ensure valid Python identifiers
# SHA256 hashes can start with digits (0-9), which are invalid as Python identifiers
# By prefixing with "object_", we ensure all import names are valid
OUVERTURE_IMPORT_PREFIX = "object_"


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
    alias_mapping maps: actual_function_hash (without prefix) -> alias_in_lang

    Input format expected:
        from ouverture.pool import object_c0ff33 as kawa

    Output:
        - import becomes: from ouverture.pool import object_c0ff33
        - alias_mapping stores: {"c0ff33...": "kawa"} (actual hash without object_ prefix)
    """
    new_imports = []
    alias_mapping = {}

    for imp in imports:
        if isinstance(imp, ast.ImportFrom) and imp.module == 'ouverture.pool':
            # Rewrite: from ouverture.pool import object_c0ffeebad as kawa
            # To: from ouverture.pool import object_c0ffeebad
            new_names = []
            for alias in imp.names:
                import_name = alias.name  # e.g., "object_c0ff33..."

                # Extract actual hash by stripping the prefix
                if import_name.startswith(OUVERTURE_IMPORT_PREFIX):
                    actual_hash = import_name[len(OUVERTURE_IMPORT_PREFIX):]
                else:
                    # Backward compatibility: no prefix (shouldn't happen in new code)
                    actual_hash = import_name

                # Track the alias mapping using actual hash
                if alias.asname:
                    alias_mapping[actual_hash] = alias.asname

                # Create new import without alias (but keep object_ prefix in import name)
                new_names.append(ast.alias(name=import_name, asname=None))

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
    E.g., kawa(...) becomes object_c0ffeebad._ouverture_v_0(...)

    alias_mapping maps actual hash (without prefix) -> alias name
    The replacement uses object_<hash> to match the import name.
    """
    class OuvertureCallReplacer(ast.NodeTransformer):
        def visit_Name(self, node):
            # If this name is an alias for an ouverture function
            for func_hash, alias in alias_mapping.items():
                if node.id == alias:
                    # Replace with object_c0ffeebad._ouverture_v_0
                    # Use prefixed name to match the import statement
                    prefixed_name = OUVERTURE_IMPORT_PREFIX + func_hash
                    return ast.Attribute(
                        value=ast.Name(id=prefixed_name, ctx=ast.Load()),
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


def config_get_path() -> Path:
    """
    Get the path to the config file.
    Config is stored in ~/.config/ouverture/config.json (XDG Base Directory spec)
    """
    home = os.environ.get('HOME', os.path.expanduser('~'))
    config_dir = Path(home) / '.config' / 'ouverture'
    return config_dir / 'config.json'


def config_read() -> Dict[str, any]:
    """
    Read the configuration file.
    Returns default config if file doesn't exist.
    """
    config_path = config_get_path()

    if not config_path.exists():
        return {
            'user': {
                'username': '',
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


def config_write(config: Dict[str, any]):
    """
    Write the configuration file.
    """
    config_path = config_get_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"Error: Failed to write config file: {e}", file=sys.stderr)
        sys.exit(1)


def command_init():
    """
    Initialize ouverture directory and config file.
    """
    # Create ouverture directory
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'
    objects_dir.mkdir(parents=True, exist_ok=True)

    # Create config file with defaults
    config_path = config_get_path()
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
        config_write(config)
        print(f"Created config file: {config_path}")

    print(f"Initialized ouverture directory: {ouverture_dir}")


def command_whoami(subcommand: str, value: list = None):
    """
    Get or set user configuration.

    Args:
        subcommand: One of 'username', 'email', 'public-key', 'language'
        value: New value(s) to set (None to get current value)
    """
    config = config_read()

    # Map CLI subcommand to config key
    key_map = {
        'username': 'username',
        'email': 'email',
        'public-key': 'public_key',
        'language': 'languages'
    }

    if subcommand not in key_map:
        print(f"Error: Unknown subcommand: {subcommand}", file=sys.stderr)
        print("Valid subcommands: username, email, public-key, language", file=sys.stderr)
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

        config_write(config)

        if subcommand == 'language':
            print(f"Set {subcommand}: {' '.join(value)}")
        else:
            print(f"Set {subcommand}: {value[0]}")


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

        def visit_Attribute(self, node):
            # Replace object_c0ffeebad._ouverture_v_0(...) with alias(...)
            if (isinstance(node.value, ast.Name) and
                node.attr == '_ouverture_v_0'):
                prefixed_name = node.value.id
                # Strip object_ prefix to get actual hash
                if prefixed_name.startswith(OUVERTURE_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(OUVERTURE_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name  # Backward compatibility

                if actual_hash in hash_to_alias:
                    # Return just the alias name
                    return ast.Name(id=hash_to_alias[actual_hash], ctx=node.ctx)
            self.generic_visit(node)
            return node

        def visit_ImportFrom(self, node):
            # Add aliases back to 'from ouverture.pool import object_X'
            if node.module == 'ouverture.pool':
                node.module = 'ouverture.pool'
                # Add aliases back
                new_names = []
                for alias_node in node.names:
                    import_name = alias_node.name  # e.g., "object_c0ff33..."

                    # Strip object_ prefix to get actual hash
                    if import_name.startswith(OUVERTURE_IMPORT_PREFIX):
                        actual_hash = import_name[len(OUVERTURE_IMPORT_PREFIX):]
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


def command_remote_add(name: str, url: str):
    """
    Add a remote repository.

    Args:
        name: Remote name
        url: Remote URL (HTTP/HTTPS or file://)
    """
    config = config_read()

    if name in config['remotes']:
        print(f"Error: Remote '{name}' already exists", file=sys.stderr)
        sys.exit(1)

    # Validate URL format
    if not (url.startswith('http://') or url.startswith('https://') or url.startswith('file://')):
        print(f"Error: Invalid URL format. Must start with http://, https://, or file://", file=sys.stderr)
        sys.exit(1)

    config['remotes'][name] = {
        'url': url
    }

    config_write(config)
    print(f"Added remote '{name}': {url}")


def command_remote_remove(name: str):
    """
    Remove a remote repository.

    Args:
        name: Remote name to remove
    """
    config = config_read()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    del config['remotes'][name]
    config_write(config)
    print(f"Removed remote '{name}'")


def command_remote_list():
    """
    List all configured remotes.
    """
    config = config_read()

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
    config = config_read()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    remote = config['remotes'][name]
    url = remote['url']

    print(f"Pulling from remote '{name}': {url}")
    print()

    # TODO: Implement actual network operations
    # For file:// URLs, we could copy from local filesystem
    # For http:// and https:// URLs, we would need a server API

    if url.startswith('file://'):
        # Local file system remote
        import shutil
        remote_path = Path(url[7:])  # Remove file:// prefix

        if not remote_path.exists():
            print(f"Error: Remote path does not exist: {remote_path}", file=sys.stderr)
            sys.exit(1)

        # Copy functions from remote to local pool
        local_ouverture = directory_get_ouverture()
        local_objects = local_ouverture / 'objects'
        remote_objects = remote_path / 'objects'

        if not remote_objects.exists():
            print(f"Error: Remote objects directory not found: {remote_objects}", file=sys.stderr)
            sys.exit(1)

        # Copy all objects
        pulled_count = 0
        for item in remote_objects.rglob('*.json'):
            # Compute relative path
            rel_path = item.relative_to(remote_objects)
            local_item = local_objects / rel_path

            # Only copy if doesn't exist locally
            if not local_item.exists():
                local_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, local_item)
                pulled_count += 1

        print(f"Pulled {pulled_count} new functions from '{name}'")
    else:
        # HTTP/HTTPS remote
        print("Error: HTTP/HTTPS remotes not yet implemented", file=sys.stderr)
        print("TODO: Implement REST API client for pulling functions", file=sys.stderr)
        sys.exit(1)


def command_remote_push(name: str):
    """
    Publish functions to a remote repository.

    Args:
        name: Remote name to push to
    """
    config = config_read()

    if name not in config['remotes']:
        print(f"Error: Remote '{name}' not found", file=sys.stderr)
        sys.exit(1)

    remote = config['remotes'][name]
    url = remote['url']

    print(f"Pushing to remote '{name}': {url}")
    print()

    # TODO: Implement actual network operations
    # For file:// URLs, we could copy to local filesystem
    # For http:// and https:// URLs, we would need a server API

    if url.startswith('file://'):
        # Local file system remote
        import shutil
        remote_path = Path(url[7:])  # Remove file:// prefix

        # Create remote directory if it doesn't exist
        remote_path.mkdir(parents=True, exist_ok=True)

        # Copy functions from local pool to remote
        local_ouverture = directory_get_ouverture()
        local_objects = local_ouverture / 'objects'
        remote_objects = remote_path / 'objects'

        if not local_objects.exists():
            print("Error: Local objects directory not found", file=sys.stderr)
            sys.exit(1)

        # Copy all objects
        pushed_count = 0
        for item in local_objects.rglob('*.json'):
            # Compute relative path
            rel_path = item.relative_to(local_objects)
            remote_item = remote_objects / rel_path

            # Only copy if doesn't exist remotely
            if not remote_item.exists():
                remote_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, remote_item)
                pushed_count += 1

        print(f"Pushed {pushed_count} new functions to '{name}'")
    else:
        # HTTP/HTTPS remote
        print("Error: HTTP/HTTPS remotes not yet implemented", file=sys.stderr)
        print("TODO: Implement REST API client for pushing functions", file=sys.stderr)
        sys.exit(1)


def dependencies_extract(normalized_code: str) -> List[str]:
    """
    Extract ouverture dependencies from normalized code.

    Returns:
        List of actual function hashes (without object_ prefix) that this function depends on
    """
    dependencies = []
    tree = ast.parse(normalized_code)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'ouverture.pool':
            for alias in node.names:
                import_name = alias.name  # e.g., "object_c0ff33..."
                # Strip object_ prefix to get actual hash
                if import_name.startswith(OUVERTURE_IMPORT_PREFIX):
                    actual_hash = import_name[len(OUVERTURE_IMPORT_PREFIX):]
                else:
                    actual_hash = import_name  # Backward compatibility
                dependencies.append(actual_hash)

    return dependencies


def command_review(hash_value: str):
    """
    Recursively review a function and its dependencies.

    Shows the function and all functions it depends on (recursively)
    in the user's preferred languages.

    Args:
        hash_value: Function hash to review
    """
    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Get user's preferred languages
    config = config_read()
    preferred_langs = config['user'].get('languages', ['eng'])

    if not preferred_langs:
        preferred_langs = ['eng']

    # Track visited functions to avoid cycles
    visited = set()
    review_queue = [hash_value]

    print("Function Review")
    print("=" * 80)
    print()

    while review_queue:
        current_hash = review_queue.pop(0)

        if current_hash in visited:
            continue

        visited.add(current_hash)

        # Detect schema version
        version = schema_detect_version(current_hash)
        if version is None:
            print(f"Warning: Function {current_hash} not found in local pool", file=sys.stderr)
            continue

        # Try to load in user's preferred languages
        loaded = False
        for lang in preferred_langs:
            try:
                normalized_code, name_mapping, alias_mapping, docstring = function_load(current_hash, lang)
                func_name = name_mapping.get('_ouverture_v_0', 'unknown')

                # Show function
                print(f"Function: {func_name} ({lang})")
                print(f"Hash: {current_hash}")
                print("-" * 80)

                # Denormalize and show code
                normalized_code_with_doc = docstring_replace(normalized_code, docstring)
                original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)
                print(original_code)
                print()

                # Extract dependencies
                deps = dependencies_extract(normalized_code)
                if deps:
                    print(f"Dependencies: {len(deps)}")
                    for dep in deps:
                        print(f"  - {dep}")
                        if dep not in visited:
                            review_queue.append(dep)
                else:
                    print("Dependencies: None")

                print("=" * 80)
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


def command_log():
    """
    Show a git-like commit log of the function pool.

    Lists all functions with metadata (timestamp, author, hash).
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    if not objects_dir.exists():
        print("No functions in pool")
        return

    functions = []

    # Scan for v1 functions (objects/sha256/XX/YYY.../object.json)
    v1_dir = objects_dir / 'sha256'
    if v1_dir.exists():
        for hash_prefix_dir in v1_dir.iterdir():
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
                        'langs': sorted(langs),
                        'version': 1
                    })
                except (IOError, json.JSONDecodeError):
                    continue

    # Scan for v0 functions (objects/XX/YYY.json)
    for hash_prefix_dir in objects_dir.iterdir():
        if not hash_prefix_dir.is_dir():
            continue
        if hash_prefix_dir.name == 'sha256':
            continue

        for json_file in hash_prefix_dir.glob('*.json'):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                func_hash = data['hash']
                langs = list(data.get('name_mappings', {}).keys())

                functions.append({
                    'hash': func_hash,
                    'created': 'unknown',
                    'author': 'unknown',
                    'langs': sorted(langs),
                    'version': 0
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
        version_str = f"v{func['version']}"
        print(f"Hash: {func['hash']}")
        print(f"Date: {func['created']}")
        print(f"Author: {func['author']}")
        print(f"Languages: {langs_str}")
        print(f"Schema: {version_str}")
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

    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    if not objects_dir.exists():
        print("No functions in pool")
        return

    results = []

    # Scan for v1 functions
    v1_dir = objects_dir / 'sha256'
    if v1_dir.exists():
        for hash_prefix_dir in v1_dir.iterdir():
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
                                    _, name_mapping, _, docstring = function_load(func_hash, lang)
                                    func_name = name_mapping.get('_ouverture_v_0', 'unknown')

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

    # Scan for v0 functions
    for hash_prefix_dir in objects_dir.iterdir():
        if not hash_prefix_dir.is_dir():
            continue
        if hash_prefix_dir.name == 'sha256':
            continue

        for json_file in hash_prefix_dir.glob('*.json'):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                func_hash = data['hash']
                normalized_code = data['normalized_code']

                # Search in code
                code_lower = normalized_code.lower()
                if any(term in code_lower for term in search_terms):
                    # Get first available language
                    langs = list(data.get('name_mappings', {}).keys())
                    if langs:
                        lang = langs[0]
                        name_mapping = data['name_mappings'][lang]
                        docstring = data.get('docstrings', {}).get(lang, '')
                        func_name = name_mapping.get('_ouverture_v_0', 'unknown')

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
                            'docstring': docstring[:100],
                            'match_in': match_in
                        })
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
        print(f"View: ouverture.py show {result['hash']}@{result['lang']}")
        print()


def code_strip_ouverture_imports(code: str) -> str:
    """
    Strip ouverture.pool import statements from code.

    These imports are handled separately by loading dependencies into the namespace.

    Args:
        code: Source code with possible ouverture.pool imports

    Returns:
        Code with ouverture.pool imports removed
    """
    tree = ast.parse(code)

    # Filter out ouverture.pool imports
    new_body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'ouverture.pool':
            continue
        new_body.append(node)

    tree.body = new_body
    return ast.unparse(tree)


def dependencies_load_recursive(func_hash: str, lang: str, namespace: dict, loaded: set = None):
    """
    Recursively load a function and all its dependencies into a namespace.

    Creates a module-like object for each dependency that can be accessed as:
    object_HASH._ouverture_v_0(args) -> alias(args)

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
        prefixed_name = OUVERTURE_IMPORT_PREFIX + func_hash
        return namespace.get(prefixed_name)

    loaded.add(func_hash)

    # Load the function
    try:
        normalized_code, name_mapping, alias_mapping, docstring = function_load(func_hash, lang)
    except SystemExit:
        print(f"Error: Could not load dependency {func_hash}@{lang}", file=sys.stderr)
        sys.exit(1)

    # First, recursively load all dependencies (deps are actual hashes without prefix)
    deps = dependencies_extract(normalized_code)
    for dep_hash in deps:
        dependencies_load_recursive(dep_hash, lang, namespace, loaded)

    # Denormalize the code
    normalized_code_with_doc = docstring_replace(normalized_code, docstring)
    original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

    # Strip ouverture imports (dependencies are already in namespace)
    executable_code = code_strip_ouverture_imports(original_code)

    # For each alias in alias_mapping, add the dependency function to namespace with that name
    # alias_mapping maps actual_hash -> alias
    for dep_hash, alias in alias_mapping.items():
        prefixed_dep_name = OUVERTURE_IMPORT_PREFIX + dep_hash
        if prefixed_dep_name in namespace:
            # The dependency's function is already loaded, make alias point to it
            dep_module = namespace[prefixed_dep_name]
            if hasattr(dep_module, '_ouverture_v_0'):
                namespace[alias] = dep_module._ouverture_v_0

    # Execute the code in the namespace (dependencies are already loaded)
    try:
        exec(executable_code, namespace)
    except Exception as e:
        print(f"Error executing dependency {func_hash}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Get function name and create a module-like object for this hash
    func_name = name_mapping.get('_ouverture_v_0', 'unknown')

    if func_name in namespace:
        # Create a simple namespace object that has _ouverture_v_0 attribute
        # Store it under the prefixed name (object_<hash>) for lookup
        class OuvertureModule:
            pass

        module = OuvertureModule()
        module._ouverture_v_0 = namespace[func_name]
        prefixed_name = OUVERTURE_IMPORT_PREFIX + func_hash
        namespace[prefixed_name] = module

    return namespace.get(func_name)


def command_run(hash_with_lang: str, debug: bool = False, func_args: list = None):
    """
    Execute a function from the pool interactively.

    Args:
        hash_with_lang: Function hash with language (e.g., "abc123...@eng")
        debug: If True, run with debugger (pdb)
        func_args: Arguments to pass to the function (after --)
    """
    if func_args is None:
        func_args = []

    # Parse hash and language
    if '@' not in hash_with_lang:
        print("Error: Missing language suffix. Use format: HASH@lang", file=sys.stderr)
        sys.exit(1)

    hash_value, lang = hash_with_lang.rsplit('@', 1)

    # Validate language code
    if len(lang) != 3:
        print(f"Error: Language code must be 3 characters (ISO 639-3). Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load function from pool
    try:
        normalized_code, name_mapping, alias_mapping, docstring = function_load(hash_value, lang)
    except SystemExit:
        print(f"Error: Could not load function {hash_value}@{lang}", file=sys.stderr)
        sys.exit(1)

    # Get function name from mapping
    func_name = name_mapping.get('_ouverture_v_0', 'unknown_function')

    # Create execution namespace
    namespace = {}

    # First, load all dependencies recursively
    deps = dependencies_extract(normalized_code)
    if deps:
        print(f"Loading {len(deps)} dependencies...")
        for dep_hash in deps:
            dependencies_load_recursive(dep_hash, lang, namespace, set())
        print()

    # Denormalize to original language
    normalized_code_with_doc = docstring_replace(normalized_code, docstring)
    original_code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

    print(f"Running function: {func_name} ({lang})")
    print("=" * 60)
    print(original_code)
    print("=" * 60)
    print()

    # Strip ouverture imports (dependencies are already in namespace)
    executable_code = code_strip_ouverture_imports(original_code)

    # For each alias in alias_mapping, add the dependency function to namespace with that name
    # alias_mapping maps actual_hash (without prefix) -> alias
    for dep_hash, alias in alias_mapping.items():
        prefixed_dep_name = OUVERTURE_IMPORT_PREFIX + dep_hash
        if prefixed_dep_name in namespace:
            # The dependency's function is already loaded, make alias point to it
            dep_module = namespace[prefixed_dep_name]
            if hasattr(dep_module, '_ouverture_v_0'):
                namespace[alias] = dep_module._ouverture_v_0

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
    if len(source_lang) != 3:
        print(f"Error: Source language code must be 3 characters (ISO 639-3). Got: {source_lang}", file=sys.stderr)
        sys.exit(1)

    if len(target_lang) != 3:
        print(f"Error: Target language code must be 3 characters (ISO 639-3). Got: {target_lang}", file=sys.stderr)
        sys.exit(1)

    # Validate hash format
    if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
        print(f"Error: Invalid hash format. Expected 64 hex characters. Got: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load source function
    try:
        normalized_code, name_mapping_source, alias_mapping_source, docstring_source = function_load(hash_value, source_lang)
    except SystemExit:
        print(f"Error: Could not load function {hash_value}@{source_lang}", file=sys.stderr)
        sys.exit(1)

    # Show source code for reference
    print(f"Source function ({source_lang}):")
    print("=" * 60)
    normalized_code_with_doc = docstring_replace(normalized_code, docstring_source)
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
    mapping_save_v1(hash_value, target_lang, target_docstring, name_mapping_target, alias_mapping_target, comment)

    print()
    print(f"Translation saved: {hash_value}@{target_lang}")
    print(f"View with: ouverture.py show {hash_value}@{target_lang}")


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


def function_show(hash_with_lang_and_mapping: str):
    """
    Show a function from the ouverture pool with mapping selection support.

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
    if len(lang) != 3:
        print(f"Error: Language code must be 3 characters (ISO 639-3). Got: {lang}", file=sys.stderr)
        sys.exit(1)

    # Detect schema version
    version = schema_detect_version(hash_value)
    if version is None:
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Handle v0 functions (always single mapping per language)
    if version == 0:
        # v0 functions always have exactly one mapping per language
        normalized_code, name_mapping, alias_mapping, docstring = function_load(hash_value, lang)

        # Replace docstring and denormalize
        try:
            normalized_code = docstring_replace(normalized_code, docstring)
            original_code = code_denormalize(normalized_code, name_mapping, alias_mapping)
        except Exception as e:
            print(f"Error: Failed to denormalize code: {e}", file=sys.stderr)
            sys.exit(1)

        # Print the code
        print(original_code)
        return

    # Handle v1 functions
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
            print(f"ouverture.py show {hash_value}@{lang}@{m_hash}{comment_suffix}")
        return

    # Load the selected mapping
    normalized_code, name_mapping, alias_mapping, docstring = function_load(hash_value, lang, mapping_hash=selected_hash)

    # Replace docstring and denormalize
    try:
        normalized_code = docstring_replace(normalized_code, docstring)
        original_code = code_denormalize(normalized_code, name_mapping, alias_mapping)
    except Exception as e:
        print(f"Error: Failed to denormalize code: {e}", file=sys.stderr)
        sys.exit(1)

    # Print the code
    print(original_code)


def function_get(hash_with_lang: str):
    """Get a function from the ouverture pool (backward compatible with show command)"""
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


def code_migrate_add_object_prefix(normalized_code: str) -> str:
    """
    Migrate normalized code from v0 format (no object_ prefix) to v1 format (with object_ prefix).

    Transforms:
    - from ouverture.pool import c0ff33 -> from ouverture.pool import object_c0ff33
    - c0ff33._ouverture_v_0(...) -> object_c0ff33._ouverture_v_0(...)

    Args:
        normalized_code: Normalized code in v0 format (imports without object_ prefix)

    Returns:
        Migrated code with object_ prefix added to ouverture imports and calls
    """
    tree = ast.parse(normalized_code)

    class PrefixAdder(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            if node.module == 'ouverture.pool':
                # Add object_ prefix to import names
                new_names = []
                for alias in node.names:
                    # Only add prefix if not already present
                    if not alias.name.startswith(OUVERTURE_IMPORT_PREFIX):
                        new_name = OUVERTURE_IMPORT_PREFIX + alias.name
                    else:
                        new_name = alias.name
                    new_names.append(ast.alias(name=new_name, asname=alias.asname))
                node.names = new_names
            return node

        def visit_Attribute(self, node):
            # Transform hash._ouverture_v_0 -> object_hash._ouverture_v_0
            if (isinstance(node.value, ast.Name) and
                node.attr == '_ouverture_v_0' and
                not node.value.id.startswith(OUVERTURE_IMPORT_PREFIX)):
                # Check if this looks like a hash (64 hex chars or at least looks like one)
                # Add object_ prefix
                node.value.id = OUVERTURE_IMPORT_PREFIX + node.value.id
            self.generic_visit(node)
            return node

    transformer = PrefixAdder()
    tree = transformer.visit(tree)
    ast.fix_missing_locations(tree)

    return ast.unparse(tree)


def schema_migrate_function_v0_to_v1(func_hash: str, keep_v0: bool = False):
    """
    Migrate a single function from schema v0 to v1.

    Args:
        func_hash: Function hash (64-character hex)
        keep_v0: If True, keep the v0 file after migration (default: False)
    """
    # Verify it's a v0 function
    version = schema_detect_version(func_hash)
    if version != 0:
        print(f"Error: Function {func_hash} is not in v0 format (version: {version})", file=sys.stderr)
        sys.exit(1)

    # Load v0 data
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'
    v0_path = objects_dir / func_hash[:2] / f'{func_hash[2:]}.json'

    try:
        with open(v0_path, 'r', encoding='utf-8') as f:
            v0_data = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error: Failed to load v0 data: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract normalized code and migrate to add object_ prefix
    normalized_code = v0_data['normalized_code']
    migrated_code = code_migrate_add_object_prefix(normalized_code)

    # Create metadata for v1
    metadata = metadata_create()

    # Save function in v1 format (object.json only) with migrated code
    function_save_v1(func_hash, migrated_code, metadata)

    # Migrate each language mapping
    for lang in v0_data.get('name_mappings', {}).keys():
        docstring = v0_data.get('docstrings', {}).get(lang, '')
        name_mapping = v0_data['name_mappings'][lang]
        alias_mapping = v0_data.get('alias_mappings', {}).get(lang, {})

        # Save mapping in v1 format
        mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment='')

    # Validate migration
    is_valid, errors = schema_validate_v1(func_hash)
    if not is_valid:
        print(f"Error: Migration validation failed for {func_hash}:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        sys.exit(1)

    # Delete v0 file if requested
    if not keep_v0:
        try:
            v0_path.unlink()
            # Try to remove parent directory if empty
            try:
                v0_path.parent.rmdir()
            except OSError:
                pass  # Directory not empty, that's fine
        except IOError as e:
            print(f"Warning: Failed to delete v0 file: {e}", file=sys.stderr)

    print(f"Migrated {func_hash} from v0 to v1 (keep_v0={keep_v0})")


def schema_migrate_all_v0_to_v1(keep_v0: bool = False, dry_run: bool = False) -> list:
    """
    Migrate all v0 functions to v1.

    Args:
        keep_v0: If True, keep v0 files after migration (default: False)
        dry_run: If True, only report what would be migrated without actually migrating (default: False)

    Returns:
        List of function hashes that were (or would be) migrated
    """
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Find all v0 files
    v0_functions = []
    if not objects_dir.exists():
        return v0_functions

    for hash_prefix_dir in objects_dir.iterdir():
        if not hash_prefix_dir.is_dir():
            continue
        if hash_prefix_dir.name == 'sha256':
            # Skip v1 algorithm directory
            continue

        for json_file in hash_prefix_dir.glob('*.json'):
            # Reconstruct hash
            func_hash = hash_prefix_dir.name + json_file.stem

            # Verify it's v0
            version = schema_detect_version(func_hash)
            if version == 0:
                v0_functions.append(func_hash)

    if dry_run:
        print(f"Dry run: Would migrate {len(v0_functions)} functions")
        for func_hash in v0_functions:
            print(f"  - {func_hash}")
        return v0_functions

    # Migrate each function
    print(f"Migrating {len(v0_functions)} functions from v0 to v1...")
    for func_hash in v0_functions:
        try:
            schema_migrate_function_v0_to_v1(func_hash, keep_v0=keep_v0)
        except Exception as e:
            print(f"Error migrating {func_hash}: {e}", file=sys.stderr)
            # Continue with other functions

    print(f"Migration complete. Migrated {len(v0_functions)} functions.")
    return v0_functions


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
    ouverture_dir = directory_get_ouverture()
    objects_dir = ouverture_dir / 'objects'

    # Check object.json exists
    func_dir = objects_dir / 'sha256' / func_hash[:2] / func_hash[2:]
    object_json = func_dir / 'object.json'

    if not object_json.exists():
        errors.append(f"object.json not found for function {func_hash}")
        return False, errors

    # Validate object.json structure
    try:
        with open(object_json, 'r', encoding='utf-8') as f:
            func_data = json.load(f)

        # Check required fields
        required_fields = ['schema_version', 'hash', 'hash_algorithm', 'normalized_code', 'encoding', 'metadata']
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
        if item.is_dir() and item.name != 'sha256' and not item.name.endswith('.json'):
            lang_count += 1

    if lang_count == 0:
        errors.append("No language mappings found (no language directories)")

    # If we have errors, return False
    if errors:
        return False, errors

    return True, []


def main():
    parser = argparse.ArgumentParser(description='ouverture - Function pool manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize ouverture directory and config')

    # Whoami command
    whoami_parser = subparsers.add_parser('whoami', help='Get or set user configuration')
    whoami_parser.add_argument('subcommand', choices=['username', 'email', 'public-key', 'language'],
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

    # Migrate command
    migrate_parser = subparsers.add_parser('migrate', help='Migrate functions from v0 to v1')
    migrate_parser.add_argument('hash', nargs='?', help='Specific function hash to migrate (optional, migrates all if omitted)')
    migrate_parser.add_argument('--keep-v0', action='store_true', help='Keep v0 files after migration')
    migrate_parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without actually migrating')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate v1 function structure')
    validate_parser.add_argument('hash', help='Function hash to validate')

    args = parser.parse_args()

    if args.command == 'init':
        command_init()
    elif args.command == 'whoami':
        command_whoami(args.subcommand, args.value)
    elif args.command == 'add':
        function_add(args.file, args.comment)
    elif args.command == 'get':
        function_get(args.hash)
    elif args.command == 'show':
        function_show(args.hash)
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
    elif args.command == 'migrate':
        if args.hash:
            # Migrate specific function
            schema_migrate_function_v0_to_v1(args.hash, keep_v0=args.keep_v0)
        else:
            # Migrate all functions
            schema_migrate_all_v0_to_v1(keep_v0=args.keep_v0, dry_run=args.dry_run)
    elif args.command == 'validate':
        is_valid, errors = schema_validate_v1(args.hash)
        if is_valid:
            print(f"✓ Function {args.hash} is valid")
        else:
            print(f"✗ Function {args.hash} is invalid:", file=sys.stderr)
            for error in errors:
                print(f"  - {error}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
