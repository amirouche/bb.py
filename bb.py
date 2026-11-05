#!/usr/bin/env python3
"""
bb — Beyond Babel, a function pool manager for Python.

Beyond Babel normalizes Python functions into a canonical form using
AST transformation: variables are renamed to positional placeholders
(_bb_v_0, _bb_v_1, ...), imports are sorted, and docstrings are
stripped before hashing. The result is a content-addressed function
pool where identical logic always produces the same SHA-256 hash,
regardless of variable names, comments, or human language.

Typical single-user workflow:

    bb init                         # initialize a new pool
    bb add myfunction.py@eng        # add a function (English naming)
    bb show <hash>@eng              # retrieve and display it
    bb run myfunction 3 5           # execute it with arguments
    bb edit myfunction@eng          # edit it with its checks, save a fork
    bb review myfunction@eng        # read it, approve it, run its checks
    bb validate --checks            # run the checks of reviewed functions
    bb refactor <what> <from> <to>  # swap a dependency for a new version
    bb compile <hash> -o main.py    # flatten into a standalone script

Functions are stored in $BB_DIRECTORY/pool/ (default: ~/.local/bb/pool/)
as JSON objects containing normalized code and per-language name mappings.
Each language mapping preserves the original variable names, docstring,
and BB import aliases, allowing the same function to be displayed in
any language that has been added or translated.

Worklogs live in $BB_DIRECTORY/worklog/, unfinished edits in
$BB_DIRECTORY/edit/, signed reviews in $BB_DIRECTORY/review/ and the
shared list of peers in $BB_DIRECTORY/trust/. bb does not manage history or sharing: track
$BB_DIRECTORY with git and use git's own commands to share it.
"""
import ast
import argparse
import builtins
import contextlib
import hashlib
import io
import itertools
import json
import os
import re
import shlex
import subprocess
import sys
import struct
import tempfile
import time
import types
import uuid
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Set, Tuple, List, Union, Any, Generator, Callable, Optional


# Get all Python built-in names
PYTHON_BUILTINS = set(dir(builtins))

# Prefix for bb.pool imports to ensure valid Python identifiers
# SHA256 hashes can start with digits (0-9), which are invalid as Python identifiers
# By prefixing with "object_", we ensure all import names are valid
BB_IMPORT_PREFIX = "object_"



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

    def visit_ExceptHandler(self, node):
        """Handle exception variable names in except clauses"""
        if node.name and node.name in self.name_mapping:
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


def code_create_name_mapping(function_def: Union[ast.FunctionDef, ast.AsyncFunctionDef], imports: List[ast.stmt],
                        bb_aliases: Set[str] = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create mapping from original names to normalized names.
    Returns (forward_mapping, reverse_mapping)
    Forward: original -> _bb_v_X
    Reverse: _bb_v_X -> original
    """
    if bb_aliases is None:
        bb_aliases = set()

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

    # Function name is always _bb_v_0
    forward_mapping[function_def.name] = '_bb_v_0'
    reverse_mapping['_bb_v_0'] = function_def.name
    counter += 1

    # Collect all names in the function (excluding imported names, built-ins, and bb aliases)
    # Use a set to track seen names and avoid duplicates
    # Include function name in seen_names to handle recursive calls correctly
    seen_names = {function_def.name}
    all_names = list()
    for node in ast.walk(function_def):
        if isinstance(node, ast.Name) and node.id not in imported_names and node.id not in PYTHON_BUILTINS and node.id not in bb_aliases:
            if node.id not in seen_names:
                seen_names.add(node.id)
                all_names.append(node.id)
        elif isinstance(node, ast.arg) and node.arg not in imported_names and node.arg not in PYTHON_BUILTINS and node.arg not in bb_aliases:
            if node.arg not in seen_names:
                seen_names.add(node.arg)
                all_names.append(node.arg)
        elif isinstance(node, ast.ExceptHandler) and node.name and node.name not in imported_names and node.name not in PYTHON_BUILTINS and node.name not in bb_aliases:
            if node.name not in seen_names:
                seen_names.add(node.name)
                all_names.append(node.name)

    # XXX: all_names: do not sort, keep the order ast traversal
    # discovery.

    for name in all_names:
        normalized = f'_bb_v_{counter}'
        forward_mapping[name] = normalized
        reverse_mapping[normalized] = name
        counter += 1

    return forward_mapping, reverse_mapping


def code_rewrite_bb_imports(imports: List[ast.stmt]) -> Tuple[List[ast.stmt], Dict[str, str]]:
    """
    Remove aliases from 'bb' imports and track them for later restoration.
    Returns (new_imports, alias_mapping)
    alias_mapping maps: actual_function_hash (without prefix) -> alias_in_lang

    Input format expected:
        from bb.pool import object_c0ff33 as kawa

    Output:
        - import becomes: from bb.pool import object_c0ff33
        - alias_mapping stores: {"c0ff33...": "kawa"} (actual hash without object_ prefix)
    """
    new_imports = []
    alias_mapping = {}

    for imp in imports:
        if isinstance(imp, ast.ImportFrom) and imp.module == 'bb.pool':
            # Rewrite: from bb.pool import object_c0ffeebad as kawa
            # To: from bb.pool import object_c0ffeebad
            new_names = []
            for alias in imp.names:
                import_name = alias.name  # e.g., "object_c0ff33..."

                # Extract actual hash by stripping the prefix
                if import_name.startswith(BB_IMPORT_PREFIX):
                    actual_hash = import_name[len(BB_IMPORT_PREFIX):]
                else:
                    # Backward compatibility: no prefix (shouldn't happen in new code)
                    actual_hash = import_name

                # Track the alias mapping using actual hash
                if alias.asname:
                    alias_mapping[actual_hash] = alias.asname

                # Create new import without alias (but keep object_ prefix in import name)
                new_names.append(ast.alias(name=import_name, asname=None))

            new_imp = ast.ImportFrom(
                module='bb.pool',
                names=new_names,
                level=0
            )
            new_imports.append(new_imp)
        else:
            new_imports.append(imp)

    return new_imports, alias_mapping


def code_replace_bb_calls(tree: ast.AST, alias_mapping: Dict[str, str], name_mapping: Dict[str, str]):
    """
    Replace calls to aliased bb functions.
    E.g., kawa(...) becomes object_c0ffeebad._bb_v_0(...)

    alias_mapping maps actual hash (without prefix) -> alias name
    The replacement uses object_<hash> to match the import name.
    """
    class BBCallReplacer(ast.NodeTransformer):
        def visit_Name(self, node):
            # If this name is an alias for a bb function
            for func_hash, alias in alias_mapping.items():
                if node.id == alias:
                    # Replace with object_c0ffeebad._bb_v_0
                    # Use prefixed name to match the import statement
                    prefixed_name = BB_IMPORT_PREFIX + func_hash
                    return ast.Attribute(
                        value=ast.Name(id=prefixed_name, ctx=ast.Load()),
                        attr='_bb_v_0',
                        ctx=node.ctx
                    )
            return node

    replacer = BBCallReplacer()
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
    Normalize the AST according to bb rules.
    Returns (normalized_code_with_docstring, normalized_code_without_docstring, docstring, name_mapping, alias_mapping)
    """
    # Sort imports
    tree = code_sort_imports(tree)

    # Extract function and imports
    function_def, imports = code_extract_definition(tree)

    # Extract docstring from function
    docstring, function_without_docstring = code_extract_docstring(function_def)

    # Rewrite bb imports
    imports, alias_mapping = code_rewrite_bb_imports(imports)

    # Get the set of bb aliases (values in alias_mapping)
    bb_aliases = set(alias_mapping.values())

    # Create name mapping
    forward_mapping, reverse_mapping = code_create_name_mapping(function_def, imports, bb_aliases)

    # Create two modules: one with docstring (for display) and one without (for hashing)
    module_with_docstring = ast.Module(body=imports + [function_def], type_ignores=[])
    module_without_docstring = ast.Module(body=imports + [function_without_docstring], type_ignores=[])

    # Process both modules identically
    for module in [module_with_docstring, module_without_docstring]:
        # Replace bb calls with their normalized form
        module = code_replace_bb_calls(module, alias_mapping, forward_mapping)

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


def code_compute_function_hash(normalized_code: str, checks: List[str] = None) -> str:
    """
    Compute a function's hash: its identity in the pool.

    The checks attached to a function are part of its definition, so
    changing them gives a new hash (a fork). Documentation is not: it
    lives in the language mappings.

    - Without checks: SHA256 of the normalized code (without docstring),
      so functions added before checks existed keep their hash
    - With checks: SHA256 of the canonical JSON of {checks, normalized_code},
      with checks sorted and deduplicated so their order does not matter

    Args:
        normalized_code: Normalized code without docstring
        checks: Hashes of the attached check functions

    Returns:
        64-character hex hash
    """
    if not checks:
        return hash_compute(normalized_code)
    canonical_json = json.dumps({'normalized_code': normalized_code, 'checks': sorted(set(checks))},
                                sort_keys=True, ensure_ascii=False)
    return hash_compute(canonical_json)


def code_compute_mapping_hash(docstring: str, name_mapping: Dict[str, str],
                        alias_mapping: Dict[str, str], comment: str = "",
                        markdown: str = "", parent: str = "") -> str:
    """
    Compute content-addressed hash for a mapping.

    The hash is computed on the canonical JSON representation of:
    - docstring
    - name_mapping
    - alias_mapping
    - comment
    - markdown (only when non-empty, so older mappings keep their hash)
    - parent (only when set, same reason)

    This enables deduplication: identical mappings share the same hash and storage.

    Args:
        docstring: The function docstring for this mapping
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        markdown: Optional markdown documentation (from bb edit)
        parent: Optional translation this mapping derives from, as hash@lang@mapping_hash

    Returns:
        64-character hex hash (SHA256)
    """
    mapping_dict = {
        'docstring': docstring,
        'name_mapping': name_mapping,
        'alias_mapping': alias_mapping,
        'comment': comment
    }
    if markdown:
        mapping_dict['markdown'] = markdown
    if parent:
        mapping_dict['parent'] = parent

    # Create canonical JSON (sorted keys, no whitespace)
    canonical_json = json.dumps(mapping_dict, sort_keys=True, ensure_ascii=False)

    # Compute hash
    return hash_compute(canonical_json)


def code_detect_schema(func_hash: str) -> int:
    """
    Detect the schema version of a stored function.

    Checks the filesystem to determine if a function is stored in v1 format:
    - v1: $BB_DIRECTORY/objects/sha256/XX/YYYYYY.../object.json

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


def code_create_metadata(parent: str = None) -> Dict[str, any]:
    """
    Create default metadata for a function.

    Generates metadata with:
    - created: ISO 8601 timestamp
    - name: User's name from config
    - email: User's email from config
    - parent: Optional parent function hash (for lineage tracking)

    Args:
        parent: Optional parent function hash (for fork lineage tracking)

    Returns:
        Dictionary with metadata fields
    """
    from datetime import datetime, UTC

    # Get name and email from config
    config = storage_read_config()
    name = config['user'].get('name', '')
    email = config['user'].get('email', '')

    # Get current timestamp in ISO 8601 format
    timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')

    metadata = {
        'created': timestamp,
        'name': name,
        'email': email
    }

    if parent:
        metadata['parent'] = parent

    return metadata


def storage_get_bb_directory() -> Path:
    """
    Get the bb base directory from environment variable or default to '$HOME/.local/bb/'.
    Environment variable: BB_DIRECTORY

    The whole directory is meant to be tracked with git by the user;
    bb itself never runs git.

    Directory structure:
        $BB_DIRECTORY/
        ├── pool/          # Functions: XX/YYYY.../object.json and per-language mappings
        ├── worklog/       # Worklogs: <uuid7>/worklog.json and translated messages
        ├── edit/          # Unfinished bb edit files (a scratch pad)
        ├── review/        # Signed reviews: XX/YYYY.../<key id>.json and .sig
        ├── trust/         # allowed_signers: shared list of peers (not trusted by itself)
        └── config.json    # Your name, email, signing key and preferred languages

    Private state (review approvals, trusted peers) lives elsewhere: see
    storage_get_state_directory.
    """
    env_dir = os.environ.get('BB_DIRECTORY')
    if env_dir:
        return Path(env_dir)
    # Default to $HOME/.local/bb/
    home = os.environ.get('HOME', os.path.expanduser('~'))
    return Path(home) / '.local' / 'bb'


def storage_get_pool_directory() -> Path:
    """
    Get the pool directory where function objects are stored.
    Returns: $BB_DIRECTORY/pool/
    """
    return storage_get_bb_directory() / 'pool'


def storage_get_config_path() -> Path:
    """
    Get the path to the config file.
    Config is stored in $BB_DIRECTORY/config.json
    Can be overridden with BB_CONFIG_PATH environment variable for testing.
    """
    config_override = os.environ.get('BB_CONFIG_PATH')
    if config_override:
        return Path(config_override)
    return storage_get_bb_directory() / 'config.json'


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
                'signing_key': '',
                'languages': []
            }
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
    Initialize bb directory and config file.
    """
    bb_dir = storage_get_bb_directory()

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
                'name': os.environ.get('USER', os.environ.get('USERNAME', '')),
                'email': '',
                'signing_key': '',
                'languages': ['eng']
            }
        }
        storage_write_config(config)
        print(f"Created config file: {config_path}")

    print(f"Initialized bb directory: {bb_dir}")


def command_whoami(subcommand: str, value: list = None):
    """
    Get or set user configuration.

    Examples:

    ```
    bb whoami name              # get current name
    bb whoami name amirouche    # set name to amirouche
    bb whoami email azul@amirouche.dev  # set email
    bb whoami signing-key ~/.ssh/id_ed25519  # sign your reviews with this SSH key
    bb whoami language eng fra  # set preferred languages
    ```
    """
    config = storage_read_config()

    # Map CLI subcommand to config key
    key_map = {
        'name': 'name',
        'email': 'email',
        'signing-key': 'signing_key',
        'language': 'languages'
    }

    if subcommand not in key_map:
        print(f"Error: Unknown subcommand: {subcommand}", file=sys.stderr)
        print("Valid subcommands: name, email, signing-key, language", file=sys.stderr)
        sys.exit(1)

    config_key = key_map[subcommand]

    # Get current value
    if value is None or len(value) == 0:
        current = config['user'].get(config_key, '')
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


def object_save(hash_value: str, normalized_code: str, metadata: Dict[str, any],
                checks: List[str] = None) -> bool:
    """
    Save function object to bb directory.

    Creates the function directory and object.json file:
    - Directory: $BB_DIRECTORY/pool/XX/YYYYYY.../
    - File: $BB_DIRECTORY/pool/XX/YYYYYY.../object.json

    object.json is written once: an existing object is never rewritten, so
    adding the same function again keeps its original metadata (creation
    time, author, parent).

    Args:
        hash_value: Function hash (64-character hex), see code_compute_function_hash
        normalized_code: Normalized code with docstring
        metadata: Metadata dict (created, author)
        checks: Hashes of the attached check functions, part of the definition

    Returns:
        True when the object was written, False when it already existed
    """
    pool_dir = storage_get_pool_directory()

    # Create function directory: pool/XX/YYYYYY.../
    func_dir = pool_dir / hash_value[:2] / hash_value[2:]
    func_dir.mkdir(parents=True, exist_ok=True)

    # Create object.json
    object_json = func_dir / 'object.json'

    if object_json.exists():
        print(f"Hash: {hash_value}")
        return False

    data = {
        'schema_version': 1,
        'hash': hash_value,
        'normalized_code': normalized_code,
        'metadata': metadata
    }
    if checks:
        data['checks'] = sorted(set(checks))

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Hash: {hash_value}")
    return True


def object_load_checks(hash_value: str) -> List[str]:
    """Hashes of the checks attached to a function (part of its definition)."""
    return object_load(hash_value).get('checks', [])


def mapping_save(func_hash: str, lang: str, docstring: str,
                 name_mapping: Dict[str, str], alias_mapping: Dict[str, str],
                 comment: str = "", markdown: str = "", parent: str = "") -> str:
    """
    Save language mapping to bb directory.

    Creates the mapping directory and mapping.json file:
    - Directory: $BB_DIRECTORY/pool/XX/Y.../lang/ZZ/W.../
    - File: $BB_DIRECTORY/pool/XX/Y.../lang/ZZ/W.../mapping.json

    The mapping is content-addressed, enabling deduplication.

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        markdown: Optional markdown documentation, stored under the 'markdown' key
        parent: Optional translation this mapping derives from, as hash@lang@mapping_hash

    Returns:
        Mapping hash (64-character hex)
    """
    pool_dir = storage_get_pool_directory()

    # Compute mapping hash
    mapping_hash = code_compute_mapping_hash(docstring, name_mapping, alias_mapping, comment, markdown, parent)

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
    if markdown:
        data['markdown'] = markdown
    if parent:
        data['parent'] = parent

    with open(mapping_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Mapping hash: {mapping_hash}")

    return mapping_hash


def code_save(hash_value: str, lang: str, normalized_code: str, docstring: str,
                  name_mapping: Dict[str, str], alias_mapping: Dict[str, str], comment: str = "",
                  parent: str = None):
    """
    Save function to bb directory.

    This is the main entry point for saving functions.

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        normalized_code: Normalized code with docstring
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        parent: Optional parent function hash (for fork lineage tracking)
    """
    # Create metadata (with optional parent for lineage)
    metadata = code_create_metadata(parent=parent)

    # Save function (object.json)
    object_save(hash_value, normalized_code, metadata)

    # Save mapping (mapping.json)
    mapping_save(hash_value, lang, docstring, name_mapping, alias_mapping, comment)


def code_denormalize(normalized_code: str, name_mapping: Dict[str, str], alias_mapping: Dict[str, str]) -> str:
    """
    Denormalize code by applying reverse name mappings.
    name_mapping: maps normalized names (_bb_v_X) to original names
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

        def visit_ExceptHandler(self, node):
            # Replace normalized exception variable name
            if node.name and node.name in name_mapping:
                node.name = name_mapping[node.name]
            self.generic_visit(node)
            return node

        def visit_Attribute(self, node):
            # Replace object_c0ffeebad._bb_v_0(...) with alias(...)
            if (isinstance(node.value, ast.Name) and
                node.attr == '_bb_v_0'):
                prefixed_name = node.value.id
                # Strip object_ prefix to get actual hash
                if prefixed_name.startswith(BB_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(BB_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name  # Backward compatibility

                if actual_hash in hash_to_alias:
                    # Return just the alias name
                    return ast.Name(id=hash_to_alias[actual_hash], ctx=node.ctx)
            self.generic_visit(node)
            return node

        def visit_ImportFrom(self, node):
            # Add aliases back to 'from bb.pool import object_X'
            if node.module == 'bb.pool':
                node.module = 'bb.pool'
                # Add aliases back
                new_names = []
                for alias_node in node.names:
                    import_name = alias_node.name  # e.g., "object_c0ff33..."

                    # Strip object_ prefix to get actual hash
                    if import_name.startswith(BB_IMPORT_PREFIX):
                        actual_hash = import_name[len(BB_IMPORT_PREFIX):]
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


def code_extract_dependencies(normalized_code: str) -> List[str]:
    """
    Extract bb dependencies from normalized code.

    Returns:
        List of actual function hashes (without object_ prefix) that this function depends on
    """
    dependencies = []
    tree = ast.parse(normalized_code)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            for alias in node.names:
                import_name = alias.name  # e.g., "object_c0ff33..."
                # Strip object_ prefix to get actual hash
                if import_name.startswith(BB_IMPORT_PREFIX):
                    actual_hash = import_name[len(BB_IMPORT_PREFIX):]
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
        func_data = object_load(hash_value)
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


def storage_get_state_directory() -> Path:
    """
    Get the directory for private, per-user state: review approvals.

    It is deliberately outside $BB_DIRECTORY. That directory is shared
    through git, so approvals stored there could be forged by anyone able
    to commit to it, and forged approvals would let their code run.

    Environment variable: BB_STATE_DIRECTORY, then $XDG_STATE_HOME/bb,
    then ~/.local/state/bb.
    """
    env_dir = os.environ.get('BB_STATE_DIRECTORY')
    if env_dir:
        return Path(env_dir)
    xdg_state = os.environ.get('XDG_STATE_HOME')
    if xdg_state:
        return Path(xdg_state) / 'bb'
    home = os.environ.get('HOME', os.path.expanduser('~'))
    return Path(home) / '.local' / 'state' / 'bb'


def review_state_path() -> Path:
    """Path of the file holding the hashes you approved with bb review."""
    return storage_get_state_directory() / 'review_state.json'


def review_load_state() -> set:
    """
    Load the set of previously reviewed function hashes.

    Returns:
        Set of reviewed function hashes
    """
    state_file = review_state_path()

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
    state_file = review_state_path()
    state_file.parent.mkdir(parents=True, exist_ok=True)
    data = {'reviewed': sorted(reviewed)}

    with open(state_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


# ============================================================================
# Signed reviews and trusted peers
# ============================================================================
#
# A review can be shared with others through $BB_DIRECTORY, which is tracked
# with git. Anyone able to commit can write a file there, so a shared review
# only counts when it is signed (ssh-keygen -Y sign) by a key you trust:
#
#     $BB_DIRECTORY/review/XX/YYYY.../<key id>.json      the review
#     $BB_DIRECTORY/review/XX/YYYY.../<key id>.json.sig  its signature
#     $BB_DIRECTORY/trust/allowed_signers                peers shared publicly
#     <state directory>/allowed_signers                  peers YOU trust
#
# Only the private list is trusted. The public one is a directory of peers
# that you copy from, entry by entry, with `bb trust import`. Both use the
# OpenSSH allowed_signers format: "email key-type base64 [comment]".

REVIEW_SIGNATURE_NAMESPACE = 'bb-review'
REVIEW_VERDICTS = ('approve', 'reject')


def storage_get_review_directory() -> Path:
    """Get the directory of shared, signed reviews: $BB_DIRECTORY/review/"""
    return storage_get_bb_directory() / 'review'


def trust_public_path() -> Path:
    """The shared list of peers: $BB_DIRECTORY/trust/allowed_signers (not trusted by itself)."""
    return storage_get_bb_directory() / 'trust' / 'allowed_signers'


def trust_private_path() -> Path:
    """Your list of trusted peers, in the private state directory."""
    return storage_get_state_directory() / 'allowed_signers'


def trust_parse(path: Path) -> List[Tuple[str, str]]:
    """
    Read an allowed_signers file.

    Returns:
        (email, key) pairs, key being "type base64" without the comment
    """
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding='utf-8').splitlines():
        parts = line.split()
        if len(parts) >= 3 and not line.lstrip().startswith('#'):
            entries.append((parts[0], f"{parts[1]} {parts[2]}"))
    return entries


def trust_write(path: Path, entries: List[Tuple[str, str]]):
    """Write an allowed_signers file, one sorted 'email key' line per entry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{email} {key}" for email, key in sorted(set(entries))]
    path.write_text(''.join(f"{line}\n" for line in lines), encoding='utf-8')


def helper_key_id(public_key: str) -> str:
    """A filesystem-safe id for a public key: SHA256 of its 'type base64' form."""
    return hash_compute(' '.join(public_key.split()[:2]))


def helper_signing_identity() -> Optional[Tuple[str, str, str]]:
    """
    Your signing identity from config: (private key path, public key, email).

    Returns:
        None when no signing key is configured or its .pub file is missing
    """
    user = storage_read_config()['user']
    key_path = os.path.expanduser(user.get('signing_key') or '')
    if not key_path:
        return None
    pub_path = Path(key_path + '.pub')
    if not pub_path.exists():
        print(f"Warning: signing key {key_path} has no {pub_path.name}; review not signed.", file=sys.stderr)
        return None
    public_key = ' '.join(pub_path.read_text(encoding='utf-8').split()[:2])
    return key_path, public_key, user.get('email') or ''


def review_publish(func_hash: str, verdict: str, comment: str = "") -> Optional[Path]:
    """
    Write a signed review of a function into $BB_DIRECTORY/review/.

    One review per reviewer and function: a new verdict replaces yours
    (git keeps the history).

    Returns:
        Path of the review, or None when you have no signing key or signing failed
    """
    assert verdict in REVIEW_VERDICTS
    identity = helper_signing_identity()
    if identity is None:
        return None
    key_path, public_key, email = identity
    if not email:
        print("Warning: set your email (bb whoami email ...) to sign reviews; review not signed.",
              file=sys.stderr)
        return None

    user = storage_read_config()['user']
    record = {
        'schema_version': 1,
        'function': func_hash,
        'verdict': verdict,
        'reviewer': {'name': user.get('name', ''), 'email': email},
        'public_key': public_key,
        'created': code_create_metadata()['created'],
        'comment': comment,
    }
    review_dir = storage_get_review_directory() / func_hash[:2] / func_hash[2:]
    review_dir.mkdir(parents=True, exist_ok=True)
    path = review_dir / f"{helper_key_id(public_key)}.json"
    path.write_text(json.dumps(record, sort_keys=True, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    signature = Path(str(path) + '.sig')
    if signature.exists():
        signature.unlink()
    try:
        result = subprocess.run(['ssh-keygen', '-Y', 'sign', '-q', '-f', key_path,
                                 '-n', REVIEW_SIGNATURE_NAMESPACE, str(path)],
                                capture_output=True, text=True)
    except FileNotFoundError:
        result = None
    if result is None or result.returncode != 0:
        path.unlink()
        detail = "ssh-keygen not found" if result is None else result.stderr.strip()
        print(f"Warning: could not sign the review ({detail}); it stays private.", file=sys.stderr)
        return None
    return path


def review_verify(path: Path, allowed_signers: Path) -> bool:
    """True when the review at path is signed by its reviewer's email in allowed_signers."""
    signature = Path(str(path) + '.sig')
    if not signature.exists() or not allowed_signers.exists():
        return False
    try:
        record = json.loads(path.read_text(encoding='utf-8'))
        email = record['reviewer']['email']
    except (OSError, json.JSONDecodeError, KeyError, TypeError):
        return False
    try:
        with open(path, 'rb') as data:
            result = subprocess.run(['ssh-keygen', '-Y', 'verify', '-f', str(allowed_signers),
                                     '-I', email, '-n', REVIEW_SIGNATURE_NAMESPACE, '-s', str(signature)],
                                    stdin=data, capture_output=True)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def review_list_shared(func_hash: str) -> List[Dict[str, Any]]:
    """
    Every shared review of a function, with its signature status.

    Each entry is the review record plus 'status': 'trusted' (valid
    signature by a key in your private list), 'untrusted' (valid signature
    by a key you do not trust) or 'invalid' (bad, missing or misplaced).
    """
    review_dir = storage_get_review_directory() / func_hash[:2] / func_hash[2:]
    if not review_dir.exists():
        return []
    reviews = []
    for path in sorted(review_dir.glob('*.json')):
        try:
            record = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            continue
        # The signed content must be about this function and its own key
        placed_right = (record.get('function') == func_hash and record.get('verdict') in REVIEW_VERDICTS
                        and path.stem == helper_key_id(record.get('public_key', '')))
        if placed_right and review_verify(path, trust_private_path()):
            record['status'] = 'trusted'
        else:
            record['status'] = 'invalid'
            if placed_right:
                # Check the signature against the key it claims, to tell bad from untrusted
                with tempfile.TemporaryDirectory() as tmp:
                    own = Path(tmp) / 'allowed_signers'
                    own.write_text(f"{record['reviewer']['email']} {record['public_key']}\n", encoding='utf-8')
                    if review_verify(path, own):
                        record['status'] = 'untrusted'
        reviews.append(record)
    return reviews


def review_approvals(hashes: List[str]) -> Set[str]:
    """
    The functions among hashes that count as approved for you.

    Approved means: you approved it yourself, or a peer you trust approved
    it in a signed review and no peer you trust rejected it. Your own
    rejection always wins.
    """
    private = review_load_state()
    rejected = review_load_rejections()
    approved = set()
    for func_hash in set(hashes):
        if func_hash in rejected:
            continue
        if func_hash in private:
            approved.add(func_hash)
            continue
        verdicts = {r['verdict'] for r in review_list_shared(func_hash) if r['status'] == 'trusted'}
        if 'approve' in verdicts and 'reject' not in verdicts:
            approved.add(func_hash)
    return approved


def review_load_rejections() -> Set[str]:
    """Hashes you rejected in bb review (private state)."""
    path = storage_get_state_directory() / 'review_rejected.json'
    try:
        return set(json.loads(path.read_text(encoding='utf-8')).get('rejected', []))
    except (OSError, json.JSONDecodeError):
        return set()


def review_save_rejections(rejected: Set[str]):
    """Save the hashes you rejected (private state)."""
    path = storage_get_state_directory() / 'review_rejected.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'rejected': sorted(rejected)}, indent=2), encoding='utf-8')


def command_reviews(identifier: str):
    """
    Show who reviewed a function: every shared review and its signature.

    trusted: signed by a peer in your private list; untrusted: validly
    signed by someone you do not trust (yet); invalid: forged, altered or
    misplaced. Only trusted reviews count.
    """
    func_hash = helper_resolve_to_hash(identifier)
    yours = ("approved" if func_hash in review_load_state() else
             "rejected" if func_hash in review_load_rejections() else "not reviewed")
    print(f"Function: {func_hash}")
    print(f"You: {yours}")
    reviews = review_list_shared(func_hash)
    if not reviews:
        print("No shared reviews.")
        return
    for record in reviews:
        reviewer = record.get('reviewer', {})
        line = (f"{record['status']}  {record.get('verdict', '?')}  "
                f"{reviewer.get('name', '')} <{reviewer.get('email', '')}>  {record.get('created', '')}")
        print(line + (f"  {record['comment']}" if record.get('comment') else ""))


def command_trust_add(email: str, key: str):
    """
    Trust a peer: add their email and SSH public key to your private list.

    key is a public key ("ssh-ed25519 AAAA...") or the path of a .pub file.
    """
    if os.path.exists(os.path.expanduser(key)):
        key = Path(os.path.expanduser(key)).read_text(encoding='utf-8')
    parts = key.split()
    if len(parts) < 2:
        print(f"Error: not an SSH public key: {key!r}", file=sys.stderr)
        sys.exit(1)
    entries = trust_parse(trust_private_path()) + [(email, f"{parts[0]} {parts[1]}")]
    trust_write(trust_private_path(), entries)
    print(f"Trusted: {email} {parts[0]} {parts[1][:16]}...")


def command_trust_remove(email: str):
    """Stop trusting a peer: remove every key of email from your private list."""
    entries = trust_parse(trust_private_path())
    kept = [(e, k) for e, k in entries if e != email]
    if len(kept) == len(entries):
        print(f"Error: {email} is not in your trusted peers", file=sys.stderr)
        sys.exit(1)
    trust_write(trust_private_path(), kept)
    print(f"No longer trusted: {email}")


def command_trust_list():
    """List the peers you trust (your private list)."""
    entries = trust_parse(trust_private_path())
    if not entries:
        print("No trusted peers.")
        return
    for email, key in entries:
        print(f"{email} {key}")


def command_trust_publish():
    """
    Add your email and signing key to the shared list of peers.

    Others can then import you with bb trust import. The shared list is
    only a directory: nobody trusts it without importing entries.
    """
    identity = helper_signing_identity()
    if identity is None or not identity[2]:
        print("Error: set a signing key and an email first "
              "(bb whoami signing-key PATH, bb whoami email ADDRESS)", file=sys.stderr)
        sys.exit(1)
    _, public_key, email = identity
    trust_write(trust_public_path(), trust_parse(trust_public_path()) + [(email, public_key)])
    print(f"Published: {email} {public_key.split()[0]} {public_key.split()[1][:16]}...")


def command_trust_import():
    """
    Import peers from the shared list into your private list, one by one.

    Anyone able to commit can add a line to the shared list, so each
    entry is shown and must be accepted explicitly.
    """
    private = trust_parse(trust_private_path())
    candidates = [entry for entry in trust_parse(trust_public_path()) if entry not in private]
    if not candidates:
        print("Nothing to import.")
        return
    for email, key in candidates:
        print(f"{email} {key}")
        try:
            answer = input("Trust this peer? [y/n]: ").strip().lower()
        except EOFError:
            print()
            break
        if answer == 'y':
            private.append((email, key))
            trust_write(trust_private_path(), private)
            print(f"Trusted: {email}")
    print(f"Trusted peers: {len(private)}")


def review_closure(func_hash: str) -> List[str]:
    """
    Every function that runs when a function's checks run.

    The function and its dependencies, then each check and its
    dependencies, dependencies first. Checks are code too: they must be
    approved before anything runs.
    """
    order: List[str] = []
    for root in [func_hash] + object_load_checks(func_hash):
        for dep in code_resolve_dependencies(root):
            if dep not in order:
                order.append(dep)
    return order


def review_order(func_hash: str) -> List[List[str]]:
    """
    Order a review: breadth-first levels of the dependency order.

    A function depends on what it imports and on its checks, so the
    functions it imports, and each of its checks, are reviewed before it.
    This holds for every function reached, dependencies included.

    Returns:
        Levels of hashes; every function comes after all it depends on.
        Each level is sorted by hash so the order is stable.

    Raises:
        ValueError: If a function is missing from the pool, or on a cycle
    """
    requires: Dict[str, Set[str]] = {}
    stack = [func_hash]
    while stack:
        current = stack.pop()
        if current in requires:
            continue
        if code_detect_schema(current) is None:
            raise ValueError(f"Function not in the local pool: {current}")
        data = object_load(current)
        requires[current] = set(code_extract_dependencies(data['normalized_code'])) | set(data.get('checks', []))
        stack.extend(requires[current])

    levels: List[List[str]] = []
    done: Set[str] = set()
    while len(done) < len(requires):
        level = sorted(h for h, needed in requires.items() if h not in done and needed <= done)
        if not level:
            raise ValueError("Dependency cycle between functions and their checks: "
                             + ', '.join(sorted(h[:12] for h in requires if h not in done)))
        levels.append(level)
        done.update(level)
    return levels


def review_render(func_hash: str, preferred_langs: List[str]) -> Optional[Tuple[str, str, str]]:
    """
    Render a function for reading, in the first preferred language it has,
    otherwise in the first language it has: the code is readable either way.

    Returns:
        (name, lang, source), or None when the function has no mapping at all
    """
    available = storage_list_languages(func_hash)
    for lang in [l for l in preferred_langs if l in available] + available:
        if lang not in available:
            continue
        normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, lang)
        source = code_denormalize(code_replace_docstring(normalized_code, docstring), name_mapping, alias_mapping)
        return name_mapping.get('_bb_v_0', 'unknown'), lang, source
    return None


def review_ask(question: str, choices: str = 'ynq') -> Optional[str]:
    """Ask for one of choices (letters); returns it, or None without input (EOF)."""
    while True:
        try:
            response = input(f"{question} [{'/'.join(choices)}]: ").strip().lower()
        except EOFError:
            print()
            return None
        if len(response) == 1 and response in choices:
            return response
        print(f"Invalid input. Please enter one of: {', '.join(choices)}.")


def review_require(func_hash: str, action: str):
    """
    Exit with an error unless you approved a function and all its dependencies.

    Used before running or compiling pool code: nothing runs unread.
    """
    closure = code_resolve_dependencies(func_hash)
    approved = review_approvals(closure)
    missing = [h for h in closure if h not in approved]
    if missing:
        print(f"Error: Refusing to {action} code you have not reviewed "
              f"({len(missing)} function(s) to approve).", file=sys.stderr)
        print(f"Review it first: bb review {func_hash}", file=sys.stderr)
        sys.exit(1)


def command_review(identifier: str, comment: str = ""):
    """
    Read and approve (or reject) a function, its dependencies and its checks.

    Functions come in breadth-first levels of the dependency order: what a
    function imports and its checks come before it, so each check that is
    not approved yet is reviewed on its own first.

    For each function: read its code and its checks' code, run the checks
    if you want to (once everything they run is approved, except the
    function you are reading), then approve the hash, reject it, or skip.
    Your verdicts are kept in a private state directory; with a signing key
    (bb whoami signing-key PATH) each one is also published as a signed
    review in $BB_DIRECTORY/review/, so peers who trust you can rely on it.
    Functions already approved by you or by peers you trust are skipped.
    Exits with status 1 when a check fails.
    """
    hash_value = helper_resolve_to_hash(identifier)

    config = storage_read_config()
    preferred_langs = config['user'].get('languages') or ['eng']

    try:
        order = [h for level in review_order(hash_value) for h in level]
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Approved by you or by trusted peers; your own verdicts are saved privately
    reviewed = review_approvals(order)
    private = review_load_state()
    rejected = review_load_rejections()
    if helper_signing_identity() is None:
        print("Note: no signing key, so your verdicts stay private (bb whoami signing-key PATH).")
        print()

    to_review = [h for h in order if h not in reviewed]
    failed = False

    if not to_review:
        print("Everything here is already reviewed.")
        print(f"To reset review state, delete: {review_state_path()}")
        print()
        if not review_run_checks(hash_value, reviewed):
            sys.exit(1)
        return

    print("Interactive Function Review")
    print("=" * 80)
    print(f"Functions to review: {len(to_review)}")
    print(f"Already reviewed: {len(order) - len(to_review)}")
    print()
    print("Dependencies and checks come first. For each function, read the code")
    print("and its checks, run the checks if you want to, then approve or not.")
    print("Press 'y' for yes, 'n' for no, 'r' to reject, 'q' to quit.")
    print("=" * 80)
    print()

    for i, current_hash in enumerate(to_review):
        rendered = review_render(current_hash, preferred_langs)
        if rendered is None:
            print(f"Warning: {current_hash} has no language mapping to show; skipped.", file=sys.stderr)
            print()
            continue
        name, lang, source = rendered

        print(f"[{i+1}/{len(to_review)}] Function: {name} ({lang})")
        print(f"Hash: {current_hash}")
        print("-" * 80)
        print(source)
        print("-" * 80)

        data = object_load(current_hash)
        deps = code_extract_dependencies(data['normalized_code'])
        if deps:
            print(f"Dependencies: {len(deps)}")
            for dep in deps:
                status = "✓" if dep in reviewed else "not approved"
                print(f"  - {dep[:12]}... [{status}]")
        else:
            print("Dependencies: None")

        checks = data.get('checks', [])
        if checks:
            print(f"Checks: {len(checks)}")
            for check_hash in checks:
                status = "✓" if check_hash in reviewed else "not approved"
                check_rendered = review_render(check_hash, preferred_langs)
                print(f"  - {check_hash[:12]}... [{status}]")
                if check_rendered:
                    print('\n'.join(f"      {line}".rstrip() for line in check_rendered[2].split('\n')))

        shared = review_list_shared(current_hash)
        if shared:
            print(f"Shared reviews: {len(shared)}")
            for record in shared:
                reviewer = record.get('reviewer', {})
                print(f"  - {record['status']} {record.get('verdict')} by "
                      f"{reviewer.get('name', '')} <{reviewer.get('email', '')}>")
        print()

        if checks:
            # You just read this function: everything else must already be approved
            results, missing = check_run_reviewed(current_hash, reviewed | {current_hash})
            if missing:
                print(f"Checks cannot run yet: {len(missing)} other function(s) are not approved.")
            else:
                answer = review_ask("Run its checks?")
                if answer is None or answer == 'q':
                    print("Review paused. Approvals so far are saved.")
                    return
                if answer == 'y':
                    if not check_report(results):
                        failed = True
            print()

        answer = review_ask("Approve this function? (r to reject)", 'ynrq')
        if answer is None:
            print("Non-interactive mode - stopping here. Approvals so far are saved.")
            return
        if answer == 'q':
            print(f"Review paused. Progress saved ({len(reviewed)} functions reviewed).")
            print("Run the command again to continue from where you left off.")
            return
        if answer == 'y':
            reviewed.add(current_hash)
            private.add(current_hash)
            rejected.discard(current_hash)
            review_save_state(private)
            review_save_rejections(rejected)
            published = review_publish(current_hash, 'approve', comment)
            print("✓ Function approved" + (" and signed review published." if published else "."))
        elif answer == 'r':
            reviewed.discard(current_hash)
            private.discard(current_hash)
            rejected.add(current_hash)
            review_save_state(private)
            review_save_rejections(rejected)
            published = review_publish(current_hash, 'reject', comment)
            print("✗ Function rejected" + (" and signed review published." if published else "."))
        else:
            print("✗ Function not approved.")

        print("=" * 80)
        print()

    print("Review complete!")
    print(f"Total reviewed: {len(private)} function(s)")
    if failed:
        sys.exit(1)


def review_run_checks(func_hash: str, reviewed: set) -> bool:
    """
    Run a function's checks if you approved everything they would run.

    Returns:
        False when a check failed, True otherwise (also when nothing ran)
    """
    if not object_load_checks(func_hash):
        print("No checks.")
        return True
    results, missing = check_run_reviewed(func_hash, reviewed)
    if missing:
        print(f"Checks not run: {len(missing)} function(s) still need your approval.")
        return True
    return check_report(results)


def command_log():
    """
    Show the history of the pool: functions and worklogs, newest first.

    Functions show their author, parent (when forked) and languages.
    Worklogs show their author, languages, attached translations and
    message, in your preferred language when translated.
    """
    entries = []

    pool_dir = storage_get_pool_directory()
    if pool_dir.exists():
        for hash_prefix_dir in pool_dir.iterdir():
            if not hash_prefix_dir.is_dir():
                continue
            for func_dir in hash_prefix_dir.iterdir():
                object_json = func_dir / 'object.json'
                if not object_json.exists():
                    continue
                try:
                    with open(object_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except (IOError, json.JSONDecodeError):
                    continue
                metadata = data.get('metadata', {})
                langs = sorted(item.name for item in func_dir.iterdir() if item.is_dir())
                lines = [f"Hash: {data['hash']}",
                         f"Date: {metadata.get('created', 'unknown')}",
                         f"Author: {helper_format_author(metadata)}"]
                if metadata.get('parent'):
                    lines.append(f"Parent: {metadata['parent']}")
                lines.append(f"Languages: {', '.join(langs) if langs else 'none'}")
                entries.append((metadata.get('created', ''), data['hash'], lines))

    worklogs = worklog_list()
    for worklog in worklogs:
        metadata = worklog['metadata']
        _, message = worklog_pick_message(worklog)
        lines = [f"Worklog: {worklog['uuid']}",
                 f"Date: {metadata.get('created', 'unknown')}",
                 f"Author: {helper_format_author(metadata)}",
                 f"Languages: {', '.join(sorted(worklog['messages']))}"]
        lines += [f"Attached: {reference}" for reference in worklog['references']]
        lines.append('')
        lines += [f"    {line}".rstrip() for line in message.split('\n')]
        entries.append((metadata.get('created', ''), worklog['uuid'], lines))

    # Newest first; ties broken by identifier so the order is stable
    entries.sort(key=lambda e: (e[0], e[1]), reverse=True)

    function_count = len(entries) - len(worklogs)
    print(f"Pool Log ({function_count} functions, {len(worklogs)} worklogs)")
    print("=" * 80)
    print()

    for _, _, lines in entries:
        for line in lines:
            print(line)
        print()


# ============================================================================
# Worklogs
# ============================================================================
#
# A worklog is a note about work done, like an empty git commit, that can be
# attached to function translations. It is neither a function nor a mapping:
#
#     $BB_DIRECTORY/worklog/<uuid7>/worklog.json
#         uuid, original language, author metadata, attached translations
#     $BB_DIRECTORY/worklog/<uuid7>/<lang>/XX/YYYY.../message.json
#         one translation of the message (the original included),
#         content-addressed by the hash of {"language", "message"}

def storage_uuid7() -> uuid.UUID:
    """
    Create a UUIDv7: 48 bits of Unix time in milliseconds, then random bits.

    UUIDv7 values sort by creation time. Uses uuid.uuid7 when Python has it (3.14+).
    """
    if hasattr(uuid, 'uuid7'):
        return uuid.uuid7()
    timestamp_ms = time.time_ns() // 1_000_000
    random_bits = int.from_bytes(os.urandom(10), 'big')
    rand_a = random_bits >> 68                   # 12 bits
    rand_b = random_bits & ((1 << 62) - 1)       # 62 bits
    value = ((timestamp_ms & ((1 << 48) - 1)) << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return uuid.UUID(int=value)


def storage_get_worklog_directory() -> Path:
    """
    Get the directory where worklogs are stored.
    Returns: $BB_DIRECTORY/worklog/
    """
    return storage_get_bb_directory() / 'worklog'


def worklog_compute_message_hash(lang: str, message: str) -> str:
    """Hash one worklog translation from its canonical JSON form."""
    canonical_json = json.dumps({'language': lang, 'message': message}, sort_keys=True, ensure_ascii=False)
    return hash_compute(canonical_json)


def worklog_save_message(worklog_id: str, lang: str, message: str) -> str:
    """
    Save one translation of a worklog message.

    Returns:
        The translation hash
    """
    message_hash = worklog_compute_message_hash(lang, message)
    message_dir = storage_get_worklog_directory() / worklog_id / lang / message_hash[:2] / message_hash[2:]
    message_dir.mkdir(parents=True, exist_ok=True)
    with open(message_dir / 'message.json', 'w', encoding='utf-8') as f:
        json.dump({'language': lang, 'message': message}, f, indent=2, ensure_ascii=False)
    return message_hash


def worklog_save(lang: str, message: str, references: List[str]) -> Tuple[str, str]:
    """
    Create a worklog with its original message.

    Args:
        lang: Language of the message
        message: The message
        references: Attached translations, as hash@lang@mapping_hash

    Returns:
        (worklog uuid, translation hash)
    """
    worklog_id = str(storage_uuid7())
    worklog_dir = storage_get_worklog_directory() / worklog_id
    worklog_dir.mkdir(parents=True)

    data = {
        'schema_version': 1,
        'uuid': worklog_id,
        'language': lang,
        'references': references,
        'metadata': code_create_metadata(),
    }
    with open(worklog_dir / 'worklog.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return worklog_id, worklog_save_message(worklog_id, lang, message)


def worklog_load(worklog_id: str) -> Dict[str, Any]:
    """
    Load a worklog with all its translations.

    Returns:
        The worklog.json data plus 'messages': {lang: [(hash, message), ...]}
    """
    worklog_dir = storage_get_worklog_directory() / worklog_id
    with open(worklog_dir / 'worklog.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    messages: Dict[str, List[Tuple[str, str]]] = {}
    for message_json in sorted(worklog_dir.glob('*/*/*/message.json')):
        with open(message_json, 'r', encoding='utf-8') as f:
            message = json.load(f)
        message_hash = message_json.parent.parent.name + message_json.parent.name
        messages.setdefault(message['language'], []).append((message_hash, message['message']))
    data['messages'] = messages
    return data


def worklog_list() -> List[Dict[str, Any]]:
    """List every worklog, whatever its language, newest first."""
    worklog_dir = storage_get_worklog_directory()
    if not worklog_dir.exists():
        return []
    worklogs = [worklog_load(d.name) for d in worklog_dir.iterdir() if (d / 'worklog.json').exists()]
    # UUIDv7 sorts by creation time
    worklogs.sort(key=lambda w: w['uuid'], reverse=True)
    return worklogs


def worklog_resolve(identifier: str) -> str:
    """Resolve a full worklog uuid or a unique prefix of one."""
    worklog_dir = storage_get_worklog_directory()
    candidates = [d.name for d in worklog_dir.iterdir()] if worklog_dir.exists() else []
    matches = [c for c in candidates if c.startswith(identifier.lower())]
    if len(matches) != 1:
        problem = "No worklog matches" if not matches else "Several worklogs match"
        print(f"Error: {problem}: {identifier}", file=sys.stderr)
        sys.exit(1)
    return matches[0]


def worklog_pick_message(worklog: Dict[str, Any], lang: str = None) -> Tuple[str, str]:
    """
    Pick the message to display: lang, then preferred languages, then the original.

    Returns:
        (language, message)
    """
    preferred = [lang] if lang else []
    preferred += storage_read_config()['user'].get('languages') or []
    for candidate in preferred + [worklog['language']]:
        if candidate in worklog['messages']:
            return candidate, worklog['messages'][candidate][0][1]
    first = sorted(worklog['messages'])[0]
    return first, worklog['messages'][first][0][1]


def helper_resolve_translation(identifier: str) -> str:
    """
    Resolve an identifier to an exact translation: hash@lang@mapping_hash.

    Accepts name@lang, hash@lang and hash@lang@mapping_hash.
    """
    parts = identifier.split('@')
    if len(parts) == 3:
        func_hash, lang, mapping_hash = parts
        if mapping_hash not in [m for m, _ in mappings_list(func_hash, lang)]:
            print(f"Error: Translation not found: {identifier}", file=sys.stderr)
            sys.exit(1)
        return identifier

    func_hash, lang = helper_parse_identifier(identifier)
    if lang is None:
        print(f"Error: Missing language suffix: {identifier} (use name@lang or hash@lang)", file=sys.stderr)
        sys.exit(1)
    mapping_hash = mapping_select(func_hash, lang)
    if mapping_hash is None:
        print(f"Error: {func_hash[:12]}... has no '{lang}' translation", file=sys.stderr)
        sys.exit(1)
    return f"{func_hash}@{lang}@{mapping_hash}"


def helper_edit_message(lang: str, context: str = "") -> str:
    """
    Ask for a message in $EDITOR. Lines starting with # are ignored.

    Args:
        lang: Language the message should be written in (shown to the user)
        context: Optional text shown as # comments, e.g. the original message

    Returns:
        The message, stripped; empty when the user wrote nothing
    """
    lines = ['', f'# Write the worklog message above, in {lang}.', '# Lines starting with # are ignored.']
    if context:
        lines += ['#'] + [f'# {line}'.rstrip() for line in context.split('\n')]
    fd, path = tempfile.mkstemp(prefix='worklog-', suffix='.txt')
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    try:
        helper_run_editor(path)
        with open(path, 'r', encoding='utf-8') as f:
            kept = [line.rstrip() for line in f if not line.startswith('#')]
    finally:
        os.unlink(path)
    return '\n'.join(kept).strip()


def command_worklog_new(references: List[str], message: str = None):
    """
    Create a worklog in your preferred language.

    A worklog is a note about work done, like an empty git commit. It can be
    attached to any number of function translations (name@lang, hash@lang
    or hash@lang@mapping_hash), or to none. The message is asked for in
    $EDITOR when --message is not given.

    Examples:

    ```
    bb worklog new -m "Explored faster series for pi"
    bb worklog new compute_pi@eng calculer_pi@fra -m "Reviewed both translations"
    ```
    """
    preferred = storage_read_config()['user'].get('languages') or ['eng']
    lang = preferred[0]
    resolved = [helper_resolve_translation(r) for r in references]

    if message is None:
        message = helper_edit_message(lang)
    message = message.strip()
    if not message:
        print("Error: Empty worklog message, nothing saved", file=sys.stderr)
        sys.exit(1)

    worklog_id, message_hash = worklog_save(lang, message, resolved)
    print(f"Worklog: {worklog_id}")
    print(f"Message hash: {message_hash}")


def command_worklog_translate(identifier: str, lang: str, message: str = None):
    """
    Add a translation of a worklog message.

    The original message is shown in $EDITOR when --message is not given.
    """
    worklog_id = worklog_resolve(identifier)
    worklog = worklog_load(worklog_id)

    if message is None:
        _, original = worklog_pick_message(worklog, worklog['language'])
        message = helper_edit_message(lang, context=f"Original ({worklog['language']}):\n{original}")
    message = message.strip()
    if not message:
        print("Error: Empty worklog message, nothing saved", file=sys.stderr)
        sys.exit(1)

    print(f"Message hash: {worklog_save_message(worklog_id, lang, message)}")


def command_worklog_list():
    """
    List every worklog, whatever its language, newest first.

    Each worklog shows in your preferred language when it has a translation
    in it, otherwise in its original language.
    """
    worklogs = worklog_list()
    if not worklogs:
        print("No worklogs.")
        return

    for worklog in worklogs:
        shown_lang, message = worklog_pick_message(worklog)
        metadata = worklog['metadata']
        print(f"Worklog: {worklog['uuid']}")
        print(f"Date: {metadata.get('created', 'unknown')}")
        print(f"Author: {helper_format_author(metadata)}")
        print(f"Languages: {', '.join(sorted(worklog['messages']))}")
        for reference in worklog['references']:
            print(f"Attached: {reference}")
        print()
        for line in message.split('\n'):
            print(f"    {line}".rstrip())
        print()


def command_worklog_show(identifier: str):
    """
    Show one worklog: identifier is a uuid (or unique prefix), with optional @lang.
    """
    part, _, lang = identifier.partition('@')
    worklog = worklog_load(worklog_resolve(part))
    if lang and lang not in worklog['messages']:
        print(f"Error: Worklog has no '{lang}' translation (has: {', '.join(sorted(worklog['messages']))})",
              file=sys.stderr)
        sys.exit(1)
    shown_lang, message = worklog_pick_message(worklog, lang or None)
    print(f"Worklog: {worklog['uuid']}@{shown_lang}")
    for reference in worklog['references']:
        print(f"Attached: {reference}")
    print()
    print(message)


def helper_format_author(metadata: Dict[str, Any]) -> str:
    """Format the author recorded in metadata as 'name <email>'."""
    name = metadata.get('name') or 'unknown'
    email = metadata.get('email')
    return f"{name} <{email}>" if email else name


def storage_scan_functions() -> Dict[str, Dict[str, Any]]:
    """
    Read every function in the pool once: metadata, plus the name and
    comment of its current mapping in each language.

    Returns:
        {hash: {'hash', 'metadata', 'names': {lang: name}, 'comments': {lang: comment}}}
    """
    functions: Dict[str, Dict[str, Any]] = {}
    pool_dir = storage_get_pool_directory()
    if not pool_dir.exists():
        return functions

    for prefix_dir in pool_dir.iterdir():
        if not prefix_dir.is_dir():
            continue
        for func_dir in prefix_dir.iterdir():
            object_json = func_dir / 'object.json'
            if not object_json.exists():
                continue
            try:
                with open(object_json, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (IOError, json.JSONDecodeError):
                continue
            func_hash = data['hash']
            names, comments = {}, {}
            for lang_dir in func_dir.iterdir():
                if not lang_dir.is_dir():
                    continue
                mapping_hash = mapping_select(func_hash, lang_dir.name)
                if mapping_hash is None:
                    continue
                mapping = mapping_load_json(func_hash, lang_dir.name, mapping_hash)
                names[lang_dir.name] = mapping.get('name_mapping', {}).get('_bb_v_0')
                comments[lang_dir.name] = mapping.get('comment', '')
            functions[func_hash] = {
                'hash': func_hash,
                'metadata': data.get('metadata', {}),
                'checks': data.get('checks', []),
                'names': names,
                'comments': comments,
            }
    return functions


def helper_find_heads_by_name(name: str, lang: str = None) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """
    Find the current versions of the functions called name.

    Matches functions named name in lang (or in any language), then
    follows their lineage forward, in every language, to the versions no
    other function names as its parent. A fork that renamed the function
    still supersedes it. Creation time plays no part, so two agents
    forking the same function give two current versions.

    Returns:
        (current version hashes, newest first; the scan from storage_scan_functions)
    """
    functions = storage_scan_functions()

    children: Dict[str, List[str]] = {}
    for function in functions.values():
        parent = function['metadata'].get('parent')
        if parent:
            children.setdefault(parent, []).append(function['hash'])

    if lang:
        matches = [h for h, f in functions.items() if f['names'].get(lang) == name]
    else:
        matches = [h for h, f in functions.items() if name in f['names'].values()]

    heads, seen, stack = set(), set(), list(matches)
    while stack:
        func_hash = stack.pop()
        if func_hash in seen:
            continue
        seen.add(func_hash)
        descendants = children.get(func_hash, [])
        if descendants:
            stack.extend(descendants)
        else:
            heads.add(func_hash)

    ordered = sorted(heads, key=lambda h: (functions[h]['metadata'].get('created', ''), h), reverse=True)
    return ordered, functions


def helper_describe_function(function: Dict[str, Any], lang: str = None) -> str:
    """One line describing a function: hash, date, author, languages and comment."""
    comment = function['comments'].get(lang) or next((c for c in function['comments'].values() if c), '')
    parts = [function['hash'], function['metadata'].get('created', 'unknown'),
             helper_format_author(function['metadata']), ', '.join(sorted(function['names']))]
    if comment:
        parts.append(comment)
    return '  '.join(parts)


def helper_resolve_name(name: str, lang: str = None) -> Tuple[str, str]:
    """
    Resolve a function name to its single current version.

    Exits with an error, instead of guessing, when the name has no
    current version, several of them, or when the current version lacks
    the requested language.

    Returns:
        (hash, lang) where lang is the requested one, or a language the
        function has (one where it is called name when possible)
    """
    lang_str = f"@{lang}" if lang else ""
    heads, functions = helper_find_heads_by_name(name, lang)

    if not heads:
        print(f"No function found with name: {name}{lang_str}", file=sys.stderr)
        sys.exit(1)

    if len(heads) > 1:
        print(f"Error: '{name}{lang_str}' is ambiguous: it has {len(heads)} current versions. "
              f"Use a hash:", file=sys.stderr)
        for func_hash in heads:
            print(f"  {helper_describe_function(functions[func_hash], lang)}", file=sys.stderr)
        sys.exit(1)

    head = heads[0]
    names = functions[head]['names']

    if lang and lang not in names:
        # Walk back to the newest ancestor that has the language
        ancestor = functions[head]['metadata'].get('parent')
        while ancestor in functions and lang not in functions[ancestor]['names']:
            ancestor = functions[ancestor]['metadata'].get('parent')
        source = helper_pick_language(head)
        print(f"Error: The current version of '{name}', {head}, has no '{lang}' translation.", file=sys.stderr)
        print(f"Translate it with: bb translate {head}@{source} {lang}", file=sys.stderr)
        if ancestor in functions:
            print(f"Or use the last version that has one: {ancestor}@{lang}", file=sys.stderr)
        sys.exit(1)

    matching = [l for l, n in sorted(names.items()) if n == name]
    return head, lang or (matching[0] if matching else helper_pick_language(head))


def command_latest(name: str, lang: str = None):
    """
    List the current versions of a function by name.

    A current version is one that no other function names as its parent.
    One line per version means the name is unambiguous; several mean
    forks diverged (or unrelated functions share the name) and commands
    need a hash.

    Args:
        name: Function name to search for
        lang: Optional language code to filter results
    """
    heads, functions = helper_find_heads_by_name(name, lang)
    lang_str = f"@{lang}" if lang else ""
    if not heads:
        print(f"No function found with name: {name}{lang_str}", file=sys.stderr)
        sys.exit(1)

    print(f"Current versions of '{name}{lang_str}': {len(heads)}")
    print()
    for func_hash in heads:
        function = functions[func_hash]
        metadata = function['metadata']
        print(f"Hash: {func_hash}")
        print(f"Date: {metadata.get('created', 'unknown')}")
        print(f"Author: {helper_format_author(metadata)}")
        if metadata.get('parent'):
            print(f"Parent: {metadata['parent']}")
        print(f"Languages: {', '.join(sorted(function['names']))}")
        print()

    if len(heads) > 1:
        print("Several current versions: use a hash to pick one.")


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
                    metadata = data.get('metadata', {})
                    created = metadata.get('created', 'unknown')

                    # Get available languages and search in mappings
                    for lang_dir in func_dir.iterdir():
                        if lang_dir.is_dir() and len(lang_dir.name) == 3:
                            lang = lang_dir.name
                            try:
                                _, name_mapping, _, docstring = code_load(func_hash, lang)
                                func_name = name_mapping.get('_bb_v_0', 'unknown')

                                # Search in function name, docstring, and original variable names
                                all_original_names = ' '.join(name_mapping.values()).lower()
                                searchable = f"{func_name} {docstring} {all_original_names}".lower()

                                if any(term in searchable for term in search_terms):
                                    # Determine where match was found
                                    match_in = []
                                    if any(term in func_name.lower() for term in search_terms):
                                        match_in.append('name')
                                    if any(term in docstring.lower() for term in search_terms):
                                        match_in.append('docstring')
                                    if any(term in all_original_names for term in search_terms):
                                        if 'name' not in match_in:  # Don't duplicate if func name matched
                                            match_in.append('variables')

                                    results.append({
                                        'hash': func_hash,
                                        'name': func_name,
                                        'lang': lang,
                                        'docstring': docstring[:100],  # First 100 chars
                                        'match_in': match_in,
                                        'created': created
                                    })
                                    break
                            except SystemExit:
                                continue
                except (IOError, json.JSONDecodeError):
                    continue

    # Sort by creation date (newest first)
    results.sort(key=lambda x: x['created'], reverse=True)

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
        print(f"Created: {result['created']}")
        print(f"Match: {match_str}")
        if result['docstring']:
            print(f"Description: {result['docstring']}...")
        print(f"View: bb show {result['hash']}@{result['lang']}")
        print()


def code_strip_bb_imports(code: str) -> str:
    """
    Strip bb.pool import statements from code.

    These imports are handled separately by loading dependencies into the namespace.

    Args:
        code: Source code with possible bb.pool imports

    Returns:
        Code with bb.pool imports removed
    """
    tree = ast.parse(code)

    # Filter out bb.pool imports
    new_body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            continue
        new_body.append(node)

    tree.body = new_body
    return ast.unparse(tree)


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


def command_run(identifier: str, func_args: list = None):
    """
    Execute a function from the pool.

    Accepts a hash or function name (with optional @lang suffix).
    Refuses unless you approved the function and its dependencies with
    bb review, then runs the exact code that was reviewed, with the
    provided arguments (passed as strings). The return value is not
    printed: functions do their own output.
    """
    hash_value, _ = helper_parse_identifier(identifier)
    review_require(hash_value, "run")

    try:
        func = code_load_normalized(hash_value)
    except ValueError as e:
        print(f"Error executing function: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        func(*(func_args or []))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def command_translate(identifier_with_lang: str, target_lang: str, recursive: bool = False):
    """
    Add a translation for an existing function.

    Accepts a hash or function name with required @lang suffix for source language.
    Prompts for new variable names and docstring in target language.

    Examples:
        bb translate abc123...@eng fra
        bb translate compute_pi@eng fra
    """
    # @lang is required for translate (source language)
    if '@' not in identifier_with_lang:
        print("Error: Missing @lang suffix. Use format: identifier@source_lang", file=sys.stderr)
        sys.exit(1)

    hash_value, source_lang = helper_parse_identifier(identifier_with_lang)

    if len(target_lang) < 3 or len(target_lang) > 256:
        print(f"Error: Target language code must be 3-256 characters. Got: {target_lang}", file=sys.stderr)
        sys.exit(1)

    # Load source function
    try:
        normalized_code, name_mapping_source, alias_mapping_source, docstring_source = code_load(hash_value, source_lang)
    except SystemExit:
        print(f"Error: Could not load function {hash_value}@{source_lang}", file=sys.stderr)
        sys.exit(1)
    # The new translation records the exact one it was made from
    source_translation = f"{hash_value}@{source_lang}@{mapping_select(hash_value, source_lang)}"

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
    mapping_hash = mapping_save(hash_value, target_lang, target_docstring, name_mapping_target, alias_mapping_target,
                                comment, parent=source_translation)

    print(f"Mapping hash: {mapping_hash}")
    print()
    print(f"Translation saved successfully!")
    print(f"View with: bb show {hash_value}@{target_lang}")

    # Recursively translate dependencies if requested
    if recursive:
        print()
        print("=" * 60)
        print("Recursively translating dependencies...")
        print("=" * 60)
        print()

        # Extract dependencies from normalized code
        dependencies = code_extract_dependencies(normalized_code)

        if not dependencies:
            print("No dependencies to translate.")
            return

        # Translate each dependency that doesn't already have a target language translation
        for dep_hash in dependencies:
            # Check if target language already exists
            try:
                code_load(dep_hash, target_lang)
                print(f"✓ {dep_hash}@{target_lang} already exists")
                continue
            except SystemExit:
                # Target language doesn't exist, translate it
                pass

            # Check if source language exists
            try:
                code_load(dep_hash, source_lang)
            except SystemExit:
                print(f"⚠ Warning: {dep_hash}@{source_lang} not found, skipping")
                continue

            print()
            print(f"→ Translating dependency: {dep_hash}")
            print()

            # Recursively translate
            command_translate(f"{dep_hash}@{source_lang}", target_lang, recursive=True)


def command_rm(identifier: str):
    """
    Remove a function or a specific language mapping.

    Accepts a hash or function name (with optional @lang suffix).

    - bb rm identifier@lang - Remove only the language mapping
    - bb rm identifier - Remove the entire function with all mappings
    """
    import shutil

    hash_value, lang = helper_parse_identifier(identifier)
    remove_lang_only = lang is not None

    pool_dir = storage_get_pool_directory()
    func_dir = pool_dir / hash_value[:2] / hash_value[2:]

    if remove_lang_only:
        # Remove only the language mapping
        lang_dir = func_dir / lang
        if not lang_dir.exists():
            print(f"Error: Language mapping not found: {hash_value}@{lang}", file=sys.stderr)
            sys.exit(1)

        # Remove the language directory
        shutil.rmtree(lang_dir)
        print(f"Removed language mapping: {hash_value}@{lang}")

        # Check if there are any remaining languages
        remaining_langs = [item.name for item in func_dir.iterdir()
                          if item.is_dir() and len(item.name) >= 3]
        if remaining_langs:
            print(f"Remaining languages: {', '.join(sorted(remaining_langs))}")
        else:
            print("Warning: No language mappings remain for this function")
    else:
        # Remove the entire function
        if not func_dir.exists():
            print(f"Error: Function directory not found: {hash_value}", file=sys.stderr)
            sys.exit(1)

        # Get list of languages before deletion
        languages = [item.name for item in func_dir.iterdir()
                    if item.is_dir() and len(item.name) >= 3]

        # Remove the function directory
        shutil.rmtree(func_dir)
        print(f"Removed function: {hash_value}")
        if languages:
            print(f"Deleted {len(languages)} language mapping(s): {', '.join(sorted(languages))}")


def code_add(file_path_with_lang: str, comment: str = ""):
    """
    Add a function to the pool.

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

    # Read and prepare the source
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    try:
        prepared = code_prepare_source(source_code, lang, where=file_path)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Save to v1 format (docstring stored separately in mapping.json)
    code_save(prepared['hash'], lang, prepared['normalized_code'], prepared['docstring'],
              prepared['name_mapping'], prepared['alias_mapping'], comment)


def code_prepare_source(source_code: str, lang: str, where: str = None) -> Dict[str, Any]:
    """
    Parse, normalize and hash a function's source, without saving it.

    Verifies that every bb import exists in the local pool.

    Args:
        source_code: Python source with imports and one function definition
        lang: Language code of the names and docstring
        where: Optional label used in error messages (defaults to "source")

    Returns:
        Dict with hash, normalized_code (without docstring), docstring,
        name_mapping and alias_mapping

    Raises:
        ValueError: With a user-facing message when the source is not valid
    """
    where = where or "source"

    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        raise ValueError(f"Failed to parse {where}: {e}")

    # Fail early with a clear message if there is no function definition
    code_extract_definition(tree)

    try:
        _, normalized_code_without_docstring, docstring, name_mapping, alias_mapping = code_normalize(tree, lang)
    except Exception as e:
        raise ValueError(f"Failed to normalize AST: {e}")

    # Verify all bb imports resolve to objects in the local pool
    pool_dir = storage_get_pool_directory()
    missing_deps = [(dep_hash, alias) for dep_hash, alias in alias_mapping.items()
                    if not (pool_dir / dep_hash[:2] / dep_hash[2:] / 'object.json').exists()]
    if missing_deps:
        lines = ["The following bb imports do not exist in the local pool:"]
        lines += [f"  - {alias} (hash: {dep_hash[:12]}...)" for dep_hash, alias in missing_deps]
        lines += ["", "Please add these functions to the pool first, or fetch them with git."]
        raise ValueError('\n'.join(lines))

    return {
        # Hash on code WITHOUT docstring (same logic = same hash regardless of language)
        'hash': code_compute_function_hash(normalized_code_without_docstring),
        'normalized_code': normalized_code_without_docstring,
        'docstring': docstring,
        'name_mapping': name_mapping,
        'alias_mapping': alias_mapping,
    }


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


# ============================================================================
# Edit file format (bb edit)
# ============================================================================
#
# An edit file is markdown with two sections:
#
#     # name
#
#     ## Implementation
#     <!-- bb:parent HASH -->
#     prose, examples...
#     ```python def
#     def name(...): ...
#     ```
#
#     ## Checks
#
#     ### check_name
#     <!-- bb:parent HASH -->
#     prose...
#     ```python def
#     def check_name(name): ...
#     ```
#
# In each section, the single block fenced as ```python def is the function
# that gets hashed. Everything else is documentation, stored in the language
# mapping under the 'markdown' key. Lines like <!-- bb:... --> are written by
# bb and are not documentation.

EDIT_DEFINITION_INFO = 'python def'
EDIT_IMPLEMENTATION_HEADING = 'Implementation'
EDIT_CHECKS_HEADING = 'Checks'
EDIT_CHECKS_HINT = ('<!-- bb: add checks as ### subsections. Each one needs a ```python def block '
                    'whose function takes the implementation as its only argument and returns True when it passes. -->')

_EDIT_FENCE_RE = re.compile(r'^(`{3,}|~{3,})\s*(.*?)\s*$')
_EDIT_MARKER_RE = re.compile(r'^<!--\s*bb:(.*?)\s*-->$')
_EDIT_PARENT_RE = re.compile(r'^parent\s+([0-9a-f]{64})$')


class EditFormatError(ValueError):
    """Raised when an edit file does not follow the bb edit format."""


def markdown_split_lines(text: str) -> List[Tuple[str, str]]:
    """
    Split markdown into lines, tagging each one with its role.

    Returns:
        List of (line, kind) tuples where kind is 'text', 'open'
        (opening fence), 'body' (inside a fence) or 'close' (closing fence)
    """
    result = []
    fence = None
    for line in text.split('\n'):
        match = _EDIT_FENCE_RE.match(line)
        if fence is None:
            if match:
                fence = match.group(1)
                result.append((line, 'open'))
            else:
                result.append((line, 'text'))
        elif (match and match.group(2) == '' and match.group(1)[0] == fence[0]
              and len(match.group(1)) >= len(fence)):
            fence = None
            result.append((line, 'close'))
        else:
            result.append((line, 'body'))
    if fence is not None:
        raise EditFormatError("Unclosed fenced code block")
    return result


def markdown_strip_blank(lines: List[str]) -> List[str]:
    """Remove leading and trailing blank lines."""
    start, end = 0, len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def markdown_parse_section(lines: List[Tuple[str, str]], where: str) -> Dict[str, Any]:
    """
    Parse one section of an edit file.

    Args:
        lines: (line, kind) tuples of the section body, heading excluded
        where: Section name used in error messages

    Returns:
        Dict with 'source' (the python def block), 'markdown' (the rest)
        and 'parent' (hash from a bb:parent marker, or None)
    """
    source = None
    parent = None
    doc = []
    i = 0
    while i < len(lines):
        line, kind = lines[i]
        if kind == 'text':
            marker = _EDIT_MARKER_RE.match(line.strip())
            if marker:
                found = _EDIT_PARENT_RE.match(marker.group(1))
                if found:
                    if parent is not None:
                        raise EditFormatError(f"{where}: more than one bb:parent marker")
                    parent = found.group(1)
                i += 1
                continue
            doc.append(line)
            i += 1
            continue
        # Opening fence: collect the whole block
        opening = line
        info = _EDIT_FENCE_RE.match(line).group(2)
        j = i + 1
        while lines[j][1] != 'close':
            j += 1
        body = [l for l, _ in lines[i + 1:j]]
        if info == EDIT_DEFINITION_INFO:
            if source is not None:
                raise EditFormatError(f"{where}: more than one ```{EDIT_DEFINITION_INFO} block")
            source = '\n'.join(body).strip('\n') + '\n'
            # Drop one blank line left behind by the removed block
            if doc and not doc[-1].strip():
                doc.pop()
        else:
            doc.extend([opening] + body + [lines[j][0]])
        i = j + 1

    if source is None:
        raise EditFormatError(f"{where}: missing ```{EDIT_DEFINITION_INFO} block")

    return {
        'source': source,
        'markdown': '\n'.join(markdown_strip_blank(doc)),
        'parent': parent,
    }


def markdown_parse_edit(text: str) -> Dict[str, Any]:
    """
    Parse an edit file into its implementation and checks.

    Returns:
        Dict with 'implementation' (a section dict) and 'checks'
        (list of section dicts, each with an extra 'title')

    Raises:
        EditFormatError: If the file does not follow the edit format
    """
    lines = markdown_split_lines(text)

    # Group lines under their level-2 headings
    sections: Dict[str, List[Tuple[str, str]]] = {}
    current = None
    for line, kind in lines:
        if kind == 'text' and line.startswith('## '):
            current = line[3:].strip()
            if current not in (EDIT_IMPLEMENTATION_HEADING, EDIT_CHECKS_HEADING):
                raise EditFormatError(f"Unknown section: '## {current}' "
                                      f"(expected '## {EDIT_IMPLEMENTATION_HEADING}' or '## {EDIT_CHECKS_HEADING}')")
            if current in sections:
                raise EditFormatError(f"Duplicate section: '## {current}'")
            sections[current] = []
        elif current is None:
            if line.strip() and not line.startswith('# ') and not _EDIT_MARKER_RE.match(line.strip()):
                raise EditFormatError(f"Text before '## {EDIT_IMPLEMENTATION_HEADING}' would be lost: {line.strip()!r}")
        else:
            sections[current].append((line, kind))

    if EDIT_IMPLEMENTATION_HEADING not in sections:
        raise EditFormatError(f"Missing '## {EDIT_IMPLEMENTATION_HEADING}' section")

    implementation = markdown_parse_section(sections[EDIT_IMPLEMENTATION_HEADING], EDIT_IMPLEMENTATION_HEADING)

    # Split the checks section into level-3 subsections
    checks = []
    title = None
    body: List[Tuple[str, str]] = []
    for line, kind in sections.get(EDIT_CHECKS_HEADING, []) + [('### ', 'text')]:
        if kind == 'text' and line.startswith('### '):
            if title is not None:
                check = markdown_parse_section(body, f"Check '{title}'")
                check['title'] = title
                checks.append(check)
            title = line[4:].strip()
            body = []
        elif title is None:
            if line.strip() and not _EDIT_MARKER_RE.match(line.strip()):
                raise EditFormatError(f"Text in '## {EDIT_CHECKS_HEADING}' outside a ### subsection would be lost: {line.strip()!r}")
        else:
            body.append((line, kind))

    return {'implementation': implementation, 'checks': checks}


def markdown_render_section(section: Dict[str, Any]) -> List[str]:
    """Render a section body: parent marker, documentation, then the python def block."""
    out = []
    if section.get('parent'):
        out.append(f"<!-- bb:parent {section['parent']} -->")
        out.append('')
    if section.get('markdown'):
        out.append(section['markdown'])
        out.append('')
    out.append(f"```{EDIT_DEFINITION_INFO}")
    out.append(section['source'].rstrip('\n'))
    out.append('```')
    return out


def markdown_render_edit(name: str, implementation: Dict[str, Any], checks: List[Dict[str, Any]]) -> str:
    """
    Render an edit file from an implementation section and check sections.

    Each check section needs a 'title' (used as its ### heading).
    """
    out = [f"# {name}", '', f"## {EDIT_IMPLEMENTATION_HEADING}", '']
    out.extend(markdown_render_section(implementation))
    out.extend(['', f"## {EDIT_CHECKS_HEADING}", ''])
    if not checks:
        out.extend([EDIT_CHECKS_HINT, ''])
    for check in checks:
        out.extend([f"### {check['title']}", ''])
        out.extend(markdown_render_section(check))
        out.append('')
    return '\n'.join(out).rstrip('\n') + '\n'


def object_load(hash_value: str) -> Dict[str, any]:
    """
    Load function object from bb directory.

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
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

    # Load the JSON data
    try:
        with open(object_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse object.json: {e}", file=sys.stderr)
        sys.exit(1)

    return data


def mappings_list(func_hash: str, lang: str) -> list:
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


def mapping_load(func_hash: str, lang: str, mapping_hash: str) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a specific language mapping.

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


def mapping_load_json(func_hash: str, lang: str, mapping_hash: str) -> Dict[str, Any]:
    """Load the raw mapping.json data ({} when missing or unreadable)."""
    pool_dir = storage_get_pool_directory()
    mapping_json = (pool_dir / func_hash[:2] / func_hash[2:] / lang /
                    mapping_hash[:2] / mapping_hash[2:] / 'mapping.json')
    try:
        with open(mapping_json, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError):
        return {}


def mapping_load_markdown(func_hash: str, lang: str, mapping_hash: str) -> str:
    """
    Load the markdown documentation of a mapping ('' when it has none).
    """
    return mapping_load_json(func_hash, lang, mapping_hash).get('markdown', '')


def mapping_leaves(func_hash: str, lang: str) -> List[str]:
    """
    List the current mappings of a function in a language.

    A mapping that another mapping of the same function and language names
    as its parent was superseded and is left out.

    Returns:
        Mapping hashes, sorted
    """
    mappings = [m for m, _ in mappings_list(func_hash, lang)]
    prefix = f"{func_hash}@{lang}@"
    superseded = set()
    for mapping_hash in mappings:
        parent = mapping_load_json(func_hash, lang, mapping_hash).get('parent', '')
        if parent.startswith(prefix):
            superseded.add(parent[len(prefix):])
    return sorted(m for m in mappings if m not in superseded)


def mapping_select(func_hash: str, lang: str) -> Optional[str]:
    """
    Pick the mapping used by default for a function in a language.

    Superseded mappings are skipped; among the remaining variants the
    first one alphabetically wins for now.

    Returns:
        Mapping hash, or None if the language has no mapping
    """
    leaves = mapping_leaves(func_hash, lang)
    return leaves[0] if leaves else None


def code_load(hash_value: str, lang: str, mapping_hash: str = None) -> Tuple[str, Dict[str, str], Dict[str, str], str]:
    """
    Load a function from the bb pool (v1 format only).

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
    func_data = object_load(hash_value)
    normalized_code = func_data['normalized_code']

    # Determine which mapping to load
    if mapping_hash is not None:
        # Explicit mapping requested
        selected_hash = mapping_hash
    else:
        selected_hash = mapping_select(hash_value, lang)
        if selected_hash is None:
            print(f"Error: No mappings found for language '{lang}'", file=sys.stderr)
            sys.exit(1)

    # Load the mapping
    docstring, name_mapping, alias_mapping, comment = mapping_load(hash_value, lang, selected_hash)

    return normalized_code, name_mapping, alias_mapping, docstring


def code_show(identifier: str):
    """
    Show a function from the pool.

    Accepts a hash or function name (with optional @lang suffix).

    - identifier: List available languages
    - identifier@lang: Show single mapping, or menu if multiple exist
    - hash@lang@mapping_hash: Show specific mapping
    """
    # Special case: triple-@ format is always hash@lang@mapping
    parts = identifier.split('@')
    if len(parts) == 3:
        hash_value, lang, mapping_hash = parts
        # Validate hash format for triple-@ (must be a real hash)
        if len(hash_value) != 64 or not all(c in '0123456789abcdef' for c in hash_value.lower()):
            print(f"Error: Invalid hash format in triple-@ notation. Got: {hash_value}", file=sys.stderr)
            sys.exit(1)
        version = code_detect_schema(hash_value)
        if version is None:
            print(f"Error: Function not found: {hash_value}", file=sys.stderr)
            sys.exit(1)
    else:
        # Use unified identifier parsing (hash, hash@lang, name, name@lang)
        mapping_hash = None
        hash_value, lang = helper_parse_identifier(identifier)

        if lang is None:
            # No language — list available languages
            languages = storage_list_languages(hash_value)
            if not languages:
                print(f"No languages found for {hash_value}", file=sys.stderr)
                sys.exit(1)

            print(f"Available languages for {hash_value}:")
            for l in languages:
                mappings = mappings_list(hash_value, l)
                print(f"  {l} - {len(mappings)} mapping(s)")
            return

    # Get available mappings for the language
    mappings = mappings_list(hash_value, lang)

    if len(mappings) == 0:
        print(f"Error: No mappings found for language '{lang}'", file=sys.stderr)
        sys.exit(1)

    # Determine which mapping to show
    if mapping_hash is not None:
        # Explicit mapping hash provided
        selected_hash = mapping_hash
    elif len(mapping_leaves(hash_value, lang)) == 1:
        # One current mapping (older ones were superseded) - show it directly
        selected_hash = mapping_leaves(hash_value, lang)[0]
    else:
        # Several current variants - show selection menu
        leaves = set(mapping_leaves(hash_value, lang))
        print(f"Multiple mappings found for '{lang}'. Please choose one:\n")
        for m_hash, comment in sorted(m for m in mappings if m[0] in leaves):
            comment_suffix = f"  # {comment}" if comment else ""
            print(f"bb show {hash_value}@{lang}@{m_hash}{comment_suffix}")
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



def schema_validate(func_hash: str) -> tuple:
    """
    Validate a function's storage structure.

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
    Validate the entire bb directory structure.

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

    bb_dir = storage_get_bb_directory()

    # Check if bb directory exists
    if not bb_dir.exists():
        errors.append(f"BB directory does not exist: {bb_dir}")
        return False, errors, stats

    # Validate config file
    config_path = bb_dir / 'config.json'
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
            is_valid, func_errors = schema_validate(func_hash)
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
        is_valid, _ = schema_validate(func_hash)
        if not is_valid:
            continue

        try:
            func_data = object_load(func_hash)
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


def command_caller(identifier: str):
    """
    Find all functions that depend on the given function.

    Accepts a hash or function name (with optional @lang suffix).
    Scans all functions in the pool and lists each function that
    imports the given hash.
    """
    hash_value = helper_resolve_to_hash(identifier)

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
        print(f"bb show {caller_hash}")


def command_tree(identifier: str):
    """
    Display the dependency tree of a function.

    Accepts a hash or function name (with optional @lang suffix).
    Shows function names and one-liner docstrings in a tree format similar
    to the Unix tree command. Uses the preferred language from bb whoami.
    """
    hash_value = helper_resolve_to_hash(identifier)

    # Get preferred language from config
    config = storage_read_config()
    preferred_langs = config['user'].get('languages', ['eng'])
    if not preferred_langs:
        preferred_langs = ['eng']
    lang = preferred_langs[0]

    _tree_print_node(hash_value, lang, "", True, True, set())


def _tree_print_node(func_hash: str, lang: str, prefix: str,
                     is_last: bool, is_root: bool, visited: set):
    """Print a single node and recurse into its dependencies."""
    # Load function info
    normalized_code = None
    name_mapping = None
    docstring = None

    try:
        normalized_code, name_mapping, _, docstring = code_load(func_hash, lang)
    except SystemExit:
        # Try any available language
        langs = storage_list_languages(func_hash)
        if langs:
            try:
                normalized_code, name_mapping, _, docstring = code_load(func_hash, langs[0])
            except SystemExit:
                pass

    if normalized_code is None:
        label = f"{func_hash[:12]}... (not found)"
        if is_root:
            print(label)
        else:
            print(f"{prefix}{'└── ' if is_last else '├── '}{label}")
        return

    func_name = name_mapping.get('_bb_v_0', func_hash[:12])

    # One-liner docstring
    doc_part = ""
    if docstring:
        first_line = docstring.strip().split('\n')[0].strip()
        doc_part = f" — {first_line}"

    # Print this node
    if is_root:
        print(f"{func_name}{doc_part}")
        child_prefix = ""
    else:
        connector = "└── " if is_last else "├── "
        print(f"{prefix}{connector}{func_name}{doc_part}")
        child_prefix = prefix + ("    " if is_last else "│   ")

    # Get dependencies
    deps = code_extract_dependencies(normalized_code)
    if not deps:
        return

    # Guard against cycles (but allow diamonds — each branch gets its own set)
    if func_hash in visited:
        return
    new_visited = visited | {func_hash}

    for i, dep_hash in enumerate(deps):
        _tree_print_node(dep_hash, lang, child_prefix, i == len(deps) - 1, False, new_visited)


def helper_parse_identifier(identifier: str) -> Tuple[str, Optional[str]]:
    """
    Parse a function identifier into (hash, lang).

    Accepts: hash, hash@lang, name, name@lang.
    Resolves names to their single current version (see helper_resolve_name).

    Examples:
        helper_parse_identifier("abc123...")        → ("abc123...", None)
        helper_parse_identifier("abc123...@eng")    → ("abc123...", "eng")
        helper_parse_identifier("compute_pi")       → ("abc123...", "eng")
        helper_parse_identifier("compute_pi@eng")   → ("abc123...", "eng")
    """
    if '@' in identifier:
        part, lang = identifier.rsplit('@', 1)
    else:
        part = identifier
        lang = None

    if len(part) == 64 and all(c in '0123456789abcdef' for c in part.lower()):
        # It's a hash — verify it exists
        version = code_detect_schema(part)
        if version is None:
            print(f"Error: Function not found: {part}", file=sys.stderr)
            sys.exit(1)
        return part, lang

    # It's a name — resolve it to its single current version
    return helper_resolve_name(part, lang)


def helper_resolve_to_hash(identifier: str) -> str:
    """
    Resolve a function identifier to a hash.

    Thin wrapper around helper_parse_identifier that returns only the hash.
    """
    func_hash, _ = helper_parse_identifier(identifier)
    return func_hash


# ============================================================================
# Checks and bb edit
# ============================================================================
#
# A check is an ordinary pool function that takes the implementation as its
# only argument and returns a truthy value when it passes. Checks do not
# import the function they test, so one check can run against every fork.
# They are part of an implementation's definition: object.json lists their
# hashes under 'checks', and they enter the function hash, so changing the
# checks forks the function. bb edit writes them.

def helper_pick_language(func_hash: str, lang: str = None) -> Optional[str]:
    """
    Pick the language to load a function in.

    Uses lang when the function has it, then the user's preferred languages,
    then the first available language.

    Returns:
        A language code, or None if the function has no mapping at all
    """
    languages = storage_list_languages(func_hash)
    if not languages:
        return None
    if lang in languages:
        return lang
    preferred = storage_read_config()['user'].get('languages') or []
    for candidate in preferred:
        if candidate in languages:
            return candidate
    return languages[0]


def code_execute_source(source_code: str, lang: str) -> Callable:
    """
    Execute unsaved source (from an edit file) and return its function.

    Only imports and the function definition run, the same parts that get
    hashed and stored. bb.pool imports are loaded from the local pool, from
    their normalized code (see code_load_normalized).

    Raises:
        ValueError: If a bb import cannot be loaded or the source fails to execute
    """
    tree = ast.parse(source_code)
    function_def, _ = code_extract_definition(tree)

    namespace: Dict[str, Any] = {}
    cache: Dict[str, Callable] = {}
    body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            for alias in node.names:
                dep = code_load_normalized(alias.name[len(BB_IMPORT_PREFIX):], cache)
                if alias.asname:
                    namespace[alias.asname] = dep
                else:
                    namespace[alias.name] = types.SimpleNamespace(_bb_v_0=dep)
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)) or node is function_def:
            body.append(node)
    tree.body = body

    try:
        exec(compile(tree, f'<bb edit {function_def.name}>', 'exec'), namespace)
    except Exception as e:
        raise ValueError(f"{function_def.name} failed to load: {type(e).__name__}: {e}")

    return namespace[function_def.name]


def check_run(check: Callable, implementation: Callable) -> Tuple[str, str]:
    """
    Run one check against an implementation.

    Returns:
        (status, detail) where status is PASS, FAIL or ERROR
    """
    try:
        result = check(implementation)
    except Exception as e:
        return 'ERROR', f"{type(e).__name__}: {e}"
    return ('PASS', '') if result else ('FAIL', '')


def check_report(results: List[Tuple[str, str, str]], indent: str = "") -> bool:
    """
    Print check results and a summary line.

    Args:
        results: List of (status, check name, detail)
        indent: Prefix for every printed line

    Returns:
        True when every check passed (also when there are none)
    """
    if not results:
        print(f"{indent}No checks.")
        return True

    counts = {'PASS': 0, 'FAIL': 0, 'ERROR': 0}
    for status, name, detail in results:
        counts[status] += 1
        print(f"{indent}{status} {name}" + (f": {detail}" if detail else ""))

    parts = []
    if counts['PASS']:
        parts.append(f"{counts['PASS']} passed")
    if counts['FAIL']:
        parts.append(f"{counts['FAIL']} failed")
    if counts['ERROR']:
        parts.append(f"{counts['ERROR']} errors")
    print(f"{indent}{len(results)} checks: {', '.join(parts)}")

    return counts['PASS'] == len(results)


def code_load_normalized(func_hash: str, cache: Dict[str, Callable] = None) -> Callable:
    """
    Load a pool function from its normalized code, exactly as hashed.

    No language mapping is applied, so what runs is the code that was
    hashed and reviewed, whatever names a mapping displays. Each function
    runs in its own namespace, with its bb imports bound to their own
    loaded functions.

    Raises:
        ValueError: If the function or a dependency is missing or fails to load
    """
    if cache is None:
        cache = {}
    if func_hash in cache:
        return cache[func_hash]

    if code_detect_schema(func_hash) is None:
        raise ValueError(f"Function not in the local pool: {func_hash}")
    tree = ast.parse(object_load(func_hash)['normalized_code'])

    namespace: Dict[str, Any] = {}
    body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            for alias in node.names:
                dep = code_load_normalized(alias.name[len(BB_IMPORT_PREFIX):], cache)
                namespace[alias.asname or alias.name] = types.SimpleNamespace(_bb_v_0=dep)
            continue
        body.append(node)
    tree.body = body

    try:
        exec(compile(tree, f'<bb {func_hash[:12]}>', 'exec'), namespace)
    except Exception as e:
        raise ValueError(f"{func_hash[:12]}... failed to load: {type(e).__name__}: {e}")

    cache[func_hash] = namespace['_bb_v_0']
    return cache[func_hash]


def helper_function_label(func_hash: str) -> str:
    """A function's name in your preferred language, or a short hash."""
    lang = helper_pick_language(func_hash)
    if lang is None:
        return f"{func_hash[:12]}..."
    mapping = mapping_load_json(func_hash, lang, mapping_select(func_hash, lang))
    return mapping.get('name_mapping', {}).get('_bb_v_0') or f"{func_hash[:12]}..."


def check_run_reviewed(func_hash: str, reviewed: set) -> Tuple[Optional[List[Tuple[str, str, str]]], List[str]]:
    """
    Run a function's checks, only if you approved everything that would run.

    Pool functions can come from anyone with access to the shared
    directory, and Python has no sandbox: reading the code first is the
    protection. So nothing runs unless the function, its checks, and all
    their dependencies are in your review approvals.

    Returns:
        (results for check_report, sorted by check name; []) when everything
        was approved, or (None, hashes still to approve) when nothing ran
    """
    missing = [h for h in review_closure(func_hash) if h not in reviewed]
    if missing:
        return None, missing

    cache: Dict[str, Callable] = {}
    try:
        implementation = code_load_normalized(func_hash, cache)
    except ValueError as e:
        return [('ERROR', helper_function_label(func_hash), str(e))], []

    results = []
    for check_hash in object_load_checks(func_hash):
        name = helper_function_label(check_hash)
        try:
            check = code_load_normalized(check_hash, cache)
        except ValueError as e:
            results.append(('ERROR', name, str(e)))
            continue
        status, detail = check_run(check, implementation)
        results.append((status, name, detail))

    results.sort(key=lambda r: r[1])
    return results, []


def command_validate_checks(identifier: str = None, unreviewed: bool = False):
    """
    Run the checks of reviewed functions: one function, or the whole pool.

    Only functions whose checks, and everything those checks run, you
    approved with bb review are run; the others are listed as not
    reviewed. Exits with status 1 when a check fails (or, for a single
    function, when it is not reviewed).

    With --dangerously-unreviewed, every check runs, approved or not. That
    is only safe on a disposable machine with nothing worth stealing (no
    secrets, no write access, a fresh runner), such as a CI job.
    """
    if unreviewed:
        print("Warning: running checks nobody reviewed (--dangerously-unreviewed). "
              "This machine must have no secrets and no write access.", file=sys.stderr)

    if identifier:
        targets = [helper_resolve_to_hash(identifier)]
    else:
        targets = sorted(h for h, f in storage_scan_functions().items() if f['checks'])

    failed = False
    not_reviewed = []
    for func_hash in targets:
        if not object_load_checks(func_hash):
            print("No checks.")
            continue
        closure = review_closure(func_hash)
        approved = set(closure) if unreviewed else review_approvals(closure)
        results, missing = check_run_reviewed(func_hash, approved)
        if missing:
            not_reviewed.append((func_hash, len(missing)))
            continue
        print(f"{helper_function_label(func_hash)} {func_hash}")
        if not check_report(results, indent="  "):
            failed = True
        print()

    if not_reviewed:
        print("Not reviewed, checks not run:")
        for func_hash, count in not_reviewed:
            print(f"  {helper_function_label(func_hash)} {func_hash} "
                  f"({count} to approve: bb review {func_hash})")

    if failed or (identifier and not_reviewed):
        sys.exit(1)


def edit_load_section(func_hash: str, lang: str) -> Dict[str, Any]:
    """
    Load a pool function as an edit-file section.

    Falls back to another language when the function has no mapping in lang.

    Returns:
        Section dict (source, markdown, parent, title) plus 'mapping', the
        exact translation loaded (hash@lang@mapping_hash), and
        'fingerprint', used to tell whether the user changed anything
    """
    section_lang = helper_pick_language(func_hash, lang)
    if section_lang is None:
        print(f"Error: Function not found in the local pool: {func_hash}", file=sys.stderr)
        sys.exit(1)

    mapping_hash = mapping_select(func_hash, section_lang)
    normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, section_lang, mapping_hash)
    source = code_denormalize(code_replace_docstring(normalized_code, docstring), name_mapping, alias_mapping)
    markdown = mapping_load_markdown(func_hash, section_lang, mapping_hash)

    return {
        'title': name_mapping.get('_bb_v_0', func_hash[:12]),
        'source': source + '\n',
        'markdown': markdown,
        'parent': func_hash,
        'mapping': f"{func_hash}@{section_lang}@{mapping_hash}",
        'fingerprint': edit_fingerprint(func_hash, docstring, name_mapping, alias_mapping, markdown),
    }


def edit_fingerprint(func_hash: str, docstring: str, name_mapping: Dict[str, str],
                     alias_mapping: Dict[str, str], markdown: str) -> Tuple[str, str]:
    """Identify a section by function hash and mapping content (comment excluded)."""
    return func_hash, code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "", markdown)


def edit_assign_parents(checks: List[Tuple[str, Optional[str]]], originals: List[Tuple[str, str]]) -> List[Optional[str]]:
    """
    Guess the parent of check sections that lost their bb:parent marker.

    An unmarked check descends from an original check that no marker
    claims: first one with the same function name, otherwise the one at
    the same position. A check that matches neither has no parent.

    Args:
        checks: (function name, parent from marker or None) per edited check, in file order
        originals: (function name, hash) per check that was loaded, in file order

    Returns:
        Parent hash (or None) per edited check
    """
    parents = [parent for _, parent in checks]
    available = {h for _, h in originals} - {p for p in parents if p}

    for i, (name, parent) in enumerate(checks):
        if parent is None:
            for original_name, original_hash in originals:
                if original_name == name and original_hash in available:
                    parents[i] = original_hash
                    available.discard(original_hash)
                    break

    for i, (name, parent) in enumerate(checks):
        if parents[i] is None and i < len(originals) and originals[i][1] in available:
            parents[i] = originals[i][1]
            available.discard(originals[i][1])

    return parents


def edit_prepare(text: str, lang: str) -> Dict[str, Any]:
    """
    Parse an edit file, normalize every section and run the checks.

    Returns:
        Dict with 'implementation' and 'checks' (lists of (section, prepared)
        pairs, as given by markdown_parse_edit and code_prepare_source) and
        'results' (check results for check_report)

    Raises:
        ValueError: With a user-facing message when the file cannot be used
    """
    parsed = markdown_parse_edit(text)

    implementation = parsed['implementation']
    prepared_implementation = code_prepare_source(implementation['source'], lang, where="Implementation")
    checks = [(check, code_prepare_source(check['source'], lang, where=f"check '{check['title']}'"))
              for check in parsed['checks']]

    # The sections were in front of you; what they import from the pool was
    # not. Refuse to run imports you have not approved with bb review.
    imported = {dep for _, p in [(implementation, prepared_implementation)] + checks
                for dep in p['alias_mapping']}
    needed = {h for dep in imported for h in code_resolve_dependencies(dep)}
    missing = sorted(needed - review_approvals(list(needed)))
    if missing:
        raise ValueError("The edit imports functions you have not reviewed, so nothing was run:\n"
                         + '\n'.join(f"  bb review {h}" for h in missing))

    function = code_execute_source(implementation['source'], lang)

    results = []
    for check, prepared in checks:
        name = prepared['name_mapping']['_bb_v_0']
        try:
            status, detail = check_run(code_execute_source(check['source'], lang), function)
        except ValueError as e:
            status, detail = 'ERROR', str(e)
        results.append((status, name, detail))

    return {
        'implementation': (implementation, prepared_implementation),
        'checks': checks,
        'results': results,
    }


def edit_save_section(section: Dict[str, Any], prepared: Dict[str, Any], lang: str,
                      comment: str, checks: List[str] = None, unchanged: bool = False,
                      mapping_parent: str = "") -> bool:
    """
    Save one edit-file section to the pool.

    A new function gets the section's bb:parent marker as its parent, and
    checks (for the implementation) as part of its definition; prepared's
    hash must already include them. An existing function is never
    rewritten: it only gets a new mapping when its names or documentation
    changed. A new mapping records mapping_parent, the translation it was
    edited from.

    Returns:
        True when something was written
    """
    func_hash = prepared['hash']
    parent = section.get('parent')
    is_new = object_save(func_hash, prepared['normalized_code'],
                         code_create_metadata(parent=parent if parent != func_hash else None),
                         checks=checks)

    if is_new or not unchanged:
        mapping_save(func_hash, lang, prepared['docstring'], prepared['name_mapping'],
                     prepared['alias_mapping'], comment, section['markdown'], mapping_parent)
        return True

    return False


def helper_run_editor(path: str):
    """
    Open path in the user's editor ($EDITOR, then $VISUAL, then vi).

    The variable may include arguments, e.g. EDITOR="code --wait".
    """
    editor = os.environ.get('EDITOR') or os.environ.get('VISUAL') or 'vi'
    result = subprocess.run(shlex.split(editor) + [path])
    if result.returncode != 0:
        raise ValueError(f"Editor exited with status {result.returncode}")


def helper_ask_yes(question: str) -> bool:
    """Ask a yes/no question, defaulting to yes; no input (EOF) means no."""
    try:
        answer = input(f"{question} [Y/n] ").strip().lower()
    except EOFError:
        print()
        return False
    return answer in ('', 'y', 'yes')


def command_edit(identifier: str, comment: str = None):
    """
    Edit a function and its checks as one markdown file, then save a fork.

    Opens $EDITOR on a file with an Implementation section and a Checks
    section. Each section holds prose and examples, and exactly one code
    block fenced as `python def`: the function that is hashed. Each check
    takes the implementation as its only argument and returns True when it
    passes.

    Saving requires the file to parse and every check to pass. The new
    function records the edited one as its parent and lists its checks;
    changed checks record their own parents. The comment (asked for when
    --comment is not given) is stored in the new mappings.

    Examples:

    ```
    bb edit compute_pi@eng
    bb edit compute_pi@eng --comment "Faster convergence"
    ```
    """
    hash_value, lang = helper_parse_identifier(identifier)
    lang = lang or helper_pick_language(hash_value)
    if lang not in storage_list_languages(hash_value):
        print(f"Error: {hash_value[:12]}... has no '{lang}' mapping. Translate it first with bb translate.",
              file=sys.stderr)
        sys.exit(1)

    # Load the function and the checks attached to it
    original = edit_load_section(hash_value, lang)
    original_checks = []
    for check_hash in object_load_checks(hash_value):
        if code_detect_schema(check_hash) is None:
            print(f"Error: Check {check_hash[:12]}... is attached to this function "
                  f"but missing from the local pool. Fetch it with git first.", file=sys.stderr)
            sys.exit(1)
        original_checks.append(edit_load_section(check_hash, lang))

    text = markdown_render_edit(original['title'], original, original_checks)
    # Kept next to the pool, so an abandoned edit is easy to find again
    edit_dir = storage_get_bb_directory() / 'edit'
    edit_dir.mkdir(parents=True, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix=f"{original['title']}-", suffix='.md', dir=edit_dir)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write(text)

    # Edit until the file is usable and every check passes
    while True:
        try:
            helper_run_editor(path)
            with open(path, 'r', encoding='utf-8') as f:
                edited = f.read()
            plan = edit_prepare(edited, lang)
            passed = check_report(plan['results'])
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            passed = False
        if passed:
            break
        # Only offer another round when someone is at the terminal
        if not sys.stdin.isatty() or not helper_ask_yes("Edit again?"):
            print(f"Nothing saved. Your edit is kept in {path}", file=sys.stderr)
            sys.exit(1)

    section, prepared = plan['implementation']
    # Without a marker, the implementation still descends from the edited function
    section['parent'] = section['parent'] or hash_value
    # Checks without a marker are matched to the loaded checks by name, then position
    parents = edit_assign_parents([(p['name_mapping']['_bb_v_0'], c['parent']) for c, p in plan['checks']],
                                  [(c['title'], c['parent']) for c in original_checks])
    for (check, _), parent in zip(plan['checks'], parents):
        check['parent'] = parent
    check_hashes = sorted(set(p['hash'] for _, p in plan['checks']))
    # The checks are part of the implementation's definition, hence of its hash
    prepared['hash'] = code_compute_function_hash(prepared['normalized_code'], check_hashes)

    # Compare with what was loaded: hashes, names and documentation
    original_prints = {s['fingerprint'] for s in [original] + original_checks}

    def unchanged(sec, prep):
        return edit_fingerprint(prep['hash'], prep['docstring'], prep['name_mapping'],
                                prep['alias_mapping'], sec['markdown']) in original_prints

    if (prepared['hash'] == hash_value and unchanged(section, prepared)
            and all(unchanged(s, p) for s, p in plan['checks'])):
        os.unlink(path)
        print("No changes.")
        return

    if comment is None:
        try:
            comment = input("Comment (press Enter to skip): ").strip()
        except EOFError:
            comment = ""

    # Each saved mapping descends from the translation its parent was loaded in
    loaded_mappings = {s['parent']: s['mapping'] for s in [original] + original_checks}

    # Save checks first, so the implementation never lists a missing check
    with contextlib.redirect_stdout(io.StringIO()):
        new_checks = []
        for check, prep in plan['checks']:
            if edit_save_section(check, prep, lang, comment, unchanged=unchanged(check, prep),
                                 mapping_parent=loaded_mappings.get(check['parent'], "")):
                new_checks.append((prep['name_mapping']['_bb_v_0'], prep['hash']))
        edit_save_section(section, prepared, lang, comment, checks=check_hashes,
                          unchanged=unchanged(section, prepared),
                          mapping_parent=loaded_mappings.get(section['parent'], ""))

    os.unlink(path)

    print(f"Hash: {prepared['hash']}")
    if prepared['hash'] != hash_value:
        print(f"Parent: {hash_value}")
    for name, check_hash in new_checks:
        print(f"Check: {check_hash} {name}")


def command_refactor(what_hash: str, from_hash: str, to_hash: str, at_hash: str = None, comment: str = None):
    """
    Replace a dependency hash with another in a function.

    Supports two modes:
    1. Global refactor: Replaces FROM with TO everywhere in WHAT's dependency graph
    2. Surgical refactor: Replaces FROM with TO only in the path from AT to WHAT

    Creates new functions where references to from_hash are replaced
    with to_hash, each recording the function it was rewritten from as
    its parent. Copies the current language mappings, updating
    alias_mappings to use the new hash key. Like a batch of bb edit, one
    comment (asked for when --comment is not given) goes on every new
    mapping.

    Args:
        what_hash: Function hash to modify (64-character hex)
        from_hash: Dependency hash to replace (64-character hex)
        to_hash: New dependency hash (64-character hex)
        at_hash: Optional. If provided, only refactor the path from AT to WHAT (surgical refactor)
        comment: Comment for every new mapping (asked for when None)
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
    func_data = object_load(what_hash)
    normalized_code = func_data['normalized_code']
    # Get all languages from v1 directory structure
    pool_dir = storage_get_pool_directory()
    func_dir = pool_dir / what_hash[:2] / what_hash[2:]
    languages = []
    for item in func_dir.iterdir():
        if item.is_dir() and len(item.name) == 3:
            languages.append(item.name)

    # Check that the function actually depends on from_hash (direct or transitive)
    all_deps = code_resolve_dependencies(what_hash)
    if from_hash not in all_deps:
        print(f"Error: Function {what_hash} does not depend on {from_hash}", file=sys.stderr)
        sys.exit(1)

    # Surgical refactor mode: if at_hash is provided
    if at_hash is not None:
        # Validate at_hash format
        if len(at_hash) != 64 or not all(c in '0123456789abcdef' for c in at_hash.lower()):
            print(f"Error: Invalid at hash format. Expected 64 hex characters. Got: {at_hash}", file=sys.stderr)
            sys.exit(1)

        # Check if at function exists
        at_version = code_detect_schema(at_hash)
        if at_version is None:
            print(f"Error: AT function not found: {at_hash}", file=sys.stderr)
            sys.exit(1)

        # Verify AT is in transitive dependencies of WHAT
        if at_hash not in all_deps and at_hash != what_hash:
            print(f"Error: AT function {at_hash} is not in the dependency graph of {what_hash}", file=sys.stderr)
            sys.exit(1)

        # Verify FROM is in transitive dependencies of AT (or AT itself)
        if at_hash == what_hash:
            # Degenerate case: AT == WHAT, just use global refactor
            at_deps = all_deps
        else:
            at_deps = code_resolve_dependencies(at_hash)

        if from_hash not in at_deps and from_hash != at_hash:
            print(f"Error: FROM function {from_hash} is not in the dependency graph of AT {at_hash}", file=sys.stderr)
            sys.exit(1)

        # Build the "affected set" - all functions from AT to WHAT
        # This is the set of functions that need to be refactored
        affected_set = set()

        # Add AT itself if it needs refactoring
        if at_hash != from_hash:
            affected_set.add(at_hash)

        # Walk up from AT to WHAT, adding all functions that transitively depend on AT
        for dep_hash in all_deps:
            if dep_hash == what_hash:
                break  # Don't process what_hash in the loop

            # Check if this function depends on AT (or any function in affected_set)
            dep_data = object_load(dep_hash)
            dep_code = dep_data['normalized_code']
            direct_deps = code_extract_dependencies(dep_code)

            # If this function depends on AT or any affected function, it's affected
            if at_hash in direct_deps or any(ad in direct_deps for ad in affected_set):
                affected_set.add(dep_hash)

        # Filter all_deps to only include affected functions
        all_deps = [d for d in all_deps if d in affected_set or d == what_hash]

    # One comment describes the whole refactor, like a bb edit comment
    if comment is None:
        try:
            comment = input("Comment (press Enter to skip): ").strip()
        except EOFError:
            comment = ""

    # Build hash replacement map for recursive refactoring
    # We need to refactor all functions in the chain that depend on from_hash
    hash_map = {from_hash: to_hash}  # Maps old_hash -> new_hash

    # Process functions in topological order (dependencies first)
    for dep_hash in all_deps:
        if dep_hash == what_hash:
            break  # Stop before processing what_hash itself

        # Load this dependency's code
        dep_data = object_load(dep_hash)
        dep_code = dep_data['normalized_code']

        # Check if this function depends on any hash in our replacement map
        direct_deps = code_extract_dependencies(dep_code)
        needs_refactor = any(d in hash_map for d in direct_deps)

        if needs_refactor:
            # Refactor this function with all accumulated replacements
            dep_tree = ast.parse(dep_code)

            class DependencyReplacer(ast.NodeTransformer):
                def visit_ImportFrom(self, node):
                    if node.module == 'bb.pool':
                        new_names = []
                        for alias in node.names:
                            import_name = alias.name
                            if import_name.startswith(BB_IMPORT_PREFIX):
                                actual_hash = import_name[len(BB_IMPORT_PREFIX):]
                            else:
                                actual_hash = import_name

                            # Replace with mapped hash if it exists
                            if actual_hash in hash_map:
                                new_name = BB_IMPORT_PREFIX + hash_map[actual_hash]
                                new_names.append(ast.alias(name=new_name, asname=alias.asname))
                            else:
                                new_names.append(alias)
                        node.names = new_names
                    return node

                def visit_Attribute(self, node):
                    if (isinstance(node.value, ast.Name) and node.attr == '_bb_v_0'):
                        prefixed_name = node.value.id
                        if prefixed_name.startswith(BB_IMPORT_PREFIX):
                            actual_hash = prefixed_name[len(BB_IMPORT_PREFIX):]
                        else:
                            actual_hash = prefixed_name

                        if actual_hash in hash_map:
                            node.value.id = BB_IMPORT_PREFIX + hash_map[actual_hash]
                    self.generic_visit(node)
                    return node

            replacer = DependencyReplacer()
            dep_tree = replacer.visit(dep_tree)
            ast.fix_missing_locations(dep_tree)

            new_dep_code = ast.unparse(dep_tree)

            # Compute new hash (without docstring)
            new_dep_tree = ast.parse(new_dep_code)
            for node in new_dep_tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    _, func_without_docstring = code_extract_docstring(node)
                    imports = [n for n in new_dep_tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
                    module_without_docstring = ast.Module(body=imports + [func_without_docstring], type_ignores=[])
                    ast.fix_missing_locations(module_without_docstring)
                    code_without_docstring = ast.unparse(module_without_docstring)
                    break

            # The rewritten function keeps its checks, which are part of its hash
            dep_checks = object_load_checks(dep_hash)
            new_dep_hash = code_compute_function_hash(code_without_docstring, dep_checks)

            # Save the new function
            metadata = code_create_metadata(parent=dep_hash)
            object_save(new_dep_hash, new_dep_code, metadata, checks=dep_checks)

            # Copy language mappings
            dep_func_dir = pool_dir / dep_hash[:2] / dep_hash[2:]
            dep_languages = []
            for item in dep_func_dir.iterdir():
                if item.is_dir() and len(item.name) == 3:
                    dep_languages.append(item.name)

            for lang in dep_languages:
                # Copy the current mappings; superseded ones stay reachable through parent
                for mapping_hash in mapping_leaves(dep_hash, lang):
                    docstring, name_mapping, alias_mapping, _ = mapping_load(dep_hash, lang, mapping_hash)

                    # Update alias_mapping with all replacements
                    new_alias_mapping = {}
                    for h, alias in alias_mapping.items():
                        if h in hash_map:
                            new_alias_mapping[hash_map[h]] = alias
                        else:
                            new_alias_mapping[h] = alias

                    mapping_save(new_dep_hash, lang, docstring, name_mapping, new_alias_mapping, comment,
                                 mapping_load_markdown(dep_hash, lang, mapping_hash),
                                 f"{dep_hash}@{lang}@{mapping_hash}")

            # Track this replacement for subsequent functions
            hash_map[dep_hash] = new_dep_hash

    # Now refactor what_hash itself with all accumulated replacements
    tree = ast.parse(normalized_code)

    class DependencyReplacer(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            if node.module == 'bb.pool':
                new_names = []
                for alias in node.names:
                    import_name = alias.name
                    # Check if this is the from_hash import
                    if import_name.startswith(BB_IMPORT_PREFIX):
                        actual_hash = import_name[len(BB_IMPORT_PREFIX):]
                    else:
                        actual_hash = import_name

                    if actual_hash == from_hash:
                        # Replace with to_hash
                        new_name = BB_IMPORT_PREFIX + to_hash
                        new_names.append(ast.alias(name=new_name, asname=alias.asname))
                    else:
                        new_names.append(alias)
                node.names = new_names
            return node

        def visit_Attribute(self, node):
            # Transform object_from_hash._bb_v_0 -> object_to_hash._bb_v_0
            if (isinstance(node.value, ast.Name) and
                node.attr == '_bb_v_0'):
                prefixed_name = node.value.id
                if prefixed_name.startswith(BB_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(BB_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name

                if actual_hash == from_hash:
                    node.value.id = BB_IMPORT_PREFIX + to_hash
            self.generic_visit(node)
            return node

    class FinalReplacer(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            if node.module == 'bb.pool':
                new_names = []
                for alias in node.names:
                    import_name = alias.name
                    if import_name.startswith(BB_IMPORT_PREFIX):
                        actual_hash = import_name[len(BB_IMPORT_PREFIX):]
                    else:
                        actual_hash = import_name

                    # Replace with mapped hash if it exists
                    if actual_hash in hash_map:
                        new_name = BB_IMPORT_PREFIX + hash_map[actual_hash]
                        new_names.append(ast.alias(name=new_name, asname=alias.asname))
                    else:
                        new_names.append(alias)
                node.names = new_names
            return node

        def visit_Attribute(self, node):
            if (isinstance(node.value, ast.Name) and node.attr == '_bb_v_0'):
                prefixed_name = node.value.id
                if prefixed_name.startswith(BB_IMPORT_PREFIX):
                    actual_hash = prefixed_name[len(BB_IMPORT_PREFIX):]
                else:
                    actual_hash = prefixed_name

                if actual_hash in hash_map:
                    node.value.id = BB_IMPORT_PREFIX + hash_map[actual_hash]
            self.generic_visit(node)
            return node

    replacer = FinalReplacer()
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

    # The rewritten function keeps its checks, which are part of its hash
    what_checks = object_load_checks(what_hash)
    new_hash = code_compute_function_hash(code_without_docstring, what_checks)

    # Create metadata for the new function
    metadata = code_create_metadata(parent=what_hash)

    # Save the new function (object.json) with docstring
    object_save(new_hash, new_normalized_code, metadata, checks=what_checks)

    # Copy the current language mappings from what_hash to new_hash;
    # superseded ones stay reachable through parent
    for lang in languages:
        for mapping_hash in mapping_leaves(what_hash, lang):
            docstring, name_mapping, alias_mapping, _ = mapping_load(what_hash, lang, mapping_hash)

            # Update alias_mapping: replace all mapped hashes
            new_alias_mapping = {}
            for dep_hash, alias in alias_mapping.items():
                if dep_hash in hash_map:
                    new_alias_mapping[hash_map[dep_hash]] = alias
                else:
                    new_alias_mapping[dep_hash] = alias

            mapping_save(new_hash, lang, docstring, name_mapping, new_alias_mapping, comment,
                         mapping_load_markdown(what_hash, lang, mapping_hash),
                         f"{what_hash}@{lang}@{mapping_hash}")

    # Print the new function hash
    print(f'new hash: {new_hash}')


# =============================================================================
# Compilation Functions
# =============================================================================

def compile_generate_runtime(func_hash: str, lang: str, output_dir: Path) -> Path:
    """
    Generate the bb runtime module for the compiled executable.

    Args:
        func_hash: Function hash to compile
        lang: Language for the function
        output_dir: Directory to write runtime to

    Returns:
        Path to the generated runtime module
    """
    runtime_dir = output_dir / 'bb_runtime'
    runtime_dir.mkdir(parents=True, exist_ok=True)

    # Copy the necessary parts of bb for runtime
    runtime_code = '''"""
BB Runtime - Minimal runtime for compiled functions
"""
import ast
import json
import os
import sys
from pathlib import Path


BB_IMPORT_PREFIX = "object_"


def storage_get_bundle_directory() -> Path:
    """Get the bundled objects directory."""
    # When compiled, objects are bundled alongside the executable
    exe_dir = Path(sys.executable).parent
    return exe_dir / 'bundle'


def object_load(hash_value: str):
    """Load function from v1 format."""
    bundle_dir = storage_get_bundle_directory()
    func_dir = bundle_dir / 'sha256' / hash_value[:2] / hash_value[2:]
    object_path = func_dir / 'object.json'

    if not object_path.exists():
        raise FileNotFoundError(f"Function not found: {hash_value}")

    with open(object_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def mapping_load(func_hash: str, lang: str, mapping_hash: str):
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


def mappings_list(func_hash: str, lang: str):
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
    func_data = object_load(hash_value)
    normalized_code = func_data['normalized_code']

    mappings = mappings_list(hash_value, lang)
    if not mappings:
        raise ValueError(f"No mapping found for language: {lang}")

    if mapping_hash:
        selected_hash = mapping_hash
    else:
        selected_hash = mappings[0][0]

    docstring, name_mapping, alias_mapping, comment = mapping_load(hash_value, lang, selected_hash)

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

        def visit_ExceptHandler(self, node):
            if node.name and node.name in name_mapping:
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
    func_name = name_mapping.get('_bb_v_0', '_bb_v_0')
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
            mappings = mappings_list(dep_hash, lang)
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
                    error_lines.append(f"  bb translate {dep_hash}@{available[0]} {lang}")
            raise ValueError("\n".join(error_lines))

    # Load all functions
    functions = []
    # Create a mapping from hash to unique function name
    hash_to_func_name = {}

    # In debug mode, we need to load mappings first to build proper unique names
    if debug_mode:
        for dep_hash in deps:
            mappings = mappings_list(dep_hash, lang)
            if mappings:
                mapping_hash = mappings[0][0]
                _, name_mapping, _, _ = mapping_load(dep_hash, lang, mapping_hash)
                original_func_name = name_mapping.get('_bb_v_0', '_bb_v_0')
                hash_to_func_name[dep_hash] = f'{original_func_name}_{dep_hash[:8]}'
            else:
                hash_to_func_name[dep_hash] = f'_bb_{dep_hash[:8]}'
    else:
        for dep_hash in deps:
            hash_to_func_name[dep_hash] = f'_bb_{dep_hash[:8]}'

    for dep_hash in deps:
        func_data = object_load(dep_hash)
        normalized_code = func_data['normalized_code']

        if debug_mode:
            # Debug mode: denormalize to human-readable names
            mappings = mappings_list(dep_hash, lang)
            mapping_hash = mappings[0][0]
            docstring, name_mapping, alias_mapping, _ = mapping_load(dep_hash, lang, mapping_hash)

            # Denormalize the code
            normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
            code = code_denormalize(normalized_code_with_doc, name_mapping, alias_mapping)

            # Make function name unique by appending hash prefix (prevents name collisions)
            original_func_name = name_mapping.get('_bb_v_0', '_bb_v_0')
            func_name = f'{original_func_name}_{dep_hash[:8]}'

            # Rename the denormalized function to the unique name
            tree = ast.parse(code)
            class FunctionRenamer(ast.NodeTransformer):
                def visit_Name(self, node):
                    # Replace recursive calls
                    if node.id == original_func_name:
                        node.id = func_name
                    return node

                def visit_FunctionDef(self, node):
                    # Replace function definition name
                    if node.name == original_func_name:
                        node.name = func_name
                    self.generic_visit(node)
                    return node

                def visit_AsyncFunctionDef(self, node):
                    if node.name == original_func_name:
                        node.name = func_name
                    self.generic_visit(node)
                    return node

            tree = FunctionRenamer().visit(tree)
            code = ast.unparse(tree)

            # Replace calls to other bb functions with their unique names
            # alias_mapping structure: {hash: alias_name}
            for other_hash, alias in alias_mapping.items():
                # Get the unique function name for this hash
                unique_name = hash_to_func_name.get(other_hash)
                if unique_name:
                    # Replace alias() with unique_name()
                    code = code.replace(f'{alias}(', f'{unique_name}(')
        else:
            # Normal mode: use normalized code with unique function names
            code = normalized_code
            func_name = hash_to_func_name[dep_hash]

            # Rename the function from _bb_v_0 to unique name using AST
            # This properly handles recursive calls (not just the definition)
            tree = ast.parse(code)

            class FunctionRenamer(ast.NodeTransformer):
                def visit_Name(self, node):
                    # Replace recursive calls to _bb_v_0
                    if node.id == '_bb_v_0':
                        node.id = func_name
                    return node

                def visit_FunctionDef(self, node):
                    # Replace function definition name
                    if node.name == '_bb_v_0':
                        node.name = func_name
                    self.generic_visit(node)
                    return node

                def visit_AsyncFunctionDef(self, node):
                    if node.name == '_bb_v_0':
                        node.name = func_name
                    self.generic_visit(node)
                    return node

            tree = FunctionRenamer().visit(tree)
            code = ast.unparse(tree)

            # Replace calls to other bb functions with their unique names
            for other_hash, other_name in hash_to_func_name.items():
                # Replace object_HASH._bb_v_0 with the unique function name
                code = code.replace(f'object_{other_hash}._bb_v_0', other_name)

        # Strip bb imports - dependencies will be included inline
        code = code_strip_bb_imports(code)

        functions.append({
            'hash': dep_hash,
            'code': code,
            'func_name': func_name,
        })

    # Build the Python file
    lines = ['#!/usr/bin/env python3', '"""', f'Compiled bb function: {func_hash}', '"""', '']

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

    # Extract function signature for usage message
    main_tree = ast.parse(main_func['code'])
    main_func_def = None
    for node in main_tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            main_func_def = node
            break

    # Build parameter list for usage message
    param_names = []
    if main_func_def and main_func_def.args.args:
        for arg in main_func_def.args.args:
            param_names.append(arg.arg)

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
    if param_names:
        params_str = ' '.join(param_names)
        lines.append('        print(f"Usage: {sys.argv[0]} ' + params_str + '")')
    else:
        lines.append('        print(f"Usage: {sys.argv[0]}")')
    lines.append('')

    return '\n'.join(lines)


def command_compile(identifier: str, debug_mode: bool = False, output_path: Optional[str] = None):
    """
    Compile a function into a standalone Python file.

    Accepts a hash or function name (with optional @lang suffix).
    Refuses unless you approved the function and its dependencies with
    bb review. When a name is provided with @lang, debug mode is implicit.
    The generated file can be run directly with Python or compiled separately
    with tools like Nuitka, PyInstaller, or PyOxidizer.
    """
    # Check if identifier is a name (not a full hash) with @lang suffix
    # If so, implicitly enable debug mode
    base_identifier = identifier.split('@')[0]
    has_lang_suffix = '@' in identifier
    is_full_hash = len(base_identifier) == 64 and all(c in '0123456789abcdef' for c in base_identifier.lower())

    # Implicit debug mode: name@lang (not hash@lang)
    if has_lang_suffix and not is_full_hash and not debug_mode:
        debug_mode = True

    func_hash, lang = helper_parse_identifier(identifier)

    # A compiled file runs anywhere: only package code you have read
    review_require(func_hash, "compile")

    # Debug mode requires language
    if debug_mode and lang is None:
        print("Error: --debug requires @lang suffix. Use format: identifier@lang", file=sys.stderr)
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

    # Generate single Python file
    print("Generating Python file...")
    dest = Path(output_path) if output_path else Path('main.py')

    try:
        python_code = compile_generate_python(func_hash, lang, debug_mode=debug_mode)
        with open(dest, 'w', encoding='utf-8') as f:
            f.write(python_code)
        print(f"Python file created: {dest}")
        print(f"Run with: python3 {dest} [args...]")
        print(f"Or compile separately with: nuitka --onefile {dest}")
    except Exception as e:
        print(f"Error generating Python file: {e}", file=sys.stderr)
        sys.exit(1)

    print("Compilation complete!")


def _skills_parse_docstring(func):
    """Extract title and body from a function's docstring, stripping Args/Returns blocks."""
    import textwrap
    if not func or not func.__doc__:
        return "", ""
    raw = func.__doc__.strip()
    doc_lines = raw.split('\n')
    title = doc_lines[0].strip()
    # Collect body lines, skipping Args: and Returns: sections
    body_lines = []
    in_section = False
    for line in doc_lines[1:]:
        stripped = line.strip()
        if stripped in ('Args:', 'Returns:'):
            in_section = True
            continue
        if in_section:
            # Section continues while indented or empty
            if stripped == '' or line.startswith('        '):
                continue
            in_section = False
        body_lines.append(line)
    body = textwrap.dedent('\n'.join(body_lines)).strip()
    return title, body


def _skills_format_arg_usage(action):
    """Format a single argparse action for usage line display."""
    if action.option_strings:
        if action.nargs == 0:
            return f"[{action.option_strings[0]}]"
        metavar = action.metavar or action.dest.upper()
        return f"[{action.option_strings[0]} {metavar}]"
    metavar = action.metavar or action.dest
    if action.nargs == '?':
        return f"[{metavar}]"
    elif action.nargs == '*':
        return f"[{metavar}...]"
    elif action.nargs == '+':
        return f"{metavar} [{metavar}...]"
    return f"<{metavar}>"


def command_skills():
    """Generate a skills reference document from argparse introspection."""
    parser = _build_parser()

    # Find the main subparsers action
    subparsers_action = None
    for action in parser._subparsers._actions:
        if isinstance(action, argparse._SubParsersAction):
            subparsers_action = action
            break

    if not subparsers_action:
        print("No commands found", file=sys.stderr)
        sys.exit(1)

    # Map command names to handler functions for docstring extraction
    handler_map = {
        'init': command_init,
        'whoami': command_whoami,
        'add': code_add,
        'show': code_show,
        'translate': command_translate,
        'rm': command_rm,
        'run': command_run,
        'edit': command_edit,
        'review': command_review,
        'reviews': command_reviews,
        'log': command_log,
        'search': command_search,
        'latest': command_latest,
        'validate': schema_validate_directory,
        'caller': command_caller,
        'tree': command_tree,
        'refactor': command_refactor,
        'compile': command_compile,
    }

    # Map worklog subcommand names to handler functions
    worklog_handler_map = {
        'new': command_worklog_new,
        'translate': command_worklog_translate,
        'list': command_worklog_list,
        'show': command_worklog_show,
    }

    trust_handler_map = {
        'add': command_trust_add,
        'remove': command_trust_remove,
        'list': command_trust_list,
        'publish': command_trust_publish,
        'import': command_trust_import,
    }

    nested_handler_maps = {
        'worklog': worklog_handler_map,
        'trust': trust_handler_map,
    }

    lines = []
    lines.append("# bb Skills")
    lines.append("")

    # Include module docstring as introduction
    import textwrap
    module_doc = sys.modules[__name__].__doc__
    if module_doc:
        lines.append(textwrap.dedent(module_doc).strip())
        lines.append("")

    for cmd_name, cmd_parser in subparsers_action.choices.items():
        if cmd_name == 'skills':
            continue

        # Get docstring from handler function, fall back to argparse help
        handler = handler_map.get(cmd_name)
        title, body = _skills_parse_docstring(handler)
        if not title:
            # Fallback: use argparse help string from pseudo-actions
            for pa in subparsers_action._choices_actions:
                if pa.dest == cmd_name:
                    title = pa.help or ""
                    break

        # Check for nested subcommands
        nested_action = None
        for action in cmd_parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                nested_action = action
                break

        # Build usage and args for the main command
        usage_parts = [f"bb {cmd_name}"]
        arg_descriptions = []

        for action in cmd_parser._actions:
            if isinstance(action, (argparse._HelpAction, argparse._SubParsersAction)):
                continue
            usage_parts.append(_skills_format_arg_usage(action))

            if action.option_strings:
                name = "/".join(action.option_strings)
                optional = True
            else:
                name = f"`{action.metavar or action.dest}`"
                optional = action.nargs in ('?', '*')

            desc = action.help or ""
            if action.choices:
                desc += f" (choices: {', '.join(str(c) for c in action.choices)})"
            arg_descriptions.append((name, optional, desc))

        lines.append(f"## `bb {cmd_name}` — {title}")
        lines.append("")
        lines.append("```")
        lines.append(" ".join(usage_parts))
        lines.append("```")
        lines.append("")

        if arg_descriptions:
            for name, optional, desc in arg_descriptions:
                opt_marker = " *(optional)*" if optional else ""
                lines.append(f"- **{name}**{opt_marker}: {desc}")
            lines.append("")

        if body:
            lines.append(body)
            lines.append("")

        # Handle nested subcommands (e.g., worklog new/translate/list/show)
        if nested_action:
            for sub_name, sub_parser in nested_action.choices.items():
                sub_handler = nested_handler_maps.get(cmd_name, {}).get(sub_name)
                sub_title, sub_body = _skills_parse_docstring(sub_handler)

                sub_usage = [f"bb {cmd_name} {sub_name}"]
                sub_args = []

                for sub_action in sub_parser._actions:
                    if isinstance(sub_action, argparse._HelpAction):
                        continue
                    sub_usage.append(_skills_format_arg_usage(sub_action))

                    if sub_action.option_strings:
                        name = "/".join(sub_action.option_strings)
                        optional = True
                    else:
                        name = f"`{sub_action.metavar or sub_action.dest}`"
                        optional = sub_action.nargs in ('?', '*')

                    desc = sub_action.help or ""
                    sub_args.append((name, optional, desc))

                lines.append(f"### `bb {cmd_name} {sub_name}` — {sub_title}")
                lines.append("")
                lines.append("```")
                lines.append(" ".join(sub_usage))
                lines.append("```")
                lines.append("")

                if sub_args:
                    for name, optional, desc in sub_args:
                        opt_marker = " *(optional)*" if optional else ""
                        lines.append(f"- **{name}**{opt_marker}: {desc}")
                    lines.append("")

                if sub_body:
                    lines.append(sub_body)
                    lines.append("")

        lines.append("---")
        lines.append("")

    print("\n".join(lines))


def _build_parser():
    """Build and return the argparse parser for bb."""
    parser = argparse.ArgumentParser(description='bb - Function pool manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize bb directory and config')

    # Whoami command
    whoami_parser = subparsers.add_parser('whoami', help='Get or set user configuration')
    whoami_parser.add_argument('subcommand', choices=['name', 'email', 'signing-key', 'language'],
                               help='Configuration field to get/set')
    whoami_parser.add_argument('value', nargs='*', help='New value(s) to set (omit to get current value)')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a function to the pool')
    add_parser.add_argument('file', help='Path to Python file with @lang suffix (e.g., file.py@eng)')
    add_parser.add_argument('--comment', default='', help='Optional comment explaining this mapping variant')

    # Show command
    show_parser = subparsers.add_parser('show', help='Show a function from the pool')
    show_parser.add_argument('identifier', help='Hash or name, with optional @lang (e.g., compute_pi@eng, abc123...@eng@mapping)')

    # Translate command
    translate_parser = subparsers.add_parser('translate', help='Add translation for existing function')
    translate_parser.add_argument('identifier', help='Hash or name with @lang suffix (e.g., compute_pi@eng)')
    translate_parser.add_argument('target_lang', help='Target language code (e.g., fra, spa)')
    translate_parser.add_argument('-r', '--recursive', action='store_true', help='Recursively translate dependencies')

    # Rm command
    rm_parser = subparsers.add_parser('rm', help='Remove function or language mapping')
    rm_parser.add_argument('identifier', help='Hash or name, with optional @lang to remove only that language')

    # Run command
    run_parser = subparsers.add_parser('run', help='Execute a function from the pool')
    run_parser.add_argument('identifier', help='Hash or name, with optional @lang (e.g., compute_pi@eng)')
    run_parser.add_argument('func_args', nargs='*', help='Arguments to pass to function')

    # Edit command
    edit_parser = subparsers.add_parser('edit', help='Edit a function and its checks, then save a fork')
    edit_parser.add_argument('identifier', help='Hash or name, with optional @lang (e.g., compute_pi@eng)')
    edit_parser.add_argument('--comment', '-m', default=None, help='Comment for the new version (asked when omitted)')


    # Review command
    review_parser = subparsers.add_parser('review', help='Read, run the checks of, and approve or reject functions')
    review_parser.add_argument('identifier', help='Hash or name, with optional @lang')
    review_parser.add_argument('--comment', '-m', default="", help='Comment stored in your signed reviews')

    # Reviews command
    reviews_parser = subparsers.add_parser('reviews', help='Show who reviewed a function, with signature status')
    reviews_parser.add_argument('identifier', help='Hash or name, with optional @lang')

    # Trust command with subcommands
    trust_parser = subparsers.add_parser('trust', help='Manage the peers whose signed reviews you trust')
    trust_subparsers = trust_parser.add_subparsers(dest='trust_command', help='Trust subcommands')
    trust_add_parser = trust_subparsers.add_parser('add', help='Trust a peer (private list)')
    trust_add_parser.add_argument('email', help='Email the peer signs with')
    trust_add_parser.add_argument('key', help='SSH public key, or path of a .pub file')
    trust_remove_parser = trust_subparsers.add_parser('remove', help='Stop trusting a peer')
    trust_remove_parser.add_argument('email', help='Email of the peer')
    trust_subparsers.add_parser('list', help='List the peers you trust')
    trust_subparsers.add_parser('publish', help='Add yourself to the shared list of peers')
    trust_subparsers.add_parser('import', help='Import peers from the shared list, one by one')

    # Log command
    log_parser = subparsers.add_parser('log', help='Show functions and worklogs, newest first')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search and list functions by query')
    search_parser.add_argument('query', nargs='+', help='Search terms')

    # Latest command
    latest_parser = subparsers.add_parser('latest', help='Find most recent version of function by name')
    latest_parser.add_argument('name', help='Function name to find')
    latest_parser.add_argument('lang', nargs='?', help='Optional language code to filter (e.g., eng, fra)')

    # Worklog command with subcommands
    worklog_parser = subparsers.add_parser('worklog', help='Write, translate and list worklogs')
    worklog_subparsers = worklog_parser.add_subparsers(dest='worklog_command', help='Worklog subcommands')

    worklog_new_parser = worklog_subparsers.add_parser('new', help='Create a worklog in your preferred language')
    worklog_new_parser.add_argument('references', nargs='*',
                                    help='Translations to attach: name@lang, hash@lang or hash@lang@mapping_hash')
    worklog_new_parser.add_argument('--message', '-m', default=None, help='Message (asked in $EDITOR when omitted)')

    worklog_translate_parser = worklog_subparsers.add_parser('translate', help='Translate a worklog message')
    worklog_translate_parser.add_argument('identifier', help='Worklog uuid or unique prefix')
    worklog_translate_parser.add_argument('lang', help='Target language code (e.g., fra)')
    worklog_translate_parser.add_argument('--message', '-m', default=None, help='Message (asked in $EDITOR when omitted)')

    worklog_subparsers.add_parser('list', help='List every worklog, newest first')

    worklog_show_parser = worklog_subparsers.add_parser('show', help='Show a worklog')
    worklog_show_parser.add_argument('identifier', help='Worklog uuid or unique prefix, with optional @lang')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate function or entire bb directory')
    validate_parser.add_argument('identifier', nargs='?', help='Hash or name to validate (omit for whole directory)')
    validate_parser.add_argument('--checks', action='store_true',
                                 help='Run the checks of functions you approved with bb review')
    validate_parser.add_argument('--dangerously-unreviewed', action='store_true',
                                 help='With --checks: run every check, approved or not. Only on a disposable '
                                      'machine without secrets, such as CI')
    validate_parser.add_argument('--all', '-a', action='store_true',
                                 help='Validate entire bb directory including pool and config')

    # Caller command
    caller_parser = subparsers.add_parser('caller', help='Find functions that depend on a given function')
    caller_parser.add_argument('identifier', help='Hash or name, with optional @lang')

    # Tree command
    tree_parser = subparsers.add_parser('tree', help='Display dependency tree of a function')
    tree_parser.add_argument('identifier', help='Hash or name, with optional @lang')

    # Refactor command
    refactor_parser = subparsers.add_parser('refactor', help='Replace a dependency in a function')
    refactor_parser.add_argument('what', help='Function hash to modify')
    refactor_parser.add_argument('from_hash', metavar='from', help='Dependency hash to replace')
    refactor_parser.add_argument('to_hash', metavar='to', help='New dependency hash')
    refactor_parser.add_argument('at_hash', metavar='at', nargs='?', help='Optional: Limit refactor to path from AT to WHAT (surgical refactor)')
    refactor_parser.add_argument('--comment', '-m', default=None, help='Comment for the new versions (asked when omitted)')

    # Compile command
    compile_parser = subparsers.add_parser('compile', help='Compile function to standalone Python file')
    compile_parser.add_argument('identifier', help='Hash or name, with optional @lang. @lang required with --debug')
    compile_parser.add_argument('--output', '-o', default=None,
                                help='Output file path (default: main.py)')
    compile_parser.add_argument('--debug', action='store_true',
                                help='Use human-readable names (requires @lang and all translations)')

    # Skills command
    skills_parser = subparsers.add_parser('skills', help='Generate skills reference for AI agents')

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == 'init':
        command_init()
    elif args.command == 'whoami':
        command_whoami(args.subcommand, args.value)
    elif args.command == 'add':
        code_add(args.file, args.comment)
    elif args.command == 'show':
        code_show(args.identifier)
    elif args.command == 'translate':
        command_translate(args.identifier, args.target_lang, recursive=args.recursive)
    elif args.command == 'rm':
        command_rm(args.identifier)
    elif args.command == 'run':
        command_run(args.identifier, func_args=args.func_args)
    elif args.command == 'edit':
        command_edit(args.identifier, comment=args.comment)
    elif args.command == 'review':
        command_review(args.identifier, comment=args.comment)
    elif args.command == 'reviews':
        command_reviews(args.identifier)
    elif args.command == 'trust':
        if args.trust_command == 'add':
            command_trust_add(args.email, args.key)
        elif args.trust_command == 'remove':
            command_trust_remove(args.email)
        elif args.trust_command == 'list':
            command_trust_list()
        elif args.trust_command == 'publish':
            command_trust_publish()
        elif args.trust_command == 'import':
            command_trust_import()
        else:
            parser.parse_args([args.command, '--help'])
    elif args.command == 'log':
        command_log()
    elif args.command == 'search':
        command_search(args.query)
    elif args.command == 'latest':
        command_latest(args.name, args.lang)
    elif args.command == 'worklog':
        if args.worklog_command == 'new':
            command_worklog_new(args.references, message=args.message)
        elif args.worklog_command == 'translate':
            command_worklog_translate(args.identifier, args.lang, message=args.message)
        elif args.worklog_command == 'list':
            command_worklog_list()
        elif args.worklog_command == 'show':
            command_worklog_show(args.identifier)
        else:
            parser.parse_args([args.command, '--help'])
    elif args.command == 'validate':
        if args.dangerously_unreviewed and not args.checks:
            print("Error: --dangerously-unreviewed only applies to --checks", file=sys.stderr)
            sys.exit(1)
        if args.checks:
            command_validate_checks(args.identifier, unreviewed=args.dangerously_unreviewed)
        elif args.all or not args.identifier:
            # Validate entire directory
            is_valid, errors, stats = schema_validate_directory()
            print("BB Directory Validation")
            print("=" * 60)
            print(f"Functions total:   {stats['functions_total']}")
            print(f"Functions valid:   {stats['functions_valid']}")
            print(f"Functions invalid: {stats['functions_invalid']}")
            print(f"Languages found:   {stats['languages_total']}")
            print(f"Missing deps:      {len(stats['dependencies_missing'])}")
            print()

            if is_valid:
                print("✓ BB directory is valid")
            else:
                print("✗ Validation errors found:", file=sys.stderr)
                for error in errors[:20]:  # Limit to first 20 errors
                    print(f"  - {error}", file=sys.stderr)
                if len(errors) > 20:
                    print(f"  ... and {len(errors) - 20} more errors", file=sys.stderr)
                sys.exit(1)
        else:
            # Validate single function
            func_hash = helper_resolve_to_hash(args.identifier)
            is_valid, errors = schema_validate(func_hash)
            if is_valid:
                print(f"✓ Function {func_hash} is valid")
            else:
                print(f"✗ Function {func_hash} is invalid:", file=sys.stderr)
                for error in errors:
                    print(f"  - {error}", file=sys.stderr)
                sys.exit(1)
    elif args.command == 'caller':
        command_caller(args.identifier)
    elif args.command == 'tree':
        command_tree(args.identifier)
    elif args.command == 'refactor':
        command_refactor(args.what, args.from_hash, args.to_hash, args.at_hash if hasattr(args, 'at_hash') else None,
                         comment=args.comment)
    elif args.command == 'compile':
        command_compile(args.identifier, debug_mode=args.debug, output_path=args.output)
    elif args.command == 'skills':
        command_skills()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
