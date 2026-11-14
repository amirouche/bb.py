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

Writing functions for the pool:

- One function per file (def or async def), with its imports and, if
  you like, a module docstring. Anything else at module level (classes,
  constants, variables, code) is refused: put data in functions.
- Use another pool function through `import bb`: bb.resolve("name")
  names it (bb.latest("name") is the same), bb.ref("<hash>") names an
  exact version (`from bb.pool import` is gone):

      import bb

      parse_amount = bb.resolve("parse_amount")  # by name
      parse_amount = bb.ref("<hash>")            # or an exact version

  These lines go at module level, above the def, with the other imports:
  bb refuses `import bb` and bb.resolve inside a function body.

  A name is resolved when you save (bb add, bb edit), in the language you
  save in (then your `bb whoami language` languages, then any), to its one
  current version (the end of that name's lineage of edits, never by
  date); the stored code holds that hash, so the function keeps
  that version. A name with several current versions (two functions
  called alike, or a fork) is refused: name one of them with bb.ref
  (`bb search name` shows hashes). bb.resolve is never looked up when the
  function runs. So add helpers before the functions
  that use them. Stored code refers to each one as bb.ref("<hash>"). bb
  show and bb edit display references by name (bb.resolve), so saving an
  edit brings a function up to date: a function keeps the versions it was
  saved with until someone edits it (its debt, listed by bb search
  --outdated and noted in drafts as `# bb: out of date`). What you write
  with bb.ref stays pinned across edits. `bb update app@lang` pays the
  debt of app and everything it uses at once.
- Checks run the function they test, with what it imports from the pool.
  bb runs only code you have read: functions you saved yourself (bb add,
  bb edit, recorded in your private state) and functions you approved
  with bb review. So a check may test a function that uses your own pool
  functions; one that uses someone else's needs a bb review first.
  Running a program (bb run) or serving it still requires bb review.
- Arguments are positional: bb stores parameters as placeholders
  (_bb_v_0, ...), so a keyword cannot reach them. Call pool functions,
  the function itself and functions defined inside it without keywords
  (several options go in one dict), and define no keyword-only
  parameters or **kwargs. `bb add` and `bb edit` refuse such code.
- A function is named by `name@lang` (or its hash). Changing it with
  `bb add` or `bb edit` stores a new function with a new hash; functions
  importing the old one keep using it until they are updated:
  `bb edit name@lang --refactor app@lang` saves the change and updates
  everything between app and it, and lists what it rewrote (or run
  `bb update app@lang` later: it brings app and everything it uses to
  their current versions in one step, keeping pins). An update is saved
  only when the checks of what it rewrites pass, as in bb edit: one that
  fails stays on its version, the rest is updated, the failure is printed
  and the exit status is 1.
- Finding functions: `bb search` alone lists every current function;
  `bb search WORDS` searches names, docs and code; `bb latest
  name@lang --hash` prints only the current hash (for scripts); `bb log`
  is the history, with names. After a save, a note may list other
  functions whose code looks like the one you saved ("NAME looks like
  these other functions"): use one instead, or ignore the note if yours
  is meant to be different. Small functions are never compared.
- Versions are followed per name and per language: each name has its
  own lineage (bb log lists the steps), so two names may share identical
  code and each follows its own edits; a rename supersedes the old name.
  One function may not use two names of one code (they would be one
  alias): use one name, or give them different code. An edit in one
  language leaves the other languages on their version until someone
  translates it (bb latest, bb search --outdated and bb tree say "not yet
  translated").
- `bb translate name@lang TARGET FILE` translates through a file: when
  FILE is missing, bb writes the function in its language with a
  `- name -> ` line to fill in for each name (and for each pool function
  it uses, prefilled when that one is already translated), and sections
  for the docstring and documentation; when FILE exists, bb saves it as
  TARGET's translation. The code and its hash do not change.
  `bb translate --tree app@lang TARGET FILE` does it for app and
  everything it uses in one file, a section per function not yet in
  TARGET: a function's first Names line is its name in every caller (no
  aliases to fill in), and nothing is saved until every section is right.
- `bb edit name@lang FILE` works on an edit file instead of an editor:
  when FILE is missing, bb writes the function there (a template for a
  new name) and saves nothing; when it exists, bb saves it. The file is
  markdown: a `## Implementation` section holding one ```python def
  block, and an optional `## Checks` section.
  - For a new name you may write FILE yourself in that format.
  - For an existing function, start from the file bb writes and keep its
    `<!-- bb:parent <step> -->` line: it names the lineage step you
    drafted from (a step id from bb log, not the function's hash).
    A file without it, or drafted from a version that is no longer the
    current one, is refused: write a new file and make the change there.
  - After a save, bb rewrites FILE in stored form (single quotes, its own
    line breaks, `# bb:` notes) so you can go on editing it: read it again
    before changing it rather than editing the text you remember.
  - The documentation is the prose under `## Implementation`, before the
    code (per language, outside the hash). Each check is a `###` section
    whose function takes the implementation as its only argument and
    states what must hold with assert (at least one; it passes when it
    runs without raising, its return value is ignored); checks run when
    you save and are part of the definition (changing them forks the
    function). A check does not
    import what it tests, so the same check can test a fork or another
    function with the same contract. Checks are worth it for central
    functions (many callers, see `bb caller`) and tricky logic, less for
    plain views. A file with every part (bb adds the bb:parent line):

      # parse_amount

      ## Implementation

      Turns what a user typed in a price or quantity field into a number of
      cents. Every form that takes money calls it, so it accepts the usual
      ways people type amounts ("12", "12.5", "12,50", " 1 200.00 ") and
      returns None for anything else, never raising: the caller shows the
      form error.

      ```python def
      def parse_amount(text):
          \"\"\"The amount typed in text, in cents, or None when it is not an amount.\"\"\"
          cleaned = text.strip().replace(' ', '').replace(',', '.')
          if not cleaned or cleaned.count('.') > 1:
              return None
          whole, _, cents = cleaned.partition('.')
          if not whole.isdigit() or (cents and (not cents.isdigit())) or len(cents) > 2:
              return None
          return int(whole) * 100 + int(cents.ljust(2, '0') or 0)
      ```

      ## Checks

      ### check_rejects_garbage

      ```python def
      def check_rejects_garbage(parse):
          \"\"\"Anything that is not an amount gives None instead of raising.\"\"\"
          assert all((parse(text) is None for text in ['', 'abc', '1.2.3', '-5', '1.999']))
      ```

      ### check_reads_common_forms

      ```python def
      def check_reads_common_forms(parse):
          \"\"\"Whole, decimal point, decimal comma and spaced thousands all read right.\"\"\"
          assert parse('12') == 1200
          assert parse('12.5') == 1250
          assert parse('12,50') == 1250
          assert parse(' 1 200.00 ') == 120000
      ```

- `bb scratchpad a@lang b@lang FILE` does the same for several functions
  in one file: the sections are edit files one after another, each from
  its own `# name` heading. When FILE is missing bb writes them all
  (`--tree name@lang` adds a function and everything it uses); when it
  exists, bb saves every section that changed, functions before the
  sections that use them, and stops at the first that cannot be saved.
  `-m why --refactor app@lang` then brings app and everything between it and
  the saved functions up to date (bb update app@lang).
  Add a function by adding a `# new_name` section (a section that is only
  a ```python def block needs no `## Implementation` heading). Edit related functions
  together this way rather than one draft each. Saving an existing file
  needs no names (`bb scratchpad FILE -m why`): its language is the first of
  your `bb whoami` languages unless a NAME@lang says otherwise.
- Context: `bb tree name@lang` shows what a function uses, down to the
  leaves, with each one's docstring, and marks what is `[out of date]`,
  `[behind]`, `[pinned]` or `[not yet translated ...]`; `bb caller
  name@lang` shows the current versions that use it (`--all` adds replaced
  ones), so it lists nothing once a name is unused. Look at both before
  changing a function.

Functions are stored in $BB_DIRECTORY/pool/ (default: ~/.local/bb/pool/):
pool/functions/ holds JSON objects with the normalized code, pool/mappings/
the per-language name mappings, and pool/lineage/ the lineage steps that
say which version of each name comes after which. Each language mapping
preserves the original variable names, docstring, and BB import aliases,
allowing the same function to be displayed in any language that has been
added or translated. A pool written by an older bb (pool/XX/..., restore/)
is converted by `bb migrate`; track $BB_DIRECTORY with git first.

Worklogs live in $BB_DIRECTORY/worklog/, unfinished edits in
$BB_DIRECTORY/edit/, signed reviews in $BB_DIRECTORY/review/ and the
shared list of peers in $BB_DIRECTORY/trust/. bb does not manage history or sharing: track
$BB_DIRECTORY with git and use git's own commands to share it.
"""
import ast
import argparse
import builtins
import contextlib
import copy
import hashlib
import io
import itertools
import json
import keyword
import os
import re
import shlex
import subprocess
import sys
import struct
import symtable
import shutil
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

# Version of the normalization that produced a function's code, and so its
# hash. An identity holds under one normalization: the same source can hash
# differently under another. Stored in object.json as 'normalization';
# objects without it were produced by version 1.
#   1: names bound by imports inside a function body were renamed
#   2: they are kept, like top-level imports
#   3: `import a.b` binds `a`, which is kept too (version 2 renamed it, so
#      such functions failed at run time: there is nothing to repair)
#   4: a pool function is referred to as bb.ref("<hash>") where it is used,
#      instead of `from bb.pool import object_<hash>` and object_<hash>._bb_v_0
NORMALIZATION_VERSION = 4

_HASH_RE = re.compile(r'^[0-9a-f]{64}$')
BB_POOL_IMPORT_HINT = ("`from bb.pool import ...` is gone: write `import bb`, then "
                       "`NAME = bb.resolve(\"NAME\")` (by name) or `NAME = bb.ref(\"<hash>\")` (an exact version)")
# Calls naming a pool function by name, resolved when saving: bb.resolve,
# and bb.latest, its older spelling (the same thing)
BB_NAME_CALLS = ('resolve', 'latest')


def code_latest_name(node: ast.AST) -> Optional[str]:
    """The name in a bb.resolve("name") call (or bb.latest, the same), or None when node is not one."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in BB_NAME_CALLS
            and isinstance(node.func.value, ast.Name) and node.func.value.id == 'bb'
            and len(node.args) == 1 and not node.keywords and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str) and node.args[0].value.isidentifier()):
        return node.args[0].value
    return None


def code_replace_spans(source_code: str, spans: List[Tuple[ast.AST, str]]) -> str:
    """Replace the source of each node with its text (AST columns count UTF-8 bytes)."""
    lines = source_code.splitlines(keepends=True)
    for node, text in sorted(spans, key=lambda span: (span[0].lineno, span[0].col_offset), reverse=True):
        first = lines[node.lineno - 1].encode('utf-8')
        last = lines[node.end_lineno - 1].encode('utf-8')
        merged = first[:node.col_offset].decode('utf-8') + text + last[node.end_col_offset:].decode('utf-8')
        lines[node.lineno - 1:node.end_lineno] = [merged]
    return ''.join(lines)


def code_ref_hash(node: ast.AST) -> Optional[str]:
    """The hash in a bb.ref("<hash>") call, or None when node is not one."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'ref'
            and isinstance(node.func.value, ast.Name) and node.func.value.id == 'bb'
            and len(node.args) == 1 and not node.keywords and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str) and _HASH_RE.match(node.args[0].value)):
        return node.args[0].value
    return None


def code_make_ref(func_hash: str) -> ast.Call:
    """The expression bb.ref("<hash>"): the pool function func_hash."""
    return ast.Call(func=ast.Attribute(value=ast.Name(id='bb', ctx=ast.Load()), attr='ref', ctx=ast.Load()),
                    args=[ast.Constant(value=func_hash)], keywords=[])


def code_pool_import_hash(name: str) -> Optional[str]:
    """The hash in an import name object_<hash>, or None (then it is a function's name)."""
    if name.startswith(BB_IMPORT_PREFIX) and _HASH_RE.match(name[len(BB_IMPORT_PREFIX):]):
        return name[len(BB_IMPORT_PREFIX):]
    return None


def code_swap_refs(tree: ast.Module, hash_map: Dict[str, str]) -> ast.Module:
    """
    Point a stored function's pool references at other versions (old -> new hash).

    Handles both stored forms: bb.ref("<hash>") (normalization 4) and
    `from bb.pool import object_<hash>` with object_<hash>._bb_v_0 (before).
    Imports are sorted again, so the result stays in normal form.
    """
    class Swapper(ast.NodeTransformer):
        def visit_Call(self, node):
            func_hash = code_ref_hash(node)
            if func_hash in hash_map:
                return code_make_ref(hash_map[func_hash])
            self.generic_visit(node)
            return node

        def visit_ImportFrom(self, node):
            if node.module == 'bb.pool':
                for alias in node.names:
                    func_hash = alias.name[len(BB_IMPORT_PREFIX):]
                    if alias.name.startswith(BB_IMPORT_PREFIX) and func_hash in hash_map:
                        alias.name = BB_IMPORT_PREFIX + hash_map[func_hash]
            return node

        def visit_Attribute(self, node):
            if isinstance(node.value, ast.Name) and node.attr == '_bb_v_0':
                func_hash = node.value.id[len(BB_IMPORT_PREFIX):]
                if node.value.id.startswith(BB_IMPORT_PREFIX) and func_hash in hash_map:
                    node.value.id = BB_IMPORT_PREFIX + hash_map[func_hash]
            self.generic_visit(node)
            return node

    tree = Swapper().visit(tree)
    tree = code_sort_imports(tree)
    ast.fix_missing_locations(tree)
    return tree



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

    def visit_ClassDef(self, node):
        # A class defined inside the function is renamed like its uses
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
                # `import a.b` binds `a`
                imported.add(alias.asname if alias.asname else alias.name.split('.')[0])
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
                # `import a.b` binds `a`: never rename it
                imported_names.add(alias.asname if alias.asname else alias.name.split('.')[0])
        elif isinstance(imp, ast.ImportFrom):
            for alias in imp.names:
                imported_names.add(alias.asname if alias.asname else alias.name)

    # Imports inside the function body bind names too: they are never renamed,
    # otherwise the stored code would import `random` but call `_bb_v_2`
    for node in ast.walk(function_def):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                imported_names.add((alias.asname or alias.name).split('.')[0])

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


def code_collect_pool_refs(tree: ast.Module) -> Tuple[ast.Module, Dict[str, str]]:
    """
    Take the pool functions a module names out of its top level.

    They are named by `alias = bb.ref("<hash>")` (bb.resolve("name") is
    resolved to bb.ref first, see code_resolve_pool_names) or used as
    bb.ref("<hash>") directly; `import bb` is dropped, as the loader
    provides bb. Every use is then
    written bb.ref("<hash>") (see code_replace_bb_calls).

    Returns:
        (module without them, alias_mapping {hash: alias})

    Raises:
        ValueError: for a pool import not resolved to a hash, a pool function
        given two names, or a use of `bb` other than bb.ref("<hash>")
    """
    alias_mapping: Dict[str, str] = {}

    def bind(func_hash, alias):
        if alias and alias_mapping.get(func_hash, alias) != alias:
            raise ValueError(f"{func_hash[:12]}... is imported twice, as {alias_mapping[func_hash]} and {alias}")
        if alias:
            alias_mapping[func_hash] = alias

    body = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            names = ', '.join(alias.name for alias in node.names)
            raise ValueError(f"from bb.pool import {names}: {BB_POOL_IMPORT_HINT}")
        if isinstance(node, ast.Import) and [(a.name, a.asname) for a in node.names] == [('bb', None)]:
            continue
        if (isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                and code_ref_hash(node.value)):
            bind(code_ref_hash(node.value), node.targets[0].id)
            continue
        body.append(node)
    tree.body = body

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'bb':
            if node.attr in BB_NAME_CALLS:
                raise ValueError(f"{ast.unparse(node)}: bb.{node.attr} is resolved when saving (bb add, bb edit)")
            if node.attr != 'ref':
                raise ValueError(f"bb.{node.attr}: bb provides bb.resolve(\"NAME\") and bb.ref(\"<hash>\")")
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'ref'
              and isinstance(node.func.value, ast.Name) and node.func.value.id == 'bb' and code_ref_hash(node) is None):
            raise ValueError(f"{ast.unparse(node)}: bb.ref takes one pool function's hash, as a string "
                             f"(by name: bb.resolve(\"NAME\"))")
        elif ((isinstance(node, ast.Name) and node.id == 'bb' and not isinstance(node.ctx, ast.Load))
              or (isinstance(node, ast.arg) and node.arg == 'bb')):
            raise ValueError("the name bb is reserved for bb.ref: give this variable another name")
    return tree, alias_mapping


def code_replace_bb_calls(tree: ast.AST, alias_mapping: Dict[str, str], name_mapping: Dict[str, str]):
    """
    Write every use of a pool function as bb.ref("<hash>").

    E.g. kawa(...) becomes bb.ref("c0ffeebad...")(...).

    alias_mapping maps hash -> alias name
    """
    by_alias = {alias: func_hash for func_hash, alias in alias_mapping.items()}

    class BBCallReplacer(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id in by_alias and isinstance(node.ctx, ast.Load):
                return code_make_ref(by_alias[node.id])
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

    # Pool functions are referred to by bb.ref("<hash>"), not imported
    tree, alias_mapping = code_collect_pool_refs(tree)

    # Extract function and imports
    function_def, imports = code_extract_definition(tree)

    # Extract docstring from function
    docstring, function_without_docstring = code_extract_docstring(function_def)

    # bb itself is never renamed, nor the aliases of pool functions
    bb_aliases = set(alias_mapping.values()) | {'bb'}

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


def mapping_build(func_hash: str, lang: str, docstring: str, name_mapping: Dict[str, str],
                  alias_mapping: Dict[str, str], comment: str = "", markdown: str = "",
                  pinned: List[str] = None) -> Dict[str, Any]:
    """
    The content of a mapping file, as it is stored and hashed.

    'markdown' and 'pinned' are left out when empty. The function and the
    language are part of it, so a mapping names what it maps.
    """
    data = {
        'function': func_hash,
        'lang': lang,
        'docstring': docstring,
        'name_mapping': name_mapping,
        'alias_mapping': alias_mapping,
        'comment': comment,
    }
    if markdown:
        data['markdown'] = markdown
    if pinned:
        data['pinned'] = sorted(set(pinned))
    return data


def code_compute_mapping_hash(docstring: str, name_mapping: Dict[str, str],
                              alias_mapping: Dict[str, str], comment: str = "",
                              markdown: str = "", pinned: List[str] = None,
                              function: str = "", lang: str = "") -> str:
    """
    Compute content-addressed hash for a mapping.

    The hash is computed on the canonical JSON (sorted keys,
    ensure_ascii=False) of the mapping file's content, see mapping_build:
    function, lang, docstring, name_mapping, alias_mapping, comment, plus
    markdown and pinned when set (pinned: pool functions the author named
    by hash on purpose, bb.ref("<hash>"), so displays keep them pinned).

    This enables deduplication: identical mappings share the same hash and storage.

    Args:
        docstring: The function docstring for this mapping
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        markdown: Optional markdown documentation (from bb edit)
        pinned: Optional hashes referred to by bb.ref on purpose
        function: The function hash the mapping belongs to
        lang: The mapping's language

    Returns:
        64-character hex hash (SHA256)
    """
    mapping_dict = mapping_build(function, lang, docstring, name_mapping, alias_mapping, comment, markdown, pinned)
    return hash_compute(json.dumps(mapping_dict, sort_keys=True, ensure_ascii=False))


def code_detect_schema(func_hash: str) -> int:
    """
    Detect the schema version of a stored function.

    Returns:
        1 when pool/functions/XX/YYYY.../object.json exists, None if the
        function is not in the pool
    """
    return 1 if storage_object_path(func_hash).exists() else None


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
        ├── pool/          # functions/, mappings/ and lineage/ (see storage_get_functions_directory)
        ├── worklog/       # Worklogs: <uuid7>/worklog.json and translated messages
        ├── edit/          # Unfinished bb edit files (a scratch pad)
        ├── review/        # Signed reviews: XX/YYYY.../<key id>.json and .sig
        └── trust/         # allowed_signers: shared list of peers (not trusted by itself)

    Private state (config, review approvals, trusted peers) lives
    elsewhere: see storage_get_state_directory.
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


# Pool layout (storage v2):
#
#     pool/functions/XX/<rest>/object.json         a function: normalized code, checks,
#                                                  author; written once
#     pool/mappings/XX/<rest>/<lang>/<mapping>.json  one language's names and documentation
#                                                  of function XX<rest>, content-addressed
#     pool/lineage/XX/<rest>.json                  one lineage step, content-addressed
#
# A lineage step says that a (function, language, mapping) came after
# another step: an edit, a rename, a translation... Steps are per language
# and per name, so a name's current version is the end of its own lineage
# and other languages wait for a translation.

def storage_get_functions_directory() -> Path:
    """$BB_DIRECTORY/pool/functions: one object.json per function."""
    return storage_get_pool_directory() / 'functions'


def storage_get_mappings_directory() -> Path:
    """$BB_DIRECTORY/pool/mappings: per function, per language, the mappings."""
    return storage_get_pool_directory() / 'mappings'


def storage_get_lineage_directory() -> Path:
    """$BB_DIRECTORY/pool/lineage: one file per lineage step."""
    return storage_get_pool_directory() / 'lineage'


def storage_object_path(func_hash: str) -> Path:
    """Where a function's object.json lives."""
    return storage_get_functions_directory() / func_hash[:2] / func_hash[2:] / 'object.json'


def storage_mapping_directory(func_hash: str) -> Path:
    """The directory holding a function's mappings, one subdirectory per language."""
    return storage_get_mappings_directory() / func_hash[:2] / func_hash[2:]


def storage_mapping_path(func_hash: str, lang: str, mapping_hash: str) -> Path:
    """Where one mapping of a function lives."""
    return storage_mapping_directory(func_hash) / lang / f'{mapping_hash}.json'


def storage_step_path(step_id: str) -> Path:
    """Where one lineage step lives."""
    return storage_get_lineage_directory() / step_id[:2] / f'{step_id[2:]}.json'


OLD_LAYOUT_MESSAGE = ("This pool uses the old layout: run `bb migrate` "
                      "(track the directory with git first to keep a copy).")


def storage_pool_is_old_layout() -> bool:
    """
    Whether $BB_DIRECTORY holds a pool in the layout before storage v2.

    That layout kept functions in pool/XX/<rest>/ (two hex digits right
    under pool/) and restore records in $BB_DIRECTORY/restore/.
    """
    pool_dir = storage_get_pool_directory()
    if pool_dir.is_dir() and any(child.is_dir() and re.fullmatch(r'[0-9a-f]{2}', child.name)
                                 for child in pool_dir.iterdir()):
        return True
    restore_dir = storage_get_bb_directory() / 'restore'
    return restore_dir.is_dir() and any(restore_dir.glob('*.json'))


# Scans of the pool are cached for the length of a command, keyed by pool
# directory, and dropped by every write.
_STORAGE_CACHE: Dict[Tuple[str, str], Any] = {}


def storage_cache_clear():
    """Forget every cached scan (after a write)."""
    _STORAGE_CACHE.clear()


def storage_cached(key: str, build: Callable[[], Any]) -> Any:
    """The cached result of build() for this pool directory."""
    full_key = (str(storage_get_pool_directory()), key)
    if full_key not in _STORAGE_CACHE:
        _STORAGE_CACHE[full_key] = build()
    return _STORAGE_CACHE[full_key]


def storage_list_functions() -> Set[str]:
    """Hashes of every function in the pool (a directory listing)."""
    def build():
        found = set()
        functions_dir = storage_get_functions_directory()
        if not functions_dir.is_dir():
            return found
        for prefix_dir in functions_dir.iterdir():
            if not prefix_dir.is_dir():
                continue
            for func_dir in prefix_dir.iterdir():
                if (func_dir / 'object.json').exists():
                    found.add(prefix_dir.name + func_dir.name)
        return found
    return storage_cached('functions', build)


def storage_get_config_path() -> Path:
    """
    Get the path to the config file: your name, email, signing key and
    preferred languages.

    It is private state, in storage_get_state_directory(), not in the
    shared $BB_DIRECTORY: people sharing a store through git each keep
    their own identity. Can be overridden with BB_CONFIG_PATH.

    A config.json left in $BB_DIRECTORY by an older bb is copied here
    once, with a note on stderr.
    """
    config_override = os.environ.get('BB_CONFIG_PATH')
    if config_override:
        return Path(config_override)
    config_path = storage_get_state_directory() / 'config.json'
    old_path = storage_get_bb_directory() / 'config.json'
    if not config_path.exists() and old_path.exists():
        import shutil
        config_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(old_path, config_path)
        print(f"Note: moved your config to {config_path} (private, out of the shared directory). "
              f"You can delete {old_path}.", file=sys.stderr)
    return config_path


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
                checks: List[str] = None, normalization: int = NORMALIZATION_VERSION) -> bool:
    """
    Save function object to bb directory.

    Writes $BB_DIRECTORY/pool/functions/XX/YYYYYY.../object.json.

    object.json is written once: an existing object is never rewritten, so
    adding the same function again keeps its original metadata (creation
    time, author). Which version comes after which is told by lineage
    steps, not by the object.

    Args:
        hash_value: Function hash (64-character hex), see code_compute_function_hash
        normalized_code: Normalized code with docstring
        metadata: Metadata dict (created, author)
        checks: Hashes of the attached check functions, part of the definition
        normalization: Version of the normalization that produced the code
            (see NORMALIZATION_VERSION); code rewritten from a stored object
            keeps that object's version

    Returns:
        True when the object was written, False when it already existed
    """
    object_json = storage_object_path(hash_value)
    object_json.parent.mkdir(parents=True, exist_ok=True)

    if object_json.exists():
        print(f"Hash: {hash_value}")
        return False

    data = {
        'schema_version': 1,
        'hash': hash_value,
        'normalization': normalization,
        'normalized_code': normalized_code,
        'metadata': metadata
    }
    if checks:
        data['checks'] = sorted(set(checks))

    with open(object_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    storage_cache_clear()

    print(f"Hash: {hash_value}")
    return True


def object_normalization(hash_value: str) -> int:
    """The normalization version of a stored function (1 when not recorded)."""
    return object_load(hash_value).get('normalization', 1)


def object_load_checks(hash_value: str) -> List[str]:
    """Hashes of the checks attached to a function (part of its definition)."""
    return object_load(hash_value).get('checks', [])


def mapping_save(func_hash: str, lang: str, docstring: str,
                 name_mapping: Dict[str, str], alias_mapping: Dict[str, str],
                 comment: str = "", markdown: str = "", pinned: List[str] = None) -> str:
    """
    Save language mapping to bb directory.

    Writes $BB_DIRECTORY/pool/mappings/XX/Y.../<lang>/<mapping hash>.json,
    content-addressed (see code_compute_mapping_hash), enabling
    deduplication. Saving a mapping does not make it current: a lineage
    step does (see lineage_save).

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant
        markdown: Optional markdown documentation, stored under the 'markdown' key
        pinned: Optional hashes the author named by bb.ref on purpose

    Returns:
        Mapping hash (64-character hex)
    """
    data = mapping_build(func_hash, lang, docstring, name_mapping, alias_mapping, comment, markdown, pinned)
    mapping_hash = hash_compute(json.dumps(data, sort_keys=True, ensure_ascii=False))

    mapping_json = storage_mapping_path(func_hash, lang, mapping_hash)
    if not mapping_json.exists():
        mapping_json.parent.mkdir(parents=True, exist_ok=True)
        with open(mapping_json, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        storage_cache_clear()

    print(f"Mapping hash: {mapping_hash}")

    return mapping_hash


def code_save(hash_value: str, lang: str, normalized_code: str, docstring: str,
              name_mapping: Dict[str, str], alias_mapping: Dict[str, str], comment: str = "",
              pinned: List[str] = None) -> str:
    """
    Save a new function (bb add): its object, its mapping and an 'add' step.

    No step is added when the same mapping of the same function is
    already current (adding a file twice changes nothing).

    Args:
        hash_value: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")
        normalized_code: Normalized code with docstring
        docstring: Function docstring for this language
        name_mapping: Normalized name -> original name mapping
        alias_mapping: BB function hash -> alias mapping
        comment: Optional comment explaining this mapping variant

    Returns:
        The mapping hash
    """
    object_save(hash_value, normalized_code, code_create_metadata())
    mapping_hash = mapping_save(hash_value, lang, docstring, name_mapping, alias_mapping, comment, pinned=pinned)
    index = lineage_index()
    if not any(s in index['current'] for s in lineage_steps_on(hash_value, lang, mapping=mapping_hash)):
        lineage_save_new(hash_value, lang, mapping_hash, comment)
    return mapping_hash


def lineage_save_new(func_hash: str, lang: str, mapping_hash: str, comment: str = "") -> str:
    """
    Record a mapping saved without a step to continue: a new name, or a translation.

    When func_hash is current in other languages, the mapping translates
    it (bb add of the same code in another language is a translation,
    see translate_save_step), from your first language that has it;
    otherwise it starts a lineage ('add').

    Returns:
        The step's id
    """
    index = lineage_index()
    steps = index['steps']
    others = [s for s in lineage_steps_on(func_hash) if steps[s]['lang'] != lang and s in index['current']]
    if not others:
        return lineage_save(None, func_hash, lang, mapping_hash, 'add', comment)
    preferred = storage_read_config()['user'].get('languages') or []
    rank = {l: i for i, l in enumerate(preferred)}
    source = min(others, key=lambda s: (rank.get(steps[s]['lang'], len(rank)), steps[s]['lang'], s))
    return translate_save_step(func_hash, steps[source]['lang'], steps[source]['mapping'], lang, mapping_hash,
                               comment)


def _ast_unparser_class():
    """The class behind ast.unparse, or None: private, and loaded lazily by Python 3.14."""
    try:
        from _ast_unparse import Unparser  # Python 3.14
        return Unparser
    except ImportError:
        return getattr(ast, '_Unparser', None)


class _ReadableUnparser(_ast_unparser_class() or object):
    """
    ast.unparse for display: strings holding newlines are written as
    triple-quoted literals, as people write them (a stylesheet, a
    template), instead of one line of \\n escapes. Display only: what is
    shown parses back to the same tree, so to the same hash.
    """

    def visit_Constant(self, node):
        value = node.value
        if (isinstance(value, str) and '\n' in value.rstrip('\n') and node.kind is None
                and not getattr(self, '_avoid_backslashes', False) and not getattr(self, '_in_fstring', False)):
            for quote in ('"""', "'''"):
                if quote in value or value.endswith(quote[0]):
                    continue
                literal = quote + value.replace('\\', '\\\\') + quote
                try:
                    if ast.literal_eval(literal) == value:
                        self.write(literal)
                        return
                except (SyntaxError, ValueError):
                    pass
        super().visit_Constant(node)


def code_unparse_readable(tree: ast.AST) -> str:
    """Source for display (see _ReadableUnparser); plain ast.unparse where that is unavailable."""
    if _ReadableUnparser.__bases__ == (object,):
        return ast.unparse(tree)
    source = _ReadableUnparser().visit(tree)
    try:
        if ast.dump(ast.parse(source)) == ast.dump(ast.parse(ast.unparse(tree))):
            return source
    except SyntaxError:
        pass
    return ast.unparse(tree)


def code_denormalize(normalized_code: str, name_mapping: Dict[str, str], alias_mapping: Dict[str, str]) -> str:
    """
    Denormalize code by applying reverse name mappings.
    name_mapping: maps normalized names (_bb_v_X) to original names
    alias_mapping: maps function hashes to alias names

    Normalized code refers to pool functions as bb.ref("<hash>")
    (normalization 4) or object_<hash>._bb_v_0 with an import (before).
    Uses become the alias again; for bb.ref, each one used is bound once
    after the imports, as `alias = bb.ref("<hash>")`.
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

        def visit_ClassDef(self, node):
            # A class defined inside the function is renamed like its uses
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

        def visit_Call(self, node):
            # Replace bb.ref("c0ffeebad...")(...) with alias(...)
            func_hash = code_ref_hash(node)
            if func_hash in hash_to_alias:
                bound.setdefault(func_hash, hash_to_alias[func_hash])
                return ast.Name(id=hash_to_alias[func_hash], ctx=ast.Load())
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

    bound: Dict[str, str] = {}
    denormalizer = Denormalizer()
    tree = denormalizer.visit(tree)

    # Each pool function used is named once, after the imports: alias = bb.ref("<hash>")
    if bound:
        if not code_imports_bb(tree):
            tree.body.insert(0, ast.Import(names=[ast.alias(name='bb')]))
            tree = code_sort_imports(tree)
        position = sum(1 for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom)))
        bindings = [ast.Assign(targets=[ast.Name(id=alias, ctx=ast.Store())], value=code_make_ref(func_hash))
                    for func_hash, alias in bound.items()]
        tree.body[position:position] = bindings
        ast.fix_missing_locations(tree)

    return code_unparse_readable(tree)


def code_extract_dependencies(normalized_code: str) -> List[str]:
    """
    Extract bb dependencies from normalized code.

    Returns:
        List of actual function hashes (without object_ prefix) that this function depends on
    """
    dependencies = []
    tree = ast.parse(normalized_code)

    for node in ast.walk(tree):
        func_hash = code_ref_hash(node)
        if func_hash and func_hash not in dependencies:
            dependencies.append(func_hash)
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

    Copies each function's object (functions/XX/YYYY.../) and its mappings
    (mappings/XX/YYYY.../), in the pool's layout.

    Args:
        hashes: List of function hashes to bundle
        output_dir: Directory to copy files to

    Returns:
        Path to the output directory
    """
    import shutil

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for func_hash in hashes:
        if code_detect_schema(func_hash) is None:
            raise ValueError(f"Function not found: {func_hash}")
        for src_dir, kind in ((storage_object_path(func_hash).parent, 'functions'),
                              (storage_mapping_directory(func_hash), 'mappings')):
            if src_dir.exists():
                shutil.copytree(src_dir, output_dir / kind / func_hash[:2] / func_hash[2:], dirs_exist_ok=True)

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


def review_authored_path() -> Path:
    """Path of the file listing the functions you saved yourself (private state)."""
    return storage_get_state_directory() / 'authored.json'


def review_load_authored() -> Set[str]:
    """Hashes of the functions you saved with bb add or bb edit (or a refactor of them)."""
    path = review_authored_path()
    try:
        return set(json.loads(path.read_text(encoding='utf-8')).get('authored', []))
    except (OSError, ValueError):
        return set()


def review_approve_own(hashes: List[str]):
    """
    Record functions you just saved yourself.

    You wrote them, so bb edit may run them when it runs checks: a check
    on a function that imports your own pool functions needs no bb review
    of them first. This is all the record allows: bb run, bb validate
    --checks and serving still require bb review. It goes to your private
    state, never to the shared pool, so a function someone else commits
    under your name is not on it.
    """
    hashes = {h for h in hashes if h}
    if hashes:
        path = review_authored_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'authored': sorted(review_load_authored() | hashes)}, indent=2),
                        encoding='utf-8')


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


def command_reviews(identifier: str, as_json: bool = False):
    """
    Show who reviewed a function: every shared review and its signature.

    trusted: signed by a peer in your private list; untrusted: validly
    signed by someone you do not trust (yet); invalid: forged, altered or
    misplaced. Only trusted reviews count.
    """
    func_hash = helper_resolve_to_hash(identifier)
    yours = ("approved" if func_hash in review_load_state() else
             "rejected" if func_hash in review_load_rejections() else "not reviewed")
    reviews = review_list_shared(func_hash)
    if as_json:
        print(json.dumps({'function': func_hash, 'you': yours, 'reviews': reviews}, indent=2, ensure_ascii=False))
        return
    print(f"Function: {func_hash}")
    print(f"You: {yours}")
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


def command_log(identifier: str = None):
    """
    Show the history of the pool: functions, lineage steps and worklogs, newest first.

    With NAME (NAME@lang for one language), only that name's history: its
    lineage steps, the functions they saved and the worklogs attaching
    them. With a hash, the steps on that function.

    Functions show their names, author and languages. Lineage steps show
    what happened to a name in a language (add, edit, document, rename,
    refactor, restore, translate), from which step, by whom, and their
    comment. Worklogs show their author, languages, attached translations
    and message, in your preferred language when translated.
    """
    entries = []
    functions = storage_scan_functions()
    steps = lineage_index()['steps']
    keep_steps = keep_functions = None
    if identifier:
        name, _, lang = identifier.partition('@')
        if re.fullmatch(r'[0-9a-f]{64}', name):
            keep_steps = {i for i, r in steps.items() if r['function'] == name}
            keep_functions = {name}
        else:
            keep_steps = {i for i, r in steps.items() if r['name'] == name and (not lang or r['lang'] == lang)}
            keep_functions = {steps[i]['function'] for i in keep_steps}
        if not keep_steps and not (keep_functions & set(functions)):
            print(f"Error: No history for {identifier}", file=sys.stderr)
            sys.exit(1)
        functions = {h: f for h, f in functions.items() if h in keep_functions}
        steps = {i: r for i, r in steps.items() if i in keep_steps}
    for func_hash, function in functions.items():
        metadata = function['metadata']
        lines = [f"Hash: {func_hash}"]
        if function['names']:
            lines.append(f"Names: {', '.join(f'{name}@{lang}' for lang, name in sorted(function['names'].items()))}")
        lines += [f"Date: {metadata.get('created', 'unknown')}",
                  f"Author: {helper_format_author(metadata)}"]
        langs = storage_list_languages(func_hash)
        lines.append(f"Languages: {', '.join(langs) if langs else 'none'}")
        entries.append((metadata.get('created', ''), func_hash, lines))

    all_steps = lineage_index()['steps']
    for step_id, record in steps.items():
        metadata = record.get('metadata', {})
        lines = [f"Step: {step_id}",
                 f"{record['relation'].capitalize()}: {record['name']}@{record['lang']} {record['function']}"]
        previous = all_steps.get(record.get('from') or '')
        if previous:
            lines.append(f"From: {previous['name']}@{previous['lang']} {previous['function']}")
        translated = all_steps.get(record.get('translates') or '')
        if translated:
            lines.append(f"Translates: {translated['name']}@{translated['lang']} {translated['function']}")
        lines += [f"Date: {metadata.get('created', 'unknown')}",
                  f"Author: {helper_format_author(metadata)}"]
        if record.get('comment'):
            lines += ['', f"    {record['comment']}"]
        entries.append((metadata.get('created', ''), step_id, lines))

    worklogs = worklog_list()
    if keep_functions is not None:
        worklogs = [w for w in worklogs if any(h in str(reference) for reference in w['references']
                                               for h in keep_functions)]
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

    stepped = f", {len(steps)} lineage step{'' if len(steps) == 1 else 's'}" if steps else ""
    title = f"Log of {identifier}" if identifier else "Pool Log"
    print(f"{title} ({len(functions)} functions{stepped}, {len(worklogs)} worklogs)")
    print("=" * 80)
    print()

    for _, _, lines in entries:
        for line in lines:
            print(line)
        print()


def helper_previous_version(func_hash: str, lang: str = None, name: str = None) -> Optional[str]:
    """The function the current step of func_hash (in lang, called name) came from, when another one."""
    steps = lineage_index()['steps']
    for step_id in lineage_current_steps(lang, name, func_hash):
        previous = steps.get(steps[step_id].get('from') or '')
        if previous and previous['function'] != func_hash:
            return previous['function']
    return None


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


def lineage_prune() -> int:
    """
    Remove the lineage steps whose version is gone and that no step follows.

    A version is gone when its mapping (or its whole function) was
    removed. Steps that later steps come after are kept, so the history
    of the versions that remain stays connected. Repeats until nothing
    changes, and returns the number of steps removed.
    """
    removed = 0
    while True:
        storage_cache_clear()
        index = lineage_index()
        gone = [step_id for step_id, record in index['steps'].items()
                if step_id not in index['successors']
                and not storage_mapping_path(record['function'], record['lang'], record['mapping']).exists()]
        if not gone:
            break
        for step_id in gone:
            storage_step_path(step_id).unlink()
            removed += 1
    storage_cache_clear()
    return removed


# ============================================================================
# Lineage
# ============================================================================
#
# A lineage step is an append-only record in pool/lineage/XX/<rest>.json:
#
#     {from: <step id> | null, function, lang, mapping, name, relation,
#      comment, metadata: {created, name, email}, translates?: <step id>}
#
# Its id is the hash of that canonical JSON. A step stands for the version
# (function, lang, mapping) of the name 'name' (the mapping's _bb_v_0) in
# 'lang', coming after the step 'from'. Relations: add (a new name), edit
# (new code), document (same code, new names or documentation), rename
# (the name changed: the old name is superseded), refactor (references
# rewritten mechanically), restore (an older version's code again),
# translate (a mapping in another language; 'translates' is the step it
# translates).
#
# The current steps are those no step comes after; the current versions of
# NAME@LANG are the functions of the current steps of LANG called NAME. So
# each name, in each language, follows its own lineage: two names may
# share one code, and a French name stays on its version until someone
# translates the English edit.

LINEAGE_RELATIONS = ('add', 'edit', 'document', 'rename', 'refactor', 'restore', 'translate')


def storage_scan_lineage() -> Dict[str, Dict[str, Any]]:
    """Every lineage step in the pool, by id."""
    def build():
        steps = {}
        lineage_dir = storage_get_lineage_directory()
        if not lineage_dir.is_dir():
            return steps
        for prefix_dir in lineage_dir.iterdir():
            if not prefix_dir.is_dir():
                continue
            for path in prefix_dir.glob('*.json'):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        steps[prefix_dir.name + path.stem] = json.load(f)
                except (OSError, json.JSONDecodeError):
                    continue
        return steps
    return storage_cached('lineage', build)


def lineage_step_id(record: Dict[str, Any]) -> str:
    """A step's id: the hash of its canonical JSON."""
    return hash_compute(json.dumps(record, sort_keys=True, ensure_ascii=False))


def lineage_save(previous: Optional[str], func_hash: str, lang: str, mapping_hash: str, relation: str,
                 comment: str = "", translates: str = None, metadata: Dict[str, Any] = None) -> str:
    """
    Record a lineage step: (func_hash, lang, mapping_hash) comes after previous.

    Args:
        previous: The step this one follows (None: the start of a lineage)
        relation: One of LINEAGE_RELATIONS
        translates: For a translation, the step in the source language
        metadata: Author and date (default: code_create_metadata())

    Returns:
        The step's id
    """
    assert relation in LINEAGE_RELATIONS, relation
    record = {
        'from': previous or None,
        'function': func_hash,
        'lang': lang,
        'mapping': mapping_hash,
        'name': mapping_name(func_hash, lang, mapping_hash) or '',
        'relation': relation,
        'comment': comment or '',
        'metadata': metadata or code_create_metadata(),
    }
    if translates:
        record['translates'] = translates
    step_id = lineage_step_id(record)
    path = storage_step_path(step_id)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        storage_cache_clear()
    return step_id


def lineage_index() -> Dict[str, Any]:
    """
    The lineage of the whole pool, indexed.

    Returns:
        Dict with 'steps' ({id: record}), 'successors' ({id: ids of the
        steps right after it}), 'by_function' ({hash: ids of the steps on
        it}), 'current' (ids of the steps nothing follows, whose function
        is in the pool) and 'memo' (for lineage_ancestors). A step whose
        'from' is not in the pool starts a lineage.
    """
    def build():
        steps = storage_scan_lineage()
        present = storage_list_functions()
        successors: Dict[str, List[str]] = {}
        by_function: Dict[str, List[str]] = {}
        for step_id, record in sorted(steps.items()):
            if record.get('from') in steps:
                successors.setdefault(record['from'], []).append(step_id)
            by_function.setdefault(record['function'], []).append(step_id)
        current = {step_id for step_id, record in steps.items()
                   if step_id not in successors and record['function'] in present}
        return {'steps': steps, 'successors': successors, 'by_function': by_function,
                'current': current, 'memo': {}}
    return storage_cached('lineage_index', build)


def lineage_created(step_id: str) -> str:
    """When a step was recorded (ISO 8601), '' when unknown."""
    return lineage_index()['steps'][step_id].get('metadata', {}).get('created', '')


def lineage_steps_on(func_hash: str, lang: str = None, name: str = None, mapping: str = None) -> List[str]:
    """Ids of the steps on func_hash, in lang, called name, with mapping (each when given)."""
    index = lineage_index()
    steps = index['steps']
    return [s for s in index['by_function'].get(func_hash, [])
            if (lang is None or steps[s]['lang'] == lang) and (name is None or steps[s]['name'] == name)
            and (mapping is None or steps[s]['mapping'] == mapping)]


def lineage_current_steps(lang: str = None, name: str = None, function: str = None) -> List[str]:
    """Ids of the current steps, in lang, called name, on function (each when given), sorted."""
    index = lineage_index()
    steps = index['steps']
    return sorted(s for s in index['current']
                  if (lang is None or steps[s]['lang'] == lang) and (name is None or steps[s]['name'] == name)
                  and (function is None or steps[s]['function'] == function))


def lineage_follow(func_hash: str, lang: str = None, name: str = None) -> List[str]:
    """
    The current steps reached from the steps on func_hash (in lang, called name).

    They stand for the up-to-date versions of func_hash: itself when one
    of its steps is current, otherwise what came after it.
    """
    index = lineage_index()
    stack = lineage_steps_on(func_hash, lang, name)
    seen, reached = set(), set()
    while stack:
        step_id = stack.pop()
        if step_id in seen:
            continue
        seen.add(step_id)
        following = index['successors'].get(step_id)
        if following:
            stack.extend(following)
        elif step_id in index['current']:
            reached.add(step_id)
    return sorted(reached)


def lineage_step_for(func_hash: str, lang: str, name: str = None, mapping: str = None) -> Optional[str]:
    """
    The step an edit of func_hash in lang continues from.

    Prefers a step on mapping, then one called name, then a current step,
    then the latest one. None when func_hash has no step in lang.
    """
    index = lineage_index()
    candidates = lineage_steps_on(func_hash, lang)
    if not candidates:
        return None
    steps = index['steps']
    return max(candidates, key=lambda s: (mapping is not None and steps[s]['mapping'] == mapping,
                                          name is not None and steps[s]['name'] == name,
                                          s in index['current'], lineage_created(s), s))


def lineage_root_step(step_id: str, translations: bool = False) -> str:
    """
    The first step of step_id's lineage (following 'from').

    With translations, a translation that starts a lineage leads on to
    the step it translates, so every language of one function's history
    has one root.
    """
    steps = lineage_index()['steps']
    node, seen = step_id, set()
    while node not in seen:
        seen.add(node)
        record = steps[node]
        if record.get('from') in steps:
            node = record['from']
        elif translations and record.get('translates') in steps:
            node = record['translates']
        else:
            break
    return node


def lineage_ancestors(step_id: str) -> Set[str]:
    """step_id and every step before it, through 'from' and 'translates'."""
    index = lineage_index()
    memo = index['memo']
    if step_id in memo:
        return memo[step_id]
    steps = index['steps']
    found, stack = set(), [step_id]
    while stack:
        node = stack.pop()
        if node in found or node not in steps:
            continue
        if node in memo and node != step_id:
            found |= memo[node]
            continue
        found.add(node)
        stack.extend(link for link in (steps[node].get('from'), steps[node].get('translates')) if link)
    memo[step_id] = found
    return found


def lineage_waiting(step_id: str) -> List[str]:
    """
    The languages that have a newer version than step_id, not yet translated.

    A step S (language L, function F) waits for translation when another
    language M has a current step G on another function, with S (or a
    step S translates) among G's ancestors: G came after what S stands
    for. E.g. an English edit leaves the French name on the previous
    version: that French step waits for a translation of the edit.

    Returns:
        Sorted language codes (the M above), [] when S is up to date
    """
    index = lineage_index()
    steps = index['steps']
    record = steps[step_id]
    same = {step_id}
    node = record.get('translates')
    while node in steps and node not in same:
        same.add(node)
        node = steps[node].get('translates')
    langs = set()
    for other in index['current']:
        other_record = steps[other]
        if other_record['lang'] == record['lang'] or other_record['function'] == record['function']:
            continue
        if same & lineage_ancestors(other):
            langs.add(other_record['lang'])
    return sorted(langs)


def lineage_name_for(func_hash: str, lang: str, alias: str = None) -> Optional[str]:
    """
    The name a caller follows func_hash by, in lang.

    alias when func_hash is called so in lang, otherwise its only name
    there; None when it has several (or none) and alias is none of them.
    """
    names = sorted({lineage_index()['steps'][s]['name'] for s in lineage_steps_on(func_hash, lang)})
    if alias in names:
        return alias
    return names[0] if len(names) == 1 else None


def lineage_preferred_language(func_hash: str) -> Optional[str]:
    """The language to follow func_hash's lineage in: yours first, then any it has steps in."""
    langs = sorted({lineage_index()['steps'][s]['lang'] for s in lineage_steps_on(func_hash)})
    preferred = storage_read_config()['user'].get('languages') or []
    for lang in preferred + langs:
        if lang in langs:
            return lang
    return None


def lineage_newer_version(func_hash: str, lang: str = None, name: str = None) -> Optional[str]:
    """
    The one up-to-date version of func_hash, when it is not func_hash.

    Follows func_hash's steps in lang (default: lineage_preferred_language),
    called name when given, to the current steps. A caller may use a
    function that was edited since: rewriting the version it uses would
    fork it beside that edit, so refactors and updates build on the
    current version instead. With several current versions (a fork) there
    is no single one: None, with a warning.
    """
    lang = lang or lineage_preferred_language(func_hash)
    if lang is None:
        return None
    steps = lineage_index()['steps']
    heads = sorted({steps[s]['function'] for s in lineage_follow(func_hash, lang, name)})
    if not heads or func_hash in heads:
        return None
    if len(heads) > 1:
        print(f"Warning: {helper_function_label(func_hash)} has {len(heads)} current versions; "
              f"keeping the one in use ({func_hash[:12]}...)", file=sys.stderr)
        return None
    return heads[0]


def lineage_continue(old_hash: str, new_hash: str, comment: str, hash_map: Dict[str, str] = None,
                     relation: str = 'refactor', keep: Callable[[Dict[str, Any]], bool] = None) -> str:
    """
    Move every name of old_hash on to new_hash, copying their mappings.

    For each current step on old_hash (every language, every name), or,
    when old_hash is current nowhere, its latest step per language and
    name, new_hash gets a copy of the step's mapping (references renamed
    through hash_map) and a step after it. Names that share the code move
    together, unless keep (given a step record) says a name stays. A step
    on a function its lineage already went through is a restore.

    Returns:
        'restored' when new_hash was an older version, 'new' otherwise
    """
    hash_map = hash_map or {}
    index = lineage_index()
    steps = index['steps']
    on_old = lineage_steps_on(old_hash)
    starts = [s for s in on_old if s in index['current']]
    if not starts:
        latest: Dict[Tuple[str, str], str] = {}
        for s in on_old:
            key = (steps[s]['lang'], steps[s]['name'])
            if key not in latest or (lineage_created(s), s) > (lineage_created(latest[key]), latest[key]):
                latest[key] = s
        starts = sorted(latest.values())
    restored = False
    moves = []
    for step_id in starts:
        record = steps[step_id]
        if keep is not None and not keep(record):
            continue
        lang, mapping_hash = record['lang'], record['mapping']
        data = mapping_load_json(old_hash, lang, mapping_hash)
        if not data:
            continue
        was = new_hash in {steps[s]['function'] for s in lineage_ancestors(step_id)}
        moves.append((step_id, lang, data, was))
    for step_id, lang, data, was in moves:
        with contextlib.redirect_stdout(io.StringIO()):
            new_mapping = mapping_save(new_hash, lang, data.get('docstring', ''), data.get('name_mapping', {}),
                                       {hash_map.get(h, h): a for h, a in data.get('alias_mapping', {}).items()},
                                       comment, data.get('markdown', ''),
                                       pinned=[hash_map.get(h, h) for h in data.get('pinned') or []])
        restored = restored or was
        lineage_save(step_id, new_hash, lang, new_mapping, 'restore' if was else relation, comment)
    return 'restored' if restored else 'new'


def storage_scan_functions() -> Dict[str, Dict[str, Any]]:
    """
    Read every function in the pool once: metadata, checks, normalized
    code, plus its default mapping in each language and its current names.

    Returns:
        {hash: {'hash', 'metadata', 'checks', 'normalized_code', 'normalization',
                'names': {lang: name} (a display name per language),
                'current_names': {lang: [names]} (the names it is current for),
                'current': whether it is the current version of some name,
                'comments': {lang: comment}, 'mappings': {lang: mapping data},
                'all_mappings': {lang: [mapping data]} (current or not superseded)}}
    """
    def build():
        index = lineage_index()
        steps = index['steps']
        functions: Dict[str, Dict[str, Any]] = {}
        for func_hash in sorted(storage_list_functions()):
            try:
                with open(storage_object_path(func_hash), 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (IOError, json.JSONDecodeError):
                continue
            names, comments, mappings, all_mappings = {}, {}, {}, {}
            for lang in storage_list_languages(func_hash):
                mapping_hash = mapping_select(func_hash, lang)
                if mapping_hash is None:
                    continue
                mapping = mapping_load_json(func_hash, lang, mapping_hash)
                mappings[lang] = mapping
                names[lang] = mapping.get('name_mapping', {}).get('_bb_v_0')
                comments[lang] = mapping.get('comment', '')
                shown = {s_mapping for s_mapping in mapping_leaves(func_hash, lang)}
                shown |= {steps[s]['mapping'] for s in lineage_steps_on(func_hash, lang) if s in index['current']}
                all_mappings[lang] = [mapping_load_json(func_hash, lang, m) for m in sorted(shown)]
            current_names: Dict[str, Set[str]] = {}
            for step_id in index['by_function'].get(func_hash, []):
                if step_id in index['current']:
                    current_names.setdefault(steps[step_id]['lang'], set()).add(steps[step_id]['name'])
            functions[func_hash] = {
                'hash': func_hash,
                'metadata': data.get('metadata', {}),
                'checks': data.get('checks', []),
                'normalization': data.get('normalization', 1),
                'normalized_code': data.get('normalized_code', ''),
                'names': names,
                'current_names': {lang: sorted(n) for lang, n in sorted(current_names.items())},
                'current': bool(current_names),
                'comments': comments,
                'mappings': mappings,
                'all_mappings': all_mappings,
            }
        return functions
    return storage_cached('scan_functions', build)


def helper_find_heads_by_name(name: str, lang: str = None) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """
    Find the current versions of the functions called name.

    They are the functions of the current lineage steps called name in
    lang (in any language without lang). Each name follows its own
    lineage: a rename ends the old name's, and an edit in one language
    does not move the others. Creation time plays no part, so two agents
    forking the same function give two current versions.

    Returns:
        (current version hashes, newest first; the scan from storage_scan_functions)
    """
    functions = storage_scan_functions()
    created: Dict[str, str] = {}
    steps = lineage_index()['steps']
    for step_id in lineage_current_steps(lang, name):
        func_hash = steps[step_id]['function']
        if func_hash in functions:
            created[func_hash] = max(created.get(func_hash, ''), lineage_created(step_id))
    ordered = sorted(created, key=lambda h: (created[h], h), reverse=True)
    return ordered, functions


def helper_resolve_heads(name: str, lang: str) -> Tuple[List[str], Dict[str, Dict[str, Any]], Optional[str]]:
    """
    The current versions of name, through your favourite languages.

    Tries lang (the language being saved), then your languages (bb whoami
    language) in order, then any language; the first that knows the name
    decides, even when it has several current versions.

    Returns:
        (heads, the scan, the language they were found in, None for "any")
    """
    preferred = storage_read_config()['user'].get('languages') or []
    for candidate in [lang] + [l for l in preferred if l != lang]:
        heads, functions = helper_find_heads_by_name(name, candidate)
        if heads:
            return heads, functions, candidate
    heads, functions = helper_find_heads_by_name(name)
    return heads, functions, None


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
    current version (in lang, when given) or several of them.

    Returns:
        (hash, lang) where lang is the requested one, or a language in
        which the function is currently called name
    """
    lang_str = f"@{lang}" if lang else ""
    heads, functions = helper_find_heads_by_name(name, lang)

    if not heads:
        print(f"No function found with name: {name}{lang_str}", file=sys.stderr)
        elsewhere = sorted({lineage_index()['steps'][s]['lang'] for s in lineage_current_steps(name=name)})
        if lang and elsewhere:
            print(f"'{name}' is in {', '.join(elsewhere)}: translate it with "
                  f"bb translate {name}@{elsewhere[0]} {lang}", file=sys.stderr)
        sys.exit(1)

    if len(heads) > 1:
        print(f"Error: '{name}{lang_str}' is ambiguous: it has {len(heads)} current versions. "
              f"Use a hash:", file=sys.stderr)
        for func_hash in heads:
            print(f"  {helper_describe_function(functions[func_hash], lang)}", file=sys.stderr)
        sys.exit(1)

    head = heads[0]
    if lang:
        return head, lang
    steps = lineage_index()['steps']
    langs = sorted({steps[s]['lang'] for s in lineage_current_steps(name=name, function=head)})
    return head, langs[0]


def command_latest(name: str, lang: str = None, as_json: bool = False, hash_only: bool = False):
    """
    List the current versions of a function by name.

    A current version is the end of the name's lineage in its language:
    no edit, rename or refactor came after it there. One line per version
    means the name is unambiguous; several mean forks diverged (or
    unrelated functions share the name) and commands need a hash. A
    version waiting for the translation of a newer one (another language
    moved on) says so.

    Args:
        name: Function name to search for
        lang: Optional language code to filter results
        hash_only: print only the hashes, one per line (for scripts)
    """
    heads, functions = helper_find_heads_by_name(name, lang)
    lang_str = f"@{lang}" if lang else ""
    if not heads:
        print(f"No function found with name: {name}{lang_str}", file=sys.stderr)
        sys.exit(1)

    if hash_only:
        for func_hash in heads:
            print(func_hash)
        return

    def waiting(func_hash):
        """(newer language, this language) pairs: versions still to translate."""
        steps = lineage_index()['steps']
        return sorted({(other, steps[s]['lang']) for s in lineage_current_steps(lang, name, func_hash)
                       for other in lineage_waiting(s)})

    if as_json:
        print(json.dumps([{'hash': h, 'created': functions[h]['metadata'].get('created', ''),
                           'author': helper_format_author(functions[h]['metadata']),
                           'parent': helper_previous_version(h, lang, name),
                           'languages': sorted(functions[h]['names']),
                           'newer_in': sorted({other for other, _ in waiting(h)})} for h in heads],
                         indent=2, ensure_ascii=False))
        return

    print(f"Current versions of '{name}{lang_str}': {len(heads)}")
    print()
    for func_hash in heads:
        function = functions[func_hash]
        metadata = function['metadata']
        print(f"Hash: {func_hash}")
        print(f"Date: {metadata.get('created', 'unknown')}")
        print(f"Author: {helper_format_author(metadata)}")
        parent = helper_previous_version(func_hash, lang, name)
        if parent:
            print(f"Parent: {parent}")
        print(f"Languages: {', '.join(sorted(function['names']))}")
        for other, here in waiting(func_hash):
            print(f"Note: newer version in {other}, not yet translated to {here}")
        print()

    if len(heads) > 1:
        print("Several current versions: use a hash to pick one.")


def storage_pool_index() -> Dict[str, Any]:
    """
    Scan the pool once and index its relations.

    Returns:
        Dict with 'functions' (see storage_scan_functions), 'lineage' (see
        lineage_index), 'current' (hashes of functions that are the
        current version of some name), 'importers' (hash -> functions
        importing it) and 'checked_by' (check hash -> functions it is
        attached to)
    """
    functions = storage_scan_functions()
    importers: Dict[str, List[str]] = {}
    checked_by: Dict[str, List[str]] = {}
    for func_hash, function in functions.items():
        for dep in code_extract_dependencies(function['normalized_code']):
            importers.setdefault(dep, []).append(func_hash)
        for check_hash in function['checks']:
            checked_by.setdefault(check_hash, []).append(func_hash)
    return {'functions': functions, 'lineage': lineage_index(),
            'current': {h for h, f in functions.items() if f['current']},
            'importers': importers, 'checked_by': checked_by}


def helper_display_language(function: Dict[str, Any], prefer: List[str] = None) -> Optional[str]:
    """
    The language to show a function in: one where it is current first
    (your languages, then prefer, then any), then any it has.
    """
    preferred = storage_read_config()['user'].get('languages') or []
    for pool in (function.get('current_names', {}), function['names']):
        for lang in preferred + (prefer or []) + sorted(pool):
            if lang in pool and lang in function['names']:
                return lang
    return None


def search_uses_under_name(caller: Dict[str, Any], dep_hash: str, identifier: str) -> bool:
    """
    Whether caller uses dep_hash under the name identifier gives (NAME@lang).

    One code may carry several names (stats_card and stat_card): by name,
    a caller counts only when its current or unreplaced mappings in that
    language call the code by that name. A hash, or a name without
    language, matches every caller.
    """
    name, _, lang = identifier.partition('@')
    if not lang or re.fullmatch(r'[0-9a-f]{64}', name):
        return True
    aliases = [m.get('alias_mapping', {}).get(dep_hash) for m in caller['all_mappings'].get(lang, [])]
    aliases = [alias for alias in aliases if alias]
    return not aliases or name in aliases


def search_pins(function: Dict[str, Any]) -> Set[str]:
    """Every reference a function pins on purpose (bb.ref), in any language."""
    return {pin for mapping in function['mappings'].values() for pin in (mapping.get('pinned') or [])}


def search_outdated_refs(index: Dict[str, Any], func_hash: str) -> List[str]:
    """
    The versions a function uses that are no longer current, pins excepted.

    A use of D, under alias A in the caller's mapping in language L, is
    out of date when D's lineage for A in L (D's own name there when A is
    not one) went on to another function: the current version of that
    name in L is not D. Languages where the caller is current are looked
    at (all its languages when it is current nowhere). A D with no
    lineage in L is followed in every language.
    """
    memo = index.setdefault('outdated_memo', {})
    if func_hash in memo:
        return memo[func_hash]
    functions = index['functions']
    function = functions[func_hash]
    steps = index['lineage']['steps']
    pins = search_pins(function)
    langs = sorted(function['current_names']) or sorted(function['mappings'])
    outdated = []
    for dep in code_extract_dependencies(function['normalized_code']):
        if dep in pins or dep not in functions or dep in outdated:
            continue
        for lang in langs:
            # Every mapping of the language: two names of one code may use dep by two names
            mappings = function['all_mappings'].get(lang) or [function['mappings'].get(lang, {})]
            for alias in {m.get('alias_mapping', {}).get(dep) for m in mappings}:
                if lineage_steps_on(dep, lang):
                    heads = {steps[s]['function']
                             for s in lineage_follow(dep, lang, lineage_name_for(dep, lang, alias))}
                else:
                    heads = {steps[s]['function'] for s in lineage_follow(dep)}
                if heads and dep not in heads:
                    outdated.append(dep)
                    break
            if dep in outdated:
                break
    memo[func_hash] = outdated
    return outdated


def search_untranslated(func_hash: str) -> List[Tuple[str, str]]:
    """(newer language, language) pairs where func_hash waits for a translation (see lineage_waiting)."""
    steps = lineage_index()['steps']
    return sorted({(other, steps[s]['lang']) for s in lineage_current_steps(function=func_hash)
                   for other in lineage_waiting(s)})


def search_behind(index: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Current functions that must follow once what they use is brought up to date.

    A function is behind when a current function it uses (not pinned) is
    out of date or behind itself: saving that one makes a new version this
    one does not use yet. Maps hash -> the functions it uses that hold it back.
    """
    functions, current = index['functions'], index['current']
    memo: Dict[str, bool] = {}

    def lags(func_hash: str, seen: Tuple[str, ...] = ()) -> bool:
        if func_hash in memo:
            return memo[func_hash]
        if func_hash in seen or func_hash not in functions:
            return False
        function = functions[func_hash]
        pins = search_pins(function)
        deps = [d for d in code_extract_dependencies(function['normalized_code']) if d not in pins]
        result = bool(search_outdated_refs(index, func_hash)) or \
            any(d in current and lags(d, seen + (func_hash,)) for d in deps)
        memo[func_hash] = result
        return result

    behind = {}
    for func_hash in current:
        function = functions[func_hash]
        pins = search_pins(function)
        held = [d for d in code_extract_dependencies(function['normalized_code'])
                if d not in pins and d in current and lags(d)]
        if held:
            behind[func_hash] = held
    return behind


def search_describe(index: Dict[str, Any], func_hash: str, approved: Set[str],
                    head_counts: Dict[Tuple[str, str, str], Set[str]], match: List[str] = None,
                    prefer: List[str] = None, behind: Dict[str, List[str]] = None) -> Dict[str, Any]:
    """Everything search shows about one function, as a dict (also the --json form)."""
    function = index['functions'][func_hash]
    lang = helper_display_language(function, prefer)
    name = function['names'].get(lang) if lang else None
    if lang in function['current_names'] and name not in function['current_names'][lang]:
        name = function['current_names'][lang][0]
    mapping = function['mappings'].get(lang, {}) if lang else {}
    if mapping.get('name_mapping', {}).get('_bb_v_0') != name:
        mapping = next((m for m in function['all_mappings'].get(lang, [])
                        if m.get('name_mapping', {}).get('_bb_v_0') == name), mapping)
    docstring = mapping.get('docstring', '')
    is_current = func_hash in index['current']
    on = lineage_current_steps(lang, name, func_hash) if lang else []
    root = lineage_root_step(on[0]) if on else None
    forks = len(head_counts.get((lang, name, root), ())) if root else 0
    namesakes = sum(len(heads) for (l, n, r), heads in head_counts.items() if (l, n) == (lang, name) and r != root)
    return {
        'hash': func_hash,
        'name': name,
        'lang': lang,
        'languages': sorted(function['names']),
        'created': function['metadata'].get('created', ''),
        'author': helper_format_author(function['metadata']),
        'parent': helper_previous_version(func_hash, lang, name) if lang else None,
        'current': is_current,
        'diverged': is_current and forks > 1,
        'outdated': search_outdated_refs(index, func_hash),
        # The names this function calls them by (a hash may carry several names)
        'aliases': dict(mapping.get('alias_mapping') or {}),
        'behind': (behind or {}).get(func_hash, []),
        'untranslated': [list(pair) for pair in search_untranslated(func_hash)],
        'pinned': sorted(set(mapping.get('pinned') or [])),
        'forks': forks,
        'namesakes': namesakes,
        'checks': len(function['checks']),
        'normalization': function['normalization'],
        'is_check': func_hash in index['checked_by'],
        'reviewed': func_hash in approved,
        'callers': len(index['importers'].get(func_hash, [])),
        'description': docstring.strip().split('\n')[0] if docstring else '',
        'match': match or [],
    }


def command_search(query: List[str], all_versions: bool = False, as_json: bool = False,
                   untested: bool = False, unreviewed: bool = False, diverged: bool = False,
                   unused: bool = False, uses: str = None, outdated: bool = False, similar: bool = False):
    """
    Search the pool by words, by status, and by architecture.

    Words must all appear, in any language, in a function's name,
    docstring, documentation, variable names or code. Results show in
    your preferred language, with their checks, your review status and
    whether their name has diverged into several current versions. Only
    current versions (the end of some name's lineage) are listed unless --all.

    With no words and no filter, every current function is listed.

    Filters, combinable with words or used alone:

    ```
    --untested    functions without checks (checks themselves excluded)
    --unreviewed  functions you have not approved (see bb review)
    --diverged    forks: one lineage with several current versions (not
                  unrelated functions that share a name)
    --unused      functions nothing imports and no function uses as a check
    --outdated    functions using versions that are no longer current (their
                  debt: editing one brings it up to date; pins are kept),
                  and the functions above them that must follow (behind);
                  ends with the `bb update` commands that settle it all.
                  Also versions waiting for a translation: a newer version
                  in another language, not yet translated to this one
    --uses X      functions that import X directly
    --similar     functions whose code looks like another current function,
                  or that one code carries under several names
                  (the comparison behind the note after a save): candidates
                  for merging into one shared function
    --json        machine-readable output
    ```
    """
    uses_hash = helper_resolve_to_hash(uses) if uses else None
    terms = [term.lower() for term in query]
    index = storage_pool_index()
    functions = index['functions']

    # A name diverged when one lineage has several current versions (a fork);
    # unrelated functions that happen to share a name are counted apart
    head_counts: Dict[Tuple[str, str, str], Set[str]] = {}
    steps = index['lineage']['steps']
    for step_id in index['lineage']['current']:
        record = steps[step_id]
        if record['function'] in functions:
            key = (record['lang'], record['name'], lineage_root_step(step_id))
            head_counts.setdefault(key, set()).add(record['function'])

    candidates = []
    for func_hash, function in functions.items():
        if not all_versions and func_hash not in index['current']:
            continue

        # Every term must appear somewhere; remember where, and in which languages
        fields: Dict[str, str] = {'code': function['normalized_code'].lower()}
        langs_matched = []
        for lang, mappings in function['all_mappings'].items():
            for mapping in mappings:
                texts = {
                    'name': (mapping.get('name_mapping', {}).get('_bb_v_0') or ''),
                    'docstring': mapping.get('docstring', ''),
                    'docs': mapping.get('markdown', ''),
                    'variables': ' '.join(mapping.get('name_mapping', {}).values()),
                }
                for key, text in texts.items():
                    fields[key] = fields.get(key, '') + ' ' + text.lower()
                if terms and lang not in langs_matched and \
                        all(any(t in text.lower() for text in texts.values()) for t in terms):
                    langs_matched.append(lang)
        if terms and not all(any(t in text for text in fields.values()) for t in terms):
            continue
        match = [key for key in ('name', 'docstring', 'docs', 'variables', 'code')
                 if any(t in fields.get(key, '') for t in terms)]
        candidates.append((func_hash, match, langs_matched))

    approved = review_approvals([h for h, _, _ in candidates])
    behind = search_behind(index)
    results = [search_describe(index, h, approved, head_counts, match, langs, behind)
               for h, match, langs in candidates]

    if untested:
        results = [r for r in results if r['checks'] == 0 and not r['is_check']]
    if unreviewed:
        results = [r for r in results if not r['reviewed']]
    if diverged:
        results = [r for r in results if r['diverged']]
    if unused:
        results = [r for r in results if r['callers'] == 0 and not r['is_check']]
    if outdated:
        results = [r for r in results if r['outdated'] or r['behind'] or r['untranslated']]
    if uses_hash:
        results = [r for r in results if r['hash'] in index['importers'].get(uses_hash, [])
                   and search_uses_under_name(index['functions'][r['hash']], uses_hash, uses)]
    if similar:
        for r in results:
            r['similar'] = [(round(score, 2), other) for score, other in
                            code_find_similar(r['hash'], helper_lineage(r['hash']) | set(
                                index['functions'][r['hash']]['checks']))]
            # The plainest duplicate: one code saved under several names (same hash, so no score)
            names = index['functions'][r['hash']]['current_names'].get(r['lang'], [])
            r['same_code'] = names if len(names) > 1 else []
        results = [r for r in results if r['similar'] or r['same_code']]

    results.sort(key=lambda r: (r['created'], r['hash']), reverse=True)

    if as_json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return

    asked = ' '.join(query + [flag for flag, on in (('--untested', untested), ('--unreviewed', unreviewed),
                                                    ('--diverged', diverged), ('--unused', unused),
                                                    ('--outdated', outdated), ('--similar', similar)) if on]
                     + ([f"--uses {uses}"] if uses else []))
    if asked:
        print(f"Search Results ({len(results)} matches for: {asked})")
    else:
        print(f"Functions ({len(results)} current versions)")
    print("=" * 80)
    print()

    if not results:
        if outdated and not query:
            print("No matches found: nothing outdated")
        else:
            print("No matches found" if asked else "Nothing listed")
        return

    for r in results:
        print(f"Name: {r['name']} ({r['lang']})")
        print(f"Hash: {r['hash']}")
        print(f"Created: {r['created']}")
        print(f"Languages: {', '.join(r['languages'])}")
        status = [f"{r['checks']} checks" if r['checks'] else ("is a check" if r['is_check'] else "no checks"),
                  "reviewed" if r['reviewed'] else "not reviewed"]
        if not r['current']:
            status.append("superseded")
        if r['diverged']:
            status.append(f"name diverged: {r['forks']} current versions")
        if r['outdated']:
            names = ', '.join(r['aliases'].get(dep) or helper_function_label(dep) for dep in r['outdated'])
            status.append(f"uses {len(r['outdated'])} out-of-date version(s): {names}")
        if r['behind']:
            names = ', '.join(helper_function_label(dep) for dep in r['behind'])
            status.append(f"behind: uses {names}, which is out of date")
        for newer, here in r['untranslated']:
            status.append(f"newer version in {newer}, not yet translated to {here}")
        if r['pinned']:
            status.append(f"{len(r['pinned'])} pinned")
        if r.get('same_code'):
            status.append("same code under the names " + ', '.join(r['same_code']))
        if r.get('similar'):
            status.append("looks like " + ', '.join(f"{helper_function_label(other)} {score:.0%}"
                                                    for score, other in r['similar']))
        if r['current'] and r['namesakes']:
            status.append(f"{r['namesakes']} unrelated function(s) also called {r['name']}: "
                          f"bb.resolve(\"{r['name']}\") is ambiguous")
        print(f"Status: {', '.join(status)}")
        if r['match']:
            print(f"Match: {', '.join(r['match'])}")
        if r['description']:
            print(f"Description: {r['description']}")
        print(f"View: bb show {r['hash']}@{r['lang']}")
        print()

    if outdated:
        # The functions nothing outdated uses: updating them reaches all the others
        # (a version waiting for a translation needs a translator, not bb update)
        listed = {r['hash'] for r in results if r['outdated'] or r['behind']}
        tops = [r for r in results if r['hash'] in listed
                and not any(user in listed for user in index['importers'].get(r['hash'], []))]
        if tops:
            print("To bring them all up to date in one step:")
            for r in tops:
                print(f"  bb update {r['name']}@{r['lang']}" if r['name'] else f"  bb update {r['hash']}")


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
    List available languages for a function hash: those it has mappings in.

    Args:
        func_hash: Function hash (64 hex characters)

    Returns:
        Sorted list of language codes
    """
    mapping_dir = storage_mapping_directory(func_hash)
    if not mapping_dir.is_dir():
        return []
    return sorted(item.name for item in mapping_dir.iterdir()
                  if item.is_dir() and 3 <= len(item.name) <= 256 and any(item.glob('*.json')))


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


TRANSLATE_MARKER_RE = re.compile(r'^<!--\s*bb:translate\s+([0-9a-f]{64})@(\w+)@([0-9a-f]{64})\s+(\w+)\s*-->$')
TRANSLATE_LINE_RE = re.compile(r'^-\s+(\S+)(?:\s+\(([0-9a-f]{12,64})\.*\))?\s*->\s*(.*?)\s*$')


def translate_render_file(hash_value: str, source_lang: str, target_lang: str, name: str = None) -> str:
    """
    A translation file: the function in its source language, then what to fill in.

    `## Names` and `## Aliases` hold one `- source -> target` line per
    variable name and per pool function the code names; `## Docstring` and
    `## Documentation` hold the target text. The marker says which
    translation the file was made from (with name, the one called so:
    two names may share one code).
    """
    mapping_hash = mapping_select(hash_value, source_lang, name)
    normalized_code, names, aliases, docstring = code_load(hash_value, source_lang, mapping_hash)
    markdown = mapping_load_markdown(hash_value, source_lang, mapping_hash)
    source = code_denormalize(code_replace_docstring(normalized_code, docstring), names, aliases)
    title = names.get('_bb_v_0', hash_value[:12])
    out = [f"# Translate {title} from {source_lang} into {target_lang}", '',
           f"<!-- bb:translate {hash_value}@{source_lang}@{mapping_hash} {target_lang} -->", '',
           f"Fill in each name after `->`, then the docstring and the documentation in {target_lang},",
           f"and run the same bb translate command again to save. The code does not change.", '',
           f"## Source ({source_lang})", '', '```python', source.rstrip('\n'), '```', '']
    if markdown:
        out += [markdown.rstrip('\n'), '']
    out += ['## Names', '']
    out += [f"- {names[k]} -> " for k in sorted(names, key=lambda k: int(k.rsplit('_', 1)[1]))]
    if aliases:
        out += ['', '## Aliases', '']
        for dep, alias in sorted(aliases.items(), key=lambda kv: kv[1]):
            # A pool function's name in the target language, when it has one
            known = ''
            if target_lang in storage_list_languages(dep):
                known = mapping_load_json(dep, target_lang, mapping_select(dep, target_lang)).get(
                    'name_mapping', {}).get('_bb_v_0', '')
            out.append(f"- {alias} ({dep[:12]}...) -> {known}")
    out += ['', f"## Docstring ({target_lang})", '', '', f"## Documentation ({target_lang})", '', '']
    return '\n'.join(out)


def translate_parse_file(text: str, hash_value: str, source_lang: str, target_lang: str,
                         name: str = None) -> Dict[str, Any]:
    """
    Read a filled-in translation file into a mapping for target_lang.

    Raises:
        ValueError: listing every missing or invalid name, a marker for
        another function or language, or a source translation that changed
    """
    lines = text.split('\n')
    marker = next((TRANSLATE_MARKER_RE.match(l.strip()) for l in lines if TRANSLATE_MARKER_RE.match(l.strip())), None)
    if marker is None:
        raise ValueError("no <!-- bb:translate ... --> line: start from the file bb translate writes")
    if (marker.group(1), marker.group(2), marker.group(4)) != (hash_value, source_lang, target_lang):
        raise ValueError(f"the file translates {marker.group(1)[:12]}...@{marker.group(2)} into "
                         f"{marker.group(4)}, not this function and these languages")
    if marker.group(3) != mapping_select(hash_value, source_lang, name):
        raise ValueError(f"the {source_lang} translation changed since the file was written: write a new file")

    sections: Dict[str, List[str]] = {}
    current = None
    fence = None
    for line in lines:
        opening = re.match(r'^(`{3,}|~{3,})', line.strip())
        if fence is None and line.startswith('## '):
            current = line[3:].strip()
            sections[current] = []
            continue
        if opening:
            fence = None if fence and opening.group(1)[0] == fence[0] and len(opening.group(1)) >= len(fence) \
                else (fence or opening.group(1))
        if current is not None:
            sections[current].append(line)

    _, names, aliases, _ = code_load(hash_value, source_lang, marker.group(3))
    by_source = {v: k for k, v in names.items()}
    by_alias = {v: k for k, v in aliases.items()}
    problems = []

    def read(section, known):
        found = {}
        for line in sections.get(section, []):
            m = TRANSLATE_LINE_RE.match(line.strip())
            if not m:
                continue
            source_name, target = m.group(1), m.group(3)
            if source_name not in known:
                problems.append(f"{section}: '{source_name}' is not in the function")
            elif not target:
                problems.append(f"{section}: '{source_name}' has no {target_lang} name")
            elif not target.isidentifier() or keyword.iskeyword(target):
                problems.append(f"{section}: '{target}' is not a valid name")
            else:
                found[known[source_name]] = target
        for source_name, key in known.items():
            if key not in found and not any(p.startswith(f"{section}: '{source_name}'") for p in problems):
                problems.append(f"{section}: '{source_name}' is missing")
        return found

    target_names = read('Names', by_source)
    target_aliases = read('Aliases', by_alias)
    taken = {}
    for key, target in list(target_names.items()) + [(f"alias {h[:12]}", a) for h, a in target_aliases.items()]:
        if target in taken:
            problems.append(f"'{target}' is given to two names")
        taken[target] = key
        if target in PYTHON_BUILTINS or target == 'bb':
            problems.append(f"'{target}' would hide a Python built-in or bb")
    if problems:
        raise ValueError("the translation is not complete:\n" + '\n'.join(f"  {p}" for p in problems))

    def text_of(section):
        return '\n'.join(sections.get(section, [])).strip()

    return {'name_mapping': target_names, 'alias_mapping': target_aliases,
            'docstring': text_of(f"Docstring ({target_lang})"),
            'markdown': text_of(f"Documentation ({target_lang})"),
            'source_mapping': marker.group(3)}


def translate_save_step(hash_value: str, source_lang: str, source_mapping: str, target_lang: str,
                        target_mapping: str, comment: str = "") -> str:
    """
    Record a translation in the lineage; returns the step's id.

    The step translates the source mapping's step. It comes after the
    target language's current step in the same history (the translation
    of a previous version), when there is one: so the target name follows
    on to this version. Otherwise it starts the target's lineage.
    """
    steps = lineage_index()['steps']
    source_step = lineage_step_for(hash_value, source_lang, mapping=source_mapping)
    previous = None
    if source_step:
        family = lineage_root_step(source_step, translations=True)
        candidates = [s for s in lineage_current_steps(target_lang)
                      if lineage_root_step(s, translations=True) == family]
        if candidates:
            previous = max(candidates, key=lambda s: (lineage_created(s), s))
    if previous and (steps[previous]['function'], steps[previous]['mapping']) == (hash_value, target_mapping):
        return previous
    return lineage_save(previous, hash_value, target_lang, target_mapping, 'translate', comment,
                        translates=source_step)


def translate_save_parsed(hash_value: str, source_lang: str, target_lang: str, parsed: Dict[str, Any],
                          comment: str = None) -> str:
    """Save a parsed translation file (translate_parse_file) as a mapping and a lineage step; returns the mapping hash."""
    pins = mapping_load_json(hash_value, source_lang, parsed['source_mapping']).get('pinned')
    with contextlib.redirect_stdout(io.StringIO()):
        mapping_hash = mapping_save(hash_value, target_lang, parsed['docstring'], parsed['name_mapping'],
                                    parsed['alias_mapping'], comment or "", parsed['markdown'], pinned=pins)
    translate_save_step(hash_value, source_lang, parsed['source_mapping'], target_lang, mapping_hash, comment or "")
    return mapping_hash


def translate_tree_plan(identifier: str, target_lang: str) -> Dict[str, Any]:
    """
    What bb translate --tree translates: ROOT@lang, every function it uses
    and their checks, each with its source name.

    Returns:
        Dict with 'lang', 'order' (hashes, callers before what they use),
        'source_names' ({hash: name in lang}), 'known' ({hash: name in
        target_lang} for functions already current there, left out of the file)
    """
    root, lang = helper_parse_identifier(identifier)
    functions = storage_scan_functions()
    order, seen = [], set()

    def visit(h):
        if h in seen or h not in functions:
            return
        seen.add(h)
        order.append(h)
        for dep in code_extract_dependencies(functions[h]['normalized_code']) + functions[h]['checks']:
            visit(dep)

    visit(root)
    source_names, known, missing = {}, {}, []
    for h in order:
        current = functions[h]['current_names']
        name = (current.get(lang) or [functions[h]['names'].get(lang)])[0]
        if name is None:
            missing.append(h)
            continue
        source_names[h] = name
        if current.get(target_lang):
            known[h] = current[target_lang][0]
    if missing:
        print(f"Error: these functions have no '{lang}' name: "
              + ', '.join(f"{helper_function_label(h)} ({h[:12]}...)" for h in missing), file=sys.stderr)
        sys.exit(1)
    return {'lang': lang, 'order': order, 'source_names': source_names, 'known': known}


def translate_tree_render(plan: Dict[str, Any], target_lang: str, root_label: str) -> str:
    """
    One file translating a whole tree: a section per function (as a
    single translation file, under a `# name` heading) without Aliases:
    the first Names line of a section names that function in every
    caller.
    """
    lang = plan['lang']
    todo = [h for h in plan['order'] if h not in plan['known']]
    out = [f"<!-- bb:translate-tree {root_label} {target_lang} -->", '',
           f"Translate {len(todo)} function(s) from {lang} into {target_lang}. In each section, fill in every",
           f"name after `->`, then the docstring and the documentation. The first Names line of a section",
           f"is that function's {target_lang} name, and every caller uses it: there are no aliases to fill in.",
           f"Run the same bb translate --tree command again to save them all. The code does not change.", '']
    if plan['known']:
        out += [f"Already in {target_lang}, used as they are: "
                + ', '.join(f"{plan['source_names'][h]} -> {plan['known'][h]}" for h in plan['order']
                            if h in plan['known']), '']
    for h in todo:
        lines = translate_render_file(h, lang, target_lang, plan['source_names'][h]).split('\n')
        lines = lines[2:]  # the title line: the section has its own
        lines = [l for l in lines if not l.startswith(('Fill in each name after', 'and run the same bb translate'))]
        # Aliases are filled in from the other sections when saving
        if '## Aliases' in lines:
            start = lines.index('## Aliases')
            end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith('## '))
            lines = lines[:start] + lines[end:]
        out += [f"# {plan['source_names'][h]}", ''] + lines
    return '\n'.join(out).rstrip('\n') + '\n'


def translate_tree_split(text: str) -> Dict[str, str]:
    """{function hash: its section} of a translate --tree file; a section starts at its bb:translate line."""
    lines = text.split('\n')
    starts = [i for i, l in enumerate(lines) if TRANSLATE_MARKER_RE.match(l.strip())]
    sections = {}
    for n, start in enumerate(starts):
        end = starts[n + 1] if n + 1 < len(starts) else len(lines)
        if n + 1 < len(starts):
            # Leave out the next section's `# name` heading, just above its marker
            while end - 1 > start and not lines[end - 1].strip():
                end -= 1
            if lines[end - 1].startswith('# '):
                end -= 1
        sections[TRANSLATE_MARKER_RE.match(lines[start].strip()).group(1)] = '\n'.join(lines[start:end])
    return sections


def command_translate_tree(identifier: str, target_lang: str, file: str, comment: str = None):
    """
    bb translate --tree ROOT@lang TARGET FILE: translate ROOT and everything it uses in one file.

    When FILE is missing, bb writes a section for every function of the
    tree not yet current in TARGET (and its checks). When FILE exists,
    bb reads every section first and lists every problem at once (a name
    missing or invalid, one TARGET name given to two functions, a source
    changed since) without saving anything; otherwise it saves them all.
    Each function's TARGET name, its first Names line, is used by every
    caller, so the translated pool calls each function by one name.
    """
    if '@' not in identifier:
        print("Error: Missing @lang suffix. Use format: ROOT@source_lang", file=sys.stderr)
        sys.exit(1)
    plan = translate_tree_plan(identifier, target_lang)
    lang = plan['lang']
    if not os.path.exists(file):
        todo = [h for h in plan['order'] if h not in plan['known']]
        if not todo:
            print(f"Nothing to translate: {identifier} and everything it uses are already in {target_lang}.")
            return
        with open(file, 'w', encoding='utf-8') as f:
            f.write(translate_tree_render(plan, target_lang, identifier))
        print(f"Wrote {file} with {len(todo)} function(s) to translate"
              + (f" ({len(plan['known'])} already in {target_lang})" if plan['known'] else "")
              + ". Fill it in, then run the same command again to save them.")
        return

    with open(file, 'r', encoding='utf-8') as f:
        sections = translate_tree_split(f.read())
    todo = [h for h in plan['order'] if h not in plan['known']]
    problems = [f"{plan['source_names'][h]}: no section (write a new file)" for h in todo if h not in sections]
    # Each function's target name: the first Names line of its section, or its name already in target
    target_names = dict(plan['known'])
    for h in todo:
        if h not in sections:
            continue
        names = sections[h].split('## Names', 1)[1].split('\n## ', 1)[0] if '## Names' in sections[h] else ''
        first = next((TRANSLATE_LINE_RE.match(l.strip()) for l in names.split('\n')
                      if TRANSLATE_LINE_RE.match(l.strip())), None)
        if first and first.group(3):
            target_names[h] = first.group(3)
    owners = {}
    for h in todo:
        if h in target_names:
            owners.setdefault(target_names[h], []).append(plan['source_names'][h])
    shared = {name for name, sources in owners.items() if len(sources) > 1}
    problems += [f"'{name}' is given to {', '.join(sorted(owners[name]))}" for name in sorted(shared)]

    parsed = {}
    for h in todo:
        if h not in sections:
            continue
        mapping_hash = mapping_select(h, lang, plan['source_names'][h])
        _, _, aliases, _ = code_load(h, lang, mapping_hash)
        alias_lines = [f"- {alias} ({dep[:12]}...) -> {target_names.get(dep, '')}" for dep, alias in aliases.items()]
        text = sections[h] + ('\n\n## Aliases\n\n' + '\n'.join(alias_lines) + '\n' if alias_lines else '')
        try:
            parsed[h] = translate_parse_file(text, h, lang, target_lang, plan['source_names'][h])
        except ValueError as e:
            problems += [f"{plan['source_names'][h]}: {line.strip()}" for line in str(e).split('\n')
                         if line.strip() and not line.startswith('the translation is not complete')
                         # already said once for the whole file
                         and not any(line.strip() == f"'{name}' is given to two names" for name in shared)]
    if problems:
        print(f"Error: {file}: nothing saved; fix these and run the command again:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        sys.exit(1)

    for h in todo:
        translate_save_parsed(h, lang, target_lang, parsed[h], comment)
    print(f"Saved {len(todo)} translation(s) into {target_lang}: "
          + ', '.join(f"{plan['source_names'][h]} -> {target_names[h]}" for h in todo))


def command_translate(identifier_with_lang: str, target_lang: str, recursive: bool = False,
                      file: str = None, comment: str = None):
    """
    Add a translation for an existing function.

    Accepts a hash or function name with required @lang suffix for source language.
    Prompts for new variable names and docstring in target language.

    With FILE, no prompts: when FILE is missing, bb writes the function
    there with a line to fill in for each name (and each pool function it
    names), and sections for the docstring and documentation, and saves
    nothing; when FILE exists, bb saves it as the target language's
    translation (the code and hash do not change). A file made from a
    source translation that changed since is refused.

    A translation is a lineage step in the target language: when the
    target name was on an older version of this function's history (an
    edit in the source language left it waiting), it moves on to this
    version. Until then it stays where it was ("not yet translated").

    Examples:
        bb translate abc123...@eng fra
        bb translate compute_pi@eng fra
        bb translate compute_pi@eng fra compute_pi.fra.md   # write, fill in, run again to save
    """
    # @lang is required for translate (source language)
    if '@' not in identifier_with_lang:
        print("Error: Missing @lang suffix. Use format: identifier@source_lang", file=sys.stderr)
        sys.exit(1)

    hash_value, source_lang = helper_parse_identifier(identifier_with_lang)
    source_name = helper_identifier_name(identifier_with_lang)

    if len(target_lang) < 3 or len(target_lang) > 256:
        print(f"Error: Target language code must be 3-256 characters. Got: {target_lang}", file=sys.stderr)
        sys.exit(1)

    if file is not None:
        if recursive:
            print("Error: --recursive does not go with FILE: translate one function per file", file=sys.stderr)
            sys.exit(1)
        if not os.path.exists(file):
            with open(file, 'w', encoding='utf-8') as f:
                f.write(translate_render_file(hash_value, source_lang, target_lang, source_name))
            print(f"Wrote {file}. Fill it in, then run the same command again to save it.")
            return
        with open(file, 'r', encoding='utf-8') as f:
            try:
                parsed = translate_parse_file(f.read(), hash_value, source_lang, target_lang, source_name)
            except ValueError as e:
                print(f"Error: {file}: {e}\nNothing saved.", file=sys.stderr)
                sys.exit(1)
        mapping_hash = translate_save_parsed(hash_value, source_lang, target_lang, parsed, comment)
        print(f"Translation: {hash_value}@{target_lang}@{mapping_hash}")
        print(f"Name: {parsed['name_mapping'].get('_bb_v_0')}@{target_lang}")
        return

    # Load source function
    source_mapping = mapping_select(hash_value, source_lang, source_name)
    try:
        normalized_code, name_mapping_source, alias_mapping_source, docstring_source = \
            code_load(hash_value, source_lang, source_mapping)
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
    source_pins = mapping_load_json(hash_value, source_lang, source_mapping).get('pinned')
    mapping_hash = mapping_save(hash_value, target_lang, target_docstring, name_mapping_target, alias_mapping_target,
                                comment, pinned=source_pins)
    # The translation is a lineage step: the target name follows on to this version
    translate_save_step(hash_value, source_lang, source_mapping, target_lang, mapping_hash, comment)

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


def command_rm(identifier: str, force: bool = False):
    """
    Remove a function, or one name of it.

    Accepts a hash or function name (with optional @lang suffix).

    - bb rm NAME@lang - Remove that name: the function's mappings in lang
      that call it NAME (with HASH@lang: all its mappings in lang), and
      their lineage steps. The name's previous version, if it had one,
      is current again; the function and its other names stay.
    - bb rm NAME or bb rm HASH - Remove the entire function: its object,
      all its mappings and its lineage steps.

    Steps that later steps come after are kept, so the history of what
    remains stays connected (see lineage_prune). A function that other
    functions still use (as a dependency or a check) is not removed,
    since they would then refer to nothing; bb lists them. --force
    removes it anyway.
    """
    import shutil

    hash_value, lang = helper_parse_identifier(identifier)
    name = helper_identifier_name(identifier)
    # Only an explicit @lang removes one mapping: a bare name resolves with a language too
    remove_lang_only = '@' in identifier

    if remove_lang_only:
        mappings = [m for m, _ in mappings_list(hash_value, lang)
                    if name is None or mapping_name(hash_value, lang, m) == name]
        if not mappings:
            print(f"Error: Language mapping not found: {hash_value}@{lang}", file=sys.stderr)
            sys.exit(1)
        for mapping_hash in mappings:
            storage_mapping_path(hash_value, lang, mapping_hash).unlink()
        lang_dir = storage_mapping_directory(hash_value) / lang
        if lang_dir.is_dir() and not any(lang_dir.iterdir()):
            lang_dir.rmdir()
        lineage_prune()
        print(f"Removed language mapping: {hash_value}@{lang}" + (f" ({name})" if name else ""))

        remaining_langs = storage_list_languages(hash_value)
        if remaining_langs:
            print(f"Remaining languages: {', '.join(remaining_langs)}")
        else:
            print(f"Note: {hash_value[:12]} has no name left; it stays in the pool "
                  f"(bb rm {hash_value} removes it)")
        storage_cache_clear()
        if name:
            # The name's previous version is current again: say so, an author removing a duplicate expects it gone
            back = helper_find_heads_by_name(name, lang)[0]
            for head in back:
                print(f"{name}@{lang} is current again at its previous version {head} "
                      f"(bb rm {name}@{lang} again removes that one too)")
    else:
        if code_detect_schema(hash_value) is None:
            print(f"Error: Function not found: {hash_value}", file=sys.stderr)
            sys.exit(1)
        index = storage_pool_index()
        users = sorted(set(index['importers'].get(hash_value, [])) | set(index['checked_by'].get(hash_value, [])))
        if users and not force:
            print(f"Error: {helper_function_label(hash_value)} ({hash_value}) is still in use; nothing removed:",
                  file=sys.stderr)
            for user in users:
                state = "current" if user in index['current'] else "older version"
                print(f"  used by {helper_function_label(user)} {user} ({state})", file=sys.stderr)
            print("Remove those first, or use --force to leave them referring to nothing.", file=sys.stderr)
            sys.exit(1)

        languages = storage_list_languages(hash_value)
        shutil.rmtree(storage_object_path(hash_value).parent)
        if storage_mapping_directory(hash_value).exists():
            shutil.rmtree(storage_mapping_directory(hash_value))
        lineage_prune()
        print(f"Removed function: {hash_value}")
        if languages:
            print(f"Deleted {len(languages)} language mapping(s): {', '.join(languages)}")


def migrate_read_old_pool() -> Tuple[Dict[str, Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Dict[str, Any]]],
                                     Dict[str, Dict[str, Any]], Dict[str, Path]]:
    """
    Read a pool in the layout before storage v2.

    That layout kept pool/XX/<rest>/object.json, the mappings in
    pool/XX/<rest>/<lang>/ZZ/<rest>/mapping.json (with a 'parent' link,
    hash@lang@mapping, to the translation they were made from) and restore
    records in $BB_DIRECTORY/restore/<id>.json.

    Returns:
        (objects {hash: object.json data}, mappings {(hash, lang): {mapping hash: data}},
         restores {id: record}, object paths {hash: path})
    """
    pool_dir = storage_get_pool_directory()
    objects: Dict[str, Dict[str, Any]] = {}
    paths: Dict[str, Path] = {}
    mappings: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    for prefix_dir in sorted(pool_dir.iterdir()):
        if not (prefix_dir.is_dir() and re.fullmatch(r'[0-9a-f]{2}', prefix_dir.name)):
            continue
        for func_dir in sorted(prefix_dir.iterdir()):
            object_json = func_dir / 'object.json'
            if not object_json.exists():
                continue
            try:
                with open(object_json, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (IOError, json.JSONDecodeError):
                continue
            func_hash = prefix_dir.name + func_dir.name
            objects[func_hash] = data
            paths[func_hash] = object_json
            for lang_dir in sorted(func_dir.iterdir()):
                if not lang_dir.is_dir():
                    continue
                for mapping_json in sorted(lang_dir.glob('*/*/mapping.json')):
                    try:
                        with open(mapping_json, 'r', encoding='utf-8') as f:
                            mapping = json.load(f)
                    except (IOError, json.JSONDecodeError):
                        continue
                    mapping_hash = mapping_json.parent.parent.name + mapping_json.parent.name
                    mappings.setdefault((func_hash, lang_dir.name), {})[mapping_hash] = mapping
    restores: Dict[str, Dict[str, Any]] = {}
    for path in sorted((storage_get_bb_directory() / 'restore').glob('*.json')):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                restores[path.stem] = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
    return objects, mappings, restores, paths


def migrate_plan(objects: Dict[str, Dict[str, Any]], mappings: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]],
                 restores: Dict[str, Dict[str, Any]], save_mapping: Callable, save_step: Callable) -> Dict[str, int]:
    """
    Turn the old pool's lineage into lineage steps, deterministically.

    Old versions (functions, and restore records standing for the version
    they restore) are taken parents first, by creation time. For each
    version, in each language: its first mapping continues the step of
    the nearest older version that has the language (an edit, or a rename
    when the name differs, a refactor when only references changed), or
    starts a lineage (an add, or a translation when the mapping was made
    from another language's); a mapping made from another of the same
    function and language is a 'document' step after it. A restore record
    is a 'restore' step from the superseded version's step to the restored
    version's mapping, in each language both have.

    Args:
        save_mapping: (func_hash, lang, old mapping data) -> new mapping hash
        save_step: (previous, func_hash, lang, mapping_hash, relation, comment,
                    translates, metadata) -> step id

    Returns:
        Counts: functions, mappings, steps, restores
    """
    def created_of(node):
        if node in objects:
            return objects[node].get('metadata', {}).get('created', '')
        return restores[node].get('created', '')

    def previous(node):
        parent = (objects[node].get('metadata', {}).get('parent') if node in objects
                  else restores[node].get('supersedes'))
        return parent if parent in objects or parent in restores else None

    # Parents first, then by creation time
    order: List[str] = []
    placed: Set[str] = set()
    for node in sorted(list(objects) + [r for r in restores if r not in objects],
                       key=lambda n: (created_of(n), n)):
        chain = []
        while node is not None and node not in placed and node not in chain:
            chain.append(node)
            node = previous(node)
        for item in reversed(chain):
            placed.add(item)
            order.append(item)

    new_mapping: Dict[Tuple[str, str, str], str] = {}   # (hash, lang, old mapping) -> new mapping
    step_of: Dict[Tuple[str, str, str], str] = {}       # (hash, lang, old mapping) -> step
    final: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}  # (node, lang) -> [(step, name, old mapping)]
    counts = {'functions': len(objects), 'mappings': 0, 'steps': 0, 'restores': 0}

    def name_of(data):
        return data.get('name_mapping', {}).get('_bb_v_0', '')

    def nearest(node, lang):
        """The final steps in lang of the nearest version before node that has some."""
        seen = set()
        node = previous(node)
        while node is not None and node not in seen:
            seen.add(node)
            if final.get((node, lang)):
                return final[(node, lang)]
            node = previous(node)
        return []

    def pick(candidates, name, old_mapping=None):
        for step_id, step_name, mapping_hash in candidates:
            if old_mapping and mapping_hash == old_mapping:
                return step_id, step_name
        for step_id, step_name, _ in candidates:
            if step_name == name:
                return step_id, step_name
        return (candidates[0][0], candidates[0][1]) if candidates else (None, None)

    def parent_link(data):
        parts = (data.get('parent') or '').split('@')
        return tuple(parts) if len(parts) == 3 else None

    def metadata_of(node):
        if node in objects:
            meta = objects[node].get('metadata', {})
        else:
            meta = restores[node]
        return {'created': meta.get('created', ''), 'name': meta.get('name', ''), 'email': meta.get('email', '')}

    def migrate_function(func_hash):
        langs = sorted(lang for (h, lang) in mappings if h == func_hash)
        # Languages a translation was made from go first
        def depends(lang):
            return any((link := parent_link(d)) and link[0] == func_hash and link[1] != lang
                       for d in mappings[(func_hash, lang)].values())
        langs.sort(key=lambda lang: (depends(lang), lang))
        for lang in langs:
            own = mappings[(func_hash, lang)]
            doc_parent = {}
            for m, data in own.items():
                link = parent_link(data)
                if link and link[0] == func_hash and link[1] == lang and link[2] in own and link[2] != m:
                    doc_parent[m] = link[2]
            # Mappings in chain order: a mapping after the one it was made from
            ordered, done = [], set()
            for m in sorted(own):
                chain = []
                while m is not None and m not in done and m not in chain:
                    chain.append(m)
                    m = doc_parent.get(m)
                for item in reversed(chain):
                    done.add(item)
                    ordered.append(item)
            for m in ordered:
                data = own[m]
                new_hash = save_mapping(func_hash, lang, data)
                new_mapping[(func_hash, lang, m)] = new_hash
                counts['mappings'] += 1
                name = name_of(data)
                link = parent_link(data)
                translates = None
                if m in doc_parent:
                    from_step = step_of[(func_hash, lang, doc_parent[m])]
                    parent_name = name_of(own[doc_parent[m]])
                    relation = 'rename' if parent_name != name else 'document'
                else:
                    candidates = nearest(func_hash, lang)
                    old_mapping = link[2] if link and link[1] == lang else None
                    from_step, from_name = pick(candidates, name, old_mapping)
                    if link and link[1] != lang:
                        translates = step_of.get(link)
                    if from_step is None:
                        relation = 'translate' if translates else 'add'
                    elif from_name != name:
                        relation = 'rename'
                    elif translates:
                        relation = 'translate'
                    else:
                        relation = 'edit'
                        source = mappings.get((link[0], lang), {}).get(link[2]) if link else None
                        if source and source.get('name_mapping') == data.get('name_mapping') and \
                                source.get('docstring') == data.get('docstring') and \
                                sorted(source.get('alias_mapping', {}).values()) == \
                                sorted(data.get('alias_mapping', {}).values()):
                            relation = 'refactor'
                step_of[(func_hash, lang, m)] = save_step(from_step, func_hash, lang, new_hash, relation,
                                                          data.get('comment', ''), translates, metadata_of(func_hash))
                counts['steps'] += 1
            leaves = [m for m in ordered if m not in doc_parent.values()]
            final[(func_hash, lang)] = [(step_of[(func_hash, lang, m)], name_of(own[m]), m) for m in leaves]

    def migrate_restore(record_id):
        record = restores[record_id]
        restored = record.get('restores')
        langs = sorted({lang for (h, lang) in mappings if h == restored})
        if restored not in objects:
            for lang in sorted({lang for (_, lang) in final}):
                final[(record_id, lang)] = nearest(record_id, lang)
            return
        counts['restores'] += 1
        for lang in langs:
            candidates = nearest(record_id, lang)
            if not candidates:
                continue
            leaves = [(n, m) for _, n, m in final.get((restored, lang), [])] or \
                [(name_of(d), m) for m, d in sorted(mappings[(restored, lang)].items())]
            from_step, from_name = candidates[0][0], candidates[0][1]
            name, m = next(((n, m) for n, m in leaves if n == from_name), leaves[0])
            step_id = save_step(from_step, restored, lang, new_mapping.get((restored, lang, m)) or
                                save_mapping(restored, lang, mappings[(restored, lang)][m]),
                                'restore', record.get('comment', ''), None, metadata_of(record_id))
            counts['steps'] += 1
            final[(record_id, lang)] = [(step_id, name, m)]

    for node in order:
        if node in objects:
            migrate_function(node)
        else:
            migrate_restore(node)
    return counts


def command_migrate():
    """
    Convert a pool from the layout before storage v2 to the current one.

    Functions move to pool/functions/, their mappings to pool/mappings/
    (without their 'parent' link: the lineage says it now), and the
    lineage of the pool becomes lineage steps in pool/lineage/, per
    language and per name: edits, renames, translations, documentation
    changes and restores (the old $BB_DIRECTORY/restore/ records). Current
    versions stay what they were. Function hashes do not change.

    The old files stay until the new ones are written, then pool/XX/ and
    restore/ are removed. Track $BB_DIRECTORY with git before migrating:
    `git checkout` then brings the old layout back. Running it on a pool
    already migrated says so and changes nothing.
    """
    import shutil

    if not storage_pool_is_old_layout():
        print("Already migrated: this pool uses the current layout.")
        return

    storage_cache_clear()
    objects, mappings, restores, paths = migrate_read_old_pool()

    for func_hash in sorted(objects):
        target = storage_object_path(func_hash)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(paths[func_hash], target)
    storage_cache_clear()

    def save_mapping(func_hash, lang, data):
        with contextlib.redirect_stdout(io.StringIO()):
            return mapping_save(func_hash, lang, data.get('docstring', ''), data.get('name_mapping', {}),
                                data.get('alias_mapping', {}), data.get('comment', ''), data.get('markdown', ''),
                                pinned=data.get('pinned'))

    def save_step(previous, func_hash, lang, mapping_hash, relation, comment, translates, metadata):
        return lineage_save(previous, func_hash, lang, mapping_hash, relation, comment, translates, metadata)

    counts = migrate_plan(objects, mappings, restores, save_mapping, save_step)

    # Everything is written: the old layout can go
    pool_dir = storage_get_pool_directory()
    for prefix_dir in sorted(pool_dir.iterdir()):
        if prefix_dir.is_dir() and re.fullmatch(r'[0-9a-f]{2}', prefix_dir.name):
            shutil.rmtree(prefix_dir)
    restore_dir = storage_get_bb_directory() / 'restore'
    if restore_dir.is_dir():
        shutil.rmtree(restore_dir)
    storage_cache_clear()

    print(f"Migrated {counts['functions']} functions, {counts['mappings']} mappings and "
          f"{counts['restores']} restore records into {counts['steps']} lineage steps.")
    print("Removed the old layout (pool/XX/ and restore/); git keeps it if you tracked the directory.")


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
        pins = code_pinned_refs(source_code)
        source_code, resolved = code_resolve_pool_names(source_code, lang, where=file_path)
        prepared = code_prepare_source(source_code, lang, where=file_path)
        prepared['pinned'] = sorted(pins & set(code_extract_dependencies(prepared['normalized_code'])))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Save to v1 format (docstring stored separately in mapping.json)
    code_save(prepared['hash'], lang, prepared['normalized_code'], prepared['docstring'],
              prepared['name_mapping'], prepared['alias_mapping'], comment, pinned=prepared['pinned'])
    review_approve_own([prepared['hash']])

    for name, func_hash in sorted(resolved.items()):
        print(f"Import: {name} -> {func_hash}")
    helper_report_similar(prepared['hash'])


def code_shape_tokens(normalized_code: str) -> List[str]:
    """
    The shape of normalized code, as a sequence of tokens for comparison.

    A pre-order walk of the AST emits node types (For, BinOp, Call,
    Constant:int, Attribute.append...). Built-ins and imported names keep
    their names: they carry meaning. Each use of a local variable becomes
    the distance back to its previous use (var0 for a first use), in the
    spirit of de Bruijn indices and Baker's parameterized matching: adding
    a variable early does not shift every later one, as _bb_v_N numbering
    would.
    """
    try:
        tree = ast.parse(normalized_code)
    except SyntaxError:
        return []
    tokens: List[str] = []
    last_use: Dict[str, int] = {}

    def visit(node):
        if isinstance(node, ast.Name) and node.id.startswith('_bb_v_'):
            position = len(tokens)
            tokens.append(f"var{position - last_use[node.id]}" if node.id in last_use else "var0")
            last_use[node.id] = position
            return
        if isinstance(node, ast.arg):
            last_use[node.arg] = len(tokens)
            tokens.append("arg")
            return
        label = type(node).__name__
        if isinstance(node, ast.Constant):
            # The value counts too: two helpers that differ only in the store
            # they name ('orders', 'products') are not the same function
            label += f":{type(node.value).__name__}:{str(node.value)[:40]}"
        elif isinstance(node, ast.Name):
            label += f":{node.id}"
        elif isinstance(node, ast.Attribute):
            label += f".{node.attr}"
        tokens.append(label)
        for child in ast.iter_child_nodes(node):
            if not isinstance(child, ast.expr_context):
                visit(child)

    visit(tree)
    return tokens


def code_shingles(normalized_code: str, size: int = 3) -> Set[Tuple[str, ...]]:
    """
    The n-grams of a function's shape (see code_shape_tokens), used to
    compare structure. Names are normalized and variables are encoded by
    distance, so naming, language and variable order do not matter.
    """
    tokens = code_shape_tokens(normalized_code)
    return {tuple(tokens[i:i + size]) for i in range(max(1, len(tokens) - size + 1))}


def code_find_similar(func_hash: str, exclude: Set[str] = None,
                      threshold: float = 0.5, min_size: int = 20) -> List[Tuple[float, str]]:
    """
    Find current functions whose structure is close to func_hash.

    Similarity is the Jaccard index of 3-grams of the code's shape: AST
    node types and constant values, with variables encoded by distance (see
    code_shape_tokens). On the transcripts, small edits of a function score
    0.48 to 0.88 and unrelated functions or real rewrites at most 0.31. On
    seven sites built by agents, pages of the same form (login and register)
    scored 0.41 to 0.45, which is noise to an author, hence 0.5; a helper
    copied for another store scored 0.81.
    Small functions (under min_size 3-grams, about a three-line body) are
    never reported: they look alike by nature (two stores that each return
    shared(name, factory) score 100%). Checks are left out on both sides for the same reason.

    Args:
        func_hash: The function to compare
        exclude: Hashes not to report (its lineage, its checks)

    Returns:
        (similarity, hash) pairs, most similar first, at most 3
    """
    index = storage_pool_index()
    functions = index['functions']
    if func_hash not in functions or func_hash in index['checked_by']:
        return []
    mine = code_shingles(functions[func_hash]['normalized_code'])
    if len(mine) < min_size:
        return []
    skip = (exclude or set()) | {func_hash}
    found = []
    for other_hash, other in functions.items():
        if (other_hash in skip or other_hash not in index['current'] or other_hash in index['checked_by']):
            continue
        theirs = code_shingles(other['normalized_code'])
        if len(theirs) < min_size:
            continue
        score = len(mine & theirs) / len(mine | theirs)
        if score >= threshold:
            found.append((score, other_hash))
    return sorted(found, reverse=True)[:3]


def helper_report_similar(func_hash: str, exclude: Set[str] = None):
    """Tell, on stderr, about existing functions close to func_hash."""
    similar = code_find_similar(func_hash, exclude)
    if not similar:
        return
    mine = helper_function_label(func_hash)
    print(f"Note: {mine} looks like these other functions; "
          f"consider using or forking one instead:", file=sys.stderr)
    for score, other_hash in similar:
        label = helper_function_label(other_hash)
        same = "  (another function with the same name)" if label == mine else ""
        print(f"  {score:.0%} like {label} ({other_hash}){same}", file=sys.stderr)


def helper_lineage(func_hash: str) -> Set[str]:
    """func_hash and every function its lineage steps came from (any language, translations too)."""
    steps = lineage_index()['steps']
    found = {func_hash}
    for step_id in lineage_steps_on(func_hash):
        found |= {steps[s]['function'] for s in lineage_ancestors(step_id)}
    return found


def code_pinned_refs(source_code: str) -> Set[str]:
    """Hashes the author named with bb.ref("<hash>") (not bb.resolve): versions pinned on purpose."""
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return set()
    return {code_ref_hash(node) for node in ast.walk(tree) if code_ref_hash(node)}


def code_uses_bb(tree: ast.AST) -> bool:
    """Whether code calls bb.ref."""
    return any(isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'bb'
               for node in ast.walk(tree))


def code_imports_bb(tree: ast.Module) -> bool:
    """Whether a module has `import bb` at its top level."""
    return any(isinstance(node, ast.Import) and ('bb', None) in [(a.name, a.asname) for a in node.names]
               for node in tree.body)


def code_resolve_pool_names(source_code: str, lang: str, where: str = "source") -> Tuple[str, Dict[str, str]]:
    """
    Turn pool functions named by name into references by hash.

    bb.resolve("helper") (or bb.latest("helper"), the same) names the
    function currently called helper, looked up through your favourite
    languages: lang (the one being saved) first, then your languages (bb
    whoami language) in order, then any language. It is resolved by
    lineage to the name's single current version and becomes
    bb.ref("<hash>"): stored code always holds a hash, so a function keeps
    the version it was saved with. Never resolved by time: a name with
    several current versions (two unrelated functions, or a fork) or none
    is refused, and so are `from bb.pool import ...` (gone), bb.resolve or
    bb.ref without `import bb`, and two names of one code in one function
    (they would be one alias).

    Returns:
        (source with every bb.resolve turned into bb.ref, {name: hash} resolved)

    Raises:
        ValueError: naming each reference that does not resolve to one version
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return source_code, {}  # code_prepare_source reports it
    resolved: Dict[str, str] = {}
    problems: List[str] = []
    spans = []
    if code_uses_bb(tree) and not code_imports_bb(tree):
        problems.append("  bb.resolve and bb.ref need `import bb` at the top of the file")
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool':
            problems.append(f"  line {node.lineno}: {BB_POOL_IMPORT_HINT}")
        name = code_latest_name(node)
        if name is None:
            continue
        heads, functions, _ = helper_resolve_heads(name, lang)
        if len(heads) == 1:
            resolved[name] = heads[0]
            spans.append((node, f"bb.ref({heads[0]!r})"))
        elif not heads:
            problems.append(f"  line {node.lineno}: no function called '{name}' in {lang} "
                            f"(nor in your other languages)")
        else:
            problems.append(f"  line {node.lineno}: '{name}' has {len(heads)} current versions; "
                            f"name one with bb.ref(\"<hash>\"):")
            problems += [f"    {helper_describe_function(functions[h], lang)}" for h in heads]
    by_hash: Dict[str, List[str]] = {}
    for name, func_hash in sorted(resolved.items()):
        by_hash.setdefault(func_hash, []).append(name)
    for func_hash, names in sorted(by_hash.items()):
        if len(names) > 1:
            raise ValueError(f"{where}: {' and '.join(names)} are the same function here (same code, "
                             f"{func_hash[:12]}...): use one name, or give them different code")
    if problems:
        raise ValueError(f"{where}: pool functions must be named by bb.resolve(\"NAME\") with one current "
                         f"version, or by bb.ref(\"<hash>\"):\n" + '\n'.join(problems))
    return code_replace_spans(source_code, spans), resolved


CODE_DISPLAY_WIDTH = 100


def code_pretty_expression(node: ast.AST, indent: int, width: int = CODE_DISPLAY_WIDTH, start: int = None) -> str:
    """
    An expression as source, broken over lines when it does not fit in width.

    Calls, subscripts (h.div()[...] children), lists, tuples, sets and
    dicts too long for one line put one item per line, indented by four,
    with a trailing comma (none after a lone subscript item, which would
    make it a tuple). Comprehensions, conditionals and long `+`, `and` and
    `or` chains go in parentheses, one part per line. Anything else stays
    on one line. indent is the indentation of continuation lines; start,
    the column where the expression begins (indent when not given).
    """
    flat = ast.unparse(node)
    if (indent if start is None else start) + len(flat) <= width and '\n' not in flat:
        return flat
    inner = indent + 4
    pad, end = ' ' * inner, ' ' * indent

    def block(opening: str, items: List[str], closing: str, comma: bool = True) -> str:
        if not items:
            return opening + closing
        lines = (',\n'.join(pad + item for item in items)) + (',' if comma else '')
        return f"{opening}\n{lines}\n{end}{closing}"

    def item(value: ast.AST) -> str:
        return code_pretty_expression(value, inner, width)

    loose = (ast.IfExp, ast.Lambda, ast.NamedExpr, ast.BoolOp, ast.Compare, ast.Yield, ast.YieldFrom, ast.Await)
    tight_ops = (ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.MatMult, ast.Pow)

    def operand(value: ast.AST, first: bool = False) -> str:
        """An operand of a + chain, in parentheses when it binds looser than +."""
        text = item(value)
        if isinstance(value, loose) or isinstance(value, ast.UnaryOp) and isinstance(value.op, ast.Not):
            return f"({text})"
        if isinstance(value, ast.BinOp) and not isinstance(value.op, tight_ops) and \
                not (first and isinstance(value.op, (ast.Add, ast.Sub))):
            return f"({text})"
        return text

    def clause(value: ast.AST) -> str:
        """A part of an if/else, a boolean chain or a for clause, in parentheses when ambiguous."""
        text = item(value)
        return f"({text})" if isinstance(value, (ast.IfExp, ast.Lambda, ast.NamedExpr, ast.BoolOp)) else text

    def callee(value: ast.AST) -> str:
        """What is called or subscripted, in parentheses unless it is a name, attribute, call or subscript."""
        text = code_pretty_expression(value, indent, width)
        return text if isinstance(value, (ast.Name, ast.Attribute, ast.Call, ast.Subscript)) else f"({text})"

    if isinstance(node, ast.Call):
        items = [('*' + item(a.value)) if isinstance(a, ast.Starred) else item(a) for a in node.args]
        items += [(f"{k.arg}=" if k.arg else '**') + item(k.value) for k in node.keywords]
        return block(callee(node.func) + '(', items, ')')
    if isinstance(node, ast.Subscript) and not isinstance(node.slice, ast.Slice):
        value = callee(node.value)
        if isinstance(node.slice, ast.Tuple) and node.slice.elts and \
                not any(isinstance(e, (ast.Slice, ast.Starred)) for e in node.slice.elts):
            return block(value + '[', [item(e) for e in node.slice.elts], ']')
        lone = ast.unparse(node.slice)
        last = value.rsplit('\n', 1)[-1]
        if len(last) + len(lone) + 2 <= width and '\n' not in lone and not isinstance(node.slice, ast.Tuple):
            return f"{value}[{lone}]"  # a short lone child stays on the line it closes
        return block(value + '[', [item(node.slice)], ']', comma=False)
    if isinstance(node, (ast.List, ast.Set)) and not any(isinstance(e, ast.Starred) for e in node.elts):
        brackets = '[]' if isinstance(node, ast.List) else '{}'
        return block(brackets[0], [item(e) for e in node.elts], brackets[1])
    if isinstance(node, ast.Tuple) and node.elts and not any(isinstance(e, ast.Starred) for e in node.elts):
        return block('(', [item(e) for e in node.elts], ')')
    if isinstance(node, ast.Dict):
        items = [('**' + item(v)) if k is None else f"{item(k)}: {item(v)}" for k, v in zip(node.keys, node.values)]
        return block('{', items, '}')

    def parts(opening: str, pieces: List[str], closing: str) -> str:
        """One piece per line inside brackets, no commas: parentheses never change the tree."""
        return opening + '\n' + '\n'.join(pad + piece for piece in pieces) + '\n' + end + closing

    def clauses(generators: List[ast.comprehension]) -> List[str]:
        out = []
        for gen in generators:
            out.append(f"{'async ' if gen.is_async else ''}for {ast.unparse(gen.target)} in {clause(gen.iter)}")
            out.extend(f"if {clause(cond)}" for cond in gen.ifs)
        return out

    if isinstance(node, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
        brackets = {ast.ListComp: '[]', ast.SetComp: '{}', ast.GeneratorExp: '()'}[type(node)]
        return parts(brackets[0], [item(node.elt)] + clauses(node.generators), brackets[1])
    if isinstance(node, ast.DictComp):
        return parts('{', [f"{item(node.key)}: {item(node.value)}"] + clauses(node.generators), '}')
    if isinstance(node, ast.IfExp):
        return parts('(', [clause(node.body), f"if {clause(node.test)}", f"else {clause(node.orelse)}"], ')')
    if isinstance(node, ast.BoolOp):
        word = 'and' if isinstance(node.op, ast.And) else 'or'
        return parts('(', [clause(node.values[0])] + [f"{word} {clause(v)}" for v in node.values[1:]], ')')
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        chain, current = [], node
        while isinstance(current, ast.BinOp) and isinstance(current.op, ast.Add):
            chain.append(current.right)
            current = current.left
        chain.append(current)
        chain.reverse()
        return parts('(', [operand(chain[0], first=True)] + [f"+ {operand(v)}" for v in chain[1:]], ')')
    return flat


def code_pretty(source_code: str, width: int = CODE_DISPLAY_WIDTH) -> str:
    """
    Source as bb shows it: long statements broken over lines, for reading and editing.

    Only display: drafts are parsed back to an AST when saved, so their
    layout never reaches the stored code or its hash. Simple statements
    (expressions, return, assignments) longer than width are rebuilt with
    code_pretty_expression; the result must parse to the same tree, else
    the source is returned as it was.
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return source_code
    lines = source_code.split('\n')
    replacements = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Expr, ast.Return, ast.Assign, ast.AugAssign, ast.AnnAssign)):
            continue
        if node.lineno != node.end_lineno or len(lines[node.lineno - 1]) <= width:
            continue
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            continue  # a docstring
        value = getattr(node, 'value', None)
        if value is None:
            continue
        line = lines[node.lineno - 1].encode('utf-8')  # ast columns count UTF-8 bytes
        head = line[:value.col_offset].decode('utf-8')
        if line[value.end_col_offset:].strip():
            continue  # something after the value on the line: leave it
        statement_indent = len(head) - len(head.lstrip())
        replacements.append((node.lineno - 1,
                             head + code_pretty_expression(value, statement_indent, width, start=len(head))))
    if not replacements:
        return source_code
    for index, text in sorted(replacements, reverse=True):
        lines[index:index + 1] = text.split('\n')
    pretty = '\n'.join(lines)
    try:
        if ast.dump(ast.parse(pretty)) == ast.dump(tree):
            return pretty
    except SyntaxError:
        pass
    return source_code


def code_show_pool_names(source_code: str, lang: str, pinned: List[str] = ()) -> str:
    """
    Write the pool functions a displayed function uses by name.

    Each `alias = bb.ref("<hash>")` (and the older `from bb.pool import
    object_<hash> as alias`) is shown as `alias = bb.resolve("NAME")`, so
    that saving the draft brings the function up to date with the current
    version of every name: editing a function is when its debt is paid,
    and a function nobody edits keeps the versions it was saved with.
    NAME is the function's name in lang (the alias, when it is one of its
    names there), or in another language when it has none in lang.
    Kept as `bb.ref("<hash>")`: hashes the author pinned on purpose
    (pinned, from the mapping) and functions whose name does not pick one
    current version. A `# bb:` comment line says when a reference is out
    of date; comments are not stored, so it goes when saving.
    """
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return source_code
    pinned = set(pinned or ())

    def as_text(func_hash, alias):
        name = None
        for candidate in [lang] + [l for l in storage_list_languages(func_hash) if l != lang]:
            if candidate in storage_list_languages(func_hash):
                name = lineage_name_for(func_hash, candidate, alias) or mapping_name(
                    func_hash, candidate, mapping_select(func_hash, candidate, alias))
                if name:
                    break
        heads = helper_resolve_heads(name, lang)[0] if name else []
        if func_hash in pinned:
            note = (f"# bb: pinned; the current {name} is {heads[0][:12]}...\n"
                    if len(heads) == 1 and heads[0] != func_hash else "")
            return f"{note}{alias} = bb.ref({func_hash!r})"
        if len(heads) == 1:
            note = (f"# bb: out of date: uses {func_hash[:12]}..., saving picks {heads[0][:12]}...\n"
                    if heads[0] != func_hash else "")
            return f"{note}{alias} = bb.resolve({name!r})"
        why = f"{name} has {len(heads)} current versions" if heads else f"no current version is called {name}"
        return f"# bb: {why}\n{alias} = bb.ref({func_hash!r})"

    replacements = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and code_ref_hash(node.value):
            replacements.append((node.lineno, node.end_lineno, as_text(code_ref_hash(node.value), node.targets[0].id)))
        elif (isinstance(node, ast.ImportFrom) and node.module == 'bb.pool'
              and all(code_pool_import_hash(a.name) and a.asname for a in node.names)):
            replacements.append((node.lineno, node.end_lineno, '\n'.join(
                as_text(code_pool_import_hash(a.name), a.asname) for a in node.names)))
    if replacements and not code_imports_bb(tree):
        first = min(start for start, _, _ in replacements)
        replacements.append((first, first - 1, 'import bb'))
    lines = source_code.splitlines(keepends=True)
    for start, end, text in sorted(replacements, key=lambda r: (r[0], r[1]), reverse=True):
        lines[start - 1:end] = [text + '\n']
    return ''.join(lines)


def code_check_positional(source_code: str, tree: ast.Module, where: str):
    """
    Refuse keyword arguments where bb's renaming would break them.

    bb stores parameters under positional placeholders (_bb_v_N), so that
    the same logic in any human language has one hash. A keyword names a
    parameter in one language at the call site: it cannot reach a pool
    function, the function itself or a function defined inside it. So
    calls to those take positional arguments only, and no definition in
    the file has keyword-only parameters or **kwargs (a pool caller could
    never fill them). Calls to anything else (the standard library,
    other packages) keep their keywords.

    Raises:
        ValueError: listing each offending call or definition, by line
    """
    function_def, _ = code_extract_definition(tree)
    pool_aliases = {alias.asname or alias.name for node in tree.body
                    if isinstance(node, ast.ImportFrom) and node.module == 'bb.pool' for alias in node.names}
    pool_aliases |= {node.targets[0].id for node in tree.body if isinstance(node, ast.Assign)
                     and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and code_ref_hash(node.value)}
    local = {function_def.name}
    definitions = []
    for node in ast.walk(function_def):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            definitions.append(node)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            local.add(node.name)
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Lambda):
            local.update(target.id for target in node.targets if isinstance(target, ast.Name))

    def show(node):
        return (ast.get_source_segment(source_code, node) or ast.unparse(node)).split('\n')[0]

    problems = []
    for node in ast.walk(function_def):
        if (isinstance(node, ast.Call) and node.keywords and isinstance(node.func, ast.Name)
                and node.func.id in pool_aliases | local):
            names = ', '.join(f"{k.arg}=" if k.arg else "**" for k in node.keywords)
            problems.append((node.lineno, f"{show(node)}: keyword arguments ({names}) to {node.func.id}"))
    for node in definitions:
        if node.args.kwonlyargs or node.args.kwarg:
            name = getattr(node, 'name', 'lambda')
            kinds = ([f"keyword-only {', '.join(a.arg for a in node.args.kwonlyargs)}"] if node.args.kwonlyargs
                     else []) + ([f"**{node.args.kwarg.arg}"] if node.args.kwarg else [])
            problems.append((node.lineno, f"def {name}: {' and '.join(kinds)}"))
    if problems:
        lines = [f"{where}: bb stores parameters by position, so keywords cannot reach them:"]
        lines += [f"  line {line}: {text}" for line, text in sorted(problems)[:10]]
        if len(problems) > 10:
            lines.append(f"  ... and {len(problems) - 10} more")
        lines.append("Call pool functions, the function itself and functions defined inside it with")
        lines.append("positional arguments only (pass several options as one dict), and give their")
        lines.append("definitions no keyword-only parameters or **kwargs. Defaults and *args are fine.")
        raise ValueError('\n'.join(lines))


MODULE_GLOBALS = {'__name__', '__file__', '__doc__', '__builtins__', '__spec__', '__loader__', '__package__'}


def code_check_defined(source_code: str, tree: ast.Module, where: str):
    """
    Refuse a function that reads a name defined nowhere.

    A name is defined when the function (or a function inside it) binds
    it, an import or a bb binding (NAME = bb.resolve("NAME") or bb.ref)
    gives it, or it is a builtin. Anything else would only fail with a
    NameError when the function runs, for example a bb.resolve binding
    that was dropped while unused and is now used again. A file with
    `from x import *` is not checked.

    Raises:
        ValueError: listing each undefined name with the line of its first use
    """
    if any(isinstance(node, ast.ImportFrom) and any(a.name == '*' for a in node.names) for node in tree.body):
        return
    try:
        table = symtable.symtable(source_code, where, 'exec')
    except SyntaxError:
        return
    defined = {sym.get_name() for sym in table.get_symbols() if sym.is_assigned() or sym.is_imported()}
    defined |= PYTHON_BUILTINS | MODULE_GLOBALS | {'bb'}
    undefined = set()

    def visit(scope):
        for sym in scope.get_symbols():
            if (sym.is_referenced() and sym.is_global() and not sym.is_assigned()
                    and not sym.is_parameter() and not sym.is_local() and not sym.is_free()
                    and sym.get_name() not in defined):
                undefined.add(sym.get_name())
        for child in scope.get_children():
            visit(child)

    for child in table.get_children():
        visit(child)
    undefined |= {sym.get_name() for sym in table.get_symbols()
                  if sym.is_referenced() and not sym.is_assigned() and not sym.is_imported()
                  and sym.get_name() not in defined}
    if not undefined:
        return
    first = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in undefined and isinstance(node.ctx, ast.Load):
            first[node.id] = min(first.get(node.id, node.lineno), node.lineno)
    lines = [f"{where} uses names that are defined nowhere (they would fail with a NameError when it runs):"]
    lines += [f"  line {first.get(name, '?')}: {name}" for name in sorted(undefined, key=lambda n: (first.get(n, 0), n))]
    lines.append('Define them in the function, import them, or name a pool function with')
    lines.append('NAME = bb.resolve("NAME") above the def.')
    raise ValueError('\n'.join(lines))


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

    # Only imports and the one function are stored: refuse rather than drop the rest
    dropped = [node for node in tree.body
               if not isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef))
               and not (isinstance(node, ast.Assign) and len(node.targets) == 1
                        and isinstance(node.targets[0], ast.Name) and code_ref_hash(node.value))
               and not (node is tree.body[0] and isinstance(node, ast.Expr)
                        and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str))]
    if dropped:
        lines = [f"{where} has code outside its function, which bb would not store:"]
        for node in dropped[:5]:
            first = (ast.get_source_segment(source_code, node) or ast.unparse(node)).split('\n')[0]
            lines.append(f"  line {node.lineno}: {first}")
        if len(dropped) > 5:
            lines.append(f"  ... and {len(dropped) - 5} more")
        lines.append("A pool function is imports plus one function: move constants and setup into it,")
        lines.append("or into functions of their own.")
        raise ValueError('\n'.join(lines))

    code_check_positional(source_code, tree, where)
    code_check_defined(source_code, tree, where)

    try:
        _, normalized_code_without_docstring, docstring, name_mapping, alias_mapping = code_normalize(tree, lang)
    except Exception as e:
        raise ValueError(f"Failed to normalize AST: {e}")

    # Verify all bb imports resolve to objects in the local pool
    missing_deps = [(dep_hash, alias) for dep_hash, alias in alias_mapping.items()
                    if code_detect_schema(dep_hash) is None]
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
                    'whose function takes the implementation as its only argument and states what must hold with assert. -->')

_EDIT_FENCE_RE = re.compile(r'^(`{3,}|~{3,})\s*(.*?)\s*$')
_EDIT_MARKER_RE = re.compile(r'^<!--\s*bb:(.*?)\s*-->$')
# A whole-line HTML comment (<!-- ... -->): invisible in markdown, so dropping it loses nothing
_EDIT_COMMENT_RE = re.compile(r'^<!--.*-->$')
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
        elif current is None and kind == 'open' and not sections:
            # A code block with no heading above it is the implementation (a function with no prose)
            current = EDIT_IMPLEMENTATION_HEADING
            sections[current] = [(line, kind)]
        elif current is None:
            if (line.strip() and not line.startswith('# ') and not _EDIT_MARKER_RE.match(line.strip())
                    and not _EDIT_COMMENT_RE.match(line.strip())):
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
            if line.strip() and not _EDIT_MARKER_RE.match(line.strip()) and not _EDIT_COMMENT_RE.match(line.strip()):
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
    object_json = storage_object_path(hash_value)

    if not object_json.exists():
        print(f"Error: Function not found: {hash_value}", file=sys.stderr)
        sys.exit(1)

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

    Args:
        func_hash: Function hash (64-character hex)
        lang: Language code (e.g., "eng", "fra")

    Returns:
        List of (mapping_hash, comment) tuples, sorted by mapping hash
    """
    lang_dir = storage_mapping_directory(func_hash) / lang
    if not lang_dir.is_dir():
        return []
    mappings = []
    for path in sorted(lang_dir.glob('*.json')):
        data = mapping_load_json(func_hash, lang, path.stem)
        if data:
            mappings.append((path.stem, data.get('comment', '')))
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
    mapping_json = storage_mapping_path(func_hash, lang, mapping_hash)
    if not mapping_json.exists():
        print(f"Error: Mapping not found: {func_hash}@{lang} (mapping hash: {mapping_hash})", file=sys.stderr)
        sys.exit(1)
    try:
        with open(mapping_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse mapping: {e}", file=sys.stderr)
        sys.exit(1)

    return (data.get('docstring', ''), data.get('name_mapping', {}), data.get('alias_mapping', {}),
            data.get('comment', ''))


def mapping_load_json(func_hash: str, lang: str, mapping_hash: str) -> Dict[str, Any]:
    """Load the raw mapping data ({} when missing or unreadable). Do not modify the result: it is cached."""
    if not mapping_hash:
        return {}

    def build():
        try:
            with open(storage_mapping_path(func_hash, lang, mapping_hash), 'r', encoding='utf-8') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError):
            return {}
    return storage_cached(f'mapping:{func_hash}:{lang}:{mapping_hash}', build)


def mapping_load_markdown(func_hash: str, lang: str, mapping_hash: str) -> str:
    """
    Load the markdown documentation of a mapping ('' when it has none).
    """
    return mapping_load_json(func_hash, lang, mapping_hash).get('markdown', '')


def mapping_name(func_hash: str, lang: str, mapping_hash: str) -> Optional[str]:
    """The function's name in a mapping (its _bb_v_0), or None."""
    return mapping_load_json(func_hash, lang, mapping_hash).get('name_mapping', {}).get('_bb_v_0')


def mapping_leaves(func_hash: str, lang: str) -> List[str]:
    """
    List the mappings of a function in a language that were not superseded.

    When current lineage steps stand on the function in that language,
    their mappings (plus mappings no step stands on). Otherwise, the
    mappings no step on the same function and language came after (a
    documentation change, or a rename, supersedes a mapping).

    Returns:
        Mapping hashes, sorted
    """
    files = [m for m, _ in mappings_list(func_hash, lang)]
    index = lineage_index()
    steps = index['steps']
    stepped, superseded, current = set(), set(), set()
    for step_id in lineage_steps_on(func_hash, lang):
        record = steps[step_id]
        stepped.add(record['mapping'])
        if step_id in index['current']:
            current.add(record['mapping'])
        for following in index['successors'].get(step_id, []):
            after = steps[following]
            if after['function'] == func_hash and after['lang'] == lang and after['mapping'] != record['mapping']:
                superseded.add(record['mapping'])
    if current:
        return sorted(m for m in files if m in current or m not in stepped)
    return sorted(m for m in files if m not in superseded)


def mapping_select(func_hash: str, lang: str, name: str = None) -> Optional[str]:
    """
    Pick the mapping used by default for a function in a language.

    Mappings that current lineage steps stand on come first, then those
    not superseded (see mapping_leaves), then any; with name, mappings
    giving the function that name are preferred (two names may share one
    code). Ties go to the first mapping hash alphabetically.

    Returns:
        Mapping hash, or None if the language has no mapping
    """
    files = [m for m, _ in mappings_list(func_hash, lang)]
    if not files:
        return None
    index = lineage_index()
    current = sorted({index['steps'][s]['mapping'] for s in lineage_steps_on(func_hash, lang)
                      if s in index['current']} & set(files))
    leaves = mapping_leaves(func_hash, lang)

    def named(mappings):
        return [m for m in mappings if mapping_name(func_hash, lang, m) == name] if name else []

    for group in (named(current), named(leaves), named(files), current, leaves, files):
        if group:
            return group[0]
    return None


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
        name = helper_identifier_name(identifier)

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

    # Determine which mapping to show: by name, the ones calling it so (two names may share a code)
    leaves = mapping_leaves(hash_value, lang)
    if mapping_hash is None and name:
        leaves = [m for m in leaves if mapping_name(hash_value, lang, m) == name] or \
            [mapping_select(hash_value, lang, name)]
    if mapping_hash is not None:
        # Explicit mapping hash provided
        selected_hash = mapping_hash
    elif len(leaves) == 1:
        # One current mapping (older ones were superseded) - show it directly
        selected_hash = leaves[0]
    else:
        # Several current variants - show selection menu
        leaves = set(leaves)
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
        pinned = mapping_load_json(hash_value, lang, selected_hash).get('pinned') or []
        original_code = code_pretty(code_show_pool_names(code_denormalize(normalized_code, name_mapping, alias_mapping), lang, pinned))
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
    - All mapping files are valid JSON and name this function and language

    Args:
        func_hash: Function hash (64-character hex)

    Returns:
        Tuple of (is_valid, errors) where errors is a list of error messages
    """
    errors = []
    object_json = storage_object_path(func_hash)

    if not object_json.exists():
        errors.append(f"object.json not found for function {func_hash}")
        return False, errors

    try:
        with open(object_json, 'r', encoding='utf-8') as f:
            func_data = json.load(f)

        for field in ['schema_version', 'hash', 'normalized_code', 'metadata']:
            if field not in func_data:
                errors.append(f"Missing required field in object.json: {field}")

        if func_data.get('schema_version') != 1:
            errors.append(f"Invalid schema version: {func_data.get('schema_version')}")

    except (IOError, json.JSONDecodeError) as e:
        errors.append(f"Failed to parse object.json: {e}")
        return False, errors

    languages = storage_list_languages(func_hash)
    if not languages:
        errors.append("No language mappings found (no language directories)")
    for lang in languages:
        for path in sorted((storage_mapping_directory(func_hash) / lang).glob('*.json')):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                errors.append(f"Failed to parse mapping {lang}/{path.name}: {e}")
                continue
            if (data.get('function'), data.get('lang')) != (func_hash, lang):
                errors.append(f"Mapping {lang}/{path.name} names another function or language")

    if errors:
        return False, errors

    return True, []


def schema_validate_directory() -> tuple:
    """
    Validate the entire bb directory structure.

    Checks:
    - Config file exists and is valid JSON
    - Pool directory exists, in the current layout
    - All functions in pool are valid
    - All dependencies are resolvable
    - Every lineage step stands on a mapping in the pool, after a step in the pool

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
    if not bb_dir.exists():
        errors.append(f"BB directory does not exist: {bb_dir}")
        return False, errors, stats

    config_path = storage_get_config_path()
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            if not isinstance(config, dict):
                errors.append("Config file is not a valid JSON object")
        except json.JSONDecodeError as e:
            errors.append(f"Config file is invalid JSON: {e}")

    pool_dir = storage_get_pool_directory()
    if not pool_dir.exists():
        errors.append(f"Pool directory does not exist: {pool_dir}")
        return False, errors, stats
    if storage_pool_is_old_layout():
        errors.append(OLD_LAYOUT_MESSAGE)
        return False, errors, stats

    all_hashes = storage_list_functions()
    valid = set()
    for func_hash in sorted(all_hashes):
        stats['functions_total'] += 1
        is_valid, func_errors = schema_validate(func_hash)
        if is_valid:
            stats['functions_valid'] += 1
            valid.add(func_hash)
            stats['languages_total'].update(storage_list_languages(func_hash))
        else:
            stats['functions_invalid'] += 1
            for err in func_errors:
                errors.append(f"[{func_hash[:12]}...] {err}")

    for func_hash in sorted(valid):
        try:
            for dep in code_extract_dependencies(object_load(func_hash)['normalized_code']):
                if dep not in all_hashes:
                    stats['dependencies_missing'].append((func_hash, dep))
                    errors.append(f"[{func_hash[:12]}...] Missing dependency: {dep[:12]}...")
        except (Exception, SystemExit):
            pass

    steps = lineage_index()['steps']
    for step_id, record in sorted(steps.items()):
        missing = record.get('from') and record['from'] not in steps
        if not storage_mapping_path(record['function'], record['lang'], record['mapping']).exists() and \
                step_id not in lineage_index()['successors']:
            errors.append(f"[step {step_id[:12]}...] its mapping is not in the pool")
        if missing:
            errors.append(f"[step {step_id[:12]}...] comes after a step not in the pool: {record['from'][:12]}...")

    stats['languages_total'] = len(stats['languages_total'])

    is_valid = len(errors) == 0
    return is_valid, errors, stats


def command_caller(identifier: str, as_json: bool = False, every: bool = False):
    """
    Find the functions that depend on the given function.

    Accepts a hash or function name (with optional @lang suffix). Lists
    the current versions that import the given hash: a caller replaced
    by a later version is left out, so `bb caller old_name@eng` lists
    nothing once nothing uses that name any more. With every (--all),
    replaced versions are listed too, marked "not current". By name,
    when one code carries several names, only the callers that use it
    under that name are listed. --json gives hash, name, language and
    whether the caller is current.
    """
    hash_value, lang = helper_parse_identifier(identifier)
    functions = storage_scan_functions()
    callers = sorted(func_hash for func_hash, function in functions.items()
                     if hash_value in code_extract_dependencies(function['normalized_code']))
    if not every:
        callers = [c for c in callers if functions[c]['current']]
    callers = [c for c in callers if search_uses_under_name(functions[c], hash_value, identifier)]

    if as_json:
        out = []
        for caller_hash in callers:
            lang = helper_pick_language(caller_hash)
            out.append({'hash': caller_hash, 'name': helper_function_label(caller_hash), 'lang': lang,
                        'current': functions[caller_hash]['current']})
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return
    for caller_hash in callers:
        print(f"bb show {caller_hash}" + ("" if functions[caller_hash]['current'] else "  # not current"))


def command_tree(identifier: str, as_json: bool = False):
    """
    Display the dependency tree of a function.

    Accepts a hash or function name (with optional @lang suffix).
    Shows function names and one-liner docstrings in a tree format similar
    to the Unix tree command, in the identifier's language (else your
    first language from bb whoami). Nodes are marked:

    ```
    out of date                 uses a version that is no longer current
                                for the name it uses it by
    behind                      a current version that uses something out
                                of date or behind: it must follow
    pinned                      named by hash on purpose (bb.ref)
    not yet translated to LANG  current in LANG while another language has
                                a newer version
    ```
    """
    hash_value, lang = helper_parse_identifier(identifier)
    if '@' not in identifier:
        preferred_langs = storage_read_config()['user'].get('languages') or ['eng']
        lang = preferred_langs[0]

    index = storage_pool_index()
    tree = _tree_build(hash_value, lang, set(), index)
    if as_json:
        print(json.dumps(tree, indent=2, ensure_ascii=False))
        return
    _tree_print(tree, "", True, True)


def _tree_marks(index: Dict[str, Any], func_hash: str, lang: str, caller: Optional[str],
                behind: Dict[str, List[str]]) -> List[str]:
    """The marks of a tree node (see command_tree): how up to date func_hash is, as its caller uses it."""
    marks = []
    if caller is not None:
        caller_function = index['functions'].get(caller)
        if caller_function and func_hash in search_pins(caller_function):
            marks.append('pinned')
        elif func_hash in search_outdated_refs(index, caller):
            marks.append('out of date')
    elif func_hash in index['functions'] and func_hash not in index['current']:
        marks.append('out of date')
    if func_hash in behind and 'out of date' not in marks:
        marks.append('behind')
    for newer, here in search_untranslated(func_hash):
        if here == lang:
            marks.append(f"not yet translated to {here} (newer in {newer})")
    return marks


def _tree_print(node: Dict[str, Any], prefix: str, is_last: bool, is_root: bool):
    """Print a node of _tree_build and its dependencies, like the Unix tree command."""
    if not node['found']:
        label = f"{node['hash'][:12]}... (not found)"
    else:
        label = node['name'] or node['hash'][:12]
        if node['description']:
            label += f" — {node['description'].strip()}"
        if node['marks']:
            label += f"  [{', '.join(node['marks'])}]"
    if is_root:
        print(label)
        child_prefix = ""
    else:
        print(f"{prefix}{'└── ' if is_last else '├── '}{label}")
        child_prefix = prefix + ("    " if is_last else "│   ")
    deps = node['dependencies']
    for i, dep in enumerate(deps):
        _tree_print(dep, child_prefix, i == len(deps) - 1, False)


def _tree_build(func_hash: str, lang: str, visited: set, index: Dict[str, Any] = None,
                caller: str = None, behind: Dict[str, List[str]] = None) -> Dict[str, Any]:
    """The dependency tree of a function as nested dicts: hash, name, description, marks, dependencies."""
    index = index or storage_pool_index()
    if behind is None:
        behind = search_behind(index)
    if code_detect_schema(func_hash) is None:
        return {'hash': func_hash, 'name': None, 'description': '', 'found': False, 'marks': [],
                'dependencies': []}
    shown = lang if lang in storage_list_languages(func_hash) else helper_pick_language(func_hash, lang)
    alias = None
    if caller is not None:
        caller_lang = lang if lang in storage_list_languages(caller) else helper_pick_language(caller, lang)
        alias = index['functions'].get(caller, {}).get('mappings', {}).get(caller_lang, {}).get(
            'alias_mapping', {}).get(func_hash)
    mapping_hash = mapping_select(func_hash, shown, lineage_name_for(func_hash, shown, alias) or alias)
    normalized_code, name_mapping, _, docstring = code_load(func_hash, shown, mapping_hash)
    node = {
        'hash': func_hash,
        'name': name_mapping.get('_bb_v_0'),
        'lang': shown,
        'description': docstring.strip().split('\n')[0] if docstring else '',
        'found': True,
        'marks': _tree_marks(index, func_hash, lang, caller, behind),
        'dependencies': [],
    }
    if func_hash not in visited:
        node['dependencies'] = [_tree_build(dep, lang, visited | {func_hash}, index, func_hash, behind)
                                for dep in code_extract_dependencies(normalized_code)]
    return node


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


def helper_identifier_name(identifier: str) -> Optional[str]:
    """The name in an identifier (name or name@lang), None for a hash."""
    part = identifier.split('@')[0]
    if len(part) == 64 and all(c in '0123456789abcdef' for c in part.lower()):
        return None
    return part or None


# ============================================================================
# Checks and bb edit
# ============================================================================
#
# A check is an ordinary pool function that takes the implementation as its
# only argument and states what must hold with assert: it passes when it
# runs without raising, and needs at least one assert. Checks do not
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
    namespace['bb'] = code_load_refs(tree, cache)
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
        if isinstance(node, ast.Import) and [(a.name, a.asname) for a in node.names] == [('bb', None)]:
            continue  # bb is bb.ref, bound above
        if (isinstance(node, (ast.Import, ast.ImportFrom)) or node is function_def
                or (isinstance(node, ast.Assign) and code_ref_hash(node.value))):
            body.append(node)
    tree.body = body

    try:
        exec(compile(tree, f'<bb edit {function_def.name}>', 'exec', optimize=0), namespace)
    except Exception as e:
        raise ValueError(f"{function_def.name} failed to load: {type(e).__name__}: {e}")

    return namespace[function_def.name]


CHECK_NO_ASSERT = ("no assert: a check passes when it runs without raising, so it needs at least one "
                   "assert (assert f(2) == 4); rewrite it with bb edit")


def check_has_assert(code: str) -> bool:
    """True when the code of a check holds at least one assert statement."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return True  # not this function's business: running it says what is wrong
    return any(isinstance(node, ast.Assert) for node in ast.walk(tree))


def check_explain(source: str, check: Callable, error: AssertionError,
                  names: Dict[str, str] = None) -> str:
    """
    Say why a check failed, from the assert that raised.

    The assert statement is found in source, the check's code, by the line
    the traceback reaches in the check, and evaluated again piece by piece
    with the values the check had: the first false part of an `and` chain,
    and for a comparison both of its sides, so `assert parse('12,50') ==
    1250` shows `1200 == 1250`. The assert's own message, when it has one,
    heads the line. Anything that cannot be evaluated is left out. names
    maps the normalized names of stored code (_bb_v_1) back to the ones
    its author wrote, for display only.
    """
    detail = str(error) or "assertion failed"
    code = getattr(check, '__code__', None)
    tb, found = error.__traceback__, None
    while tb is not None:
        if tb.tb_frame.f_code is code:
            found = tb
        tb = tb.tb_next
    if not source or found is None:
        return detail
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return detail
    line, scope = found.tb_lineno, found.tb_frame
    node = next((n for n in ast.walk(tree)
                 if isinstance(n, ast.Assert) and n.lineno <= line <= n.end_lineno), None)
    if node is None:
        return detail  # raised by something the check called

    def evaluate(expression: ast.expr):
        compiled = compile(ast.fix_missing_locations(ast.Expression(expression)), '<check>', 'eval')
        return eval(compiled, scope.f_globals, scope.f_locals)

    def show(expression: ast.expr) -> str:
        shown = copy.deepcopy(expression)
        for name in ast.walk(shown):
            if isinstance(name, ast.Name) and name.id in (names or {}):
                name.id = names[name.id]
        return ast.unparse(shown)

    try:
        part = node.test
        while isinstance(part, ast.BoolOp) and isinstance(part.op, ast.And):
            part = next((value for value in part.values if not evaluate(value)), None)
            if part is None:
                return detail
        detail += f": {show(part)}"
        if isinstance(part, ast.Call) and getattr(part.func, 'id', None) == 'all' and len(part.args) == 1:
            return f"{detail} is False"
        if isinstance(part, ast.Compare) and len(part.ops) == 1:
            left, right = evaluate(part.left), evaluate(part.comparators[0])
            operator = ast.unparse(ast.Compare(left=ast.Name('a'), ops=part.ops, comparators=[ast.Name('b')]))[2:-2]
            detail += f" ({left!r} {operator} {right!r})"
        elif part is not node.test:
            detail += " is false"
    except Exception:
        pass
    return detail


def check_run(check: Callable, implementation: Callable, source: str = None,
              names: Dict[str, str] = None) -> Tuple[str, str]:
    """
    Run one check against an implementation.

    A check passes when it runs without raising: it states what must hold
    with assert, and its return value is ignored. A failed assert is FAIL
    and says which one (check_explain); any other exception is ERROR. With
    source, the check's code as it was compiled, a check without any
    assert is an ERROR, since nothing could ever fail it.

    Returns:
        (status, detail) where status is PASS, FAIL or ERROR
    """
    if source and not check_has_assert(source):
        return 'ERROR', CHECK_NO_ASSERT
    try:
        check(implementation)
    except AssertionError as e:
        return 'FAIL', check_explain(source, check, e, names)
    except Exception as e:
        return 'ERROR', f"{type(e).__name__}: {e}"
    return 'PASS', ''


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


def code_restore_inner_imports(tree: ast.Module, func_hash: str):
    """
    Give back their real names to modules imported inside a function body.

    Before the normalizer was fixed, it kept `import random` inside a
    function as written but renamed its uses (`_bb_v_2.randint`), so
    objects stored then cannot run as is. The mapping knows that _bb_v_2
    is `random`. To keep mappings from
    changing what runs, a normalized name is restored only to a name that
    an import statement in this very code binds.

    Modifies tree in place.
    """
    bound = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            bound.update((alias.asname or alias.name).split('.')[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module != 'bb.pool':
            bound.update(alias.asname or alias.name for alias in node.names)
    if not bound:
        return

    lang = helper_pick_language(func_hash)
    if lang is None:
        return
    mapping = mapping_load_json(func_hash, lang, mapping_select(func_hash, lang))
    rename = {norm: name for norm, name in mapping.get('name_mapping', {}).items()
              if norm.startswith('_bb_v_') and norm != '_bb_v_0' and name in bound}
    if rename:
        ASTNormalizer(rename).visit(tree)


def code_load_refs(tree: ast.AST, cache: Dict[str, Callable]) -> types.SimpleNamespace:
    """
    The `bb` a function runs with: bb.ref("<hash>") gives the pool function.

    Every function the code refers to is loaded first, so a missing one
    fails at load time, not when it is first called.
    """
    loaded = {}
    for node in ast.walk(tree):
        func_hash = code_ref_hash(node)
        if func_hash and func_hash not in loaded:
            loaded[func_hash] = code_load_normalized(func_hash, cache)
    return types.SimpleNamespace(ref=loaded.__getitem__)


def code_load_normalized(func_hash: str, cache: Dict[str, Callable] = None,
                         source: str = None, like: str = None) -> Callable:
    """
    Load a pool function from its normalized code, exactly as hashed.

    No language mapping is applied, so what runs is the code that was
    hashed and reviewed, whatever names a mapping displays. Each function
    runs in its own namespace, with its bb imports bound to their own
    loaded functions. With source (and like, the function it rewrites),
    code that is not in the pool yet is loaded under func_hash.

    Raises:
        ValueError: If the function or a dependency is missing or fails to load
    """
    if cache is None:
        cache = {}
    if func_hash in cache:
        return cache[func_hash]

    if source is None:
        if code_detect_schema(func_hash) is None:
            raise ValueError(f"Function not in the local pool: {func_hash}")
        data = object_load(func_hash)
        source, like = data['normalized_code'], func_hash
    else:
        data = object_load(like)  # a version not saved yet, loaded like the one it rewrites
    tree = ast.parse(source)
    if data.get('normalization', 1) < 2:
        code_restore_inner_imports(tree, like)

    namespace: Dict[str, Any] = {'bb': code_load_refs(tree, cache)}
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
        exec(compile(tree, f'<bb {func_hash[:12]}>', 'exec', optimize=0), namespace)
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

    return check_run_all(func_hash, object_load_checks(func_hash)), []


def helper_display_names(func_hash: str) -> Dict[str, str]:
    """The names a function's author wrote, by normalized name, in the language you read it in."""
    lang = helper_pick_language(func_hash)
    if lang is None:
        return {}
    return mapping_load_json(func_hash, lang, mapping_select(func_hash, lang)).get('name_mapping', {})


def check_run_all(func_hash: str, checks: List[str], source: str = None,
                  like: str = None) -> List[Tuple[str, str, str]]:
    """
    Run checks against a function, already approved: results for check_report.

    With source and like, the function is code not saved yet (see
    code_load_normalized). Callers make sure everything that runs was approved.
    """
    cache: Dict[str, Callable] = {}
    try:
        implementation = code_load_normalized(func_hash, cache, source=source, like=like)
    except ValueError as e:
        return [('ERROR', helper_function_label(like or func_hash), str(e))]

    results = []
    for check_hash in checks:
        name = helper_function_label(check_hash)
        try:
            check = code_load_normalized(check_hash, cache)
        except ValueError as e:
            results.append(('ERROR', name, str(e)))
            continue
        status, detail = check_run(check, implementation, object_load(check_hash).get('normalized_code'),
                                   helper_display_names(check_hash))
        results.append((status, name, detail))

    results.sort(key=lambda r: r[1])
    return results


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


def edit_load_section(func_hash: str, lang: str, name: str = None) -> Dict[str, Any]:
    """
    Load a pool function as an edit-file section.

    Falls back to another language when the function has no mapping in
    lang. With name, loads the mapping calling the function so (two names
    may share one code).

    Returns:
        Section dict (source, markdown, parent, title) where parent is the
        lineage step the section continues (None when the function has no
        step in that language), plus 'mapping', the exact translation
        loaded (hash@lang@mapping_hash), and 'fingerprint', used to tell
        whether the user changed anything
    """
    section_lang = helper_pick_language(func_hash, lang)
    if section_lang is None:
        print(f"Error: Function not found in the local pool: {func_hash}", file=sys.stderr)
        sys.exit(1)

    mapping_hash = mapping_select(func_hash, section_lang, name)
    normalized_code, name_mapping, alias_mapping, docstring = code_load(func_hash, section_lang, mapping_hash)
    pinned = mapping_load_json(func_hash, section_lang, mapping_hash).get('pinned') or []
    source = code_denormalize(code_replace_docstring(normalized_code, docstring), name_mapping, alias_mapping)
    source = code_pretty(code_show_pool_names(source, section_lang, pinned))
    markdown = mapping_load_markdown(func_hash, section_lang, mapping_hash)
    title = name_mapping.get('_bb_v_0', func_hash[:12])

    return {
        'title': title,
        'source': source + '\n',
        'markdown': markdown,
        'parent': lineage_step_for(func_hash, section_lang, title, mapping_hash),
        'mapping': f"{func_hash}@{section_lang}@{mapping_hash}",
        'fingerprint': edit_fingerprint(func_hash, docstring, name_mapping, alias_mapping, markdown, pinned),
    }


def edit_section_name(section: Dict[str, Any], prepared: Dict[str, Any]) -> str:
    """The name an edit file gives its function."""
    return prepared['name_mapping'].get('_bb_v_0') or section.get('title', '')


def edit_fingerprint(func_hash: str, docstring: str, name_mapping: Dict[str, str],
                     alias_mapping: Dict[str, str], markdown: str, pinned: List[str] = None) -> Tuple[str, str]:
    """Identify a section by function hash and mapping content (comment excluded)."""
    return func_hash, code_compute_mapping_hash(docstring, name_mapping, alias_mapping, "", markdown, pinned,
                                                function=func_hash)


def edit_marker_step(marker: Optional[str], lang: str, name: str = None) -> Optional[str]:
    """
    The lineage step a bb:parent marker stands for.

    Drafts name the step they were made from. Older drafts named a
    function hash: that function's step in lang (called name) stands for
    it. Anything else is kept as is, so the draft is refused as stale.
    """
    if marker is None or marker in lineage_index()['steps']:
        return marker
    if code_detect_schema(marker) is not None:
        return lineage_step_for(marker, lang, name) or marker
    return marker


def edit_describe_step(step_id: Optional[str]) -> str:
    """A lineage step for messages: its function hash and a short step id."""
    record = lineage_index()['steps'].get(step_id or '')
    if record is None:
        return step_id or 'nothing'
    return f"{record['function']} (step {step_id[:12]}...)"


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


def edit_prepare(text: str, lang: str, unreviewed: bool = False) -> Dict[str, Any]:
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
    # Pool imports by name become imports by hash before anything else reads the code
    resolved = {}
    pins = {}
    for section, where in [(implementation, "Implementation")] + [(c, f"check '{c['title']}'") for c in parsed['checks']]:
        pins[id(section)] = code_pinned_refs(section['source'])
        section['source'], names = code_resolve_pool_names(section['source'], lang, where=where)
        resolved.update(names)
    prepared_implementation = code_prepare_source(implementation['source'], lang, where="Implementation")
    for check in parsed['checks']:
        if not check_has_assert(check['source']):
            raise ValueError(f"check '{check['title']}' has {CHECK_NO_ASSERT}")
    checks = [(check, code_prepare_source(check['source'], lang, where=f"check '{check['title']}'"))
              for check in parsed['checks']]
    for section, prepared in [(implementation, prepared_implementation)] + checks:
        prepared['pinned'] = sorted(pins[id(section)] & set(code_extract_dependencies(prepared['normalized_code'])))

    # Saving is not running: only checks run the code. Without them nothing
    # is executed, so nothing it imports needs to have been reviewed.
    if not checks:
        return {'implementation': (implementation, prepared_implementation), 'checks': [], 'results': [],
                'resolved': resolved}

    # The sections were in front of you; what they import from the pool was
    # not. Refuse to run imports you have not approved with bb review.
    imported = {dep for _, p in [(implementation, prepared_implementation)] + checks
                for dep in p['alias_mapping']}
    needed = {h for dep in imported for h in code_resolve_dependencies(dep)}
    # Functions you saved yourself may run in your checks; others need bb review
    missing = sorted(needed - review_approvals(list(needed)) - review_load_authored())
    if missing and unreviewed:
        print(f"Warning: running {len(missing)} imported function(s) nobody reviewed "
              "(--dangerously-unreviewed).", file=sys.stderr)
    elif missing:
        raise ValueError("The edit imports functions you have not reviewed, so nothing was run:\n"
                         + '\n'.join(f"  bb review {h}" for h in missing))

    function = code_execute_source(implementation['source'], lang)

    results = []
    for check, prepared in checks:
        name = prepared['name_mapping']['_bb_v_0']
        try:
            status, detail = check_run(code_execute_source(check['source'], lang), function, check['source'])
        except ValueError as e:
            status, detail = 'ERROR', str(e)
        results.append((status, name, detail))

    return {
        'implementation': (implementation, prepared_implementation),
        'checks': checks,
        'results': results,
        'resolved': resolved,
    }


def edit_save_section(section: Dict[str, Any], prepared: Dict[str, Any], lang: str,
                      comment: str, checks: List[str] = None, unchanged: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """
    Save one edit-file section to the pool, with its lineage step.

    The function is saved with checks (for the implementation) as part of
    its definition; prepared's hash must already include them. An
    existing function is never rewritten. When something changed, the
    section's mapping is saved and a step records it after the section's
    bb:parent step: 'rename' when the name changed, 'document' when only
    the mapping did, 'restore' when the code is an older version's, 'edit'
    otherwise; 'add' without a parent step (a new name). Code another
    name already has is fine: two names may share one code, each with its
    own lineage.

    Returns:
        (the step now standing for the section, the relation recorded or None)
    """
    func_hash = prepared['hash']
    object_save(func_hash, prepared['normalized_code'], code_create_metadata(), checks=checks)
    if unchanged:
        return section.get('parent'), None

    mapping_hash = mapping_save(func_hash, lang, prepared['docstring'], prepared['name_mapping'],
                                prepared['alias_mapping'], comment, section['markdown'],
                                pinned=prepared.get('pinned'))
    steps = lineage_index()['steps']
    parent = section.get('parent') if section.get('parent') in steps else None
    if parent and steps[parent]['lang'] != lang:
        parent = None  # loaded in another language: this language's lineage starts here
    if parent is None:
        existing = lineage_current_steps(lang, prepared['name_mapping'].get('_bb_v_0'), func_hash)
        existing = [s for s in existing if steps[s]['mapping'] == mapping_hash]
        if existing:
            return existing[0], None
        step_id = lineage_save_new(func_hash, lang, mapping_hash, comment)
        return step_id, lineage_index()['steps'][step_id]['relation']
    previous = steps[parent]
    if (previous['function'], previous['mapping']) == (func_hash, mapping_hash):
        return parent, None
    if prepared['name_mapping'].get('_bb_v_0') != previous['name']:
        relation = 'rename'
    elif func_hash == previous['function']:
        relation = 'document'
    elif func_hash in {steps[s]['function'] for s in lineage_ancestors(parent)}:
        relation = 'restore'
    else:
        relation = 'edit'
    return lineage_save(parent, func_hash, lang, mapping_hash, relation, comment), relation


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


def command_edit(identifier: str, comment: str = None, unreviewed: bool = False,
                 refactor: str = None, at: str = None, file: str = None):
    """
    Edit a function and its checks as one markdown file, then save a fork.

    Opens $EDITOR on a file with an Implementation section and a Checks
    section. Each section holds prose and examples, and exactly one code
    block fenced as `python def`: the function that is hashed. Each check
    takes the implementation as its only argument and states what must hold
    with assert (at least one: a check passes when it runs without raising;
    one without an assert is refused).

    Saving requires the file to parse and every check to pass. Only checks
    run the code: a function without checks is saved without running it,
    whatever it imports. With checks, the pool functions it imports must be
    yours (saved by you with bb add or bb edit, recorded in your private
    state) or reviewed with bb review, unless --dangerously-unreviewed (for
    experiments) runs them anyway. The new
    version lists its checks, and a lineage step in the edit's language
    records that it comes after the edited one (a rename when the name
    changed: the old name is then superseded); changed checks get their
    own steps. Other languages stay on the version they had until someone
    translates the new one. The comment (asked for when --comment is not
    given) is stored in the new mappings and steps.

    Saving code that an older version of the same name already has
    restores that version (a restore step makes it current again). Code
    that another name has is fine: two names may share one code, each
    following its own lineage.

    --refactor ROOT then updates ROOT to the new version: every function
    between ROOT and the edited one gets a new version using it, as
    `bb refactor ROOT OLD NEW` does (with --at AT, only along the path from
    AT to ROOT). ROOT and AT are names (resolved by lineage) or hashes, and
    ROOT must use the edited function. Without --refactor, functions using
    it keep the version they were saved with.

    With FILE, no editor opens: when FILE is missing, the edit file is
    written there and nothing is saved; when it exists, it is saved (and
    rewritten to the saved version, so editing can go on in it). A FILE
    drafted from another version than the current one is refused, so a
    stale draft never undoes changes made since.

    A name no function has yet (in any language) starts a new function
    from a template; it is saved with no parent. The language is then
    required, as in `bb edit slugify@eng`.

    Examples:

    ```
    bb edit compute_pi@eng
    bb edit compute_pi@eng --comment "Faster convergence"
    bb edit slugify@eng          # a new function
    bb edit slugify@eng slugify.md                # write slugify.md; run again to save it
    bb edit parse_amount@eng --refactor app@eng   # and update app to it
    ```
    """
    name, _, requested_lang = identifier.partition('@')
    is_hash = len(name) == 64 and all(c in '0123456789abcdef' for c in name.lower())
    if not is_hash and name.isidentifier() and not helper_find_heads_by_name(name)[0]:
        if not requested_lang:
            print(f"Error: '{name}' is a new function: give its language, e.g. bb edit {name}@eng",
                  file=sys.stderr)
            sys.exit(1)
        hash_value, lang = None, requested_lang
        template = f'def {name}():\n    """Say what {name} does."""\n    raise NotImplementedError\n'
        original = {'title': name, 'source': template, 'markdown': '', 'parent': None, 'mapping': '',
                    'fingerprint': None}
        original_checks = []
    else:
        hash_value, lang = helper_parse_identifier(identifier)
        lang = lang or helper_pick_language(hash_value)
        if lang not in storage_list_languages(hash_value):
            print(f"Error: {hash_value[:12]}... has no '{lang}' mapping. Translate it first with bb translate.",
                  file=sys.stderr)
            sys.exit(1)
        # Load the function (by name: the mapping calling it so) and the checks attached to it;
        # the section's parent is the lineage step the edit continues
        original = edit_load_section(hash_value, lang, None if is_hash else name)
        original_checks = []
    refactor_root = refactor_at = None
    if refactor:
        # Checked before the editor opens, so a mistake costs no work
        if hash_value is None:
            print("Error: --refactor needs an existing function: nothing uses a new one yet", file=sys.stderr)
            sys.exit(1)
        refactor_root = helper_parse_identifier(refactor)[0]
        refactor_at = helper_parse_identifier(at)[0] if at else None
        if refactor_root == hash_value or hash_value not in code_resolve_dependencies(refactor_root):
            print(f"Error: {refactor} does not use {identifier}: give a function that imports it, "
                  f"directly or not (e.g. the app)", file=sys.stderr)
            sys.exit(1)
    elif at:
        print("Error: --at goes with --refactor", file=sys.stderr)
        sys.exit(1)
    for check_hash in (object_load_checks(hash_value) if hash_value else []):
        if code_detect_schema(check_hash) is None:
            print(f"Error: Check {check_hash[:12]}... is attached to this function "
                  f"but missing from the local pool. Fetch it with git first.", file=sys.stderr)
            sys.exit(1)
        original_checks.append(edit_load_section(check_hash, lang))

    text = markdown_render_edit(original['title'], original, original_checks)
    if file is not None:
        if not os.path.exists(file):
            # First call: write the edit file, save nothing
            with open(file, 'w', encoding='utf-8') as f:
                f.write(text)
            print(f"Wrote {file}. Change it, then run the same command again to save it.")
            return
        path = file
    else:
        # Kept next to the pool, so an abandoned edit is easy to find again
        edit_dir = storage_get_bb_directory() / 'edit'
        edit_dir.mkdir(parents=True, exist_ok=True)
        fd, path = tempfile.mkstemp(prefix=f"{original['title']}-", suffix='.md', dir=edit_dir)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(text)

    def discard():
        """Remove bb's own temporary file; a file the user named is theirs."""
        if file is None:
            os.unlink(path)

    # Edit until the file is usable and every check passes
    while file is not None:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                edited = f.read()
            # A stale file is refused before anything runs
            drafted_from = edit_marker_step(markdown_parse_edit(edited)['implementation']['parent'], lang,
                                            original['title'])
            if drafted_from != original['parent']:
                what = (f"the current version of {name} is {edit_describe_step(original['parent'])}" if hash_value
                        else f"{name} has no version")
                since = (f"{file} was drafted from {edit_describe_step(drafted_from)}" if drafted_from
                         else f"{file} has no bb:parent marker")
                print(f"Error: {since}, but {what}. Nothing saved: write a new draft with "
                      f"bb edit {identifier} NEW_FILE and make your change there.", file=sys.stderr)
                sys.exit(1)
            plan = edit_prepare(edited, lang, unreviewed)
            passed = check_report(plan['results'])
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            passed = False
        if not passed:
            print(f"Not saved: fix {file} and save it again.", file=sys.stderr)
            sys.exit(1)
        break
    while file is None:
        try:
            helper_run_editor(path)
            with open(path, 'r', encoding='utf-8') as f:
                edited = f.read()
            plan = edit_prepare(edited, lang, unreviewed)
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
    if hash_value is None and section['source'].strip() == original['source'].strip() and not plan['checks']:
        discard()
        print("Nothing saved: the edited file still holds the template, so no function was created.")
        return
    # Without a marker, the implementation still descends from the edited function
    section['parent'] = edit_marker_step(section['parent'], lang, original['title']) or original['parent']
    # Checks without a marker are matched to the loaded checks by name, then position
    parents = edit_assign_parents([(p['name_mapping']['_bb_v_0'], edit_marker_step(c['parent'], lang))
                                   for c, p in plan['checks']],
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
                                prep['alias_mapping'], sec['markdown'], prep.get('pinned')) in original_prints

    if (prepared['hash'] == hash_value and unchanged(section, prepared)
            and all(unchanged(s, p) for s, p in plan['checks'])):
        discard()
        print("No changes.")
        return

    if comment is None:
        try:
            comment = input("Comment (press Enter to skip): ").strip()
        except EOFError:
            comment = ""

    def rewrite_file(func_hash, step_id):
        """The file now drafts from step_id, the step just saved, so editing can go on in it."""
        if file is None:
            return
        saved = edit_load_section(func_hash, lang, edit_section_name(section, prepared))
        saved['parent'] = step_id or saved['parent']
        saved_checks = [edit_load_section(c, lang) for c in object_load_checks(func_hash)]
        with open(file, 'w', encoding='utf-8') as f:
            f.write(markdown_render_edit(saved['title'], saved, saved_checks))

    previous_step = lineage_index()['steps'].get(section['parent'] or '')

    # Save checks first, so the implementation never lists a missing check
    with contextlib.redirect_stdout(io.StringIO()):
        new_checks = []
        for check, prep in plan['checks']:
            _, check_relation = edit_save_section(check, prep, lang, comment, unchanged=unchanged(check, prep))
            if check_relation:
                new_checks.append((prep['name_mapping']['_bb_v_0'], prep['hash']))
        saved_step, relation = edit_save_section(section, prepared, lang, comment, checks=check_hashes,
                                                 unchanged=unchanged(section, prepared))

    discard()
    review_approve_own([prepared['hash']] + check_hashes)
    rewrite_file(prepared['hash'], saved_step)

    if relation == 'restore':
        # Same code as an older version: that version is current again
        print(f"Restored: {prepared['hash']}")
        print(f"Supersedes: {previous_step['function']}")
        if refactor_root:
            print(f"Updating {refactor} to it:")
            command_refactor(refactor_root, hash_value, prepared['hash'], refactor_at, comment=comment)
        return

    for name, check_hash in new_checks:
        print(f"Check: {check_hash} {name}")
    was = {}
    if hash_value:
        # The version of each name the edited function used, to show what saving changed
        for old_hash in code_extract_dependencies(object_load(hash_value)['normalized_code']):
            if lang in storage_list_languages(old_hash):
                old_name = mapping_load_json(old_hash, lang, mapping_select(old_hash, lang)).get(
                    'name_mapping', {}).get('_bb_v_0')
                was.setdefault(old_name, old_hash)
    for name, func_hash in sorted(plan.get('resolved', {}).items()):
        changed = was.get(name) and was[name] != func_hash
        print(f"Import: {name} -> {func_hash}" + (f" (was {was[name]})" if changed else ""))
    # The saved hash last, where a reader (or `tail -1`) looks for it
    if relation == 'rename' and previous_step:
        print(f"Renamed: {previous_step['name']} -> {edit_section_name(section, prepared)} "
              f"(the old name is superseded)")
    if hash_value and prepared['hash'] != hash_value:
        print(f"Parent: {previous_step['function'] if previous_step else hash_value}")
    elif hash_value:
        print("Documentation saved: the code did not change, so the hash stays the same.")
    print(f"Hash: {prepared['hash']}")

    if refactor_root and prepared['hash'] != hash_value:
        print(f"Updating {refactor} to it:")
        try:
            command_refactor(refactor_root, hash_value, prepared['hash'], refactor_at, comment=comment)
        except SystemExit as stop:
            if stop.code:
                print(f"The edit is saved as {prepared['hash']}, but {refactor} was not updated. To retry:\n"
                      f"  bb refactor {refactor_root} {hash_value} {prepared['hash']}"
                      + (f" {refactor_at}" if refactor_at else ""), file=sys.stderr)
            raise

    if prepared['hash'] != hash_value:
        helper_report_similar(prepared['hash'], helper_lineage(prepared['hash']) | set(check_hashes))


SCRATCHPAD_REF = re.compile(r"""bb\.(?:resolve|latest)\(\s*['"]([A-Za-z_]\w*)['"]""")


def scratchpad_split(text: str) -> List[Tuple[str, str]]:
    """
    The (name, section) pairs of a scratchpad: one edit file per function, in order.

    A section starts at a level-1 heading `# name` outside code blocks and
    runs to the next one, so a scratchpad of one section is a bb edit file.
    """
    sections: List[List[Any]] = []
    fence = False
    for line in text.split('\n'):
        if line.lstrip().startswith('```'):
            fence = not fence
        if not fence and line.startswith('# '):
            sections.append([line[2:].strip(), [line]])
        elif sections:
            sections[-1][1].append(line)
        elif line.strip():
            raise ValueError("a scratchpad starts with a '# name' heading")
    return [(name, '\n'.join(lines).rstrip('\n') + '\n') for name, lines in sections]


def scratchpad_tree_names(identifier: str) -> List[str]:
    """NAME@lang and everything it uses, top down as bb tree shows them, as names in lang."""
    func_hash, lang = helper_parse_identifier(identifier)
    functions = storage_scan_functions()
    order, seen = [], set()

    def visit(h):
        if h in seen or h not in functions:
            return
        seen.add(h)
        name = functions[h]['names'].get(lang)
        if name is None:
            print(f"Error: {helper_function_label(h)} ({h[:12]}...) has no '{lang}' name: translate it "
                  f"first with bb translate", file=sys.stderr)
            sys.exit(1)
        if name not in order:
            order.append(name)
        for dep in code_extract_dependencies(functions[h]['normalized_code']):
            visit(dep)

    visit(func_hash)
    return [f"{name}@{lang}" for name in order]


def scratchpad_compact_report(report: str) -> str:
    """
    A section's bb edit report, shorter: one scratchpad saves many
    functions, so "No checks." goes and the section's imports fold into
    one line with short hashes. Check results and the hash stay.
    """
    lines, imports = [], []
    for line in report.splitlines(True):
        found = re.match(r'Import: (\S+) -> ([0-9a-f]{64})(?: \(was ([0-9a-f]{64})\))?\s*$', line)
        if found:
            was = f" (was {found.group(3)[:12]})" if found.group(3) else ""
            imports.append(f"{found.group(1)} -> {found.group(2)[:12]}{was}")
        elif line.strip() != 'No checks.':
            lines.append(line)
    if imports:
        at = next((i for i, line in enumerate(lines) if line.startswith('Hash: ')), len(lines))
        lines.insert(at, f"Imports: {', '.join(imports)}\n")
    return ''.join(lines)


def command_scratchpad(identifiers: List[str], file: str, comment: str = None, tree: str = None,
                       unreviewed: bool = False, refactor: str = None):
    """
    Edit several functions in one markdown file, then save every one that changed.

    With refactor (--refactor ROOT, e.g. app@eng), a save that went through
    then brings ROOT and everything between it and the saved functions up
    to date, as bb update ROOT does, with the same comment.

    The file is bb edit files one after another, each starting at its own
    `# name` heading: Implementation (with its bb:parent line and
    documentation) and Checks. When FILE is missing, bb writes every named
    function into it, in the order given (--tree NAME@lang adds NAME and
    everything it uses, top down as bb tree shows them), and saves nothing.
    When FILE exists, bb saves each section as bb edit would: unchanged
    sections are skipped, a section added under a new `# name` is created,
    a section removed is simply not saved. A function is saved before the
    sections that use it (bb.resolve), and saving stops at the first
    section that cannot be saved (a stale draft, a failing check), saying
    what was saved and what was not. bb then rewrites FILE: saved sections
    in stored form, the others as they were. One language per scratchpad.

    ```
    bb scratchpad login_page@eng register_page@eng pages.md   # writes pages.md
    bb scratchpad login_page@eng register_page@eng pages.md -m "sign-up pages"   # saves it
    bb scratchpad --tree checkout_page@eng checkout.md   # a page and all it uses
    ```
    """
    names = list(identifiers)
    if tree:
        names = scratchpad_tree_names(tree) + [n for n in names if n not in scratchpad_tree_names(tree)]
    langs = {n.rsplit('@', 1)[1] for n in names if '@' in n}
    if any('@' not in n for n in names) or len(langs) > 1:
        print("Error: name every function with one language, e.g. bb scratchpad a@eng b@eng FILE "
              "(one language per scratchpad)", file=sys.stderr)
        sys.exit(1)
    if not names and not os.path.exists(file):
        print("Error: name the functions to write into the scratchpad", file=sys.stderr)
        sys.exit(1)
    lang = langs.pop() if langs else None
    work = Path(tempfile.mkdtemp(prefix='bb-scratchpad-'))

    try:
        if not os.path.exists(file):
            # A check of a function in the file is written under that function's ## Checks only
            attached = set()
            for identifier in names:
                heads, _, _ = helper_resolve_heads(*identifier.split('@'))
                attached |= {check for head in heads for check in object_load_checks(head)}
            skipped = []
            for identifier in list(names):
                heads, _, _ = helper_resolve_heads(*identifier.split('@'))
                if heads and all(h in attached for h in heads):
                    skipped.append(identifier.split('@')[0])
            names = [n for n in names if n.split('@')[0] not in skipped]
            if skipped:
                print(f"Note: {', '.join(skipped)} are checks of functions in the file: they are written "
                      f"under those functions' ## Checks, not as sections of their own.")
            parts = []
            for identifier in names:
                draft = work / f"{identifier.split('@')[0]}.md"
                with contextlib.redirect_stdout(io.StringIO()):
                    command_edit(identifier, file=str(draft))
                parts.append(draft.read_text(encoding='utf-8').rstrip('\n') + '\n')
            with open(file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(parts))
            print(f"Wrote {file} with {len(parts)} function(s): {', '.join(n.split('@')[0] for n in names)}. "
                  f"Change it, then run the same command again to save it.")
            return

        try:
            sections = scratchpad_split(Path(file).read_text(encoding='utf-8'))
        except ValueError as e:
            print(f"Error: {file}: {e}", file=sys.stderr)
            sys.exit(1)
        if lang is None:
            # An existing scratchpad names its functions itself: its language is yours
            lang = (storage_read_config()['user'].get('languages') or [None])[0]
        if lang is None:
            print("Error: give the scratchpad's language with one NAME@lang, or set yours with "
                  "bb whoami language LANG", file=sys.stderr)
            sys.exit(1)
        titles = [name for name, _ in sections]
        if len(set(titles)) != len(titles):
            print(f"Error: {file} has the same function twice", file=sys.stderr)
            sys.exit(1)
        # A check is saved with its function (a ### subsection of its ## Checks): a section of
        # its own as well would be a second copy, stale once the function's section saves it
        as_check = {}
        for owner, text in sections:
            try:
                for check in markdown_parse_edit(text)['checks']:
                    as_check.setdefault(check['title'], (owner, check))
            except EditFormatError:
                continue  # the save below reports it
        duplicates = []
        for name, text in sections:
            if name not in as_check or as_check[name][0] == name:
                continue
            owner, check = as_check[name]
            try:
                own = markdown_parse_edit(text)['implementation']
            except EditFormatError:
                own = None
            if own is None or (own['source'].strip(), own.get('markdown', '').strip()) != \
                    (check['source'].strip(), check.get('markdown', '').strip()):
                print(f"Error: {name} is in {file} twice: as a section of its own and as a check of {owner} "
                      f"(### {name}), and the two differ. A check is saved with its function: change it "
                      f"under {owner}'s ## Checks and delete the # {name} section.", file=sys.stderr)
                sys.exit(1)
            duplicates.append(name)
        if duplicates:
            print(f"Note: {', '.join(duplicates)} also stand as checks of the functions above them; "
                  f"they are saved with those functions, and their own sections are dropped from {file}.")
            sections = [(n, t) for n, t in sections if n not in duplicates]
            titles = [n for n in titles if n not in duplicates]

        # A function is saved before the sections that use it
        uses = {name: {ref for ref in SCRATCHPAD_REF.findall(text) if ref in titles and ref != name}
                for name, text in sections}
        order, placed = [], set()
        while len(order) < len(titles):
            ready = [n for n in titles if n not in placed and uses[n] <= placed]
            if not ready:
                stuck = [n for n in titles if n not in placed]
                print(f"Error: these sections use each other, so none can be saved first: {', '.join(stuck)}",
                      file=sys.stderr)
                sys.exit(1)
            order.append(ready[0])
            placed.add(ready[0])

        if comment is None:
            try:
                comment = input("Comment (press Enter to skip): ").strip()
            except EOFError:
                comment = ""
        text_of = dict(sections)
        saved, unchanged, failed = {}, [], None
        for name in order:
            draft = work / f"{name}.md"
            draft.write_text(text_of[name], encoding='utf-8')
            print(f"── {name}")
            output, errors = io.StringIO(), io.StringIO()
            where = f"{file} (section {name})"
            try:
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                    command_edit(f"{name}@{lang}", comment=comment, unreviewed=unreviewed, file=str(draft))
            except SystemExit as stop:
                if stop.code:
                    failed = name
            finally:
                # bb's own draft file stands for a section of the scratchpad
                print(scratchpad_compact_report(output.getvalue().replace(str(draft), where)), end='')
                sys.stdout.flush()  # the section's report comes before the reason it stopped
                # The summary below says what was kept: drop bb edit's own "Not saved" line
                errors = io.StringIO(''.join(line for line in errors.getvalue().splitlines(True)
                                             if not line.startswith('Not saved: fix ')))
                print(errors.getvalue().replace(str(draft), where).replace(
                    f"bb edit {name}@{lang} NEW_FILE", f"bb scratchpad {' '.join(n for n in names) or name + '@' + lang} NEW_FILE"),
                    end='', file=sys.stderr)
            if failed:
                break
            if re.search(r'^(No changes\.|Nothing saved)', output.getvalue(), re.M):
                unchanged.append(name)
            saved[name] = draft.read_text(encoding='utf-8').rstrip('\n') + '\n'

        with open(file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(saved.get(name, text_of[name]) for name in titles))
        untouched = [n for n in order if n not in saved and n != failed]
        changed = [n for n in saved if n not in unchanged]
        summary = (f"saved {', '.join(changed) or 'nothing'}"
                   + (f"; unchanged {', '.join(unchanged)}" if unchanged else ""))
        if failed:
            rest = f"; not tried: {', '.join(untouched)}" if untouched else ""
            kept = f"{failed} and those" if untouched else failed
            print(f"Stopped at {failed}: {summary}{rest}. {file} keeps your text for {kept}.", file=sys.stderr)
            sys.exit(1)
        print(f"Scratchpad {file}: {summary}")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if refactor:
        command_update(refactor, comment, unreviewed)


def refactor_save(old_hash: str, new_hash: str, new_code: str, checks: List[str], comment: str,
                  hash_map: Dict[str, str], keep: Callable[[Dict[str, Any]], bool] = None) -> str:
    """
    Save new_hash as the version after old_hash; returns 'new', 'restored' or 'existing'.

    Every name of old_hash (each current step, in every language) moves
    on to new_hash with a 'refactor' step, its mapping copied with
    references renamed through hash_map (see lineage_continue). Code that
    is already in the pool keeps its hash: when the name's lineage went
    through it, it is restored (a 'restore' step); when another name has
    it, the two names now share it ('existing').
    """
    if old_hash in review_load_authored():
        # A mechanical rewrite of your own function: only its references change
        review_approve_own([new_hash])
    existed = code_detect_schema(new_hash) is not None
    if not existed:
        object_save(new_hash, new_code, code_create_metadata(), checks=checks,
                    normalization=object_normalization(old_hash))
    if lineage_continue(old_hash, new_hash, comment, hash_map, keep=keep) == 'restored':
        return 'restored'
    return 'existing' if existed else 'new'


def code_hash_rewritten(new_code: str, checks: List[str]) -> str:
    """The hash of rewritten normalized code: computed without its docstring, with its checks."""
    tree = ast.parse(new_code)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            _, func_without_docstring = code_extract_docstring(node)
            imports = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
            module_without_docstring = ast.Module(body=imports + [func_without_docstring], type_ignores=[])
            ast.fix_missing_locations(module_without_docstring)
            return code_compute_function_hash(ast.unparse(module_without_docstring), checks)
    raise ValueError("no function in code")


def update_check_rewritten(base: str, new_hash: str, new_code: str, checks: List[str],
                           unreviewed: bool = False) -> List[str]:
    """
    Run the checks of a rewrite before it is saved; the failures, one line each.

    Like bb edit, nothing runs that you have not approved: the rewrite
    imports pool functions and so do its checks. Those not approved are
    reported as failures (bb review them), unless unreviewed.
    """
    needed = set()
    for dep in code_extract_dependencies(new_code):
        needed.update(code_resolve_dependencies(dep))
    for check_hash in checks:
        needed.update(code_resolve_dependencies(check_hash))
    missing = sorted(needed - review_approvals(list(needed)) - review_load_authored())
    if missing and not unreviewed:
        return ["not run, review first: " + ', '.join(f"bb review {h}" for h in missing)]
    results = check_run_all(new_hash, checks, source=new_code, like=base)
    return [f"{status} {name}: {detail}" if detail else f"{status} {name}"
            for status, name, detail in results if status != 'PASS']


def command_update(identifier: str, comment: str = None, unreviewed: bool = False):
    """
    Bring a function and everything it uses up to date, in one step.

    Every function reached from NAME that uses a version which is no
    longer current is rewritten to use the current one, deepest first,
    then NAME itself: what `bb search --outdated` lists, paid at once,
    as if each were edited and saved unchanged. Current means current in
    NAME's language, for the name each function is used by (another
    language's newer version waits for a translation). Pinned references
    (written as bb.ref) are kept. A fork (several current versions) is
    left as it is, with a warning. One comment goes on every new version.

    Like bb edit, a rewrite is saved only when the checks of the function
    pass with the new versions it uses. One that fails is not saved: it
    stays on the version it has, its callers stay as they were, and the
    rest is updated anyway. What failed is printed, and bb update exits
    with status 1; bb edit the function to adapt it, then update again.

    ```
    bb update app@eng -m "use the new layout everywhere"
    ```
    """
    root, lang = helper_parse_identifier(identifier)
    lang = lang or lineage_preferred_language(root) or helper_pick_language(root)
    if comment is None:
        try:
            comment = input("Comment (press Enter to skip): ").strip()
        except EOFError:
            comment = ""
    done: Dict[Tuple[str, Optional[str]], str] = {}
    rewritten: List[Tuple[str, str]] = []
    failed: List[Tuple[str, str, List[str]]] = []

    def update(func_hash: str, name: Optional[str], stack: Tuple[str, ...] = ()) -> str:
        """The up-to-date hash for func_hash, used by name (itself when nothing changes)."""
        if (func_hash, name) in done:
            return done[(func_hash, name)]
        if func_hash in stack or code_detect_schema(func_hash) is None:
            return func_hash
        base = lineage_newer_version(func_hash, lang, name) or func_hash
        code = object_load(base)['normalized_code']
        pins: Set[str] = set()
        for mapping_lang in storage_list_languages(base):
            for mapping_hash in mapping_leaves(base, mapping_lang):
                pins.update(mapping_load_json(base, mapping_lang, mapping_hash).get('pinned') or [])
        shown = lang if lang in storage_list_languages(base) else helper_pick_language(base, lang)
        aliases = mapping_load_json(base, shown, mapping_select(base, shown, name)).get('alias_mapping', {})
        swaps = {}
        for dep in code_extract_dependencies(code):
            if dep in pins:
                continue
            new_dep = update(dep, lineage_name_for(dep, lang, aliases.get(dep)), stack + (func_hash,))
            if new_dep != dep:
                swaps[dep] = new_dep
        result = base
        if swaps:
            new_code = ast.unparse(code_swap_refs(ast.parse(code), swaps))
            checks = object_load_checks(base)
            result = code_hash_rewritten(new_code, checks)
            followed = {dep: lineage_name_for(dep, lang, aliases.get(dep)) for dep in swaps}
            problems = update_check_rewritten(base, result, new_code, checks, unreviewed) if checks else []
            if problems and result != base:
                failed.append((base, result, problems))
                done[(func_hash, name)] = done[(base, name)] = base
                return base

            def keep(record):
                """Names of base that use the swapped functions by other names stay (two names, one code)."""
                if record['lang'] != lang:
                    return True
                theirs = mapping_load_json(base, lang, record['mapping']).get('alias_mapping', {})
                return all(lineage_name_for(dep, lang, theirs.get(dep)) == followed[dep] for dep in swaps)

            if result != base:
                with contextlib.redirect_stdout(io.StringIO()):  # its own Hash: lines would mislead
                    refactor_save(base, result, new_code, checks, comment, swaps, keep=keep)
                rewritten.append((base, result))
        done[(func_hash, name)] = done[(base, name)] = result
        return result

    new_root = update(root, helper_identifier_name(identifier) or lineage_name_for(root, lang))
    if not rewritten and not failed:
        print(f"{helper_function_label(root)} is up to date: nothing to save")
        return
    if rewritten:
        print(f"Updated {len(rewritten)} function(s), deepest first:")
        for old, new in rewritten:
            print(f"  {helper_function_label(new)}: {old[:12]}... -> {new[:12]}...")
        print("Drafts open on these functions are now stale: write them again.")
    else:
        print("Nothing updated.")
    print(f"Hash: {new_root}")
    if failed:
        sys.stdout.flush()  # what was updated comes before what was not
        print(f"Not updated, a check failed ({len(failed)}):", file=sys.stderr)
        for old, new, problems in failed:
            print(f"  {helper_function_label(old)}: {old[:12]}... stays; its update would be {new[:12]}...",
                  file=sys.stderr)
            for problem in problems:
                print(f"    {problem}", file=sys.stderr)
        print("Edit them to adapt (bb edit NAME@lang), then update again.", file=sys.stderr)
        sys.exit(1)


def command_refactor(what_hash: str, from_hash: str, to_hash: str, at_hash: str = None, comment: str = None):
    """
    Replace a dependency hash with another in a function.

    Supports two modes:
    1. Global refactor: Replaces FROM with TO everywhere in WHAT's dependency graph
    2. Surgical refactor: Replaces FROM with TO only in the path from AT to WHAT

    Creates new functions where references to from_hash are replaced
    with to_hash, each one with a lineage step after the function it was
    rewritten from, for each of its names. A rewrite that gives back
    an older version's code (to_hash an older version of from_hash, say)
    restores that version instead, with a restore step. Copies the current
    language mappings, updating
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
    rewritten: List[Tuple[str, str]] = []

    # Process functions in topological order (dependencies first)
    for dep_hash in all_deps:
        if dep_hash == what_hash:
            break  # Stop before processing what_hash itself

        if dep_hash in hash_map:
            continue  # what is being replaced (FROM), or already rewritten
        # Build on the function's current version when it was edited since the root saw it
        base = lineage_newer_version(dep_hash) or dep_hash
        dep_data = object_load(base)
        dep_code = dep_data['normalized_code']

        # Check if this function depends on any hash in our replacement map
        direct_deps = code_extract_dependencies(dep_code)
        needs_refactor = any(d in hash_map for d in direct_deps)

        if base != dep_hash:
            print(f"  {helper_function_label(dep_hash)}: using its current version {base[:12]}... "
                  f"(the root used {dep_hash[:12]}...)")
            if not needs_refactor:
                hash_map[dep_hash] = base
                continue
        dep_hash_used, dep_hash = dep_hash, base

        if needs_refactor:
            # Refactor this function with all accumulated replacements
            dep_tree = code_swap_refs(ast.parse(dep_code), hash_map)

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

            refactor_save(dep_hash, new_dep_hash, new_dep_code, dep_checks, comment, hash_map)
            rewritten.append((dep_hash, new_dep_hash))

            # Track this replacement for subsequent functions
            hash_map[dep_hash] = new_dep_hash
            hash_map[dep_hash_used] = new_dep_hash

    # Now refactor what_hash itself with all accumulated replacements
    tree = code_swap_refs(ast.parse(normalized_code), hash_map)

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

    saved = refactor_save(what_hash, new_hash, new_normalized_code, what_checks, comment, hash_map)
    if rewritten:
        print(f"Rewrote {len(rewritten)} function(s) on the way, deepest first:")
        for old, new in rewritten:
            print(f"  {helper_function_label(new)}: {old[:12]}... -> {new[:12]}...")
        print("Drafts open on these functions are now stale: write them again.")
    if saved == 'restored':
        print(f'restored: {new_hash}')
    else:
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

        def visit_ClassDef(self, node):
            # A class defined inside the function is renamed like its uses
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


def code_inline_refs(normalized_code: str, names: Dict[str, str]) -> str:
    """
    Call bundled functions by name: bb.ref("<hash>") and object_<hash>._bb_v_0
    become names[hash], and bb.pool imports go (for bb compile).
    """
    class Inliner(ast.NodeTransformer):
        def visit_Call(self, node):
            func_hash = code_ref_hash(node)
            if func_hash in names:
                return ast.Name(id=names[func_hash], ctx=ast.Load())
            self.generic_visit(node)
            return node

        def visit_Attribute(self, node):
            func_hash = code_pool_import_hash(node.value.id) if isinstance(node.value, ast.Name) else None
            if node.attr == '_bb_v_0' and func_hash in names:
                return ast.Name(id=names[func_hash], ctx=ast.Load())
            self.generic_visit(node)
            return node

    tree = Inliner().visit(ast.parse(normalized_code))
    tree.body = [node for node in tree.body if not (isinstance(node, ast.ImportFrom) and node.module == 'bb.pool')]
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


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
        # Plain names when they are unique in the file: editing a helper then
        # changes its own lines only. A hash suffix tells apart two functions
        # called alike, and keeps a name off builtins and imported modules.
        original = {}
        imported = {'sys'}  # the entry point imports sys
        for dep_hash in deps:
            if mappings_list(dep_hash, lang):
                _, name_mapping, _, _ = mapping_load(dep_hash, lang, mapping_select(dep_hash, lang))
                original[dep_hash] = name_mapping.get('_bb_v_0', '_bb_v_0')
            for node in ast.walk(ast.parse(object_load(dep_hash)['normalized_code'])):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    imported.update((a.asname or a.name).split('.')[0] for a in node.names)
        counts = {}
        for name in original.values():
            counts[name] = counts.get(name, 0) + 1
        reserved = PYTHON_BUILTINS | set(keyword.kwlist) | imported
        for dep_hash in deps:
            name = original.get(dep_hash)
            if name is None:
                hash_to_func_name[dep_hash] = f'_bb_{dep_hash[:8]}'
            elif counts[name] == 1 and name not in reserved:
                hash_to_func_name[dep_hash] = name
            else:
                hash_to_func_name[dep_hash] = f'{name}_{dep_hash[:8]}'
    else:
        for dep_hash in deps:
            hash_to_func_name[dep_hash] = f'_bb_{dep_hash[:8]}'

    for dep_hash in deps:
        func_data = object_load(dep_hash)
        normalized_code = func_data['normalized_code']

        if debug_mode:
            # Debug mode: denormalize to human-readable names
            mapping_hash = mapping_select(dep_hash, lang)
            docstring, name_mapping, alias_mapping, _ = mapping_load(dep_hash, lang, mapping_hash)

            # Denormalize the code; pool references already name bundled functions
            normalized_code_with_doc = code_replace_docstring(normalized_code, docstring)
            code = code_denormalize(code_inline_refs(normalized_code_with_doc, hash_to_func_name),
                                    name_mapping, {})

            # The name chosen above: plain when unique, with a hash suffix otherwise
            original_func_name = name_mapping.get('_bb_v_0', '_bb_v_0')
            func_name = hash_to_func_name[dep_hash]

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

        else:
            # Normal mode: use normalized code with unique function names
            code = code_inline_refs(normalized_code, hash_to_func_name)
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

    # Arguments are passed as strings and the return value is not printed,
    # as with bb run: the program does its own parsing and output
    lines.append('')
    lines.append('if __name__ == "__main__":')
    lines.append('    import sys')
    lines.append(f'    {main_func["func_name"]}(*sys.argv[1:])')
    lines.append('')

    return '\n'.join(lines)


def command_compile(identifier: str, debug_mode: bool = False, output_path: Optional[str] = None):
    """
    Compile a function into a standalone Python file.

    Accepts a hash or function name (with optional @lang suffix).
    Compiling only writes a file, so it needs no review: running that file
    is the reader's decision, as with any script (bb run, which runs code,
    still requires bb review). When a name is provided with @lang, debug
    mode is implicit, and functions keep their names (a hash suffix only
    where two are called alike, or a name would shadow a builtin or an
    import). The generated file can be run directly with Python or compiled separately
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
        'scratchpad': command_scratchpad,
        'update': command_update,
        'compile': command_compile,
        'migrate': command_migrate,
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
    translate_parser.add_argument('file', nargs='?', default=None,
                                  help='Translation file instead of prompts: written when missing, saved from when present')
    translate_parser.add_argument('-r', '--recursive', action='store_true', help='Recursively translate dependencies')
    translate_parser.add_argument('--tree', action='store_true',
                                  help='Translate IDENTIFIER and everything it uses in one FILE (one name per function)')
    translate_parser.add_argument('--comment', '-m', default=None, help='Comment for the translation (with FILE)')

    # Rm command
    rm_parser = subparsers.add_parser('rm', help='Remove function or language mapping')
    rm_parser.add_argument('identifier', help='Hash or name, with optional @lang to remove only that language')
    rm_parser.add_argument('--force', action='store_true',
                           help='Remove a function even though other functions still use it')

    # Run command
    run_parser = subparsers.add_parser('run', help='Execute a function from the pool')
    run_parser.add_argument('identifier', help='Hash or name, with optional @lang (e.g., compute_pi@eng)')
    run_parser.add_argument('func_args', nargs='*', help='Arguments to pass to function')

    # Edit command
    scratchpad_parser = subparsers.add_parser('scratchpad', help='Edit several functions in one file, then save them')
    scratchpad_parser.add_argument('names', nargs='*', metavar='NAME@lang', help='Functions to put in the file')
    scratchpad_parser.add_argument('file', metavar='FILE', help='The scratchpad: written when missing, saved when present')
    scratchpad_parser.add_argument('--tree', metavar='NAME@lang', default=None,
                                   help='Add NAME and everything it uses, as bb tree shows them')
    scratchpad_parser.add_argument('--comment', '-m', default=None, help='Comment for every version saved')
    scratchpad_parser.add_argument('--refactor', metavar='ROOT', default=None,
                                   help='After saving, bring ROOT (e.g. app@eng) and everything it uses up to date')
    scratchpad_parser.add_argument('--dangerously-unreviewed', dest='unreviewed', action='store_true',
                                   help='Run checks even on unreviewed functions they import')

    edit_parser = subparsers.add_parser('edit', help='Edit a function and its checks, then save a fork')
    edit_parser.add_argument('identifier', help='Hash or name, with optional @lang (e.g., compute_pi@eng)')
    edit_parser.add_argument('file', nargs='?', default=None,
                             help='Edit file instead of $EDITOR: written when missing, saved from when present')
    edit_parser.add_argument('--comment', '-m', default=None, help='Comment for the new version (asked when omitted)')
    edit_parser.add_argument('--refactor', metavar='ROOT', default=None,
                             help='Then update ROOT (a function using it, e.g. app@eng) to the new version')
    edit_parser.add_argument('--at', metavar='AT', default=None,
                             help='With --refactor: only along the path from AT to ROOT')
    edit_parser.add_argument('--dangerously-unreviewed', dest='unreviewed', action='store_true',
                             help='Run imported functions nobody reviewed (for experiments)')


    # Review command
    review_parser = subparsers.add_parser('review', help='Read, run the checks of, and approve or reject functions')
    review_parser.add_argument('identifier', help='Hash or name, with optional @lang')
    review_parser.add_argument('--comment', '-m', default="", help='Comment stored in your signed reviews')

    # Reviews command
    reviews_parser = subparsers.add_parser('reviews', help='Show who reviewed a function, with signature status')
    reviews_parser.add_argument('identifier', help='Hash or name, with optional @lang')
    reviews_parser.add_argument('--json', dest='as_json', action='store_true', help='Machine-readable output')

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
    log_parser.add_argument('identifier', nargs='?', help="Only this name's history (NAME or NAME@lang), or a hash's")

    # Search command
    search_parser = subparsers.add_parser('search', help='Search functions by words, status and architecture')
    search_parser.add_argument('query', nargs='*', help='Words that must all appear (any language)')
    search_parser.add_argument('--all', dest='all_versions', action='store_true',
                               help='Include superseded versions (forked by another function)')
    search_parser.add_argument('--untested', action='store_true', help='Only functions without checks')
    search_parser.add_argument('--unreviewed', action='store_true', help='Only functions you have not approved')
    search_parser.add_argument('--diverged', action='store_true', help='Only forks: one lineage with several current versions')
    search_parser.add_argument('--unused', action='store_true',
                               help='Only functions nothing imports and nothing uses as a check')
    search_parser.add_argument('--uses', metavar='X', default=None, help='Only functions that import X directly')
    search_parser.add_argument('--outdated', action='store_true',
                               help='Only functions using versions that are no longer current (pins excluded)')
    search_parser.add_argument('--similar', action='store_true',
                               help='Only functions whose code looks like another current function, '
                                    'or one code saved under several names')
    search_parser.add_argument('--json', dest='as_json', action='store_true', help='Machine-readable output')

    # Latest command
    latest_parser = subparsers.add_parser('latest', help='Find most recent version of function by name')
    latest_parser.add_argument('name', help='Function name to find (or name@lang)')
    latest_parser.add_argument('lang', nargs='?', help='Optional language code to filter (e.g., eng, fra)')
    latest_parser.add_argument('--json', dest='as_json', action='store_true', help='Machine-readable output')
    latest_parser.add_argument('--hash', dest='hash_only', action='store_true',
                               help='Print only the hashes of current versions, one per line')

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
    caller_parser.add_argument('--json', dest='as_json', action='store_true', help='Machine-readable output')
    caller_parser.add_argument('--all', dest='every', action='store_true',
                               help='Also list callers replaced by a later version, marked "not current"')

    # Tree command
    tree_parser = subparsers.add_parser('tree', help='Display dependency tree of a function')
    tree_parser.add_argument('identifier', help='Hash or name, with optional @lang')
    tree_parser.add_argument('--json', dest='as_json', action='store_true', help='Machine-readable output')

    # Refactor command
    update_parser = subparsers.add_parser('update', help='Bring a function and everything it uses up to date')
    update_parser.add_argument('identifier', help='Function to update (name@lang or hash)')
    update_parser.add_argument('--comment', '-m', default=None, help='Comment for the new versions (asked when omitted)')
    update_parser.add_argument('--dangerously-unreviewed', action='store_true',
                               help='Run checks even on unreviewed functions they import')

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

    # Migrate command
    subparsers.add_parser('migrate', help='Convert a pool from the old layout (track it with git first)')

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()

    # Commands that read or write the pool need its current layout
    if args.command not in (None, 'init', 'whoami', 'skills', 'migrate', 'trust') and storage_pool_is_old_layout():
        print(f"Error: {OLD_LAYOUT_MESSAGE}", file=sys.stderr)
        sys.exit(1)

    if args.command == 'init':
        command_init()
    elif args.command == 'whoami':
        command_whoami(args.subcommand, args.value)
    elif args.command == 'add':
        code_add(args.file, args.comment)
    elif args.command == 'show':
        code_show(args.identifier)
    elif args.command == 'translate':
        if args.tree:
            if args.file is None or args.recursive:
                print("Error: --tree needs a FILE, and does not go with --recursive", file=sys.stderr)
                sys.exit(1)
            command_translate_tree(args.identifier, args.target_lang, args.file, comment=args.comment)
        else:
            command_translate(args.identifier, args.target_lang, recursive=args.recursive,
                              file=args.file, comment=args.comment)
    elif args.command == 'rm':
        command_rm(args.identifier, force=args.force)
    elif args.command == 'run':
        command_run(args.identifier, func_args=args.func_args)
    elif args.command == 'scratchpad':
        command_scratchpad(args.names, args.file, comment=args.comment, tree=args.tree, unreviewed=args.unreviewed,
                           refactor=args.refactor)
    elif args.command == 'edit':
        command_edit(args.identifier, comment=args.comment, unreviewed=args.unreviewed,
                     refactor=args.refactor, at=args.at, file=args.file)
    elif args.command == 'review':
        command_review(args.identifier, comment=args.comment)
    elif args.command == 'reviews':
        command_reviews(args.identifier, as_json=args.as_json)
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
        command_log(args.identifier)
    elif args.command == 'search':
        command_search(args.query, all_versions=args.all_versions, as_json=args.as_json,
                       untested=args.untested, unreviewed=args.unreviewed, diverged=args.diverged,
                       unused=args.unused, uses=args.uses, outdated=args.outdated,
                       similar=args.similar)
    elif args.command == 'latest':
        name, lang = args.name, args.lang
        if '@' in name and not lang:
            name, lang = name.rsplit('@', 1)  # name@lang, as every other command takes it
        command_latest(name, lang, as_json=args.as_json, hash_only=args.hash_only)
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
        command_caller(args.identifier, as_json=args.as_json, every=args.every)
    elif args.command == 'tree':
        command_tree(args.identifier, as_json=args.as_json)
    elif args.command == 'update':
        command_update(args.identifier, comment=args.comment, unreviewed=args.dangerously_unreviewed)
    elif args.command == 'refactor':
        command_refactor(args.what, args.from_hash, args.to_hash, args.at_hash if hasattr(args, 'at_hash') else None,
                         comment=args.comment)
    elif args.command == 'compile':
        command_compile(args.identifier, debug_mode=args.debug, output_path=args.output)
    elif args.command == 'skills':
        command_skills()
    elif args.command == 'migrate':
        command_migrate()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
