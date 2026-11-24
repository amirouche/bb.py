# CLAUDE.md - AI Assistant Guide for Beyond Babel

For practical development workflows (running tests, adding commands, writing transcripts), see **CONTRIBUTING.md**.

## Project Overview

**Beyond Babel** is a function pool manager for Python that enables multilingual function sharing through AST normalization and content-addressed storage. It allows the same logical function written in different human languages (with different variable names, docstrings, etc.) to share the same hash and be stored together.

### Core Concept

Functions with identical logic but different naming (e.g., English vs French variable names) are normalized to a canonical form, hashed, and stored in a content-addressed pool. This enables:
- **Multilingual code sharing**: Same function logic across different human languages
- **Deterministic hashing**: Identical logic produces identical hashes regardless of naming
- **Compositional functions**: Functions can reference other functions from the pool

## Architecture

### Key Design Principles

1. **AST-based normalization**: Source code is parsed into an AST, normalized, then unparsed
2. **Hash on logic, not names**: Docstrings excluded from hash computation to enable multilingual support
3. **Bidirectional mapping**: Original names preserved for reconstruction in target language
4. **Content-addressed storage**: Functions stored by hash in `$HOME/.local/bb/pool/functions/xx/yy.../object.json`, mappings in `pool/mappings/xx/yy.../<lang>/<mapping hash>.json`, lineage steps in `pool/lineage/xx/<rest>.json` (configurable via `BB_DIRECTORY` environment variable)
5. **Single-file architecture**: All code resides in `bb.py` - no modularization into separate packages. This keeps the tool simple, self-contained, and easy to distribute as a single script.
6. **Pool references by hash**: stored code refers to a pool function as `bb.ref("<hash>")` where it is used (normalization 4). Authors write `import bb`, then `func = bb.resolve("func")` (by name, resolved to a hash when saving, never at run time, through the saving language, then `bb whoami` languages, then any; `bb.latest` is the same) or `func = bb.ref("<hash>")` (an exact version). A name is a search and several functions may answer to one: a new reference is then settled once, by an answer at the terminal or `--pick NAME=HASH` (`bb add`, `bb edit`, `bb scratchpad`), and the caller's lineage step remembers it (`follows`); with no terminal (input or output redirected: the question is asked only where it would be seen) or with `--no-ask` (what agents should pass) nothing is saved, exit 1, and stderr lists the `--pick` for each candidate (`bb scratchpad FILE --show` and `bb edit NAME FILE --show` list them beforehand: the draft as a save would read it, each `bb.resolve` with the function it means and the `--pick` for the others, nothing saved, exit 0); a chosen function is still shown as `bb.resolve`, never `bb.ref`: choosing is not pinning, and only pins are left out of `bb search --outdated`; there is no `bb.follow` (it existed briefly: a hash in the source said less than the step does). `from bb.pool import ...` is refused in any form; objects stored with `object_<hash>` imports keep loading

### Storage Location Configuration

The bb function pool location is controlled by the `BB_DIRECTORY` environment variable:

- **Default**: `$HOME/.local/bb/` (follows XDG Base Directory specification)
- **Custom location**: Set `BB_DIRECTORY=/path/to/pool` to override

It holds `pool/` (`functions/`, `mappings/` and `lineage/`), `worklog/` (worklogs), `edit/` (unfinished `bb edit` files), `review/` (signed reviews, written once: `review/xx/yy.../<key id>/<review id>.json`, a new verdict being another review whose `supersedes` lists yours it replaces; only a validly signed review of the same key supersedes; a review names the `lang` and `mapping` it was read as, shown by `bb reviews`, while the verdict stays on the function hash and never gates per mapping; a review also lists what its reviewer acknowledged, `ack`: `bb review` asks security, correctness and readability one by one, yes to all approves, a no rejects or skips, and a peer's approval counts only when it names all three), and `trust/` (shared list of peers). With a signing key every record you save has `FILE.sig` next to it (see Signing), and next to any JSON file of `pool/`, `worklog/` and `review/`, and next to each `.sig`, there may be `FILE.ots`, its OpenTimestamps proof (see Anchoring). `anchor/` holds the snapshots `bb anchor` takes. Private state, your config, review approvals and trusted peers, lives outside it in `$BB_STATE_DIRECTORY` (default `~/.local/state/bb/`) so nobody can approve code for you through git. The user tracks the whole directory with git; bb has no commit, push or remote commands and never runs git.

### Anchoring

`created` is what the author says; `bb anchor` has it attested. It stamps every file that has no proof yet (one request to the calendars), `--upgrade` completes pending proofs once a Bitcoin block holds them, `--status` counts them offline, `--verify` has the client check complete ones against Bitcoin (the Python client needs a Bitcoin node). bb shells out to the OpenTimestamps client (`ots`, or the command in `$BB_OTS`, e.g. `uvx --from opentimestamps-client ots`), as it does to `ssh-keygen`, and parses proofs itself (`anchor_parse`, `anchor_state`): the block height attesting a file, and a proof that is for other content (the file changed since: reported, never stamped again), are told without the network. `bb anchor` is the only command that reaches the network: saving never anchors. Proofs are sidecars so that `ots verify FILE.ots` works without bb; directory scans glob `*.json` and do not see them; `bb rm` removes them with their file (`anchor_unlink`). Tests never call the real client: they set `BB_OTS` to a stand-in writing the same format.

### Signing

With `bb whoami signing-key`, what you save is signed with `ssh-keygen`, each kind of record in its own namespace (`bb-object`, `bb-mapping`, `bb-lineage`, `bb-worklog`, `bb-worklog-message`, `bb-review`) so a signature never passes for one of another kind. Records with an author name their key (`public_key`): lineage steps (part of the step id), `object.json`, `worklog.json`, reviews; one without a valid signature by that key under the author's email is `invalid`, not `unsigned` (`signature_status`). Mappings and worklog messages are content-addressed and name no author: their signature says which key wrote the file first, the key being read from the signature (`signature_read_key`); one whose content is not what its hash names is invalid. `worklog.json` names its original message (`message`). Statuses are `unsigned`, `trusted` (your own key, or a peer of your private `allowed_signers`), `untrusted`, `invalid`; `bb log` shows `Signed:`, `bb show` prints `# bb:` comment lines above the code for the function, the mapping and the step (what is signed and anchored). Signatures gate nothing: only reviews decide what may run. A save never fails because signing did (it warns once and saves unsigned); an object recorded for another author, or a step given its metadata, is not signed. `storage_unlink` removes a record with its signature and their proofs.

### Snapshots

A proof says a file existed, not what else did. Each `bb anchor` (without an identifier) also writes and anchors a snapshot, `anchor/<id>.json`: `{previous, added, removed, count, root, metadata}`, the list of every anchored file with its SHA256 stored as what changed since `previous[0]` (`added` also holds a file whose content differs), id = hash of the record. `snapshot_listing` folds the chain and checks `count` and `root` at each step; an altered snapshot or a missing predecessor is an error and `snapshot_save` builds nothing on it. `bb anchor --status` lists what was added, changed and removed since the last snapshot, `--snapshots` lists them all. Several heads (two people anchored before merging) are all named by the next snapshot, which continues the first (sorted by id). Proofs (`.ots`) and snapshots themselves are not in the list.

### Data Flow

```
Source Code (@lang)
    ↓
Parse to AST
    ↓
Extract docstring (language-specific)
    ↓
Normalize AST (rename vars, sort imports, rewrite bb imports)
    ↓
Compute hash (on code WITHOUT docstring)
    ↓
Store in $HOME/.local/bb/pool/ (or $BB_DIRECTORY/pool/) with:
    - normalized_code (with docstring for display)
    - per-language mappings (name_mappings, alias_mappings, docstrings)
```

## Development Conventions

### Python Code Style

1. **Type hints**: Used in function signatures (`Dict[str, str]`, `Set[str]`, etc.)
2. **Docstrings**: Required for all public functions
3. **Error handling**: Explicit error messages to stderr, exit with code 1
4. **AST manipulation**: Use `ast` module, never regex on source code
5. **Encoding**: Always use `encoding='utf-8'` for file I/O

### Naming Conventions

- **Classes**: PascalCase (`ASTNormalizer`)
- **Functions**: snake_case following `type_name_verb_complement` pattern (see CONTRIBUTING.md for details and examples)
- **Constants**: UPPER_SNAKE_CASE (`PYTHON_BUILTINS`)
- **Normalized names**: `_bb_v_N` (N = 0, 1, 2, ...)

### Important Invariants

1. **Function name always `_bb_v_0`**: First entry in name mapping
2. **Built-ins never renamed**: `len`, `sum`, `print`, etc. preserved
3. **Imported names never renamed**: `math`, `Counter`, etc. preserved
4. **Imports sorted**: Lexicographically by module name; a source whose top level binds one name to two things (two imports, an import and a pool function, an import and the function) is refused (`code_check_bindings`), and so is a scratchpad whose sections give a top-level name two meanings (`scratchpad_clashes`)
5. **Hash on logic only**: Docstrings and documentation excluded from hash computation; attached checks included
6. **Language codes**: Always 3 characters (ISO 639-3: eng, fra, spa, etc.)
7. **Hash format**: 64 lowercase hex characters (SHA256)

## Testing Strategy

### Philosophy: Grey-Box Integration First

Beyond Babel follows a **grey-box integration testing** approach as the primary testing strategy. Most tests exercise the CLI commands end-to-end while having knowledge of the internal storage format for assertions.

**Testing pyramid for Beyond Babel**:
1. **Integration tests (grey-box)** - Primary focus, organized by CLI command
2. **Unit tests** - Only for complex algorithms (AST normalization, hash computation, schema validation)

### Directory Structure

```
tests/
├── conftest.py              # Shared fixtures (CLIRunner, normalize_code_for_test)
├── transcript/
│   ├── conftest.py          # Transcript fixtures (isolated_bb_dir, --transcript option)
│   ├── parser.py            # Transcript markdown parser
│   ├── runner.py            # Transcript workflow executor
│   ├── test_parser.py       # Parser unit tests
│   ├── test_runner.py       # Runner unit tests
│   └── test_transcripts.py  # Parameterized transcript tests
├── test_internals.py        # Unit tests for complex algorithms
└── test_storage.py          # Storage schema validation tests
```

### normalize_code_for_test

All `normalized_code` values in tests MUST use the `normalize_code_for_test()` helper function. This ensures the code format matches `ast.unparse()` output (with proper line breaks and indentation). This applies to:
  - Direct assignments like `normalized_code = ...`
  - JSON fixture data like `object.json` files created in tests
  - Any string comparison involving normalized code

```python
from tests.conftest import normalize_code_for_test

# Wrong - this format never exists in practice:
normalized_code = "def _bb_v_0(): return 42"

# Correct - use the helper function:
normalized_code = normalize_code_for_test("def _bb_v_0(): return 42")
# Returns: "def _bb_v_0():\n    return 42"
```

### Verification Checklist

- [ ] Imports are sorted lexicographically
- [ ] Function renamed to `_bb_v_0`
- [ ] Variables renamed sequentially
- [ ] Built-ins NOT renamed (`sum`, `len`, `print`)
- [ ] Imports NOT renamed (`math`, `Counter`)
- [ ] Docstring stored separately per language
- [ ] Hash identical for same logic in different languages

## Import Handling Rules

Understanding how imports are processed is critical to the normalization system.

### Import Categories

#### 1. Standard Library & External Package Imports
**Examples**: `import math`, `from collections import Counter`, `import numpy as np`

**Processing**:
- **Before storage**: Sorted lexicographically, **no renaming**
- **In storage**: Identical to original (e.g., `import math`)
- **From storage**: No transformation
- **Usage**: Names like `math`, `Counter`, `np` are **never renamed** to `_bb_v_X`

#### 2. BB Imports (Pool Functions)
**Examples**: `import bb`, then `helper = bb.resolve("helper")` (by name; `bb.latest` is the same) or `helper = bb.ref("<hash>")` (an exact version). `from bb.pool import ...` is no longer accepted.

**Before storage (normalization)**:
```python
import bb
helper = bb.resolve("helper")         # resolved when saving to: helper = bb.ref("<hash>")
```
↓ becomes ↓
```python
def _bb_v_0(_bb_v_1):
    return bb.ref('<hash>')(_bb_v_1)
```
- The import is removed; every use of `helper` becomes `bb.ref('<hash>')`
- Alias tracked in `alias_mapping`: `{"<hash>": "helper"}`
- `bb` is reserved: never renamed; stored code only holds `bb.ref("<64 hex>")`

**From storage (denormalization)**:
- Each `bb.ref('<hash>')` becomes its alias, bound once after the imports (`import bb`, then `helper = bb.ref('<hash>')`)
- `bb show` and `bb edit` write that binding by name, `helper = bb.resolve('helper')`, whenever `helper@lang` has one current version, so saving an edit brings the function up to date (gradual propagation: a function nobody edits keeps its versions, its debt; `bb search --outdated` lists it and what must follow it, `bb update NAME@lang` pays it at once, walking lineage steps (each name's own pins and `follows`) and moving only names in that language, saving a rewrite only when its checks pass and leaving a function that fails one on its version with exit status 1, drafts note it with `# bb: out of date`)
- The name shown is the one the followed lineage goes by now (it may have been renamed). When several functions answer to it, `# bb:` comment lines list them, the one followed first, then each other with its `--pick NAME=HASH` and where it comes from (a fork: what it left and its comment), so humans and agents can choose from `bb show` alone; saving keeps to the one the step follows, and `--pick` moves to another. A lineage with several ends is listed the same way and asked about when saving (refused without a terminal or a `--pick`)
- References the author wrote as `bb.ref("<hash>")` are recorded in the mapping's `pinned` list (not in the hash) and stay pinned in drafts; a pin belongs to a name (two names of one code may differ: reports ask each current step, `search_uses`), is not debt (`bb search --outdated` leaves it out), and is found with `bb search --pinned` (`Pinned:` lines) and the `pinned` mark of `bb caller X`

Objects stored before normalization 4 hold `from bb.pool import object_<hash>` and `object_<hash>._bb_v_0(...)`; they load, display and refactor as before.

### Why This Design?

- **Standard imports** are universal (same across all languages)
- **BB references** have language-specific aliases (`helper`, `assistant`, `ayudante`) kept in the mappings; the stored code holds only `bb.ref('<hash>')`, so the hash is identical in every language
- A name cannot be mistaken for a hash, and a hash needs no identifier prefix

## Key Algorithms

### AST Normalization Algorithm

```
1. Parse source to AST
2. Sort imports lexicographically
3. Extract function definition
4. Extract docstring from function
5. Take pool imports and `alias = bb.ref(...)` bindings out (aliases go to alias_mapping)
6. Create name mapping (excluding builtins, imports, bb aliases)
7. Replace uses of pool functions (alias → bb.ref("HASH"))
8. Apply name normalization
9. Clear AST location info
10. Unparse to normalized code
```

### Hash Computation Strategy

```
CRITICAL: Hash excludes docstrings to enable multilingual support

1. Normalize AST twice: with and without docstring
2. Compute hash on version WITHOUT docstring
3. Store version WITH docstring for display
4. Result: Same logic = same hash, regardless of language
5. Checks are part of the definition (code_compute_function_hash):
   - no checks: hash = sha256(normalized code), as it always was
   - with checks: hash = sha256 of canonical JSON {checks (sorted), normalized_code}
   Changing checks forks the function; changing documentation does not
   A check takes the implementation as its only argument and passes when it
   runs without raising: it needs at least one `assert` (AssertionError = FAIL,
   other exceptions = ERROR, the return value is ignored). One without an
   assert is refused when saved and is an ERROR when a stored one runs
6. object.json is written once (new objects carry no 'parent'); mappings
   name their function and language and are content-addressed with them
7. Current versions follow lineage steps, never time: append-only records
   {from, function, lang, mapping, name, relation, comment, metadata,
   translates?, follows?, public_key?} in pool/lineage/, id = hash of the
   record. With a signing key the step you save names it ('public_key')
   and is signed next to it (FILE.sig, namespace bb-lineage):
   lineage_signature_status says unsigned, trusted (your key or a peer of
   your private list under the author's email), untrusted or invalid (a
   named key without a valid signature, or content that is not what the
   id names); bb log shows it as Signed:. A step given its metadata is
   never signed; signatures gate nothing, reviews do.
   'follows' maps each pool function the code uses (pins aside) to the
   step of it the reference stands on: one code may sit in two lineages
   (a fork back to an older version's code), and a caller follows one,
   from that step (lineage_ends). Steps saved before it existed are
   followed by hash. Steps are per
   (language, name): a name's current version is the end of its own
   lineage. Relations: add, edit, document, rename (supersedes the old
   name), refactor, restore (an older version's code again), translate,
   fork (`bb edit --fork`: another function on purpose; the step it left
   stays current, the fork starts its own lineage, its comment says why;
   a check it changes is a fork too, a renamed one included, so the
   origin keeps its checks and their names),
   follow (`--pick` moved a reference to another lineage holding the code
   it already used: same function, same mapping, another `follows`).
   One name may hold several functions: `bb search NAME` says how they
   relate (fork, same lineage, another origin, code alike, shared checks).
   `bb search NAME --compare` also runs each against the other's checks
   (approved code only): interchangeable, covers, covered by, incompatible.
   Two names may share one code; an edit in one language leaves the others
   on their version, waiting for a translation ("not yet translated").
   One name on one code is one function: an edit (fork or not) landing a
   name on code another current function of that name holds is refused.
   One caller using two names of one code is refused. Drafts' bb:parent
   marker names the step they continue. There is no `bb migrate`: no
   backward compatibility guarantees and no migrations until v1
```

### Public-Facing Hash Specification

Public hashes in Beyond Babel refer to content-addressed identifiers that follow strict deterministic serialization rules to ensure global consistency.

#### Hash Computation Rules

1. **Canonical serialization**: Public hashes are computed from JSON-serialized objects with:
   - **Sorted keys**: `json.dumps(obj, sort_keys=True, ...)` ensures key order is deterministic
   - **Unicode preservation**: `ensure_ascii=False` maintains Unicode characters without escape sequences
   - **No indentation**: Compact format without whitespace (no `indent` parameter)
   - **Consistent encoding**: UTF-8 encoding for all serialized data

2. **Hash vs. filename distinction**:
   - The hash in a filename (e.g., `pool/ab/cdef123.../object.json` or `eng/xy/z789.../mapping.json`) identifies the **logical content**
   - It is NOT a hash of the physical file's bytes on disk
   - The stored JSON may include metadata, formatting, or additional fields not included in hash computation

3. **Intermediate representation hashing**:
   - The hash may be computed from a canonical intermediate JSON representation
   - This intermediate form may differ from what is actually written to disk
   - Example: Function hash computed from normalized code without docstring, but stored JSON includes docstring

## Common Pitfalls for AI Assistants

1. **Don't modify hash computation**: Adding docstrings to hash breaks multilingual support
2. **Don't skip language suffix**: Commands require `@lang`, not optional
3. **Don't rename built-ins**: `PYTHON_BUILTINS` set must remain untouched
4. **Don't assume Python 3.8**: Code uses `ast.unparse()` (requires Python 3.9+)
5. **Don't break import sorting**: Lexicographic order is part of normalization
6. **Don't create duplicate mappings**: `_bb_v_0` is ALWAYS the function name

## Questions to Ask Before Making Changes

1. Does this change affect hash computation? (If yes, be very careful)
2. Does this break multilingual support? (Test with different languages)
3. Does this preserve built-in and imported name handling?
4. Does this maintain import sorting?
5. Does this change the stored JSON format? (Allowed before v1: no backward compatibility guarantees, no migrations; say so in the commit)
6. Are error messages helpful to users?

## Chore

### TODO.md Maintenance

- **Format**: TODO.md should remain a bullet list with topic, type names, and intended feature - no more than one sentence per item
- **Cleanup**: Regularly remove implemented entries in atomic commits (separate from feature commits)

### Regular Maintenance

- Regularly analyze type_name statistics and semantics

## Summary

Beyond Babel is a carefully designed system for multilingual function sharing through AST normalization. The key insight is separating logic (hashed) from presentation (language-specific names/docstrings). When modifying the code:

- Preserve the invariants listed above
- Test with multiple languages
- Ensure hash computation remains deterministic
- Before v1 there are no backward compatibility guarantees and no migrations (see README)

The codebase is self-contained (single file), well-structured (clear function boundaries), and follows Python best practices.
