# Beyond Babel Usage

Multilingual function pool: same logic, different languages → same hash.

## Top-Level Usage

```
usage: bb.py [-h]
                 {init,whoami,add,get,show,translate,run,review,log,search,remote,validate,caller,refactor,compile,commit}
                 ...

bb - Function pool manager

positional arguments:
  {init,whoami,add,get,show,translate,run,review,log,search,remote,validate,caller,refactor,compile,commit}
                        Commands
    init                Initialize bb directory and config
    whoami              Get or set user configuration
    add                 Add a function to the pool
    get                 Get a function from the pool
    show                Show a function with mapping selection support
    translate           Add translation for existing function
    run                 Execute function interactively
    review              Recursively review function and dependencies
    log                 Show git-like commit log of pool
    search              Search and list functions by query
    remote              Manage remote repositories
    validate            Validate v1 function structure
    caller              Find functions that depend on a given function
    refactor            Replace a dependency in a function
    compile             Compile function to standalone executable
    commit              Commit function to git repository for sharing

options:
  -h, --help            show this help message and exit
```

## Command Reference

| Command | Description |
|---------|-------------|
| `init` | Initialize bb directory and config |
| `whoami` | Get or set user configuration |
| `add` | Add a function to the pool |
| `get` | Get a function from the pool (deprecated, use `show`) |
| `show` | Show a function with mapping selection support |
| `translate` | Add translation for existing function |
| `run` | Execute function interactively |
| `review` | Recursively review function and dependencies |
| `log` | Show git-like commit log of pool |
| `search` | Search and list functions by query |
| `remote` | Manage remote repositories |
| `validate` | Validate v1 function structure |
| `caller` | Find functions that depend on a given function |
| `refactor` | Replace a dependency in a function |
| `compile` | Compile function to standalone executable |
| `commit` | Commit function to git repository for sharing |

---

## Configuration Commands

### `init` - Initialize configuration

```
usage: bb.py init [-h]

options:
  -h, --help  show this help message and exit
```

Creates the bb directory and configuration file. Automatically run when needed, but can be called explicitly.

- Creates: `$HOME/.local/bb/` (or `$BB_DIRECTORY`)
- Creates: `~/.config.bb/config.json`

**Example:**
```bash
python3 bb.py init
```

---

### `whoami` - User configuration

```
usage: bb.py whoami [-h] {username,email,public-key,language} [value ...]

positional arguments:
  {username,email,public-key,language}
                        Configuration field to get/set
  value                 New value(s) to set (omit to get current value)

options:
  -h, --help            show this help message and exit
```

Get or set user configuration. Without VALUE, displays current setting. With VALUE, sets new value.

**Examples:**
```bash
# Get username
python3 bb.py whoami username

# Set username
python3 bb.py whoami username johndoe

# Set email
python3 bb.py whoami email john@example.com

# Set public key URL
python3 bb.py whoami public-key https://example.com/keys/johndoe.pub

# Set preferred languages (space-separated)
python3 bb.py whoami language eng fra spa

# Get preferred languages
python3 bb.py whoami language
```

---

## Function Management Commands

### `add` - Store a function

```
usage: bb.py add [-h] [--comment COMMENT] file

positional arguments:
  file               Path to Python file with @lang suffix (e.g., file.py@eng)

options:
  -h, --help         show this help message and exit
  --comment COMMENT  Optional comment explaining this mapping variant
```

Normalizes and stores a Python function. Variable names and docstrings are language-specific; logic is hashed.

**Examples:**
```bash
python3 bb.py add calculate_average.py@eng
python3 bb.py add calculer_moyenne.py@fra --comment "version formelle"
```

Both produce the same hash if logic is identical.

---

### `show` - Display a function

```
usage: bb.py show [-h] hash

positional arguments:
  hash        Function hash with @lang[@mapping_hash] (e.g., abc123...@eng or
              abc123...@eng@xyz789...)

options:
  -h, --help  show this help message and exit
```

Display function with language-specific names. If multiple mappings exist for a language, shows selection menu.

**Examples:**
```bash
# Single mapping: displays function directly
python3 bb.py show abc123...@eng

# Multiple mappings: shows menu with commands
python3 bb.py show abc123...@eng
# Output:
# Multiple mappings found for 'eng'. Please choose one:
# bb.py show abc123...@eng@xyz789...  # Formal terminology
# bb.py show abc123...@eng@def456...  # Casual style

# Explicit mapping selection
python3 bb.py show abc123...@eng@xyz789...
```

---

### `get` - Retrieve a function (deprecated)

```
usage: bb.py get [-h] hash

positional arguments:
  hash        Function hash with @lang suffix (e.g., abc123...@eng)

options:
  -h, --help  show this help message and exit
```

Reconstructs function with language-specific names.

**Note**: This command is deprecated. Use `show` instead.

---

### `translate` - Add translation

```
usage: bb.py translate [-h] hash target_lang

positional arguments:
  hash         Function hash with source language (e.g., abc123...@eng)
  target_lang  Target language code (e.g., fra, spa)

options:
  -h, --help   show this help message and exit
```

Add a translation for an existing function. Prompts for translated variable names and docstring.

**Examples:**
```bash
# Translate English function to French
python3 bb.py translate abc123...@eng fra

# The command will:
# 1. Show the source function (English)
# 2. Prompt for French names for each variable
# 3. Prompt for French docstring
# 4. Optionally add a comment
# 5. Save the translation
```

---

### `validate` - Validate function structure

```
usage: bb.py validate [-h] hash

positional arguments:
  hash        Function hash to validate

options:
  -h, --help  show this help message and exit
```

Verify v1 function structure and hash integrity.

**Example:**
```bash
python3 bb.py validate abc123...
```

---

## Execution Commands

### `run` - Execute function interactively

```
usage: bb.py run [-h] [--debug] hash [func_args ...]

positional arguments:
  hash        Function hash with language (e.g., abc123...@eng)
  func_args   Arguments to pass to function (after --)

options:
  -h, --help  show this help message and exit
  --debug     Run with debugger (pdb)
```

Load and execute a function from the pool interactively. With `--debug`, runs with Python debugger (pdb) using native language variable names.

**Examples:**
```bash
# Run function interactively
python3 bb.py run abc123...@eng

# Run with debugger
python3 bb.py run abc123...@fra --debug

# Run with arguments
python3 bb.py run abc123...@eng -- arg1 arg2
```

---

### `compile` - Compile function to executable

```
usage: bb.py compile [-h] [--output OUTPUT] hash

positional arguments:
  hash                  Function hash with language (e.g., abc123...@eng)

options:
  -h, --help            show this help message and exit
  --output OUTPUT, -o OUTPUT
                        Output executable path
```

Compile function to standalone executable.

**Examples:**
```bash
# Compile with default output name
python3 bb.py compile abc123...@eng

# Compile with custom output path
python3 bb.py compile abc123...@eng --output my_function
python3 bb.py compile abc123...@fra -o ./bin/ma_fonction
```

---

## Discovery Commands

### `review` - Review function and dependencies

```
usage: bb.py review [-h] hash

positional arguments:
  hash        Function hash to review

options:
  -h, --help  show this help message and exit
```

Recursively review a function and all its dependencies. Displays functions in user's preferred languages (set with `whoami language`).

**Example:**
```bash
# Review function and dependencies
python3 bb.py review abc123...
```

---

### `log` - Show pool history

```
usage: bb.py log [-h]

options:
  -h, --help  show this help message and exit
```

Display a git-like commit log of all functions in the pool with metadata.

**Example:**
```bash
python3 bb.py log
# Output:
# Function Pool Log (3 functions)
# Hash: abc123...
# Date: 2025-11-21T10:00:00Z
# Author: johndoe
# Languages: eng, fra
# Schema: v1
```

---

### `search` - Search functions

```
usage: bb.py search [-h] query [query ...]

positional arguments:
  query       Search terms

options:
  -h, --help  show this help message and exit
```

Search for functions by name, docstring, or code content.

**Examples:**
```bash
# Search for "average"
python3 bb.py search average

# Search for multiple terms
python3 bb.py search calculate mean

# Results show:
# - Function name
# - Hash
# - Match location (name, docstring, or code)
# - Description preview
# - Command to view function
```

---

### `caller` - Find function callers

```
usage: bb.py caller [-h] hash

positional arguments:
  hash        Function hash to find callers of

options:
  -h, --help  show this help message and exit
```

Find functions that depend on a given function.

**Example:**
```bash
python3 bb.py caller abc123...
```

---

## Refactoring Commands

### `refactor` - Replace a dependency

```
usage: bb.py refactor [-h] what from to

positional arguments:
  what        Function hash to modify
  from        Dependency hash to replace
  to          New dependency hash

options:
  -h, --help  show this help message and exit
```

Replace a dependency in a function with a different function.

**Example:**
```bash
# Replace dependency 'old_hash' with 'new_hash' in function 'func_hash'
python3 bb.py refactor func_hash old_hash new_hash
```

---

### `fork` - Create a modified version with lineage

```
usage: bb.py fork [-h] hash

positional arguments:
  hash        Function hash with language (e.g., abc123...@eng)

options:
  -h, --help  show this help message and exit
```

Fork a function to create a modified version with parent lineage tracking. Opens the function in your `$EDITOR` (or `nano` fallback), allows modifications, and saves as a new function with the original recorded as parent in metadata.

**Workflow:**
1. Opens denormalized code in your editor
2. Make changes and save the file
3. If logic changed: creates new function with parent lineage in metadata
4. If only names/docstring changed: adds new mapping to existing function

**Examples:**
```bash
# Fork a function for modification
python3 bb.py fork abc123...@eng

# Set preferred editor
export EDITOR=vim
python3 bb.py fork abc123...@fra
```

**Lineage tracking:**
The forked function's metadata includes a `parent` field pointing to the original hash, enabling:
- Tracing function evolution history
- Understanding derivation relationships
- Auditing changes over time

---

## Remote Repository Commands

### `remote` - Manage remotes

```
usage: bb.py remote [-h] {add,remove,list,pull,push} ...

positional arguments:
  {add,remove,list,pull,push}
                        Remote subcommands
    add                 Add remote repository
    remove              Remove remote repository
    list                List configured remotes
    pull                Fetch functions from remote
    push                Publish functions to remote

options:
  -h, --help            show this help message and exit
```

---

### `remote add` - Add remote

```
usage: bb.py remote add [-h] name url

positional arguments:
  name        Remote name
  url         Remote URL (http://, https://, or file://)

options:
  -h, --help  show this help message and exit
```

Add a remote repository. Supports `file://`, `http://`, and `https://` URLs.

**Examples:**
```bash
# Add local file remote
python3 bb.py remote add shared file:///shared/pool

# Add HTTP remote (not yet fully implemented)
python3 bb.py remote add origin https://bb.example.com/pool
```

---

### `remote remove` - Remove remote

```
usage: bb.py remote remove [-h] name

positional arguments:
  name        Remote name to remove

options:
  -h, --help  show this help message and exit
```

Remove a configured remote.

**Example:**
```bash
python3 bb.py remote remove shared
```

---

### `remote list` - List remotes

```
usage: bb.py remote list [-h]

options:
  -h, --help  show this help message and exit
```

List all configured remotes.

**Example:**
```bash
python3 bb.py remote list
# Output:
# Configured remotes:
#   shared: file:///shared/pool
#   origin: https://bb.example.com/pool
```

---

### `commit` - Stage function for sharing

```
usage: bb.py commit [-h] [--comment COMMENT] hash

positional arguments:
  hash               Function hash to commit

options:
  -h, --help         show this help message and exit
  --comment COMMENT  Commit message (opens editor if omitted)
```

Commit a function and all its recursive dependencies to the git repository at `$BB_DIRECTORY/git/`. This stages functions for sharing via `remote push`.

**Key points:**
- Copies function, all language mappings, and all dependencies
- Creates a git commit with the provided message
- If `--comment` is omitted, opens your default editor for the message
- Must be done before `remote push`

**Examples:**
```bash
# Commit with inline message
python3 bb.py commit abc123def456... --comment "Add prime generator function"

# Commit with editor for message
python3 bb.py commit abc123def456...
```

---

### `remote pull` - Fetch from remote

```
usage: bb.py remote pull [-h] name

positional arguments:
  name        Remote name to pull from

options:
  -h, --help  show this help message and exit
```

Fetch functions from a remote repository. Validates remote is a valid bb pool before fetching. Supports `file://` URLs and git remotes.

**Example:**
```bash
python3 bb.py remote pull shared
```

---

### `remote push` - Publish to remote

```
usage: bb.py remote push [-h] name

positional arguments:
  name        Remote name to push to

options:
  -h, --help  show this help message and exit
```

Publish functions to a remote repository. Currently supports `file://` URLs. HTTP/HTTPS support planned.

**Example:**
```bash
python3 bb.py remote push shared
```

---

## Environment Variables

### `BB_DIRECTORY`

Storage location for function pool.

- Default: `$HOME/.local/bb/`
- Custom: `export BB_DIRECTORY=/path/to/pool`

**Example:**
```bash
export BB_DIRECTORY=/shared/pool
python3 bb.py add function.py@eng
```

### `BB_USER`

Not used. Author identity is automatically taken from `$USER` or `$USERNAME` environment variables.

---

## Further Reading

For storage format details, see [STORE.md](STORE.md).
