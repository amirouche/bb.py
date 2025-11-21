# Ouverture Usage

Multilingual function pool: same logic, different languages → same hash.

## Commands

```
usage: ouverture.py [-h]
                    {init,whoami,add,get,show,translate,run,review,log,search,remote,migrate,validate,caller,refactor,compile}
                    ...

ouverture - Function pool manager

positional arguments:
  {init,whoami,add,get,show,translate,run,review,log,search,remote,migrate,validate,caller,refactor,compile}
                        Commands
    init                Initialize ouverture directory and config
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
    migrate             Migrate functions from v0 to v1
    validate            Validate v1 function structure
    caller              Find functions that depend on a given function
    refactor            Replace a dependency in a function
    compile             Compile function to standalone executable

options:
  -h, --help            show this help message and exit
```

## Configuration

### `init` - Initialize configuration

```bash
python3 ouverture.py init
```

Creates the ouverture directory and configuration file. Automatically run when needed, but can be called explicitly.

- Creates: `$HOME/.local/ouverture/` (or `$OUVERTURE_DIRECTORY`)
- Creates: `~/.config/ouverture/config.json`

Example:
```bash
python3 ouverture.py init
```

### `whoami` - User configuration

```bash
python3 ouverture.py whoami {username|email|public-key|language} [VALUE...]
```

Get or set user configuration. Without VALUE, displays current setting. With VALUE, sets new value.

Examples:
```bash
# Get username
python3 ouverture.py whoami username

# Set username
python3 ouverture.py whoami username johndoe

# Set email
python3 ouverture.py whoami email john@example.com

# Set public key URL
python3 ouverture.py whoami public-key https://example.com/keys/johndoe.pub

# Set preferred languages (space-separated)
python3 ouverture.py whoami language eng fra spa

# Get preferred languages
python3 ouverture.py whoami language
```

## Function Management

### `add` - Store a function

```bash
python3 ouverture.py add FILENAME.py@LANG [--comment "description"]
```

Normalizes and stores a Python function. Variable names and docstrings are language-specific; logic is hashed.

Examples:
```bash
python3 ouverture.py add calculate_average.py@eng
python3 ouverture.py add calculer_moyenne.py@fra --comment "version formelle"
```

Both produce the same hash if logic is identical.

### `show` - Display a function

```bash
python3 ouverture.py show HASH@LANG[@MAPPING_HASH]
```

Display function with language-specific names. If multiple mappings exist for a language, shows selection menu.

Examples:
```bash
# Single mapping: displays function directly
python3 ouverture.py show abc123...@eng

# Multiple mappings: shows menu with commands
python3 ouverture.py show abc123...@eng
# Output:
# Multiple mappings found for 'eng'. Please choose one:
# ouverture.py show abc123...@eng@xyz789...  # Formal terminology
# ouverture.py show abc123...@eng@def456...  # Casual style

# Explicit mapping selection
python3 ouverture.py show abc123...@eng@xyz789...
```

### `migrate` - Migrate v0 to v1

```bash
python3 ouverture.py migrate [HASH] [--keep-v0] [--dry-run]
```

Migrate functions from v0 (single JSON file) to v1 (directory structure). Deletes v0 files after successful migration unless `--keep-v0` is specified.

Examples:
```bash
# Migrate all functions
python3 ouverture.py migrate

# Migrate specific function
python3 ouverture.py migrate abc123...

# Safe mode: keep v0 files
python3 ouverture.py migrate --keep-v0

# Preview without changes
python3 ouverture.py migrate --dry-run
```

### `validate` - Validate function structure

```bash
python3 ouverture.py validate HASH
```

Verify v1 function structure and hash integrity.

Example:
```bash
python3 ouverture.py validate abc123...
```

### `get` - Retrieve a function (deprecated)

```bash
python3 ouverture.py get HASH@LANG
```

Reconstructs function with language-specific names.

**Note**: This command is deprecated. Use `show` instead.

### `translate` - Add translation

```bash
python3 ouverture.py translate HASH@SOURCE_LANG TARGET_LANG
```

Add a translation for an existing function. Prompts for translated variable names and docstring.

Examples:
```bash
# Translate English function to French
python3 ouverture.py translate abc123...@eng fra

# The command will:
# 1. Show the source function (English)
# 2. Prompt for French names for each variable
# 3. Prompt for French docstring
# 4. Optionally add a comment
# 5. Save the translation
```

## Execution and Debugging

### `run` - Execute function interactively

```bash
python3 ouverture.py run HASH@LANG [--debug]
```

Load and execute a function from the pool interactively. With `--debug`, runs with Python debugger (pdb) using native language variable names.

Examples:
```bash
# Run function interactively
python3 ouverture.py run abc123...@eng

# Run with debugger
python3 ouverture.py run abc123...@fra --debug
```

## Discovery

### `review` - Review function and dependencies

```bash
python3 ouverture.py review HASH
```

Recursively review a function and all its dependencies. Displays functions in user's preferred languages (set with `whoami language`).

Example:
```bash
# Review function and dependencies
python3 ouverture.py review abc123...
```

### `log` - Show pool history

```bash
python3 ouverture.py log
```

Display a git-like commit log of all functions in the pool with metadata.

Example:
```bash
python3 ouverture.py log
# Output:
# Function Pool Log (3 functions)
# Hash: abc123...
# Date: 2025-11-21T10:00:00Z
# Author: johndoe
# Languages: eng, fra
# Schema: v1
```

### `search` - Search functions

```bash
python3 ouverture.py search QUERY...
```

Search for functions by name, docstring, or code content.

Examples:
```bash
# Search for "average"
python3 ouverture.py search average

# Search for multiple terms
python3 ouverture.py search calculate mean

# Results show:
# - Function name
# - Hash
# - Match location (name, docstring, or code)
# - Description preview
# - Command to view function
```

## Remote Repositories

### `remote add` - Add remote

```bash
python3 ouverture.py remote add NAME URL
```

Add a remote repository. Supports `file://`, `http://`, and `https://` URLs.

Examples:
```bash
# Add local file remote
python3 ouverture.py remote add shared file:///shared/pool

# Add HTTP remote (not yet fully implemented)
python3 ouverture.py remote add origin https://ouverture.example.com/pool
```

### `remote remove` - Remove remote

```bash
python3 ouverture.py remote remove NAME
```

Remove a configured remote.

Example:
```bash
python3 ouverture.py remote remove shared
```

### `remote list` - List remotes

```bash
python3 ouverture.py remote list
```

List all configured remotes.

Example:
```bash
python3 ouverture.py remote list
# Output:
# Configured remotes:
#   shared: file:///shared/pool
#   origin: https://ouverture.example.com/pool
```

### `remote pull` - Fetch from remote

```bash
python3 ouverture.py remote pull NAME
```

Fetch functions from a remote repository. Currently supports `file://` URLs. HTTP/HTTPS support planned.

Example:
```bash
python3 ouverture.py remote pull shared
```

### `remote push` - Publish to remote

```bash
python3 ouverture.py remote push NAME
```

Publish functions to a remote repository. Currently supports `file://` URLs. HTTP/HTTPS support planned.

Example:
```bash
python3 ouverture.py remote push shared
```

## Schema Management

## Environment Variables

### `OUVERTURE_DIRECTORY`

Storage location for function pool.

- Default: `$HOME/.local/ouverture/`
- Custom: `export OUVERTURE_DIRECTORY=/path/to/pool`

Example:
```bash
export OUVERTURE_DIRECTORY=/shared/pool
python3 ouverture.py add function.py@eng
```

### `OUVERTURE_USER`

Not used. Author identity is automatically taken from `$USER` or `$USERNAME` environment variables.

## Storage Structure (v1)

Default write format. Content-addressed mappings enable deduplication and multiple naming variants per language.

```
$OUVERTURE_DIRECTORY/objects/sha256/XX/YYYYYY.../
  object.json                           # Normalized code + metadata
  eng/sha256/XX/YYY.../mapping.json     # English name mapping
  eng/sha256/ZZ/WWW.../mapping.json     # Another English variant
  fra/sha256/XX/YYY.../mapping.json     # French name mapping
```

**object.json**: Function code, hash, metadata (author, timestamp, tags, dependencies)
**mapping.json**: Docstring, name mappings, alias mappings, comment (explains variant)

### Legacy Format (v0)

Read-only support maintained for backward compatibility.

```
$OUVERTURE_DIRECTORY/objects/XX/YYYYYY.json   # Single file
```

Use `migrate` command to convert v0 to v1.
