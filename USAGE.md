# Mobius Usage

Multilingual function pool: same logic, different languages → same hash.

## Commands

```
usage: mobius.py [-h]
                 {init,whoami,add,get,show,translate,run,review,log,search,remote,validate,caller,refactor,compile}
                 ...

mobius - Function pool manager

positional arguments:
  {init,whoami,add,get,show,translate,run,review,log,search,remote,validate,caller,refactor,compile}
                        Commands
    init                Initialize mobius directory and config
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

options:
  -h, --help            show this help message and exit
```

## Configuration

### `init` - Initialize configuration

```bash
python3 mobius.py init
```

Creates the mobius directory and configuration file. Automatically run when needed, but can be called explicitly.

- Creates: `$HOME/.local/mobius/` (or `$MOBIUS_DIRECTORY`)
- Creates: `~/.config/mobius/config.json`

Example:
```bash
python3 mobius.py init
```

### `whoami` - User configuration

```bash
python3 mobius.py whoami {username|email|public-key|language} [VALUE...]
```

Get or set user configuration. Without VALUE, displays current setting. With VALUE, sets new value.

Examples:
```bash
# Get username
python3 mobius.py whoami username

# Set username
python3 mobius.py whoami username johndoe

# Set email
python3 mobius.py whoami email john@example.com

# Set public key URL
python3 mobius.py whoami public-key https://example.com/keys/johndoe.pub

# Set preferred languages (space-separated)
python3 mobius.py whoami language eng fra spa

# Get preferred languages
python3 mobius.py whoami language
```

## Function Management

### `add` - Store a function

```bash
python3 mobius.py add FILENAME.py@LANG [--comment "description"]
```

Normalizes and stores a Python function. Variable names and docstrings are language-specific; logic is hashed.

Examples:
```bash
python3 mobius.py add calculate_average.py@eng
python3 mobius.py add calculer_moyenne.py@fra --comment "version formelle"
```

Both produce the same hash if logic is identical.

### `show` - Display a function

```bash
python3 mobius.py show HASH@LANG[@MAPPING_HASH]
```

Display function with language-specific names. If multiple mappings exist for a language, shows selection menu.

Examples:
```bash
# Single mapping: displays function directly
python3 mobius.py show abc123...@eng

# Multiple mappings: shows menu with commands
python3 mobius.py show abc123...@eng
# Output:
# Multiple mappings found for 'eng'. Please choose one:
# mobius.py show abc123...@eng@xyz789...  # Formal terminology
# mobius.py show abc123...@eng@def456...  # Casual style

# Explicit mapping selection
python3 mobius.py show abc123...@eng@xyz789...
```

### `validate` - Validate function structure

```bash
python3 mobius.py validate HASH
```

Verify v1 function structure and hash integrity.

Example:
```bash
python3 mobius.py validate abc123...
```

### `get` - Retrieve a function (deprecated)

```bash
python3 mobius.py get HASH@LANG
```

Reconstructs function with language-specific names.

**Note**: This command is deprecated. Use `show` instead.

### `translate` - Add translation

```bash
python3 mobius.py translate HASH@SOURCE_LANG TARGET_LANG
```

Add a translation for an existing function. Prompts for translated variable names and docstring.

Examples:
```bash
# Translate English function to French
python3 mobius.py translate abc123...@eng fra

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
python3 mobius.py run HASH@LANG [--debug]
```

Load and execute a function from the pool interactively. With `--debug`, runs with Python debugger (pdb) using native language variable names.

Examples:
```bash
# Run function interactively
python3 mobius.py run abc123...@eng

# Run with debugger
python3 mobius.py run abc123...@fra --debug
```

## Discovery

### `review` - Review function and dependencies

```bash
python3 mobius.py review HASH
```

Recursively review a function and all its dependencies. Displays functions in user's preferred languages (set with `whoami language`).

Example:
```bash
# Review function and dependencies
python3 mobius.py review abc123...
```

### `log` - Show pool history

```bash
python3 mobius.py log
```

Display a git-like commit log of all functions in the pool with metadata.

Example:
```bash
python3 mobius.py log
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
python3 mobius.py search QUERY...
```

Search for functions by name, docstring, or code content.

Examples:
```bash
# Search for "average"
python3 mobius.py search average

# Search for multiple terms
python3 mobius.py search calculate mean

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
python3 mobius.py remote add NAME URL
```

Add a remote repository. Supports `file://`, `http://`, and `https://` URLs.

Examples:
```bash
# Add local file remote
python3 mobius.py remote add shared file:///shared/pool

# Add HTTP remote (not yet fully implemented)
python3 mobius.py remote add origin https://mobius.example.com/pool
```

### `remote remove` - Remove remote

```bash
python3 mobius.py remote remove NAME
```

Remove a configured remote.

Example:
```bash
python3 mobius.py remote remove shared
```

### `remote list` - List remotes

```bash
python3 mobius.py remote list
```

List all configured remotes.

Example:
```bash
python3 mobius.py remote list
# Output:
# Configured remotes:
#   shared: file:///shared/pool
#   origin: https://mobius.example.com/pool
```

### `remote pull` - Fetch from remote

```bash
python3 mobius.py remote pull NAME
```

Fetch functions from a remote repository. Currently supports `file://` URLs. HTTP/HTTPS support planned.

Example:
```bash
python3 mobius.py remote pull shared
```

### `remote push` - Publish to remote

```bash
python3 mobius.py remote push NAME
```

Publish functions to a remote repository. Currently supports `file://` URLs. HTTP/HTTPS support planned.

Example:
```bash
python3 mobius.py remote push shared
```

## Schema Management

## Environment Variables

### `MOBIUS_DIRECTORY`

Storage location for function pool.

- Default: `$HOME/.local/mobius/`
- Custom: `export MOBIUS_DIRECTORY=/path/to/pool`

Example:
```bash
export MOBIUS_DIRECTORY=/shared/pool
python3 mobius.py add function.py@eng
```

### `MOBIUS_USER`

Not used. Author identity is automatically taken from `$USER` or `$USERNAME` environment variables.

## Further Reading

For storage format details, see [STORE.md](STORE.md).
