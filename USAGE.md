# Ouverture Usage

Multilingual function pool: same logic, different languages → same hash.

## Commands

```
usage: ouverture.py [-h] {add,get,show,migrate,validate} ...

ouverture - Function pool manager

positional arguments:
  {add,get,show,migrate,validate}
                        Commands
    add                 Add a function to the pool
    get                 Get a function from the pool
    show                Show a function with mapping selection support
    migrate             Migrate functions from v0 to v1
    validate            Validate v1 function structure

options:
  -h, --help            show this help message and exit
```

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
