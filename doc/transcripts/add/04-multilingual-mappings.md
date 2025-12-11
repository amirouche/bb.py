# Transcript 04: Multilingual mappings

**Purpose**: Verify that adding the same function in multiple languages creates separate language mapping directories under the same function hash.

## Setup

Create two versions of the same function in different languages:

**File**: `/tmp/eng.py`
```python
def double(value):
    """Double a value"""
    return value * 2
```

**File**: `/tmp/fra.py`
```python
def double(value):
    """Doubler une valeur"""
    return value * 2
```

**Note**: Logic is identical, only docstrings differ.

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Add English version: `bb.py add /tmp/eng.py@eng`
2. Capture hash (eng_hash)
3. Add French version: `bb.py add /tmp/fra.py@fra`
4. Capture hash (fra_hash)

## Expected Behavior

### Hash Equivalence
- `eng_hash == fra_hash` (same logical function)

### Language Directories

**Function directory**: `$BB_DIRECTORY/pool/{hash[0:2]}/{hash[2:]}/`

**Both language directories exist**:
- `{function_dir}/eng/` - English language mappings
- `{function_dir}/fra/` - French language mappings

Each language directory contains content-addressed mapping files:
- `eng/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`
- `fra/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`

### Command Output

**First add (English)**:
```
Added function to pool
Hash: <64-char-hash>
```

**Second add (French)**:
```
Already exists with hash <64-char-hash>, adding new language mapping
```

**Salient elements to verify**:
- Both `eng/` and `fra/` directories exist under the same function directory
- Each language has its own mapping.json with language-specific docstring
- `object.json` remains the same (not duplicated)
- Second add command recognizes existing function and adds new language mapping
- Function can be retrieved in either language using `bb.py show <hash>@eng` or `bb.py show <hash>@fra`

**Rationale**: This architecture enables storing a single normalized function with multiple language representations, supporting true multilingual code sharing.
