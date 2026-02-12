# Transcript 01: Commit copies all language mappings

**Purpose**: Verify that the `commit` command copies all language mapping directories (eng, fra) for a multilingual function to the git directory.

## Setup

Create two versions of the same function in different languages:

**File**: `/tmp/hello_eng.py`
```python
def hello():
    """Say hello"""
    return "hello"
```

**File**: `/tmp/bonjour_fra.py`
```python
def bonjour():
    """Dire bonjour"""
    return "hello"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

**Note**: Both functions have identical logic (return the same string), so they will produce the same hash but with different language mappings.

## Execution

1. Add the English version to the pool with language tag `eng`
2. Capture the returned hash
3. Add the French version to the pool with language tag `fra` (using the same hash)
4. Run the commit command with the hash and comment "Add multilingual hello"

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Committed 1 function(s) to git repository"

### File System State

**Function directory**: `$BB_DIRECTORY/git/{hash[0:2]}/{hash[2:]}/`

**Both language directories exist**:
- `{function_dir}/eng/` - Contains English mapping
- `{function_dir}/fra/` - Contains French mapping

**English mapping**: `{function_dir}/eng/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`

```json
{
  "docstring": "Say hello",
  "name_mapping": {
    "_bb_v_0": "hello"
  },
  "alias_mapping": {},
  "comment": ""
}
```

**French mapping**: `{function_dir}/fra/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`

```json
{
  "docstring": "Dire bonjour",
  "name_mapping": {
    "_bb_v_0": "bonjour"
  },
  "alias_mapping": {},
  "comment": ""
}
```

**Salient elements to verify**:
- Both `eng/` and `fra/` directories exist under the function directory
- Each language has its own `mapping.json` file (content-addressed)
- `docstring`: Language-specific docstring is preserved
- `name_mapping`: Maps normalized name `_bb_v_0` back to original function name
- Both mappings refer to the same normalized function code

### Git Commit

**Tracked files**: Both language mapping directories and their mapping.json files are tracked in git
