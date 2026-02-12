# Transcript 00: Commit copies function to git directory

**Purpose**: Verify that the `commit` command copies a function's `object.json` from the pool to the git directory with proper structure.

## Setup

Create a simple Python function file:

**File**: `/tmp/test_func.py`
```python
def hello():
    """Say hello"""
    return "hello"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Add the function to the pool with language tag `eng`
2. Capture the returned hash (64-character SHA256 hex string)
3. Run the commit command with the hash and comment "Add hello function"

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Committed 1 function(s) to git repository"

### File System State

**Git directory created**: `$BB_DIRECTORY/git/.git/`
- A git repository is initialized

**Function copied to git**: `$BB_DIRECTORY/git/{hash[0:2]}/{hash[2:]}/object.json`

The `object.json` file contains:

```json
{
  "schema_version": 1,
  "hash": "<64-character-hash>",
  "normalized_code": "def _bb_v_0():\n    return 'hello'",
  "metadata": {
    "created": "<ISO-8601-timestamp>",
    "author": "<username>"
  }
}
```

**Salient elements to verify**:
- `schema_version`: Must be `1`
- `hash`: Must match the function hash returned by add command
- `normalized_code`: Must contain the normalized function (function name changed to `_bb_v_0`)
- `metadata.created`: Must be a valid ISO 8601 timestamp
- `metadata.author`: Must be present (string)

### Git Commit

**Git log**: Contains one commit with message "Add hello function"

**Tracked files**: The `object.json` and language mapping files are tracked in git

**Status**: Working directory is clean (no uncommitted changes)
