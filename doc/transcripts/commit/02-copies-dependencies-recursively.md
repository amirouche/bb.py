# Transcript 02: Commit copies dependencies recursively

**Purpose**: Verify that the `commit` command recursively copies all function dependencies to the git directory.

## Setup

Create a helper function:

**File**: `/tmp/helper.py`
```python
def helper():
    """Helper function"""
    return 42
```

Add the helper to the pool and capture its hash.

Create a main function that depends on the helper:

**File**: `/tmp/main.py`
```python
from bb.pool import object_{helper_hash} as helper

def main():
    """Main function"""
    return helper()
```

Replace `{helper_hash}` with the actual hash captured from adding the helper function.

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Add the helper function to the pool with language tag `eng`
2. Capture the helper hash
3. Create the main function file with the bb import referencing the helper hash
4. Add the main function to the pool with language tag `eng`
5. Capture the main hash
6. Run the commit command with the **main** hash and comment "Add main with dependency"

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Committed 2 function(s) to git repository"
- **Note**: The count is 2 because both main and helper are committed

### File System State

**Main function**: `$BB_DIRECTORY/git/{main_hash[0:2]}/{main_hash[2:]}/object.json`

The main function's `object.json` contains:

```json
{
  "schema_version": 1,
  "hash": "<main-hash>",
  "normalized_code": "from bb.pool import object_{helper_hash}\ndef _bb_v_0():\n    return object_{helper_hash}._bb_v_0()",
  "metadata": { ... }
}
```

**Helper function**: `$BB_DIRECTORY/git/{helper_hash[0:2]}/{helper_hash[2:]}/object.json`

The helper function's `object.json` contains:

```json
{
  "schema_version": 1,
  "hash": "<helper-hash>",
  "normalized_code": "def _bb_v_0():\n    return 42",
  "metadata": { ... }
}
```

**Salient elements to verify**:
- Both main and helper `object.json` files exist in git directory
- Main function's `normalized_code` contains the bb import: `from bb.pool import object_{helper_hash}`
- Main function's `normalized_code` calls the helper: `object_{helper_hash}._bb_v_0()`
- Helper function's hash in main's import matches the helper's actual hash
- Dependency tree is fully resolved (all transitive dependencies are committed)

### Git Commit

**Tracked files**: Both function directories with their `object.json` and mapping files are tracked in git
