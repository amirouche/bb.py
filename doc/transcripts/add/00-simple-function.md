# Transcript 00: Add simple function

**Purpose**: Verify that the `add` command successfully adds a simple function to the pool and returns its hash.

## Setup

Create a simple Python function file:

**File**: `/tmp/simple.py`
```python
def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Run the add command: `bb.py add /tmp/simple.py@eng`
2. Capture the output containing the hash

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Hash: <64-character-hash>"
- The hash is a 64-character lowercase hexadecimal string (SHA256)

### File System State

**Function directory created**: `$BB_DIRECTORY/pool/{hash[0:2]}/{hash[2:]}/`

**object.json exists**: `{function_dir}/object.json`

The `object.json` contains:

```json
{
  "schema_version": 1,
  "hash": "<64-character-hash>",
  "normalized_code": "def _bb_v_0(_bb_v_1):\n    ...",
  "metadata": {
    "created": "<ISO-8601-timestamp>",
    "author": "<username>"
  }
}
```

**Salient elements to verify**:
- Hash is exactly 64 characters (SHA256 in hex)
- Function directory structure follows pattern: `{hash[0:2]}/{hash[2:]}/`
- `object.json` file exists and contains valid JSON
- `normalized_code`: Function renamed to `_bb_v_0`, parameters renamed to `_bb_v_1`, etc.
