# Transcript 01: Add creates v1 directory structure

**Purpose**: Verify that the `add` command creates proper v1 storage structure with object.json and language mapping directory.

## Setup

Create a Python function file:

**File**: `/tmp/math_func.py`
```python
def add_numbers(a, b):
    """Add two numbers"""
    return a + b
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Run the add command: `bb.py add /tmp/math_func.py@eng`
2. Capture the returned hash

## Expected Behavior

### File System State

**Function directory**: `$BB_DIRECTORY/pool/{hash[0:2]}/{hash[2:]}/`

**Core files and directories**:
- `object.json` - Contains normalized code and metadata
- `eng/` - Language-specific mapping directory

**object.json structure**:

```json
{
  "schema_version": 1,
  "hash": "<function-hash>",
  "normalized_code": "def _bb_v_0(_bb_v_1, _bb_v_2):\n    return _bb_v_1 + _bb_v_2",
  "metadata": {
    "created": "2025-11-25T12:00:00Z",
    "author": "testuser"
  }
}
```

**Language mapping directory**: `{function_dir}/eng/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`

The `mapping.json` contains:

```json
{
  "docstring": "Add two numbers",
  "name_mapping": {
    "_bb_v_0": "add_numbers",
    "_bb_v_1": "a",
    "_bb_v_2": "b"
  },
  "alias_mapping": {},
  "comment": ""
}
```

**Salient elements to verify**:
- `schema_version`: Must be `1` (indicates v1 storage format)
- Function directory structure: Two-level hash-based directory tree
- `object.json`: Core function data without language-specific information
- Language directory: Separate directory for each language (content-addressed mappings)
- `name_mapping`: Bidirectional mapping from normalized names to original names
- `docstring`: Stored in mapping.json, not in object.json (enables multilingual support)
