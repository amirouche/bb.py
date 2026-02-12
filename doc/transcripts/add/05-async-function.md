# Transcript 05: Add async function

**Purpose**: Verify that the `add` command correctly handles async functions with proper AST normalization.

## Setup

Create an async Python function:

**File**: `/tmp/async_func.py`
```python
async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Run the add command: `bb.py add /tmp/async_func.py@eng`
2. Capture the output and hash

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Hash: <64-character-hash>"

### Normalized Code

The `normalized_code` in `object.json` preserves async/await syntax:

```python
async def _bb_v_0(_bb_v_1):
    _bb_v_2 = await http_get(_bb_v_1)
    return _bb_v_2
```

**Salient elements to verify**:
- `async` keyword is preserved in function definition
- `await` keyword is preserved in expression
- Function name normalized to `_bb_v_0`
- Parameters normalized to `_bb_v_1`, `_bb_v_2`, etc.
- External function `http_get` is NOT renamed (not a user-defined name)

### Name Mapping

The `mapping.json` contains:

```json
{
  "docstring": "Fetch data from URL",
  "name_mapping": {
    "_bb_v_0": "fetch_data",
    "_bb_v_1": "url",
    "_bb_v_2": "response"
  },
  "alias_mapping": {},
  "comment": ""
}
```

**Rationale**: Async functions are first-class citizens in Python 3.9+ and must be handled correctly by the AST normalization pipeline. The async/await keywords are part of the syntax tree and preserved during normalization.
