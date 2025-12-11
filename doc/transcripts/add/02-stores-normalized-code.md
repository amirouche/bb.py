# Transcript 02: Add stores normalized code

**Purpose**: Verify that the `add` command normalizes function code by renaming functions, parameters, and local variables.

## Setup

Create a Python function with meaningful names:

**File**: `/tmp/normalize.py`
```python
def my_function(param):
    """Doc"""
    local = param * 2
    return local
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Run the add command: `bb.py add /tmp/normalize.py@eng`
2. Load the resulting `object.json` to inspect normalized code

## Expected Behavior

### Normalized Code

The `normalized_code` field in `object.json` contains:

```python
def _bb_v_0(_bb_v_1):
    _bb_v_2 = _bb_v_1 * 2
    return _bb_v_2
```

**Normalization rules applied**:
- Function name: `my_function` → `_bb_v_0`
- Parameter: `param` → `_bb_v_1`
- Local variable: `local` → `_bb_v_2`
- Constants and operators remain unchanged

**Salient elements to verify**:
- Original function name (`my_function`) does NOT appear in `normalized_code`
- Original parameter name (`param`) does NOT appear in `normalized_code`
- Original variable name (`local`) does NOT appear in `normalized_code`
- All user-defined names replaced with `_bb_v_N` pattern
- Normalized names are sequential: `_bb_v_0`, `_bb_v_1`, `_bb_v_2`, ...
- Function always gets `_bb_v_0` (invariant)

### Name Mapping Preservation

The `mapping.json` preserves the original names:

```json
{
  "docstring": "Doc",
  "name_mapping": {
    "_bb_v_0": "my_function",
    "_bb_v_1": "param",
    "_bb_v_2": "local"
  },
  "alias_mapping": {},
  "comment": ""
}
```

This bidirectional mapping enables reconstruction of the original code when showing or getting the function.

**Rationale**: Normalization ensures that the same logical function produces the same hash regardless of variable names, enabling multilingual function sharing.
