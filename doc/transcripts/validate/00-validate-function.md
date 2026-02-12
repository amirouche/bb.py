# Transcript 00: Validate function integrity

**Purpose**: Verify that `bb.py validate` checks function and mapping integrity in the pool.

## Setup

Create and add a function:

**File**: `/tmp/func.py`
```python
def process(data):
    """Process data"""
    return data * 2
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Validate specific function: `bb.py validate {hash}`
2. Or validate entire pool: `bb.py validate`

## Expected Behavior

### Valid Function Output

```
Validating function {hash}...
✓ object.json exists and valid
✓ Schema version: 1
✓ Normalized code valid
✓ Language mappings valid (eng: 1)
Function validation passed
```

### Valid Pool Output

```
Validating pool...
Checked 1 function(s)
✓ All functions valid
```

### Invalid Function Output

If corruption detected:

```
✗ object.json missing or corrupted
✗ Invalid JSON in mapping.json
Function validation failed
```

**Salient elements to verify**:
- Checks object.json exists and has valid JSON
- Validates schema version is supported
- Verifies normalized code is valid Python
- Checks language mappings exist and are valid
- Reports errors clearly with specific issues
- Exit code 0 for valid, non-zero for invalid

**Rationale**: The validate command ensures pool integrity, detecting corruption or manual editing errors.
