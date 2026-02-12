# Transcript 00: Find functions that depend on a function

**Purpose**: Verify that `bb.py caller` finds all functions that depend on (call) a given function.

## Setup

Create helper and caller functions:

**File**: `/tmp/helper.py`
```python
def helper():
    """Helper function"""
    return 42
```

**File**: `/tmp/main.py` (depends on helper)
```python
from bb.pool import object_{helper_hash} as helper

def main():
    """Main function"""
    return helper()
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add helper: `bb.py add /tmp/helper.py@eng`
- Capture helper hash
- Create main with bb import using helper hash
- Add main: `bb.py add /tmp/main.py@eng`

## Execution

1. Find callers of helper: `bb.py caller {helper_hash}`

## Expected Behavior

### Command Output

```
Functions that depend on {helper_hash}:

Hash: {main_hash}
Function: main (eng)
Import: from bb.pool import object_{helper_hash} as helper

Total: 1 caller(s)
```

**No callers output**:
```
No functions depend on {helper_hash}
```

**Salient elements to verify**:
- Finds all functions with bb imports referencing the hash
- Shows caller hash and function name
- Shows how it's imported
- Empty result handled gracefully

**Rationale**: The caller command enables reverse dependency lookup, useful for impact analysis before refactoring or removing functions.
