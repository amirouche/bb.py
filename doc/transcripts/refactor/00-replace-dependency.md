# Transcript 00: Replace a dependency in a function

**Purpose**: Verify that `bb.py refactor` replaces one bb import dependency with another in a function.

## Setup

Create old helper, new helper, and caller:

**File**: `/tmp/old_helper.py`
```python
def old_helper():
    return "old"
```

**File**: `/tmp/new_helper.py`
```python
def new_helper():
    return "new"
```

**File**: `/tmp/main.py`
```python
from bb.pool import object_{old_hash} as helper

def main():
    return helper()
```

**Environment**:
- Add old_helper, new_helper, main to pool
- Capture all hashes

## Execution

1. Replace old dependency with new: `bb.py refactor {main_hash} {old_hash} {new_hash}`

## Expected Behavior

### Command Output

```
Refactoring function {main_hash}...
Replacing {old_hash} → {new_hash}
New function hash: {new_main_hash}

To view: bb.py show {new_main_hash}@eng
```

### New Function Code

The refactored function imports the new helper:

```python
from bb.pool import object_{new_hash} as helper

def main():
    return helper()
```

**Salient elements to verify**:
- Creates new function with updated import
- Returns new function hash (content changed)
- Original function unchanged in pool
- Alias preserved (still `as helper`)

**Rationale**: The refactor command enables systematic dependency updates while maintaining immutability through content-addressed storage.
