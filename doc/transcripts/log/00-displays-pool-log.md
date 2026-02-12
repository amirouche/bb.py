# Transcript 00: Log displays pool functions

**Purpose**: Verify that `bb.py log` displays a git-like log of all functions in the pool.

## Setup

Create and add functions:

**File**: `/tmp/func1.py`
```python
def foo():
    pass
```

**File**: `/tmp/func2.py`
```python
def bar():
    pass
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add func1: `bb.py add /tmp/func1.py@eng`
- Add func2: `bb.py add /tmp/func2.py@eng`

## Execution

1. Run log command: `bb.py log`

## Expected Behavior

### Command Output

Displays log format:

```
=== Function Pool Log ===
Total: 2 functions

Hash: {hash1}
Date: 2025-11-25T12:00:00Z
Author: testuser
Languages: eng (1 mapping)

Hash: {hash2}
Date: 2025-11-25T12:01:00Z
Author: testuser
Languages: eng (1 mapping)
```

**Empty pool output**:
```
No functions in pool
```

**Salient elements to verify**:
- Shows all functions in pool
- Displays hash, date, author for each function
- Shows available languages and mapping counts
- Sorted by creation date (newest first)
- Empty pool handled gracefully

**Rationale**: The log command provides an overview of all functions in the pool, similar to git log, helping users discover and track functions.
