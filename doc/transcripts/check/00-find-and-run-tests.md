# Transcript 00: Find and run tests for a function

**Purpose**: Verify that `bb.py check` finds and executes test functions for a given function hash.

## Setup

Create a function and its test:

**File**: `/tmp/math_func.py`
```python
def add(a, b):
    """Add two numbers"""
    return a + b
```

**File**: `/tmp/test_math.py`
```python
from bb.pool import object_{hash} as add

def test_add():
    """Test add function"""
    assert add(2, 3) == 5
    assert add(-1, 1) == 0
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add math function: capture hash
- Add test function referencing the hash

## Execution

1. Find and run tests: `bb.py check {hash}`

## Expected Behavior

### Command Output

```
Found 1 test(s) for {hash}:

Running test: test_add ({test_hash})
✓ test_add passed

All tests passed (1/1)
```

**No tests found**:
```
No tests found for {hash}
```

**Test failed**:
```
✗ test_add failed
AssertionError: ...
```

**Salient elements to verify**:
- Finds test functions that import the target hash
- Executes tests and reports results
- Pass/fail status clear
- No tests case handled gracefully

**Rationale**: The check command enables test discovery and execution based on bb imports, linking tests to implementations.
