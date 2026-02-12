# Transcript 00: Show displays denormalized code

**Purpose**: Verify that the `show` command displays function code with original names restored (denormalization).

## Setup

Create and add a simple function:

**File**: `/tmp/greet.py`
```python
def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add the function to pool: `bb.py add /tmp/greet.py@eng`
- Capture the returned hash

## Execution

1. Run the show command with the hash and language: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the **denormalized** function code

### Displayed Code

The output should show the original function with restored names:

```python
def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
```

**Salient elements to verify**:
- Function name: `greet` (NOT `_bb_v_0`)
- Parameter name: `name` (NOT `_bb_v_1`)
- Docstring: Included and unchanged
- Logic: Identical to original
- Normalized names (`_bb_v_0`, `_bb_v_1`) do NOT appear in output

**Rationale**: The `show` command performs denormalization using the language-specific name mapping to reconstruct code that looks like the original. This enables users to view functions in their preferred human language.
