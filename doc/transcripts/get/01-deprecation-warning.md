# Transcript 01: Get shows deprecation warning

**Purpose**: Verify that the `get` command always displays a deprecation warning directing users to use `show` instead.

## Setup

Create and add a simple function:

**File**: `/tmp/func.py`
```python
def foo():
    pass
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Run the get command: `bb.py get {hash}@eng`

## Expected Behavior

### Standard Error (stderr)
Contains deprecation warning:

```
Warning: The 'get' command is deprecated and will be removed in a future version.
Please use 'show' instead: bb.py show {hash}@eng
```

### Standard Output (stdout)
Contains the function code (same as show would produce)

### Exit Code
- 0 (success) - Deprecation warning does not cause failure

**Salient elements to verify**:
- Warning appears in stderr (separate from stdout)
- Warning mentions "deprecated"
- Warning suggests using "show" instead
- Warning provides the equivalent show command
- Command still works (not an error, just a warning)

**Rationale**: Deprecation warnings help users migrate to newer APIs while maintaining backward compatibility. The warning is sent to stderr so it doesn't interfere with stdout parsing in scripts.
