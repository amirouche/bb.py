# Transcript 00: Get returns denormalized code

**Purpose**: Verify that the `get` command returns function with original names restored (deprecated, use `show` instead).

**Note**: The `get` command is deprecated in favor of `show`. It provides the same functionality but displays a deprecation warning.

## Setup

Create and add a function:

**File**: `/tmp/func.py`
```python
def process(data):
    """Process data"""
    result = data * 2
    return result
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Run the get command: `bb.py get {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains denormalized function code
- **Standard error contains deprecation warning**

### Displayed Code

```python
def process(data):
    """Process data"""
    result = data * 2
    return result
```

### Deprecation Warning

Standard error contains:

```
Warning: 'get' command is deprecated. Use 'show' instead.
```

**Salient elements to verify**:
- Function code is denormalized correctly (same as `show`)
- Original names restored: `process`, `data`, `result`
- Deprecation warning appears in stderr (not stdout)
- Exit code is 0 (success, despite warning)

**Rationale**: The `get` command is maintained for backward compatibility but users should migrate to `show` which has better support for mapping variants and language discovery.
