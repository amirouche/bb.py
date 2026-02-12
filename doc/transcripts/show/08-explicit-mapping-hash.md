# Transcript 08: Show with explicit mapping hash

**Purpose**: Verify that the `show` command with an explicit mapping hash displays the specific variant directly without a menu.

## Setup

Create a function and add it:

**File**: `/tmp/func.py`
```python
def foo():
    """Test function"""
    return 42
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng --comment "target version"`
- Capture both function hash and mapping hash from output

## Execution

1. Run the show command with explicit mapping hash: `bb.py show {hash}@eng@{mapping_hash}`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the function code directly (no menu)

### Displayed Code

The output shows the function immediately:

```python
def foo():
    """Test function"""
    return 42
```

**Salient elements to verify**:
- Function code is displayed directly
- No selection menu appears (even if multiple mappings exist)
- The correct mapping variant is used based on mapping hash
- Format: `{function_hash}@{language}@{mapping_hash}` (three components)

**Format Specification**:
- `{hash}@{lang}` - Show function with automatic mapping selection (or menu if multiple)
- `{hash}@{lang}@{mapping_hash}` - Show specific mapping variant directly

**Rationale**: Explicit mapping hash allows users to directly reference a specific variant, useful for scripting, reproducibility, or when multiple mapping variants exist.
