# Transcript 06: Validation - missing language suffix

**Purpose**: Verify that the `add` command fails with a clear error message when the language suffix is missing.

## Setup

Create a Python function file:

**File**: `/tmp/test.py`
```python
def foo():
    pass
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory

## Execution

1. Run the add command **without** language suffix: `bb.py add /tmp/test.py`
   - Note: Missing `@lang` suffix

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains: "Missing language suffix"

### Error Message Format

The error message should clearly explain the required format:
```
Error: Missing language suffix. Use format: file.py@lang
Example: bb.py add myfile.py@eng
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message is clear and actionable
- Error message explains the correct format
- No files are created in the pool directory
- No partial state is left behind

**Rationale**: The language suffix is mandatory because Beyond Babel is a multilingual function pool. Every function must be associated with a human language to enable proper name mapping and documentation in that language.
