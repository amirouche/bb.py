# Transcript 12: Validation - invalid language code

**Purpose**: Verify that the `show` command fails when the language code is too short (must be 3-256 characters).

## Setup

Create and add a function:

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

1. Run the show command with too-short language code: `bb.py show {hash}@ab`
   - Language code is only 2 characters (minimum is 3)

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains: "Language code must be 3-256 characters"

### Error Message Format

The error message should explain the constraint:

```
Error: Language code must be 3-256 characters (ISO 639-3 or longer)
Got: 'ab' (2 characters)
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message specifies the valid range
- Error message shows what was provided
- Validation happens before attempting to load the function

**Valid language code format**:
- Minimum: 3 characters (ISO 639-3)
- Maximum: 256 characters (custom identifiers)
- Examples: `eng`, `fra`, `spa`, `technical-english`

**Rationale**: Consistent validation across all commands ensures users understand the language code requirements regardless of which command they're using.
