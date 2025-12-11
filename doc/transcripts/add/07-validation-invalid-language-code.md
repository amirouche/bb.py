# Transcript 07: Validation - invalid language code

**Purpose**: Verify that the `add` command fails when the language code is too short (must be 3-256 characters).

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

1. Run the add command with a too-short language code: `bb.py add /tmp/test.py@ab`
   - Note: Language code is only 2 characters, but minimum is 3

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
- Error message specifies the valid range: 3-256 characters
- Error message shows what was provided
- No files are created in the pool directory

**Valid language codes**:
- Minimum: 3 characters (ISO 639-3 standard: `eng`, `fra`, `spa`, etc.)
- Maximum: 256 characters (allows for custom language identifiers)
- Examples: `eng`, `fra`, `spa`, `python-english`, `technical-french`

**Rationale**: The 3-character minimum aligns with ISO 639-3 language codes, which are the international standard for language identification. The 256-character maximum allows for extended language identifiers (e.g., technical jargon variants, domain-specific naming conventions).
