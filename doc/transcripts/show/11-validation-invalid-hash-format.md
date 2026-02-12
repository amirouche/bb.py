# Transcript 11: Validation - invalid hash format

**Purpose**: Verify that the `show` command fails when the hash format is invalid.

## Setup

**No function created** - testing invalid hash format

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory

## Execution

1. Run the show command with an invalid hash: `bb.py show not-valid-hash@eng`
   - Hash is not 64 characters or contains invalid characters

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains: "Invalid hash format"

### Error Message Format

The error message should explain the hash format requirements:

```
Error: Invalid hash format: 'not-valid-hash'
Expected: 64-character hexadecimal string (SHA256)
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message identifies the invalid hash
- Error message explains the correct format
- Hash validation happens before file system access

**Valid hash format**:
- Exactly 64 characters
- Lowercase hexadecimal (0-9, a-f)
- SHA256 output

**Rationale**: Early validation prevents confusing error messages later in the process and helps users understand the correct hash format.
