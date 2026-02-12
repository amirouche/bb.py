# Transcript 09: Validation - nonexistent function

**Purpose**: Verify that the `show` command fails gracefully when the function hash does not exist in the pool.

## Setup

**No function created** - testing nonexistent hash

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory

## Execution

1. Run the show command with a fake hash: `bb.py show {fake_hash}@eng`
   - Where `{fake_hash}` is a valid 64-character hex string that doesn't exist in the pool
   - Example: `0000000000000000000000000000000000000000000000000000000000000000`

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains a clear error message

### Error Message Format

The error message should indicate the function was not found:

```
Error: Function not found: {fake_hash}
```

Or alternatively:

```
Error: Function {fake_hash} does not exist in pool
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message identifies the hash that was not found
- Error message is clear and actionable
- No partial output is displayed

**Rationale**: Clear error messages help users understand when they're referencing a nonexistent function, which can happen due to typos, incorrect hash, or working with different pool directories.
