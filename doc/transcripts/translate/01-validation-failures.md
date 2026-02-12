# Transcript 01: Validation failures

**Purpose**: Verify that the `translate` command validates inputs and provides clear error messages.

## Test Cases

### Missing Source Language Suffix

**Setup**: Add a function with hash `{hash}`

**Execution**: `bb.py translate {hash} fra` (missing `@lang`)

**Expected Output**:
- Exit code: Non-zero (failure)
- Standard error: "Missing language suffix. Use format: {hash}@lang"

### Invalid Source Language Code

**Execution**: `bb.py translate {hash}@ab fra` (only 2 characters)

**Expected Output**:
- Exit code: Non-zero (failure)
- Standard error: "Source language code must be 3-256 characters"

### Invalid Target Language Code

**Execution**: `bb.py translate {hash}@eng xy` (only 2 characters)

**Expected Output**:
- Exit code: Non-zero (failure)
- Standard error: "Target language code must be 3-256 characters"

### Invalid Hash Format

**Execution**: `bb.py translate not-valid@eng fra`

**Expected Output**:
- Exit code: Non-zero (failure)
- Standard error: "Invalid hash format"

### Nonexistent Function

**Execution**: `bb.py translate {fake_hash}@eng fra`

**Expected Output**:
- Exit code: Non-zero (failure)
- Standard error: "Could not load function" or "Function not found"

**Salient elements to verify**:
- All validation errors caught before prompting for input
- Error messages are clear and actionable
- No partial state created on validation failure
- Language code validation consistent across commands (3-256 characters)

**Rationale**: Early validation prevents wasted user effort in the interactive translation process.
