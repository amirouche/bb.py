# Transcript 10: Validation - nonexistent language

**Purpose**: Verify that the `show` command fails when requesting a language that doesn't exist for a function.

## Setup

Create and add a function in English only:

**File**: `/tmp/eng_only.py`
```python
def foo():
    pass
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/eng_only.py@eng`
- Capture the returned hash
- Note: Function only has English mapping, no French mapping

## Execution

1. Run the show command requesting French language: `bb.py show {hash}@fra`

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains a clear error message

### Error Message Format

The error message should indicate the language mapping doesn't exist:

```
Error: Language 'fra' not found for function {hash}
Available languages: eng
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message identifies the missing language
- Error message shows which languages ARE available
- Helps user understand what went wrong

**Rationale**: Functions can have different language mappings. Clear error messages help users discover which languages are actually available for a given function.
