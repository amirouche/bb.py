# Transcript 08: Validation - nonexistent file

**Purpose**: Verify that the `add` command fails gracefully when the input file does not exist.

## Setup

**No file created** - intentionally testing nonexistent file path

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory

## Execution

1. Run the add command with a nonexistent file path: `bb.py add /nonexistent/file.py@eng`

## Expected Behavior

### Command Output
- Exit code: Non-zero (failure)
- Standard error contains a clear error message about the missing file

### Error Message Format

The error message should clearly indicate the file was not found:
```
Error: File not found: /nonexistent/file.py
```

Or alternatively:
```
Error: Cannot read file: /nonexistent/file.py
```

**Salient elements to verify**:
- Command fails (non-zero exit code)
- Error message identifies the file path that was not found
- No files are created in the pool directory
- No partial state is left behind

**Rationale**: File system errors should be caught early and reported clearly to the user, preventing confusing error messages later in the pipeline.
