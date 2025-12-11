# Transcript 06: Show without language lists available languages

**Purpose**: Verify that the `show` command without a language suffix lists all available languages for a function.

## Setup

Create and add a function:

**File**: `/tmp/test.py`
```python
def foo():
    pass
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add the function: `bb.py add /tmp/test.py@eng`
- Capture the returned hash

## Execution

1. Run the show command **without** language suffix: `bb.py show {hash}`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output lists available languages

### Output Format

The output should display available languages and mapping counts:

```
Available languages for function {hash}:
  eng: 1 mapping(s)

To view a specific language:
  bb.py show {hash}@eng
```

**Salient elements to verify**:
- Message indicates languages are available
- Language code is displayed: `eng`
- Mapping count is shown: `1 mapping(s)`
- Help text explains how to view a specific language
- No function code is displayed (only metadata)

**Rationale**: This feature helps users discover which languages are available for a given function hash. It's particularly useful when working with multilingual functions or when the language tag is unknown.
