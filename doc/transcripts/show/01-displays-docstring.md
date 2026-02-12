# Transcript 01: Show displays docstring

**Purpose**: Verify that the `show` command includes the complete docstring, including multi-line docstrings with formatting.

## Setup

Create and add a function with a detailed docstring:

**File**: `/tmp/documented.py`
```python
def calculate_average(numbers):
    """Calculate the average of a list of numbers.

    Args:
        numbers: List of numbers

    Returns:
        The arithmetic mean
    """
    total = sum(numbers)
    return total / len(numbers)
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add the function to pool: `bb.py add /tmp/documented.py@eng`
- Capture the returned hash

## Execution

1. Run the show command: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the complete docstring

### Displayed Code

The output includes the full docstring:

```python
def calculate_average(numbers):
    """Calculate the average of a list of numbers.

    Args:
        numbers: List of numbers

    Returns:
        The arithmetic mean
    """
    total = sum(numbers)
    return total / len(numbers)
```

**Salient elements to verify**:
- Multi-line docstring is preserved
- Docstring formatting (indentation, blank lines) is maintained
- All docstring sections (description, Args, Returns) are included
- Docstring is language-specific (retrieved from mapping.json, not object.json)

**Rationale**: Docstrings are stored in language-specific `mapping.json` files to enable multilingual documentation. The `show` command reconstructs the function by combining normalized code with the language-specific docstring and name mappings.
