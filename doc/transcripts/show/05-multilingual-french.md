# Transcript 05: Show multilingual function - French

**Purpose**: Verify that the `show` command displays the French version of a multilingual function.

## Setup

Create and add the same function in two languages:

**File**: `/tmp/eng.py`
```python
def multiply(value, factor):
    """Multiply value by factor"""
    result = value * factor
    return result
```

**File**: `/tmp/fra.py`
```python
def multiply(value, factor):
    """Multiplier valeur par facteur"""
    result = value * factor
    return result
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add English version: `bb.py add /tmp/eng.py@eng`
- Capture the hash
- Add French version: `bb.py add /tmp/fra.py@fra`

## Execution

1. Run the show command with French language tag: `bb.py show {hash}@fra`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the **French version**

### Displayed Code

The output shows the French docstring:

```python
def multiply(value, factor):
    """Multiplier valeur par facteur"""
    result = value * factor
    return result
```

**Salient elements to verify**:
- Docstring is in French: "Multiplier valeur par facteur" (NOT English)
- Function logic is identical to English version (same normalized code)
- Only the docstring differs between languages
- Function name and variable names can also differ in a true multilingual scenario

**Rationale**: This demonstrates that Beyond Babel enables true code internationalization - developers can write and read functions in their native language while sharing the same underlying logic.
