# Transcript 04: Show multilingual function - English

**Purpose**: Verify that the `show` command displays the correct language-specific version when a function has multiple language mappings.

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
- Capture the hash (both will have same hash)
- Add French version: `bb.py add /tmp/fra.py@fra`

## Execution

1. Run the show command with English language tag: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the **English version**

### Displayed Code

The output shows the English docstring:

```python
def multiply(value, factor):
    """Multiply value by factor"""
    result = value * factor
    return result
```

**Salient elements to verify**:
- Docstring is in English: "Multiply value by factor" (NOT French)
- Function name and parameters are denormalized correctly
- Same hash produces different output based on language tag
- Both language mappings stored under same function hash

**Rationale**: Beyond Babel's multilingual architecture stores a single normalized function with multiple language-specific mappings. The `@lang` suffix determines which language mapping to use for denormalization.
