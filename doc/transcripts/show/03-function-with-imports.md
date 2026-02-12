# Transcript 03: Show function with imports

**Purpose**: Verify that the `show` command displays functions with standard library imports preserved and correctly ordered.

## Setup

Create and add a function with imports:

**File**: `/tmp/with_math.py`
```python
import math

def circle_area(radius):
    """Calculate area of a circle"""
    return math.pi * radius ** 2
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add the function to pool: `bb.py add /tmp/with_math.py@eng`
- Capture the returned hash

## Execution

1. Run the show command: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the function with imports

### Displayed Code

The output includes the import statement:

```python
import math

def circle_area(radius):
    """Calculate area of a circle"""
    return math.pi * radius ** 2
```

**Salient elements to verify**:
- Import statement preserved: `import math`
- Import appears before function definition
- Imported module name (`math`) is NOT renamed
- Function uses imported module: `math.pi`
- Function name restored: `circle_area` (NOT `_bb_v_0`)
- Parameter name restored: `radius` (NOT `_bb_v_1`)

**Rationale**: Standard library and external package imports are preserved during normalization and sorted lexicographically. Only user-defined names (functions, parameters, local variables) are normalized. Imported names are excluded from renaming.
