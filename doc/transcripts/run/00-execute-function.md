# Transcript 00: Run executes function

**Purpose**: Verify that `bb.py run` executes a function with provided arguments.

## Setup

Create and add a function:

**File**: `/tmp/func.py`
```python
def greet(name):
    """Greet someone"""
    return f"Hello, {name}!"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Run the function with argument: `bb.py run {hash} -- World`
   - Note: `--` separates bb.py options from function arguments

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the function result: `Hello, World!`

**Salient elements to verify**:
- Function executes with provided arguments
- Return value is printed to stdout
- Language suffix is optional if only one language exists
- `--` delimiter separates tool options from function arguments

**Rationale**: The run command enables quick testing and execution of functions from the pool without manual code reconstruction.
