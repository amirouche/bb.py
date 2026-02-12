# Transcript 00: Compile function to standalone executable

**Purpose**: Verify that `bb.py compile` creates a standalone Python file with all dependencies inlined.

## Setup

Create helper and main functions:

**File**: `/tmp/helper.py`
```python
def helper():
    return 42
```

**File**: `/tmp/main.py`
```python
from bb.pool import object_{helper_hash} as helper

def main():
    return helper()
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add both functions to pool

## Execution

1. Compile to executable: `bb.py compile {main_hash}@eng output.py`

## Expected Behavior

### Command Output

```
Compiling {main_hash}...
Resolved 2 functions (including dependencies)
Created: output.py
```

### Generated File (`output.py`)

Contains all functions inlined:

```python
# Compiled from bb.py pool
# Main: {main_hash}

def helper():
    return 42

def main():
    return helper()

if __name__ == '__main__':
    result = main()
    print(result)
```

**Salient elements to verify**:
- All dependencies recursively inlined
- BB imports replaced with direct calls
- Executable Python file created
- Original function names restored (denormalized)
- Can run standalone: `python output.py`

**Rationale**: The compile command creates distributable Python files without bb.py dependency, useful for deployment.
