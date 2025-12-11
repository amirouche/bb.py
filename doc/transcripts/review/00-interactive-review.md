# Transcript 00: Interactive function review

**Purpose**: Verify that `bb.py review` displays function code interactively for review and approval.

## Setup

Create and add a function:

**File**: `/tmp/func.py`
```python
def process(data):
    """Process some data"""
    return data * 2
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Run `bb.py init`
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Run review command: `bb.py review {hash}`
2. Interactive prompt asks for approval

## Expected Behavior

### Interactive Display

Shows function review interface:

```
=== Interactive Function Review ===

Function: process (eng)
Hash: {hash}

def process(data):
    """Process some data"""
    return data * 2

Dependencies: None

Do you want to approve this function? [y/n]:
```

### User Input

User responds with `y` (approve) or `n` (decline)

### Command Output
- Exit code: 0 (success)
- If approved: Confirmation message shown
- Displays function code, hash, dependencies

**Salient elements to verify**:
- Function code is displayed for visual inspection
- Dependencies are resolved and listed
- Interactive approval required before proceeding
- Helps verify function logic before use

**Rationale**: The review command provides a way to inspect functions and their dependencies interactively, useful for security auditing and understanding code before execution.
