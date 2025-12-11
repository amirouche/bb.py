# Transcript 02: Show async function

**Purpose**: Verify that the `show` command correctly displays async functions with async/await keywords preserved.

## Setup

Create and add an async function:

**File**: `/tmp/async.py`
```python
async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add the function to pool: `bb.py add /tmp/async.py@eng`
- Capture the returned hash

## Execution

1. Run the show command: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains the denormalized async function

### Displayed Code

The output preserves async/await syntax:

```python
async def fetch_data(url):
    """Fetch data from URL"""
    response = await http_get(url)
    return response
```

**Salient elements to verify**:
- `async` keyword preserved in function definition
- `await` keyword preserved in expression
- Function name restored: `fetch_data` (NOT `_bb_v_0`)
- Parameters restored: `url`, `response` (NOT `_bb_v_1`, `_bb_v_2`)
- External function `http_get` unchanged (not a user-defined name)

**Rationale**: Async functions are handled correctly through the entire pipeline - normalization preserves async/await in the AST, and denormalization restores user-defined names while keeping language keywords intact.
