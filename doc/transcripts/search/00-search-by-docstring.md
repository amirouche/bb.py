# Transcript 00: Search by docstring content

**Purpose**: Verify that `bb.py search` finds functions by searching docstring content.

## Setup

Create and add functions with searchable docstrings:

**File**: `/tmp/average.py`
```python
def calculate_average(numbers):
    """Calculate the average of numbers"""
    return sum(numbers) / len(numbers)
```

**File**: `/tmp/sum.py`
```python
def calculate_sum(values):
    """Calculate the sum of values"""
    return sum(values)
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add both functions with `@eng` language tag

## Execution

1. Search for term: `bb.py search average`

## Expected Behavior

### Command Output

Shows matching functions:

```
Found 1 function(s):

Hash: {hash}
Function: calculate_average (eng)
Match: docstring contains "average"
Docstring: "Calculate the average of numbers"

To view: bb.py show {hash}@eng
```

**Empty results**:
```
No functions in pool
```

Or:
```
No matches found for query: "nonexistent"
```

**Salient elements to verify**:
- Searches in function docstrings
- Case-insensitive search
- Shows hash, function name, and match location
- Provides show command for viewing
- Empty pool and no matches handled gracefully

**Rationale**: The search command enables discovery of functions by content, complementing the log command's chronological view.
