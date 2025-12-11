# Transcript 03: Same logic produces same hash

**Purpose**: Verify that identical function logic produces identical hash regardless of variable names or docstrings.

## Setup

Create two Python files with identical logic but different names and docstrings:

**File**: `/tmp/english.py`
```python
def calculate_sum(numbers):
    """Calculate the sum of numbers"""
    total = 0
    for num in numbers:
        total = total + num
    return total
```

**File**: `/tmp/french.py`
```python
def calculate_sum(numbers):
    """Calculer la somme des nombres"""
    total = 0
    for num in numbers:
        total = total + num
    return total
```

**Key differences**:
- Docstrings are in different languages (English vs French)
- Logic is identical

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory

## Execution

1. Add the English version: `bb.py add /tmp/english.py@eng`
2. Capture the hash (eng_hash)
3. Add the French version: `bb.py add /tmp/french.py@fra`
4. Capture the hash (fra_hash)

## Expected Behavior

### Hash Equivalence
- `eng_hash == fra_hash` (both functions produce identical hash)

### Single Function Storage

**Only one object.json exists**: `$BB_DIRECTORY/pool/{hash[0:2]}/{hash[2:]}/object.json`

The normalized code is identical for both:

```python
def _bb_v_0(_bb_v_1):
    _bb_v_2 = 0
    for _bb_v_3 in _bb_v_1:
        _bb_v_2 = _bb_v_2 + _bb_v_3
    return _bb_v_2
```

### Multiple Language Mappings

**Both language directories exist**:
- `{function_dir}/eng/` - English mapping
- `{function_dir}/fra/` - French mapping

**English mapping** (`eng/.../mapping.json`):
```json
{
  "docstring": "Calculate the sum of numbers",
  "name_mapping": { "_bb_v_0": "calculate_sum", "_bb_v_1": "numbers", ... },
  "alias_mapping": {},
  "comment": ""
}
```

**French mapping** (`fra/.../mapping.json`):
```json
{
  "docstring": "Calculer la somme des nombres",
  "name_mapping": { "_bb_v_0": "calculate_sum", "_bb_v_1": "numbers", ... },
  "alias_mapping": {},
  "comment": ""
}
```

**Salient elements to verify**:
- Hash is computed on normalized code WITHOUT docstring
- Docstrings do NOT affect hash computation
- Variable names do NOT affect hash computation (after normalization)
- Same logic = same hash = shared storage
- Only language-specific data (docstrings, name mappings) differ

**Rationale**: This enables multilingual function sharing - the same logical function can have multiple human language representations while sharing the same hash and storage.
