# Transcript 07: Multiple mappings shows selection menu

**Purpose**: Verify that the `show` command displays a selection menu when multiple mapping variants exist for a language.

## Setup

Create a function and add it twice with different comments:

**File**: `/tmp/func.py`
```python
def foo():
    """Test function"""
    return 42
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add first version: `bb.py add /tmp/func.py@eng --comment "first version"`
- Add second version: `bb.py add /tmp/func.py@eng --comment "second version"`
- Both additions use the same file, creating two different mappings

## Execution

1. Run the show command: `bb.py show {hash}@eng`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output displays a selection menu

### Menu Format

The output should list all available mappings with their comments:

```
Multiple mappings found for {hash}@eng:

1. Mapping: {mapping_hash_1} - first version
   bb.py show {hash}@eng@{mapping_hash_1}

2. Mapping: {mapping_hash_2} - second version
   bb.py show {hash}@eng@{mapping_hash_2}

Use one of the commands above to view a specific mapping.
```

**Salient elements to verify**:
- Message indicates multiple mappings exist
- Each mapping is numbered (1, 2, etc.)
- Mapping hash is shown for each variant
- Comment is displayed for each variant
- Copyable command is provided for each option
- No function code is displayed (only menu)

**Rationale**: Multiple mappings allow for variant naming conventions within the same language (e.g., "technical terminology" vs "colloquial names"). The menu helps users choose which variant to view.
