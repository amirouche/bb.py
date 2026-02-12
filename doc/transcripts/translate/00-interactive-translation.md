# Transcript 00: Interactive translation

**Purpose**: Verify that the `translate` command interactively prompts for translated names and creates a new language mapping.

## Setup

Create and add a function in English:

**File**: `/tmp/func.py`
```python
def greet(name):
    """Say hello"""
    return f"Hello, {name}!"
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Add function: `bb.py add /tmp/func.py@eng`
- Capture the returned hash

## Execution

1. Run the translate command: `bb.py translate {hash}@eng fra`
2. Respond to interactive prompts

## Expected Behavior

### Interactive Prompts

The command displays the source function and prompts for translations:

```
Source function (eng):
def greet(name):
    """Say hello"""
    return f"Hello, {name}!"

Translate to 'fra'
---

Translate 'greet': saluer
Translate 'name': nom
Translate docstring "Say hello": Dire bonjour
Optional comment for this mapping variant: French translation
```

### User Input

User provides:
- Function name translation: `saluer`
- Parameter translation: `nom`
- Docstring translation: `Dire bonjour`
- Optional comment: `French translation` (or blank)

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Translation saved for language 'fra'"
- Mapping hash is displayed

### File System State

**New language mapping created**: `$BB_DIRECTORY/pool/{hash[0:2]}/{hash[2:]}/fra/{mapping_hash[0:2]}/{mapping_hash[2:]}/mapping.json`

The `mapping.json` contains:

```json
{
  "docstring": "Dire bonjour",
  "name_mapping": {
    "_bb_v_0": "saluer",
    "_bb_v_1": "nom"
  },
  "alias_mapping": {},
  "comment": "French translation"
}
```

**Salient elements to verify**:
- Command is interactive (prompts for user input)
- Source function is displayed for reference
- User provides translation for each variable name
- User provides translated docstring
- Optional comment field for mapping variant identification
- New language directory created under function hash
- French mapping can now be retrieved: `bb.py show {hash}@fra`

**Rationale**: The interactive translate command enables human translators to create language mappings by providing translations in a guided workflow.
