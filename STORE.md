# Beyond Babelia Storage Specification (v1)

This document describes the storage format for Beyond Babelia function pools.

## Overview

Beyond Babelia uses a content-addressed filesystem storage where:
- **Functions** are stored by their SHA256 hash
- **Mappings** (language-specific names/docstrings) are stored separately, also content-addressed
- **Deduplication** happens automatically - identical content shares storage

## Directory Structure

```
$BB_DIRECTORY/pool/
  ab/                              # First 2 chars of function hash
    c123def456.../                 # Function directory (remaining hash chars)
      object.json                  # Core function data
      eng/                         # Language code directory
        xy/                        # First 2 chars of mapping hash
          z789.../                 # Mapping directory (remaining hash chars)
            mapping.json           # Language mapping
      fra/                         # Another language
        mn/
          opqr.../
            mapping.json
```

### Path Components

| Component | Description | Example |
|-----------|-------------|---------|
| `pool/` | Root pool directory | `~/.local/bb/pool/` |
| `ab/` | First 2 chars of function hash | `ab/` for hash `abc123...` |
| `c123.../` | Remaining 62 chars of function hash | Directory name |
| `object.json` | Function data file | Always this name |
| `eng/` | Language code (ISO 639-3 or custom) | Up to 256 characters |
| `xy/` | First 2 chars of mapping hash | Enables sharding |
| `z789.../` | Remaining 62 chars of mapping hash | Directory name |
| `mapping.json` | Mapping data file | Always this name |

## File Formats

### object.json

Core function data, language-independent.

```json
{
  "schema_version": 1,
  "hash": "abc123def456...",
  "normalized_code": "def _bb_v_0(_bb_v_1):\n    \"\"\"Docstring\"\"\"\n    return _bb_v_1 * 2",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "username"
  }
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | integer | Yes | Always `1` for v1 format |
| `hash` | string | Yes | 64-character hex SHA256 hash |
| `normalized_code` | string | Yes | AST-normalized Python code with placeholder docstring |
| `metadata.created` | string | Yes | ISO 8601 timestamp |
| `metadata.author` | string | Yes | Author from `$USER` or `$USERNAME` |

### mapping.json

Language-specific naming and documentation.

```json
{
  "docstring": "Calculate the average of a list of numbers",
  "name_mapping": {
    "_bb_v_0": "calculate_average",
    "_bb_v_1": "numbers",
    "_bb_v_2": "total"
  },
  "alias_mapping": {
    "abc123def456...": "helper_function"
  },
  "comment": "Formal mathematical terminology"
}
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `docstring` | string | Yes | Language-specific docstring |
| `name_mapping` | object | Yes | Normalized name → original name |
| `alias_mapping` | object | Yes | Hash → import alias (for bb imports) |
| `comment` | string | Yes | Variant description (can be empty) |

## Hash Computation

### Function Hash

The function hash is computed from **normalized code WITHOUT docstring**:

```python
# 1. Normalize AST (rename variables, sort imports, etc.)
# 2. Remove docstring from normalized code
# 3. Compute SHA256
hash_value = hashlib.sha256(code_without_docstring.encode('utf-8')).hexdigest()
```

This ensures functions with identical logic but different docstrings produce the same hash.

### Mapping Hash

The mapping hash is computed from a canonical JSON representation:

```python
canonical = {
    'alias_mapping': alias_mapping,
    'comment': comment,
    'docstring': docstring,
    'name_mapping': name_mapping
}
json_str = json.dumps(canonical, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
mapping_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
```

**Canonical JSON rules:**
- Sorted keys (`sort_keys=True`)
- Unicode preserved (`ensure_ascii=False`)
- No whitespace (`separators=(',', ':')`)
- UTF-8 encoding

## Normalization

### Variable Naming

All user-defined names are renamed to `_bb_v_N`:

| Original | Normalized |
|----------|------------|
| Function name | `_bb_v_0` |
| First parameter | `_bb_v_1` |
| Second parameter | `_bb_v_2` |
| Local variables | `_bb_v_3`, `_bb_v_4`, ... |

### Excluded from Renaming

These names are **never** renamed:
- Python builtins (`len`, `sum`, `print`, `range`, etc.)
- Imported names (`math`, `Counter`, `np`, etc.)
- BB import aliases (tracked separately in `alias_mapping`)

### Import Handling

**Standard imports** are preserved unchanged:
```python
import math
from collections import Counter
```

**BB imports** have aliases removed during normalization:
```python
# Original
from bb.pool import object_abc123 as helper

# Normalized (alias tracked in alias_mapping)
from bb.pool import object_abc123
```

## Multiple Mappings

A single function can have multiple mappings per language:

```
pool/ab/c123.../
  object.json
  eng/
    xy/z789.../mapping.json    # "Formal terminology"
    de/f456.../mapping.json    # "Casual style"
  fra/
    mn/opqr.../mapping.json    # French translation
```

Use cases:
- Formal vs. informal naming conventions
- Domain-specific terminology
- Different coding styles (camelCase vs. snake_case)

The `comment` field distinguishes variants when selecting mappings.

## Language Codes

Language codes can be:
- ISO 639-3 codes: `eng`, `fra`, `spa`, `deu`, etc.
- Custom codes: `eng-formal`, `fra-canadian`, `domain-medical`
- Maximum length: 256 characters

## Storage Location

Default: `$HOME/.local/bb/`

Override with environment variable:
```bash
export BB_DIRECTORY=/custom/path
```

Pool directory: `$BB_DIRECTORY/pool/`

## Validation

A valid v1 function requires:
1. `object.json` exists and is valid JSON
2. `schema_version` equals `1`
3. `hash` matches the directory path
4. At least one language mapping exists
5. Each mapping has all required fields

## Examples

### Simple Function

**Source (`calculate_average.py`):**
```python
def calculate_average(numbers):
    """Calculate the average of a list of numbers"""
    total = sum(numbers)
    return total / len(numbers)
```

**Stored as:**
```
pool/ab/cdef.../
  object.json
  eng/xy/z789.../mapping.json
```

**object.json:**
```json
{
  "schema_version": 1,
  "hash": "abcdef...",
  "normalized_code": "def _bb_v_0(_bb_v_1):\n    \"\"\"Calculate the average of a list of numbers\"\"\"\n    _bb_v_2 = sum(_bb_v_1)\n    return _bb_v_2 / len(_bb_v_1)",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "johndoe"
  }
}
```

**mapping.json:**
```json
{
  "docstring": "Calculate the average of a list of numbers",
  "name_mapping": {
    "_bb_v_0": "calculate_average",
    "_bb_v_1": "numbers",
    "_bb_v_2": "total"
  },
  "alias_mapping": {},
  "comment": ""
}
```

### Multilingual Function

Same logic stored once, with mappings for each language:

```
pool/ab/cdef.../
  object.json          # Single normalized code
  eng/xy/.../mapping.json
  fra/mn/.../mapping.json
  spa/pq/.../mapping.json
```

Each mapping.json contains language-specific names:

**eng/mapping.json:**
```json
{
  "docstring": "Calculate the average",
  "name_mapping": {"_bb_v_0": "calculate_average", "_bb_v_1": "numbers"},
  "alias_mapping": {},
  "comment": ""
}
```

**fra/mapping.json:**
```json
{
  "docstring": "Calculer la moyenne",
  "name_mapping": {"_bb_v_0": "calculer_moyenne", "_bb_v_1": "nombres"},
  "alias_mapping": {},
  "comment": ""
}
```

### Function with BB Import

**Source:**
```python
from bb.pool import object_abc123 as helper

def process(data):
    """Process data using helper"""
    return helper(data) * 2
```

**Normalized code:**
```python
from bb.pool import object_abc123

def _bb_v_0(_bb_v_1):
    """Process data using helper"""
    return object_abc123._bb_v_0(_bb_v_1) * 2
```

**mapping.json:**
```json
{
  "docstring": "Process data using helper",
  "name_mapping": {"_bb_v_0": "process", "_bb_v_1": "data"},
  "alias_mapping": {"abc123...": "helper"},
  "comment": ""
}
```

## Design Rationale

### Why Content-Addressed Storage?

1. **Deduplication**: Identical functions/mappings share storage
2. **Integrity**: Hash serves as checksum
3. **Determinism**: Same input always produces same hash
4. **Distribution**: Easy to sync between pools

### Why Separate Code and Mappings?

1. **Multilingual support**: Same logic, different names
2. **Multiple variants**: Formal/informal styles per language
3. **Smaller syncs**: Add language without re-downloading code

### Why No Hash Algorithm in Path?

The hash algorithm (SHA256) is fixed and doesn't need to be encoded in paths. This keeps paths simpler:
- `pool/ab/cdef.../` instead of `pool/sha256/ab/cdef.../`

If a different algorithm is needed in the future, a new schema version would be created.
