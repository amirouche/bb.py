# Testing ouverture

## Quick Start

### Run the examples:

```bash
# Add a simple function (English names)
python3 ouverture.py add examples/example_simple.py@eng

# Add a function with imports (French names)
python3 ouverture.py add examples/example_with_import.py@fra

# Add a function with ouverture imports (Spanish names)
python3 ouverture.py add examples/example_with_ouverture.py@spa
```

### View the results:

```bash
# List all normalized functions
find .ouverture/objects -name "*.json"

# View the normalized code and mappings
cat .ouverture/objects/da/93b591dac76f2b71be05a0479b2eed1cb7052e106ecd231b272123b0451468.json | python3 -m json.tool
```

## Create Your Own Test

**Example 1: Simple function**

```python
# my_function.py
def calculate_area(width, height):
    """Calculate rectangle area."""
    area = width * height
    return area
```

```bash
python3 ouverture.py add my_function.py@eng
```

**Expected result:**
- Function renamed to `_ouverture_v_0`
- Variables renamed to `_ouverture_v_1`, `_ouverture_v_2`, etc.
- JSON file created with hash and mappings

**Example 2: Function with imports**

```python
# my_math.py
import math

def calculate_circle_area(radius):
    """Calculate circle area."""
    result = math.pi * radius ** 2
    return result
```

```bash
python3 ouverture.py add my_math.py@eng
```

**Expected result:**
- Import statement preserved and sorted
- `math` is NOT renamed (it's imported)
- Local variables ARE renamed

**Example 3: Function calling another ouverture function**

```python
# my_compose.py
from ouverture import abc123def as helper

def process_data(items):
    """Process items using helper."""
    cleaned = helper(items)
    return cleaned
```

```bash
python3 ouverture.py add my_compose.py@eng
```

**Expected result:**
- Import rewritten: `from couverture import abc123def`
- Call rewritten: `helper(items)` → `abc123def._ouverture_v_0(items)`
- Alias mapping stored: `{"abc123def": "helper"}`

## Test Error Handling

```bash
# Missing @lang suffix
python3 ouverture.py add examples/example_simple.py
# Error: Missing language suffix

# Invalid language code (not 3 chars)
python3 ouverture.py add examples/example_simple.py@en
# Error: Language code must be 3 characters

# File not found
python3 ouverture.py add nonexistent.py@eng
# Error: File not found
```

## Understanding the JSON Output

Each function is stored as JSON with support for **multiple languages**:

```json
{
  "version": 0,
  "hash": "b4f52910...",
  "normalized_code": "def _ouverture_v_0(_ouverture_v_1, _ouverture_v_3):...",
  "docstrings": {
    "eng": "Calculate the sum of two numbers.",
    "fra": "Calculer la somme de deux nombres.",
    "spa": "Calcular la suma de dos números."
  },
  "name_mappings": {
    "eng": {
      "_ouverture_v_0": "calculate_sum",
      "_ouverture_v_1": "first_number",
      "_ouverture_v_2": "result"
    },
    "fra": { ... },
    "spa": { ... }
  },
  "alias_mappings": {
    "eng": {},
    "fra": {},
    "spa": {}
  }
}
```

Key features:

- **hash**: SHA256 of the normalized code **WITHOUT docstring** (so same logic = same hash)
- **normalized_code**: AST unparsed after normalization (includes docstring for display)
- **docstrings**: Language-specific docstrings extracted separately
- **name_mappings**: Per-language mapping from normalized names to originals
- **alias_mappings**: Per-language mapping of ouverture function hashes to their aliases

**Important**: The hash is computed on code **without the docstring**. This means:
- Same function logic in different languages → **same hash**
- Multiple language versions stored in one JSON file
- Each language has its own docstring and name mappings

## Testing Multilingual Support

You can add the same function in multiple languages:

```bash
# Add English version
python3 ouverture.py add examples/example_simple.py@eng

# Add French version (same logic, different docstring)
python3 ouverture.py add examples/example_simple_french.py@fra

# Add Spanish version
python3 ouverture.py add examples/example_simple_spanish.py@spa
```

All three versions will be stored in the **same JSON file** because they have the same hash (same logic). Check the file to see all three docstrings:

```bash
find .ouverture/objects -name "*.json" | head -1 | xargs cat | python3 -m json.tool
```

## Verify Normalization

You can verify the normalization by checking:

1. **Imports are sorted**: `from collections` before `import math`
2. **Function is `_ouverture_v_0`**: Main function always gets index 0
3. **Variables are sequential**: `_ouverture_v_1`, `_ouverture_v_2`, etc.
4. **Built-ins preserved**: `sum`, `len`, `print` are NOT renamed
5. **Imports preserved**: Imported names like `math`, `Counter` are NOT renamed

## Clean Up

To start fresh:

```bash
rm -rf .ouverture
```

Then re-run the add commands to regenerate the function pool.
