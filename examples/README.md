# Ouverture Examples

Learn by doing! Copy and paste these commands to see how ouverture works.

## Quick Start

### 1. Add functions to the pool

```bash
# Add a simple function (English)
python3 ouverture.py add examples/example_simple.py@eng

# Add the same function in French (same code, different docstring)
python3 ouverture.py add examples/example_simple_french.py@fra

# Add the same function in Spanish (same code, different docstring)
python3 ouverture.py add examples/example_simple_spanish.py@spa
```

**Expected output:**
```
Function saved (v1): /root/.local/ouverture/objects/sha256/b4/f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf/object.json
Hash: b4f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf
Mapping saved (v1): /root/.local/ouverture/objects/sha256/b4/.../eng/sha256/ab/80a39719e18c484a7b4f2394c0431e238eb93e8d7257a1ce3515a7b705d8b1/mapping.json
Language: eng
Mapping hash: ab80a39719e18c484a7b4f2394c0431e238eb93e8d7257a1ce3515a7b705d8b1
```

All three versions share the same **function hash** but have different **mapping hashes**!

### 2. View the function

```bash
# Show the English version
python3 ouverture.py show b4f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf@eng

# Show the French version (same hash, different docstring)
python3 ouverture.py show b4f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf@fra
```

**Expected output (English):**
```python
def calculate_sum(first_number, second_number):
    """Calculate the sum of two numbers."""
    result = first_number + second_number
    return result
```

**Expected output (French):**
```python
def calculate_sum(first_number, second_number):
    """Calculer la somme de deux nombres."""
    result = first_number + second_number
    return result
```

The French version has the same variable names but a French docstring!

### 3. Explore the function pool

```bash
# List all stored functions and mappings
find ~/.local/ouverture/objects -name "*.json"
```

**Expected output:**
```
/root/.local/ouverture/objects/sha256/b4/f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf/object.json
/root/.local/ouverture/objects/sha256/b4/f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf/eng/sha256/ab/80a39719e18c484a7b4f2394c0431e238eb93e8d7257a1ce3515a7b705d8b1/mapping.json
/root/.local/ouverture/objects/sha256/b4/f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf/fra/sha256/e1/474c45d29c2f3c825082a7f240a2cecd335df9f5375391ec309451705bb98f/mapping.json
/root/.local/ouverture/objects/sha256/b4/f52910fb4b02ce1d65269bd404a5fcf66451f79d28e0094303f9e66f1e6faf/spa/sha256/a0/9ad2e8f3c5fa4065d05bcea5678eba7607c32827c0911acb6f69851a26cf96/mapping.json
```

**What's in `~/.local/ouverture/`?**
```
~/.local/ouverture/objects/
  sha256/                    # Hash algorithm
    b4/                      # First 2 chars of function hash
      f52910fb4b.../         # Full function hash (directory)
        object.json          # Normalized function code
        eng/                 # English language mappings
          sha256/ab/80a39.../mapping.json
        fra/                 # French language mappings
          sha256/e1/474c.../mapping.json
        spa/                 # Spanish language mappings
          sha256/a0/9ad2.../mapping.json
```

- One `object.json` per function (stores normalized code)
- Separate `mapping.json` files for each language variant (stores variable names and docstrings)
- Content-addressed: identical logic = same function hash, identical mappings = same mapping hash

## Try More Examples

### Function with imports

```bash
# Add a function that uses the standard library
python3 ouverture.py add examples/example_with_import.py@fra
```

The `show` command will reveal that imported names (like `Counter`) are NOT renamed:

```bash
python3 ouverture.py show <HASH>@fra
```

### Function calling another ouverture function

```bash
# Add a function that calls another function from the pool
python3 ouverture.py add examples/example_with_ouverture.py@spa
```

View it to see how ouverture handles function composition:

```bash
python3 ouverture.py show <HASH>@spa
```

## What Just Happened?

When you add a function, ouverture:

1. **Normalizes** the code: Variables get renamed to `_ouverture_v_0`, `_ouverture_v_1`, etc.
2. **Computes a function hash** based on logic (not variable names or docstrings)
3. **Stores** the normalized code in `object.json`
4. **Computes a mapping hash** based on variable names and docstring
5. **Stores** the language-specific mapping in `mapping.json`

The magic:
- Same logic → same **function hash** (shared across languages)
- Same variable names + docstring → same **mapping hash** (deduplication)

## Clean Up

To start fresh and remove all stored functions:

```bash
rm -rf ~/.local/ouverture
```

## Learn More

- Run `python3 ouverture.py --help` to see all commands
- Check out the main README.md for the full project documentation
