# Ouverture Examples

Learn by doing! Copy and paste these commands to see how ouverture works.

## Quick Start

### 1. Add functions to the pool

```bash
# Add a simple function (English)
python3 ouverture.py add examples/example_simple.py@eng

# Add the same function in French
python3 ouverture.py add examples/example_simple_french.py@fra

# Add the same function in Spanish
python3 ouverture.py add examples/example_simple_spanish.py@spa
```

**Expected output:**
```
Function added: b4f52910a8c7d3e2f1a6b9c4d5e8f7a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6
```

All three versions share the same hash because they have identical logic!

### 2. View the function

```bash
# Show the English version
python3 ouverture.py show b4f52910a8c7d3e2f1a6b9c4d5e8f7a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6@eng

# Show the French version (same hash, different names)
python3 ouverture.py show b4f52910a8c7d3e2f1a6b9c4d5e8f7a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6@fra
```

**Expected output (English):**
```python
def calculate_sum(a, b):
    """Calculate the sum of two numbers."""
    result = a + b
    return result
```

**Expected output (French):**
```python
def calculer_somme(a, b):
    """Calculer la somme de deux nombres."""
    resultat = a + b
    return resultat
```

### 3. Explore the function pool

```bash
# List all stored functions
find ~/.local/ouverture/objects -name "*.json"
```

**Expected output:**
```
/home/user/.local/ouverture/objects/b4/f52910a8c7d3e2f1a6b9c4d5e8f7a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6.json
```

**What's in `~/.local/ouverture/`?**
- `objects/XX/YYYYYY.json` - Functions stored by their hash
- Each function can have multiple language variants (English, French, Spanish, etc.)
- The hash is based on logic, not variable names or comments

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
2. **Computes a hash** based on logic (not variable names or comments)
3. **Stores** the function in `~/.local/ouverture/objects/XX/YYYYYY.json`
4. **Saves** language-specific names so you can retrieve it in any language

The magic: same logic → same hash, even across different human languages!

## Clean Up

To start fresh and remove all stored functions:

```bash
rm -rf ~/.local/ouverture
```

## Learn More

- Run `python3 ouverture.py --help` to see all commands
- Check out the main README.md for the full project documentation
