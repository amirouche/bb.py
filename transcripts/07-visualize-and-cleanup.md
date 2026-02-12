# Transcript: Visualizing Dependencies and Cleanup

## Metadata
- ID: 07
- Feature: bb tree, rm
- Date: 2026-02-12
- Status: draft
- Complexity: intermediate

## Overview

This transcript demonstrates pool maintenance commands through a realistic scenario: building a calculator library, visualizing its dependency structure with `bb tree`, then cleaning up by removing functions with `bb rm`.

The story shows:
- Building a three-level dependency hierarchy (constants → operations → calculator)
- Visualizing the dependency tree with `bb tree`
- Removing specific language mappings with `bb rm HASH@lang`
- Removing entire functions with `bb rm HASH`
- Impact of removal on dependent functions

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)
- Clean pool for testing

---

# Chapter 1: Building a Calculator Library

We'll build a simple calculator with a clear dependency hierarchy:

```
calculator_main
    ├── add_numbers
    │   └── get_pi
    └── multiply_numbers
        └── get_pi
```

Both `add_numbers` and `multiply_numbers` depend on `get_pi` (a constant provider), and `calculator_main` orchestrates them.

## Source Files

### Mathematical Constant `get_pi_eng.py`
```python
def get_pi():
    """Return the value of pi."""
    return 3.14159
```

### Mathematical Constant (French) `get_pi_fra.py`
```python
def obtenir_pi():
    """Retourner la valeur de pi."""
    return 3.14159
```

### Addition Operation `add_numbers_eng.py`
```python
from bb.pool import object_110c5390f421d6505106542ade387e1bfd34618b4cc9eba70d8ac07d32f8c039 as get_pi

def add_numbers(a, b):
    """Add two numbers and a constant pi."""
    return a + b + get_pi._bb_v_0()
```

### Multiplication Operation `multiply_numbers_eng.py`
```python
from bb.pool import object_110c5390f421d6505106542ade387e1bfd34618b4cc9eba70d8ac07d32f8c039 as get_pi

def multiply_numbers(a, b):
    """Multiply two numbers by pi."""
    return a * b * get_pi._bb_v_0()
```

### Calculator Main `calculator_main_eng.py`
```python
from bb.pool import object_7d6a6678b904949efee273af50a807c1f98d8aee6346226bc5a57f09ea966335 as add_numbers
from bb.pool import object_eff47a2b6bf77dd428e60e2edc5d15a084aab8602cd33a122f7e9c49bffedb85 as multiply_numbers

def calculator_main(x, y):
    """Perform calculations using add and multiply."""
    sum_result = add_numbers._bb_v_0(x, y)
    product_result = multiply_numbers._bb_v_0(x, y)
    return sum_result, product_result
```

## Workflow

### Step 1: Add the constant function (leaf)
```bash
$ PI_HASH=$(bb add get_pi_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 2: Add French translation of get_pi
```bash
$ PI_HASH_2=$(bb add get_pi_fra.py@fra | grep '^Hash:' | awk '{print $2}')
$ test "$PI_HASH" = "$PI_HASH_2" && echo "Same hash!"
Same hash!
```

### Step 3: Verify get_pi has both languages
```bash
$ bb log | grep "^Languages:" | head -1
Languages: eng, fra
```

### Step 4: Add addition operation
```bash
$ ADD_HASH=$(bb add add_numbers_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 5: Add multiplication operation
```bash
$ MUL_HASH=$(bb add multiply_numbers_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 6: Add calculator main
```bash
$ CALC_HASH=$(bb add calculator_main_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 7: Verify all functions are in the pool
```bash
$ bb log | grep "^Hash:" | wc -l
4
```

---

# Chapter 2: Visualizing Dependencies with `bb tree`

The `bb tree` command displays the dependency graph in a tree format, showing which functions depend on which. It uses your preferred language from `bb whoami language` to display function names.

### Step 8: Set language preference
```bash
$ bb whoami language eng
Set language: eng
```

### Step 9: Show dependency tree for calculator_main
```bash
$ bb tree $CALC_HASH | head -5
calculator_main — Perform calculations using add and multiply.
├── add_numbers — Add two numbers and a constant pi.
│   └── get_pi — Return the value of pi.
└── multiply_numbers — Multiply two numbers by pi.
    └── get_pi — Return the value of pi.
```

The tree shows the complete dependency graph with docstring summaries: `calculator_main` depends on `add_numbers` and `multiply_numbers`, both of which depend on `get_pi`.

### Step 10: Show tree for a mid-level function
```bash
$ bb tree $ADD_HASH | head -2
add_numbers — Add two numbers and a constant pi.
└── get_pi — Return the value of pi.
```

Only one level of dependencies shown.

### Step 11: Show tree for a leaf function (no dependencies)
```bash
$ bb tree $PI_HASH
get_pi — Return the value of pi.
```

Leaf functions have no dependencies, so the tree is just the function name.

---

# Chapter 3: Removing Language Mappings

Sometimes you want to remove a specific language translation while keeping others. Use `bb rm HASH@lang` to remove just one language mapping.

### Step 12: Remove French translation of get_pi
```bash
$ bb rm $PI_HASH@fra | head -1
Removed language mapping: $PI_HASH@fra
```

### Step 13: Verify only English remains
```bash
$ bb log | grep "^Languages:" | head -1
Languages: eng
```

The French mapping is gone, but the English version and the function itself remain.

### Step 14: Verify the function still works
```bash
$ bb show $PI_HASH@eng | head -1
def get_pi():
```

The function is still accessible in English.

---

# Chapter 4: Removing Entire Functions

When you want to completely remove a function from the pool, use `bb rm HASH` (without the language suffix). This removes the function and all its language mappings.

### Step 15: Remove get_pi (creates broken dependencies)
```bash
$ bb rm $PI_HASH | head -1
Removed function: $PI_HASH
```

### Step 16: Verify get_pi is gone
```bash
$ bb log | grep "^Hash:" | wc -l
3
```

Only 3 functions remain: calculator_main, add_numbers, and multiply_numbers.

### Step 17: Note that dependent functions are broken now
```bash
$ (bb tree $ADD_HASH 2>&1 || true) | tail -2
add_numbers — Add two numbers and a constant pi.
└── 110c5390f421... (not found)
```

The tree shows that `add_numbers` depends on a missing function (hash shown, marked "not found"). This demonstrates that `bb rm` doesn't prevent you from breaking dependency chains—it's your responsibility to check callers first.

---

# Chapter 5: Cleaning Up the Rest

Let's clean up the remaining functions to demonstrate complete removal.

### Step 18: Remove multiply_numbers
```bash
$ bb rm $MUL_HASH | head -1
Removed function: $MUL_HASH
```

### Step 19: Remove add_numbers
```bash
$ bb rm $ADD_HASH | head -1
Removed function: $ADD_HASH
```

### Step 20: Remove calculator_main
```bash
$ bb rm $CALC_HASH | head -1
Removed function: $CALC_HASH
```

### Step 21: Verify pool is empty
```bash
$ bb log
Function Pool Log (0 functions)
================================================================================

```

All functions removed successfully.

---

## Summary

| Command | Purpose | What We Did |
|---------|---------|-------------|
| `bb tree <hash>` | Visualize dependency tree | Showed 3-level hierarchy |
| `bb tree <name@lang>` | Tree by name | Alternative to hash |
| `bb rm <hash@lang>` | Remove language mapping | Removed French, kept English |
| `bb rm <hash>` | Remove entire function | Removed 4 functions completely |

## Key Insights

1. **`bb tree` uses preferred language**: The command respects your `bb whoami language` setting to display function names. If a function doesn't have your preferred language, it falls back to the first available language.

2. **Tree shows full dependency depth**: Unlike `bb log` which just lists functions, `bb tree` recursively shows all dependencies, making it easy to understand the impact of changes.

3. **`bb rm` has two modes**:
   - `bb rm HASH@lang` - Removes only the specified language mapping, keeps other languages and the function
   - `bb rm HASH` - Removes the function entirely with all language mappings

4. **`bb rm` doesn't check dependencies**: The tool will let you remove functions that other functions depend on, creating broken dependency chains. Use `bb caller` before removal to check if anything depends on the function.

5. **Broken dependencies are visible**: When you run `bb tree` on a function with missing dependencies, it shows `[missing: hash...]` to indicate the problem.

6. **Removal is immediate and destructive**: There's no undo for `bb rm`. The function data is deleted from the pool directory. Always double-check before removing, especially for functions with many dependents.

## Workflow Recommendations

Before removing a function:
1. **Check callers**: `bb caller HASH` to see what depends on it
2. **Visualize impact**: `bb tree <each-caller>` to understand the full impact
3. **Consider language-only removal**: If you just want to remove a translation, use `bb rm HASH@lang`

## Notes

- `bb tree` output format is similar to Unix `tree` command
- Removed functions are permanently deleted from `$BB_DIRECTORY/pool/`
- There's no recycle bin or undo for `bb rm`
- `bb tree` requires at least one language mapping for the function
