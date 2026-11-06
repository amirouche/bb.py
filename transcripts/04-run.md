# Transcript: Running functions with bb run

## Metadata
- ID: 04
- Feature: bb run
- Date: 2026-02-11
- Status: draft
- Complexity: intermediate

## Overview
`bb run` executes a pool function by name or hash. This transcript builds a pi computation function and runs it from the command line through a small entry point that prints the result.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: The Pi Function

The Leibniz series approximates pi: `pi/4 = 1 - 1/3 + 1/5 - 1/7 + ...`

More iterations produce a closer approximation. The function accepts `n`
as the number of terms to sum. It converts `n` with `int()` so it works
both when called from Python (passing an int) and from the command line
via `bb run` (which passes strings).

## Source Files

### Pi Computation `compute_pi_eng.py`
```python
def compute_pi(n):
    """Compute pi using the Leibniz series with n iterations."""
    n = int(n)
    result = 0.0
    for i in range(n):
        result += ((-1) ** i) / (2 * i + 1)
    return result * 4
```

## Workflow

### Step 1: Add compute_pi
```bash
$ PI_HASH=$(bb add compute_pi_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

---

# Chapter 2: Running from the Command Line

`bb run` executes a pool function by name or hash. Arguments arrive as
strings, which is why `compute_pi` calls `int(n)`.

`bb run` does not print the return value: output is the function's own
job. `compute_pi` returns a number so it stays easy to test, so a small
entry-point function prints the result.

## Source Files

### Print Pi `print_pi_eng.py`
```python
import bb
compute_pi = bb.ref("ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015")


def print_pi(n):
    """Print pi computed with n iterations."""
    print(compute_pi(n))
```

## Workflow

### Step 2: Add the entry point
```bash
$ PRINT_HASH=$(bb add print_pi_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 3: Run by name
```bash
# bb only runs or compiles code you have reviewed
$ yes | bb review print_pi@eng > /dev/null
$ bb run print_pi@eng 3
3.466666666666667
```

### Step 4: Run by hash
```bash
# bb only runs or compiles code you have reviewed
$ yes | bb review $PRINT_HASH@eng > /dev/null
$ bb run $PRINT_HASH@eng 100
3.1315929035585537
```

### Step 5: Calling compute_pi directly prints nothing
```bash
# bb only runs or compiles code you have reviewed
$ yes | bb review compute_pi@eng > /dev/null
$ bb run compute_pi@eng 3 | wc -c
0
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb add file@lang` | Add a function to the pool |
| `bb run (name\|hash)@lang args...` | Execute a pool function |

## Key Insights

1. **`bb run` accepts hash or name**: Both `bb run print_pi@eng 3` and `bb run HASH@eng 3` work. String arguments pass through to the function unchanged, and the return value is not printed.

2. **Output is the function's job**: Keep computations returning values so they stay easy to test, and add a small entry point when you want to print.
