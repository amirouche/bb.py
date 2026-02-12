# Transcript: Testing bb check

## Metadata
- ID: 04
- Feature: @check decorators, bb run, bb check
- Date: 2026-02-11
- Status: draft
- Complexity: intermediate

## Overview
The `@check` decorator marks a pool function as a test for another function. `bb check` finds and runs all tests for a given function, reporting PASS or FAIL for each. This transcript builds a pi computation function, adds test functions using `@check` decorators with n values from the Fibonacci sequence, demonstrates passing and failing checks, and runs the function from the command line with `bb run`.

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

# Chapter 2: Passing Checks

Each `@check` decorator marks a function as a test for `compute_pi`.
The n values come from the Fibonacci sequence (1, 2, 3, 5, 8) — every
Fibonacci number up to 10.

The Leibniz series alternates between over- and under-estimating pi:
- Odd iterations overshoot (n=1 gives 4.0, n=3 gives ~3.47)
- Even iterations undershoot (n=2 gives ~2.67)

Each check verifies a property of the convergence rather than an exact
floating-point value.

## Source Files

### Check n=1 `check_pi_n1_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_n1():
    """Check pi with 1 iteration equals 4."""
    return compute_pi(1) == 4.0
```

### Check n=2 `check_pi_n2_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_n2():
    """Check pi with 2 iterations underestimates."""
    return compute_pi(2) < 3.0
```

### Check n=3 `check_pi_n3_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_n3():
    """Check pi with 3 iterations overestimates."""
    return compute_pi(3) > 3.0
```

### Check n=5 `check_pi_n5_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_n5():
    """Check pi with 5 iterations is between 3 and 4."""
    return 3.0 < compute_pi(5) < 4.0
```

### Check n=8 `check_pi_n8_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_n8():
    """Check pi with 8 iterations is between 3 and 3.3."""
    return 3.0 < compute_pi(8) < 3.3
```

## Workflow

### Step 2: Add check functions
```bash
$ CHECK_N1=$(bb add check_pi_n1_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ CHECK_N2=$(bb add check_pi_n2_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ CHECK_N3=$(bb add check_pi_n3_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ CHECK_N5=$(bb add check_pi_n5_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

```bash
$ CHECK_N8=$(bb add check_pi_n8_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 3: Run all checks — everything passes
```bash
$ bb check $PI_HASH
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
PASS check_pi_n1
5 checks: 5 passed
```

### Step 4: Also works by name
```bash
$ bb check compute_pi@eng
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
PASS check_pi_n1
5 checks: 5 passed
```

---

# Chapter 3: A Failing Check

Not every check has to pass. Here a check claims that 2 iterations of
the Leibniz series already produce 3.0 — which is wrong (the actual
value is ~2.67). `bb check` reports the failure alongside the passes.

## Source Files

### Wrong Check `check_pi_converged_eng.py`
```python
from bb import check
from bb.pool import object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015 as compute_pi


@check(object_ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015)
def check_pi_converged():
    """Check pi with 2 iterations equals 3 (wrong on purpose)."""
    return compute_pi(2) == 3.0
```

## Workflow

### Step 5: Add the wrong check
```bash
$ CHECK_BAD=$(bb add check_pi_converged_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 6: Run checks again — one failure
```bash
$ bb check $PI_HASH
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
PASS check_pi_n1
FAIL check_pi_converged
6 checks: 5 passed, 1 failed
```

---

# Chapter 4: Running from the Command Line

`bb run` executes a pool function by name or hash. Arguments arrive as
strings, which is why `compute_pi` calls `int(n)`.

## Workflow

### Step 7: Run by name
```bash
$ bb run compute_pi@eng 3
3.466666666666667
```

### Step 8: Run by hash
```bash
$ bb run $PI_HASH@eng 100
3.1315929035585537
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb add file@lang` | Add a function to the pool |
| `bb run (name\|hash)@lang args...` | Execute a pool function |
| `bb check (hash\|name@lang)` | Run all `@check` tests for a function |

## Key Insights

1. **`@check` is metadata, not runtime**: The decorator is a no-op at execution time. Its purpose is parsed from the AST during `bb add` to record test relationships in the function's metadata.

2. **`bb check` runs tests**: It loads each test function, executes it, and reports PASS (returned truthy) or FAIL (returned falsy). Tests that raise exceptions are reported as ERROR.

3. **`bb run` accepts hash or name**: Both `bb run compute_pi@eng 3` and `bb run HASH@eng 3` work. String arguments pass through to the function unchanged.

4. **Fibonacci n values**: Using n = 1, 2, 3, 5, 8 (the Fibonacci numbers up to 10) gives a nice progression that exercises the alternating convergence of the Leibniz series.

5. **Failing checks stay in the pool**: The pool records what is tested, not whether the test passes. A failing check is still a valid pool function.
