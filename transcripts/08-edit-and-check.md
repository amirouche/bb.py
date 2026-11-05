# Transcript: Editing a function with its checks

## Metadata
- ID: 08
- Feature: bb edit, bb check
- Date: 2026-11-05
- Status: draft
- Complexity: intermediate

## Overview
`bb edit` opens a function and its checks together in one markdown file. A check is a function that takes the implementation as its argument, so the same checks keep working when the implementation changes. Saving only happens when every check passes, and the new version records the one it was edited from as its parent. This transcript attaches checks to a pi function, shows a broken edit being refused, saves a rewrite that passes, and forks a check on its own.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: The Pi Function

The Leibniz series approximates pi: `pi/4 = 1 - 1/3 + 1/5 - 1/7 + ...`.
We start with the plain loop version and no checks.

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
$ bb add compute_pi_eng.py@eng
Hash: ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015
Mapping hash: 8987e90603c35197685ba70ead7c6a845cd7d1f64edf421eb350be3e7a372bd4
```

### Step 2: Nothing is attached yet
```bash
$ bb check compute_pi@eng
No checks.
```

---

# Chapter 2: Attaching Checks

`bb edit compute_pi@eng` writes the function into a markdown file and
opens `$EDITOR` on it. The file has two sections. `## Implementation`
holds prose and the function itself, in the one block fenced as
```` ```python def ````. `## Checks` has one `###` subsection per check,
built the same way.

Each check receives the implementation as its only argument. It never
imports a hash, so it is not tied to one version of `compute_pi`.

Here the editor is replaced by `cp`, which drops in a file we prepared:
the same code, a paragraph of documentation, and five checks using the
Fibonacci numbers up to 10 (1, 2, 3, 5, 8) as `n`.

## Source Files

### Edit file with checks `pi_checks.md`
````markdown
# compute_pi

## Implementation

The Leibniz series approximates pi: `pi/4 = 1 - 1/3 + 1/5 - 1/7 + ...`
More terms give a closer approximation, alternating above and below pi.

```python def
def compute_pi(n):
    """Compute pi using the Leibniz series with n iterations."""
    n = int(n)
    result = 0.0
    for i in range(n):
        result += ((-1) ** i) / (2 * i + 1)
    return result * 4
```

## Checks

### check_pi_n1

One term of the series is exactly 4.

```python def
def check_pi_n1(compute_pi):
    """Check pi with 1 iteration equals 4."""
    return compute_pi(1) == 4.0
```

### check_pi_n2

Even iterations undershoot.

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    return compute_pi(2) < 3.0
```

### check_pi_n3

Odd iterations overshoot.

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    return compute_pi(3) > 3.0
```

### check_pi_n5

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    return 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    return 3.0 < compute_pi(8) < 3.3
```
````

## Workflow

### Step 3: Save the checks
The checks are part of a function's definition, so attaching them gives
`compute_pi` a new hash: a fork whose parent is the version without
checks. The five checks are added to the pool too. The documentation
paragraph is not part of the hash; it is stored in the English mapping.

```bash
$ EDITOR="cp pi_checks.md" bb edit compute_pi@eng -m "Attach the Leibniz checks"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Hash: 5d30e8bcc5b84de4afc63a87f701eeffd51d335c4435a1ba91e385430c98c885
Parent: ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015
Check: 1a98a8c92e2e8401f5b39ea51a5a3729eb756882860c54e8ec5c653fc6b5e620 check_pi_n1
Check: a658651655db54709ff2e2490f4ba96ebfd3ccbf27b45e043079177ad365ccd7 check_pi_n2
Check: 6ace86da1614319cf367ee008158c90486c224f0c9282f570d1dde66683a8b26 check_pi_n3
Check: 4487a0ba57b01c388b6edc53718257681a237607e616e3e765afa0b496159892 check_pi_n5
Check: 451a1d221bbeb6e40daa71aedbb82fea0a94eb3738dfa59c3b0aef94faa51025 check_pi_n8
```

### Step 4: Run them any time
```bash
$ bb check compute_pi@eng
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
```

---

# Chapter 3: A Broken Edit Is Refused

Someone "optimizes" the loop to start at 1, skipping the first term.
Four checks catch it. Nothing is written to the pool: in a terminal,
`bb edit` would offer to reopen the file; here there is no terminal, so
it stops and keeps the edit under `$BB_DIRECTORY/edit/`.

## Source Files

### Broken edit `pi_broken.md`
````markdown
# compute_pi

## Implementation

The Leibniz series approximates pi: `pi/4 = 1 - 1/3 + 1/5 - 1/7 + ...`
More terms give a closer approximation, alternating above and below pi.

```python def
def compute_pi(n):
    """Compute pi using the Leibniz series with n iterations."""
    n = int(n)
    result = 0.0
    for i in range(1, n):
        result += ((-1) ** i) / (2 * i + 1)
    return result * 4
```

## Checks

### check_pi_n1

```python def
def check_pi_n1(compute_pi):
    """Check pi with 1 iteration equals 4."""
    return compute_pi(1) == 4.0
```

### check_pi_n2

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    return compute_pi(2) < 3.0
```

### check_pi_n3

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    return compute_pi(3) > 3.0
```

### check_pi_n5

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    return 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    return 3.0 < compute_pi(8) < 3.3
```
````

## Workflow

### Step 5: Try to save the broken version
```bash
$ (EDITOR="cp pi_broken.md" bb edit compute_pi@eng -m "Skip the first term" 2>/dev/null || true)
FAIL check_pi_n1
PASS check_pi_n2
FAIL check_pi_n3
FAIL check_pi_n5
FAIL check_pi_n8
5 checks: 1 passed, 4 failed
```

### Step 6: The name still points to the checked version
```bash
$ bb latest compute_pi eng | grep Hash
Hash: 5d30e8bcc5b84de4afc63a87f701eeffd51d335c4435a1ba91e385430c98c885
```

---

# Chapter 4: A Rewrite That Passes

The same series written as one `sum` over a generator. The logic is
different, so the hash is new. Every check passes, so it is saved, and
it records the checked loop version as its parent. The checks come
along unchanged: they never depended on the old hash.

## Source Files

### Rewrite `pi_sum.md`
````markdown
# compute_pi

## Implementation

The Leibniz series approximates pi: `pi/4 = 1 - 1/3 + 1/5 - 1/7 + ...`
More terms give a closer approximation, alternating above and below pi.

```python def
def compute_pi(n):
    """Compute pi using the Leibniz series with n iterations."""
    n = int(n)
    return 4 * sum((-1) ** i / (2 * i + 1) for i in range(n))
```

## Checks

### check_pi_n1

<!-- bb:parent 1a98a8c92e2e8401f5b39ea51a5a3729eb756882860c54e8ec5c653fc6b5e620 -->

One term of the series is exactly 4.

```python def
def check_pi_n1(compute_pi):
    """Check pi with 1 iteration equals 4."""
    return compute_pi(1) == 4.0
```

### check_pi_n2

<!-- bb:parent a658651655db54709ff2e2490f4ba96ebfd3ccbf27b45e043079177ad365ccd7 -->

Even iterations undershoot.

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    return compute_pi(2) < 3.0
```

### check_pi_n3

<!-- bb:parent 6ace86da1614319cf367ee008158c90486c224f0c9282f570d1dde66683a8b26 -->

Odd iterations overshoot.

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    return compute_pi(3) > 3.0
```

### check_pi_n5

<!-- bb:parent 4487a0ba57b01c388b6edc53718257681a237607e616e3e765afa0b496159892 -->

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    return 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

<!-- bb:parent 451a1d221bbeb6e40daa71aedbb82fea0a94eb3738dfa59c3b0aef94faa51025 -->

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    return 3.0 < compute_pi(8) < 3.3
```
````

## Workflow

### Step 7: Save the rewrite
```bash
$ EDITOR="cp pi_sum.md" bb edit compute_pi@eng -m "Sum a generator instead of looping"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Hash: 1f04afa267814a78a700bfaacf4dbe5858817e14913016503d4f2804ad4bd670
Parent: 5d30e8bcc5b84de4afc63a87f701eeffd51d335c4435a1ba91e385430c98c885
```

### Step 8: The name now points to the rewrite, which carries the same checks
A version that was forked is never the latest, so `compute_pi@eng`
resolves to the rewrite.

```bash
$ bb latest compute_pi eng | grep Hash
Hash: 1f04afa267814a78a700bfaacf4dbe5858817e14913016503d4f2804ad4bd670
$ bb check compute_pi@eng
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
```

### Step 9: Rewording the documentation is not a fork
Only the prose changes, so the hash stays the same. bb writes a new
English mapping whose parent is the previous one, and `bb show` uses it.

```bash
$ sed 's/More terms give a closer approximation/Each extra term gets closer/' pi_sum.md > pi_doc.md
$ EDITOR="cp pi_doc.md" bb edit 1f04afa267814a78a700bfaacf4dbe5858817e14913016503d4f2804ad4bd670@eng -m "Reword the documentation"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Hash: 1f04afa267814a78a700bfaacf4dbe5858817e14913016503d4f2804ad4bd670
$ bb show 1f04afa267814a78a700bfaacf4dbe5858817e14913016503d4f2804ad4bd670@eng | head -1
def compute_pi(n):
```

---

# Chapter 5: Forking a Check on Its Own

A check is a pool function too, so `bb edit` works on it directly. Its
file has its own `## Implementation` and an empty `## Checks`. The fork
records the old check as its parent.

A check forked alone is attached to nothing: `compute_pi` still lists
the original `check_pi_n8`. To use the new one, paste it into the Checks
section of `bb edit compute_pi@eng`.

## Source Files

### Tighter check `n8_tighter.md`
````markdown
# check_pi_n8

## Implementation

Eight terms already land within 0.15 of pi.

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is within 0.15 of pi."""
    return abs(compute_pi(8) - 3.14159) < 0.15
```

## Checks
````

## Workflow

### Step 10: Fork the check
```bash
$ EDITOR="cp n8_tighter.md" bb edit check_pi_n8@eng -m "Tighter bound"
No checks.
Hash: e7dd543932d426965adc4081829bdb0ff3b99e05838e9dfb78c07fa99198706f
Parent: 451a1d221bbeb6e40daa71aedbb82fea0a94eb3738dfa59c3b0aef94faa51025
```

### Step 11: Editing the fork again shows where it came from
```bash
$ EDITOR=cat bb edit e7dd543932d426965adc4081829bdb0ff3b99e05838e9dfb78c07fa99198706f@eng | grep bb:parent
<!-- bb:parent e7dd543932d426965adc4081829bdb0ff3b99e05838e9dfb78c07fa99198706f -->
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb edit (name\|hash)@lang` | Edit a function and its checks in one markdown file, save a fork when all checks pass |
| `bb edit ... -m "comment"` | Give the comment up front instead of being asked |
| `bb check (name\|hash)@lang` | Run the checks attached to a function |

## Key Insights

1. **Checks take the implementation as an argument**: They never import the function they test, so they keep working across every fork.

2. **Checks are part of the definition**: A function's hash covers its code and its check hashes. Changing the checks forks the function; changing only the documentation does not.

3. **Saving is gated**: The file must parse and every check must pass before anything reaches the pool.

4. **Lineage is recorded**: A new version stores the edited one as `parent`, and each `<!-- bb:parent HASH -->` marker tells bb which check a section came from.

5. **Documentation lives in the mapping**: Prose and examples around the `python def` block are stored per language, outside the hash, like docstrings.

6. **A check forked alone applies to nothing** until it is pasted into a function's Checks section.
