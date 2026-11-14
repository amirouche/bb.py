# Transcript: Editing a function with its checks

## Metadata
- ID: 08
- Feature: bb edit, bb review, bb validate --checks
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
Mapping hash: bd77f7e41b89a83d90c9c7c3fd3b966c76485907e9efb2774ef3b7a0efbacc38
```

### Step 2: Nothing is attached yet
```bash
$ bb validate --checks compute_pi@eng
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
    assert compute_pi(1) == 4.0
```

### check_pi_n2

Even iterations undershoot.

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    assert compute_pi(2) < 3.0
```

### check_pi_n3

Odd iterations overshoot.

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    assert compute_pi(3) > 3.0
```

### check_pi_n5

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    assert 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    assert 3.0 < compute_pi(8) < 3.3
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
Check: 2167cba54fcc33557df52a907b2505f99e5353f727553761ce20361d3e9bc8a9 check_pi_n1
Check: 21e213ce99f1901b6fc990357820956883f36ea7b6e6e91b70ac80fb4965e566 check_pi_n2
Check: 1be89fa3f2bcd4945a558592e1a76c858360d104dcf7f7f21941f76359e28e83 check_pi_n3
Check: 38e0166a6c0837246b9a296f565a23deb0e1cc1142ffddedded78307ac601149 check_pi_n5
Check: 5f8de2e0762a54b00e9c4147e65a2a18ec640bcf9bb8bd42a9dc8ec1c7e7bd48 check_pi_n8
Parent: ddda76213f839382c1f0df754b73d47214cf05859da05a14fe690eff32c24015
Hash: 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9
```

### Step 4: Read before running
Pool functions can come from anyone who can commit to the shared
directory, and Python has no sandbox. So bb only runs checks once you
have read and approved everything they would run: the function, each
check, and their dependencies. Approvals are private: they live in your
state directory (`~/.local/state/bb/`), not in the shared `$BB_DIRECTORY`.

```bash
$ (bb validate --checks compute_pi@eng || true)
Not reviewed, checks not run:
  compute_pi 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9 (6 to approve: bb review 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9)
```

`bb review` goes through the dependency order level by level. A
function's checks count as its dependencies, so each check is read and
approved on its own first. Then comes `compute_pi`: you read it with its
checks, bb asks whether to run them on the exact code shown, and only
after the results does it ask you to approve the new hash. Here `yes`
answers every question; the `grep` keeps the headings and results.

```bash
$ yes | bb review compute_pi@eng | grep -E '^\[|PASS|checks:|^Total'
[1/6] Function: check_pi_n3 (eng)
[2/6] Function: check_pi_n1 (eng)
[3/6] Function: check_pi_n2 (eng)
[4/6] Function: check_pi_n5 (eng)
[5/6] Function: check_pi_n8 (eng)
[6/6] Function: compute_pi (eng)
Run its checks? [y/n/q]: PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Total reviewed: 6 function(s)
```

### Step 5: Run every reviewed check in the pool
```bash
$ bb validate --checks
compute_pi 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9
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
    assert compute_pi(1) == 4.0
```

### check_pi_n2

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    assert compute_pi(2) < 3.0
```

### check_pi_n3

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    assert compute_pi(3) > 3.0
```

### check_pi_n5

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    assert 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    assert 3.0 < compute_pi(8) < 3.3
```
````

## Workflow

### Step 6: Try to save the broken version
```bash
$ (EDITOR="cp pi_broken.md" bb edit compute_pi@eng -m "Skip the first term" 2>/dev/null || true)
FAIL check_pi_n1: assertion failed: compute_pi(1) == 4.0 (0.0 == 4.0)
PASS check_pi_n2
FAIL check_pi_n3: assertion failed: compute_pi(3) > 3.0 (-0.5333333333333332 > 3.0)
FAIL check_pi_n5: assertion failed: 3.0 < compute_pi(5) < 4.0
FAIL check_pi_n8: assertion failed: 3.0 < compute_pi(8) < 3.3
5 checks: 1 passed, 4 failed
```

### Step 6b: A check must assert
A check passes when it runs without raising, so it needs at least one `assert`. One that only returns a value could never fail, and is refused.

```bash
$ sed 's/    assert compute_pi(1) == 4.0/    return compute_pi(1) == 4.0/' pi_checks.md > pi_returns.md
$ (EDITOR="cp pi_returns.md" bb edit compute_pi@eng -m "Return instead of assert" 2>&1 || true) | grep '^Error'
Error: check 'check_pi_n1' has no assert: a check passes when it runs without raising, so it needs at least one assert (assert f(2) == 4); rewrite it with bb edit
```

### Step 7: The name still points to the checked version
```bash
$ bb latest compute_pi eng | grep Hash
Hash: 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9
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

<!-- bb:parent 2167cba54fcc33557df52a907b2505f99e5353f727553761ce20361d3e9bc8a9 -->

One term of the series is exactly 4.

```python def
def check_pi_n1(compute_pi):
    """Check pi with 1 iteration equals 4."""
    assert compute_pi(1) == 4.0
```

### check_pi_n2

<!-- bb:parent 21e213ce99f1901b6fc990357820956883f36ea7b6e6e91b70ac80fb4965e566 -->

Even iterations undershoot.

```python def
def check_pi_n2(compute_pi):
    """Check pi with 2 iterations underestimates."""
    assert compute_pi(2) < 3.0
```

### check_pi_n3

<!-- bb:parent 1be89fa3f2bcd4945a558592e1a76c858360d104dcf7f7f21941f76359e28e83 -->

Odd iterations overshoot.

```python def
def check_pi_n3(compute_pi):
    """Check pi with 3 iterations overestimates."""
    assert compute_pi(3) > 3.0
```

### check_pi_n5

<!-- bb:parent 38e0166a6c0837246b9a296f565a23deb0e1cc1142ffddedded78307ac601149 -->

```python def
def check_pi_n5(compute_pi):
    """Check pi with 5 iterations is between 3 and 4."""
    assert 3.0 < compute_pi(5) < 4.0
```

### check_pi_n8

<!-- bb:parent 5f8de2e0762a54b00e9c4147e65a2a18ec640bcf9bb8bd42a9dc8ec1c7e7bd48 -->

```python def
def check_pi_n8(compute_pi):
    """Check pi with 8 iterations is between 3 and 3.3."""
    assert 3.0 < compute_pi(8) < 3.3
```
````

## Workflow

### Step 8: Save the rewrite
```bash
$ EDITOR="cp pi_sum.md" bb edit compute_pi@eng -m "Sum a generator instead of looping"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Parent: 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9
Hash: f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e
```

### Step 9: The name now points to the rewrite, which carries the same checks
A version that was forked is never the latest, so `compute_pi@eng`
resolves to the rewrite.

```bash
$ bb latest compute_pi eng | grep Hash
Hash: f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e
```

The checks are already approved, but the rewrite is new code: only it
needs reading before the checks run again.

```bash
$ (bb validate --checks compute_pi@eng || true)
Not reviewed, checks not run:
  compute_pi f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e (1 to approve: bb review f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e)
$ yes | bb review compute_pi@eng | grep -E '^\[|PASS|checks:|^Total'
[1/1] Function: compute_pi (eng)
Run its checks? [y/n/q]: PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Total reviewed: 7 function(s)
```

### Step 10: Rewording the documentation is not a fork
Only the prose changes, so the hash stays the same. bb writes a new
English mapping, and a `document` lineage step after the previous one,
so `bb show` uses it.

```bash
$ sed 's/More terms give a closer approximation/Each extra term gets closer/' pi_sum.md > pi_doc.md
$ EDITOR="cp pi_doc.md" bb edit f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e@eng -m "Reword the documentation"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Documentation saved: the code did not change, so the hash stays the same.
Hash: f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e
$ bb show f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e@eng | head -1
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
    assert abs(compute_pi(8) - 3.14159) < 0.15
```

## Checks
````

## Workflow

### Step 11: Fork the check
```bash
$ EDITOR="cp n8_tighter.md" bb edit check_pi_n8@eng -m "Tighter bound"
No checks.
Parent: 5f8de2e0762a54b00e9c4147e65a2a18ec640bcf9bb8bd42a9dc8ec1c7e7bd48
Hash: 8e64a9a6a0709e6ddee3809bcf4f5cd45fbcacbde56f5166a74950f69b109865
```

### Step 12: The fork knows where it came from
Its lineage step follows the old check's: `bb latest` shows it as the
parent. A draft of the fork names that step in its `<!-- bb:parent -->`
line, so a draft made before another save is refused as stale.

```bash
$ bb latest check_pi_n8 eng | grep -E '^(Hash|Parent):'
Hash: 8e64a9a6a0709e6ddee3809bcf4f5cd45fbcacbde56f5166a74950f69b109865
Parent: 5f8de2e0762a54b00e9c4147e65a2a18ec640bcf9bb8bd42a9dc8ec1c7e7bd48
$ EDITOR=cat bb edit 8e64a9a6a0709e6ddee3809bcf4f5cd45fbcacbde56f5166a74950f69b109865@eng | grep -c 'bb:parent'
1
```

---

# Chapter 6: Two Forks of the Same Version

Another agent starts from the checked loop version too, and sums a list
instead of a generator. Both rewrites now descend from the same parent,
so `compute_pi` has two current versions. bb does not pick one by date:
commands that take a name refuse it and ask for a hash.

## Workflow

### Step 13: A second fork of the checked version
```bash
$ sed 's/sum((-1)/sum([(-1)/; s/range(n))$/range(n)])/' pi_sum.md > pi_list.md
$ EDITOR="cp pi_list.md" bb edit 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9@eng -m "Sum a list"
PASS check_pi_n1
PASS check_pi_n2
PASS check_pi_n3
PASS check_pi_n5
PASS check_pi_n8
5 checks: 5 passed
Parent: 314c0555136ff919d5f0f3b72fea5fe9ced8e67e95d74660b465c48cb3273de9
Hash: ec30704cedd60db0786a85051224ad26d51fd16a4095a90fcecb3dfed33cda93
```

### Step 14: The name now has two current versions
```bash
$ bb latest compute_pi eng | grep "^Hash:" | sort
Hash: ec30704cedd60db0786a85051224ad26d51fd16a4095a90fcecb3dfed33cda93
Hash: f8026db6c727e0efa3d678440e47a867a7bda4069dc07676ff178da4576ac99e
$ (bb show compute_pi@eng 2>&1 || true) | head -1
Error: 'compute_pi@eng' is ambiguous: it has 2 current versions. Use a hash:
```

### Step 15: CI runs what nobody reviewed yet
Nobody read the second fork, so its checks do not run on this machine.
A CI runner is different: it is thrown away after each job, so it may run
every check. `--dangerously-unreviewed` does that; the job must hold no
secrets and no write access.

```bash
$ bb validate --checks | tail -2
Not reviewed, checks not run:
  compute_pi ec30704cedd60db0786a85051224ad26d51fd16a4095a90fcecb3dfed33cda93 (1 to approve: bb review ec30704cedd60db0786a85051224ad26d51fd16a4095a90fcecb3dfed33cda93)
$ bb validate --checks --dangerously-unreviewed | grep -c "5 checks: 5 passed"
3
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb edit (name\|hash)@lang` | Edit a function and its checks in one markdown file, save a fork when all checks pass |
| `bb edit ... -m "comment"` | Give the comment up front instead of being asked |
| `bb review (name\|hash)@lang` | Read dependencies and checks first, then each function with its checks; run them, then approve |
| `bb validate --checks [name\|hash]` | Run the checks of reviewed functions, one or the whole pool |
| `bb validate --checks --dangerously-unreviewed` | On a disposable machine such as CI: run every check |

## Key Insights

1. **Checks take the implementation as an argument**: They never import the function they test, so they keep working across every fork.

2. **Checks are part of the definition**: A function's hash covers its code and its check hashes. Changing the checks forks the function; changing only the documentation does not.

3. **Saving is gated**: The file must parse and every check must pass before anything reaches the pool.

4. **Lineage is recorded**: A new version stores the edited one as `parent`, and each `<!-- bb:parent HASH -->` marker tells bb which check a section came from.

5. **Documentation lives in the mapping**: Prose and examples around the `python def` block are stored per language, outside the hash, like docstrings.

6. **A check forked alone applies to nothing** until it is pasted into a function's Checks section.

7. **Names never guess**: a name resolves to the versions nobody forked. When forks diverge, commands that take the name stop and list the hashes.

8. **Nothing runs unread**: checks run only on code you approved with `bb review`, and `bb edit` refuses to run pool imports you have not reviewed. Approvals stay out of the shared directory, so nobody can approve code for you.
