# Transcript: Stale on purpose, a fix never breaks anything

## Metadata
- ID: 11
- Feature: bb scratchpad (--refactor), bb search --outdated, bb update, bb.ref pins
- Date: 2026-11-14
- Status: draft
- Complexity: intermediate

## Overview
A fix to a function is a new version. It never rewrites the versions that use the old one, so nothing breaks and nobody is forced to refactor the whole pool in one go. Callers stay on what they were saved with, which is staleness on purpose: `bb search --outdated` lists the debt, and `bb update` pays it when you decide. Because a name is resolved when saving and never when running, two different `sort_numbers` can live in one program, one pinned with `bb.ref`, the other found by name. This transcript fixes a sort that drops duplicates, shows the callers still running the old one, uses both versions in one function, then pays the debt of one program.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: A Program With a Bug

`sort_numbers` goes through `set`, so duplicates vanish. `median` and `top` use it, and `report` prints both. For `3,1,1,1,2` the median should be 1 but comes out 2; the top is right.

## Source Files

### Sort `sort_numbers_eng.py`
```python
def sort_numbers(numbers):
    """Sort numbers in increasing order."""
    return sorted(set(numbers))
```

### Median `median_eng.py`
```python
import bb

sort_numbers = bb.resolve("sort_numbers")


def median(numbers):
    """The middle number of a list."""
    ordered = sort_numbers(numbers)
    return ordered[len(ordered) // 2]
```

### Top `top_eng.py`
```python
import bb

sort_numbers = bb.resolve("sort_numbers")


def top(numbers):
    """The biggest number of a list."""
    return sort_numbers(numbers)[-1]
```

### Report `report_eng.py`
```python
import bb

median = bb.resolve("median")
top = bb.resolve("top")


def report(text):
    """Print the median and the top of comma separated numbers."""
    numbers = [int(word) for word in text.split(',')]
    print("median", median(numbers), "top", top(numbers))
```

## Workflow

### Step 1: Add the program, helpers first
```bash
$ OLD_SORT=$(bb add sort_numbers_eng.py@eng | grep '^Hash:' | awk '{print $2}')
$ bb add median_eng.py@eng > /dev/null
$ bb add top_eng.py@eng > /dev/null
$ OLD_REPORT=$(bb add report_eng.py@eng | grep '^Hash:' | awk '{print $2}')
$ echo $OLD_SORT
abf0b9b3f5ca7f1d720c40937ec805492a8a0ddcc5235c0abb12177e56701921
```

### Step 2: Run it
```bash
# bb only runs code you have reviewed
$ yes | bb review report@eng > /dev/null
$ bb run report@eng 3,1,1,1,2
median 2 top 3
```

---

# Chapter 2: The Fix

A scratchpad holds functions in one file. We write `sort_numbers` into it, replace the body and add a check that fails on the old code.

## Source Files

### Scratchpad with the fix `fix.md`
````markdown
# sort_numbers

## Implementation

```python def
def sort_numbers(numbers):
    """Sort numbers in increasing order, duplicates included."""
    return sorted(numbers)
```

## Checks

### check_sort_keeps_duplicates

```python def
def check_sort_keeps_duplicates(sort_numbers):
    """Duplicates are kept."""
    assert sort_numbers([2, 1, 2]) == [1, 2, 2]
```
````

## Workflow

### Step 3: Save the fix
A scratchpad written by `bb scratchpad` carries the line naming the version it continues. Here we write it, then put our fixed file over it.

```bash
$ bb scratchpad sort_numbers@eng pad.md > /dev/null
$ PARENT=$(grep 'bb:parent' pad.md | awk '{print $3}')
$ sed "s/^## Implementation$/## Implementation\n\n<!-- bb:parent $PARENT -->/" fix.md > pad.md
$ bb scratchpad sort_numbers@eng pad.md -m "keep duplicates" | grep -v '^Hash\|^Check:\|^Parent'
── sort_numbers
PASS check_sort_keeps_duplicates
1 checks: 1 passed
Scratchpad pad.md: saved sort_numbers
```

### Step 4: Callers did not move
The fix is the current `sort_numbers`, yet `report` still runs the version it was saved with. Nothing broke, and nothing was refactored.

```bash
$ bb run report@eng 3,1,1,1,2
median 2 top 3
```

### Step 5: The debt is listed, not forced
```bash
$ bb search --outdated | grep '^Name:' | sort
Name: median (eng)
Name: report (eng)
Name: top (eng)
$ bb search --outdated report | grep '^Status:'
Status: no checks, reviewed, behind: uses median, top, which is out of date
```

---

# Chapter 3: Two Sorts in One Program

Both `sort_numbers` are in the pool, one superseded and one current. `compare` pins the old with `bb.ref` and finds the new by name. The name was resolved when `compare` was saved; at run time there is no lookup.

## Source Files

### Compare `compare_eng.py`
```python
import bb

old_sort = bb.ref("abf0b9b3f5ca7f1d720c40937ec805492a8a0ddcc5235c0abb12177e56701921")
new_sort = bb.resolve("sort_numbers")


def compare(text):
    """Print what the two versions of sort_numbers make of the same numbers."""
    numbers = [int(word) for word in text.split(',')]
    print("old", old_sort(numbers), "new", new_sort(numbers))
```

## Workflow

### Step 6: Use both
```bash
$ bb add compare_eng.py@eng > /dev/null
$ yes | bb review compare@eng > /dev/null
$ bb run compare@eng 3,1,1,1,2
old [1, 2, 3] new [1, 1, 1, 2, 3]
```

### Step 7: The tree shows which is which
```bash
$ bb tree compare@eng
compare — Print what the two versions of sort_numbers make of the same numbers.
├── sort_numbers — Sort numbers in increasing order.  [pinned]
└── sort_numbers — Sort numbers in increasing order, duplicates included.
```

---

# Chapter 4: Paying the Debt, When We Choose

`bb update report@eng` brings `report` and everything it uses to the current versions, creating new ones. The old `report` stays in the pool and still runs by its hash. `compare` was not touched: what it pins stays pinned.

## Workflow

### Step 8: Update the program
```bash
$ bb update report@eng -m "follow the fix" | head -1
Updated 3 function(s), deepest first:
```

### Step 9: The new program is fixed
```bash
# bb only runs code you have reviewed
$ yes | bb review report@eng > /dev/null
$ bb run report@eng 3,1,1,1,2
median 1 top 3
```

### Step 10: The old program still runs, with its old answer
```bash
$ bb run $OLD_REPORT@eng 3,1,1,1,2
median 2 top 3
```

### Step 11: Nothing is behind, and the pin stayed
```bash
$ bb search --outdated | tail -1
No matches found: nothing outdated
$ bb run compare@eng 3,1,1,1,2
old [1, 2, 3] new [1, 1, 1, 2, 3]
```

---

# Chapter 5: Fix and Refactor in One Command

Staleness is the default, not a rule. `bb scratchpad --refactor ROOT` saves and then runs `bb update ROOT`, for whoever wants the fix to follow at once. Only the root you name moves. A second program, `summary`, uses `top` and is added now, on the current versions. Then `sort_numbers` gets a second version, the same behavior written as a copy that is sorted in place, and only `report` follows it.

## Source Files

### Summary `summary_eng.py`
```python
import bb

top = bb.resolve("top")


def summary(text):
    """Print the top of comma separated numbers."""
    numbers = [int(word) for word in text.split(',')]
    print("top", top(numbers))
```

### Scratchpad with the second version `again.md`
````markdown
# sort_numbers

## Implementation

```python def
def sort_numbers(numbers):
    """Sort numbers in increasing order, duplicates included."""
    ordered = list(numbers)
    ordered.sort()
    return ordered
```

## Checks

### check_sort_keeps_duplicates

```python def
def check_sort_keeps_duplicates(sort_numbers):
    """Duplicates are kept."""
    assert sort_numbers([2, 1, 2]) == [1, 2, 2]
```
````

## Workflow

### Step 12: Add the second program
```bash
$ bb add summary_eng.py@eng > /dev/null
# bb only runs code you have reviewed
$ yes | bb review summary@eng > /dev/null
$ bb run summary@eng 3,1,1,1,2
top 3
```

### Step 13: Save the new sort and bring report along
```bash
$ bb scratchpad sort_numbers@eng pad.md > /dev/null
$ PARENT=$(grep -m1 'bb:parent' pad.md | awk '{print $3}')
$ sed "s/^## Implementation$/## Implementation\n\n<!-- bb:parent $PARENT -->/" again.md > pad.md
$ bb scratchpad sort_numbers@eng pad.md -m "sort a copy in place" --refactor report@eng | grep 'Scratchpad\|Updated'
Scratchpad pad.md: saved sort_numbers
Updated 3 function(s), deepest first:
```

### Step 14: Only the root we named followed
`summary` was not named, so it is behind and runs with what it was saved with. So is `compare`: its `new_sort` is found by name, so it follows the current version only when updated (its pin never moves). `bb search --outdated` lists both.

```bash
$ yes | bb review report@eng > /dev/null
$ bb run report@eng 3,1,1,1,2
median 1 top 3
$ bb search --outdated | grep '^Name:' | sort
Name: compare (eng)
Name: summary (eng)
$ bb run summary@eng 3,1,1,1,2
top 3
```

### Step 15: It catches up when we choose
```bash
$ bb update summary@eng -m "follow the sort" | head -1
Updated 1 function(s), deepest first:
$ bb update compare@eng -m "follow the sort" | head -1
Updated 1 function(s), deepest first:
$ bb search --outdated | tail -1
No matches found: nothing outdated
```

`compare` still pins the very first `sort_numbers`, so the program keeps using three versions of one name.

```bash
$ yes | bb review compare@eng > /dev/null
$ bb run compare@eng 3,1,1,1,2
old [1, 2, 3] new [1, 1, 1, 2, 3]
```
