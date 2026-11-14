# Transcript: An update that fails a check is not saved

## Metadata
- ID: 12
- Feature: bb update runs the checks of what it rewrites
- Date: 2026-11-14
- Status: draft
- Complexity: advanced

## Overview
`bb update` rewrites outdated functions onto the current versions of what they use, as if each were edited and saved unchanged. Saving through `bb edit` needs the checks to pass, and so does an update. A function whose check fails with the new version is not saved: it stays on the version it has, with the failing check printed and exit status 1. Everything else is still updated. This transcript has a `count_distinct` that counted on `sort_numbers` dropping duplicates, then fixes the sort.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: A Program That Counted on the Bug

`sort_numbers` goes through `set`. `count_distinct` is the length of its result, and its check says repeated numbers count once. `top` is just the last number. `dashboard` prints both.

## Source Files

### Sort `sort_numbers_eng.py`
```python
def sort_numbers(numbers):
    """Sort numbers in increasing order."""
    return sorted(set(numbers))
```

### Top `top_eng.py`
```python
import bb

sort_numbers = bb.resolve("sort_numbers")


def top(numbers):
    """The biggest number of a list."""
    return sort_numbers(numbers)[-1]
```

### Count distinct, with its check `count_distinct.md`
````markdown
# count_distinct

## Implementation

```python def
import bb

sort_numbers = bb.resolve("sort_numbers")


def count_distinct(numbers):
    """How many different numbers a list holds."""
    return len(sort_numbers(numbers))
```

## Checks

### check_count_distinct

```python def
def check_count_distinct(count_distinct):
    """Repeated numbers count once."""
    assert count_distinct([1, 1, 2]) == 2
```
````

### Dashboard `dashboard_eng.py`
```python
import bb

count_distinct = bb.resolve("count_distinct")
top = bb.resolve("top")


def dashboard(text):
    """Print how many different numbers there are, and the top."""
    numbers = [int(word) for word in text.split(',')]
    print("distinct", count_distinct(numbers), "top", top(numbers))
```

## Workflow

### Step 1: Add the program
```bash
$ bb add sort_numbers_eng.py@eng > /dev/null
$ yes | bb review sort_numbers@eng > /dev/null
$ bb add top_eng.py@eng > /dev/null
$ EDITOR="cp count_distinct.md" bb edit count_distinct@eng -m "count" | head -2
PASS check_count_distinct
1 checks: 1 passed
$ bb add dashboard_eng.py@eng > /dev/null
```

### Step 2: Run it
```bash
# bb only runs code you have reviewed
$ yes | bb review dashboard@eng > /dev/null
$ bb run dashboard@eng 3,1,1,1,2
distinct 3 top 3
```

---

# Chapter 2: The Fix

The sort should keep duplicates. Nothing else is touched, so the rest of the program is now behind.

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
````

## Workflow

### Step 3: Save the fix
```bash
$ bb scratchpad sort_numbers@eng pad.md > /dev/null
$ PARENT=$(grep -m1 'bb:parent' pad.md | awk '{print $3}')
$ sed "s/^## Implementation$/## Implementation\n\n<!-- bb:parent $PARENT -->/" fix.md > pad.md
$ bb scratchpad sort_numbers@eng pad.md -m "keep duplicates" | tail -1
Scratchpad pad.md: saved sort_numbers
```

### Step 4: Both are behind
```bash
$ bb search --outdated | grep '^Name:' | sort
Name: count_distinct (eng)
Name: dashboard (eng)
Name: top (eng)
```

---

# Chapter 3: The Update That Is Refused

`bb update dashboard@eng` would move `top` and `count_distinct`. With the new sort, `count_distinct([1, 1, 2])` is 3 and its check fails, so that one stays where it is. `top` has no checks and moves, and `dashboard` follows `top` alone. The failure is printed and the exit status is 1, so the command is wrapped to keep going and to show both outputs.

## Workflow

### Step 5: Update, and see what could not be
```bash
$ (bb update dashboard@eng -m "follow the fix" 2>&1 || true) | sed 's/[0-9a-f]\{64\}/HASH/; s/[0-9a-f]\{12\}\.\.\./HASH.../g'
Updated 2 function(s), deepest first:
  top: HASH... -> HASH...
  dashboard: HASH... -> HASH...
Drafts open on these functions are now stale: write them again.
Hash: HASH
Not updated, a check failed (1):
  count_distinct: HASH... stays; its update would be HASH...
    FAIL check_count_distinct: assertion failed: count_distinct([1, 1, 2]) == 2 (3 == 2)
Edit them to adapt (bb edit NAME@lang), then update again.
```

### Step 6: What is left behind is listed
`dashboard` runs, with the old `count_distinct` and the new `top`. `count_distinct` is still outdated, and so is the dashboard that uses it.

```bash
$ bb search --outdated | grep '^Name:' | sort
Name: count_distinct (eng)
Name: dashboard (eng)
$ yes | bb review dashboard@eng > /dev/null
$ bb run dashboard@eng 3,1,1,1,2
distinct 3 top 3
```

---

# Chapter 4: Adapt, Then Update Again

The way out is the one `bb edit` always had: change `count_distinct` so its check passes with the fixed sort, and save. It counts the set of the sorted numbers now.

## Source Files

### The adapted function `adapted.md`
````markdown
# count_distinct

## Implementation

```python def
import bb

sort_numbers = bb.resolve("sort_numbers")


def count_distinct(numbers):
    """How many different numbers a list holds."""
    return len(set(sort_numbers(numbers)))
```

## Checks

### check_count_distinct

```python def
def check_count_distinct(count_distinct):
    """Repeated numbers count once."""
    assert count_distinct([1, 1, 2]) == 2
```
````

## Workflow

### Step 7: Save the adapted function
```bash
$ bb edit count_distinct@eng draft.md > /dev/null
$ PARENT=$(grep -m1 'bb:parent' draft.md | awk '{print $3}')
$ sed "s/^## Implementation$/## Implementation\n\n<!-- bb:parent $PARENT -->/" adapted.md > draft.md
$ bb edit count_distinct@eng draft.md -m "count the set of the sorted numbers" | head -2
PASS check_count_distinct
1 checks: 1 passed
```

### Step 8: Now the update goes through
Only `dashboard` is behind, on the old `count_distinct`.

```bash
$ bb update dashboard@eng -m "follow count_distinct" | head -1
Updated 1 function(s), deepest first:
$ bb search --outdated | tail -1
No matches found: nothing outdated
$ yes | bb review dashboard@eng > /dev/null
$ bb run dashboard@eng 3,1,1,1,2
distinct 3 top 3
```
