# Transcript: Worklogs

## Metadata
- ID: 09
- Feature: bb worklog new/translate/list/show, bb log
- Date: 2026-11-05
- Status: draft
- Complexity: beginner

## Overview
A worklog is a note about work done, like an empty git commit. It is neither a function nor a mapping: it has its own id (a UUIDv7) and can be attached to function translations, or to nothing. Like functions, worklogs can be translated; each translation, the original included, gets its own hash. This transcript writes a worklog about a greeting function, translates it to French, and shows how the reader's preferred language decides which translation is displayed.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: A Note About a Function

Alice writes in English. Her first preferred language decides the
language of new worklogs.

## Source Files

### Greeting `greet_eng.py`
```python
def greet(name):
    """Greet someone by name."""
    return f"Hello, {name}!"
```

## Workflow

### Step 1: Alice introduces herself
```bash
$ bb whoami name Alice
Set name: Alice
$ bb whoami language eng
Set language: eng
```

### Step 2: Add the greeting
```bash
$ bb add greet_eng.py@eng
Hash: 5de04055d51c70e8e499adcbebf43621e39a43c6579fe7175639f56e5752e1e1
Mapping hash: e70b887e507a90bfe3d960e29b2262459f3760179050dfe91ace947f7755333e
```

### Step 3: Write a worklog attached to the English translation
`greet@eng` resolves to the exact translation: function hash, language
and mapping hash.

```bash
$ WORKLOG=$(bb worklog new greet@eng -m "Wrote a first greeting" | grep '^Worklog:' | awk '{print $2}')
```

### Step 4: Read it back
```bash
$ bb worklog show $WORKLOG
Worklog: $WORKLOG@eng
Attached: 5de04055d51c70e8e499adcbebf43621e39a43c6579fe7175639f56e5752e1e1@eng@e70b887e507a90bfe3d960e29b2262459f3760179050dfe91ace947f7755333e

Wrote a first greeting
```

---

# Chapter 2: Translating the Worklog

The worklog keeps its id; the French message is stored next to the
English one, with its own hash. Without `-m`, bb would open `$EDITOR`
with the original message shown as comments.

## Workflow

### Step 5: Add a French translation
```bash
$ bb worklog translate $WORKLOG fra -m "Écrit une première salutation"
Message hash: fdfaaa247ddd17ebd751cb88f9c99b968b3921059f1b35717675b7420755c50f
```

### Step 6: A French reader sees the French message
```bash
$ bb whoami language fra eng
Set language: fra eng
$ bb worklog show $WORKLOG
Worklog: $WORKLOG@fra
Attached: 5de04055d51c70e8e499adcbebf43621e39a43c6579fe7175639f56e5752e1e1@eng@e70b887e507a90bfe3d960e29b2262459f3760179050dfe91ace947f7755333e

Écrit une première salutation
```

### Step 7: The original is one suffix away
```bash
$ bb worklog show $WORKLOG@eng | head -1
Worklog: $WORKLOG@eng
```

---

# Chapter 3: Listing Everything

A worklog does not need a function: this one is a plain note, written
in French because French now comes first in Alice's languages.

`bb worklog list` shows every worklog whatever its language, newest
first. `bb log` shows functions and worklogs together.

## Workflow

### Step 8: A note attached to nothing
```bash
$ bb worklog new -m "Une note sans fonction" | grep '^Message hash:'
Message hash: fdb7cf48f4bf8b65e62f6df320bb8056e4a0c6a720af27963d342924fb241425
```

### Step 9: List all worklogs (ids and dates left out here)
```bash
$ bb worklog list | grep -v "^Worklog:" | grep -v "^Date:"
Author: Alice
Languages: fra

    Une note sans fonction

Author: Alice
Languages: eng, fra
Attached: 5de04055d51c70e8e499adcbebf43621e39a43c6579fe7175639f56e5752e1e1@eng@e70b887e507a90bfe3d960e29b2262459f3760179050dfe91ace947f7755333e

    Écrit une première salutation
```

### Step 10: The pool log counts both kinds
```bash
$ bb log | head -1
Pool Log (1 functions, 2 worklogs)
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb worklog new [name@lang ...] -m MSG` | Write a worklog in your preferred language, optionally attached to translations |
| `bb worklog translate ID LANG -m MSG` | Add a translation of a worklog |
| `bb worklog show ID[@lang]` | Show one worklog |
| `bb worklog list` | List every worklog, newest first |
| `bb log` | Functions and worklogs together, newest first |

## Key Insights

1. **Worklogs are their own kind of object**: not a function, not a mapping. They are identified by a UUIDv7, which sorts by creation time.

2. **Every translation is content-addressed**: the original and each translation get a hash of their language and text.

3. **Attachments point to exact translations**: `hash@lang@mapping_hash`, so a note about the English wording stays about that wording.

4. **The reader's languages decide what is shown**: the first preferred language with a translation wins, then the original.
