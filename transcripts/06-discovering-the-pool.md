# Transcript: Discovering the Pool - Search and Navigation

## Metadata
- ID: 06
- Feature: bb whoami, search, latest, caller
- Date: 2026-02-12
- Status: draft
- Complexity: beginner

## Overview

This transcript tells the story of a developer discovering Beyond Babel's pool navigation commands. Starting with an empty pool, we'll build a small text processing library with functions that depend on each other, then demonstrate how to explore and navigate the pool using discovery commands.

The story demonstrates:
- Setting user preferences with `bb whoami`
- Finding functions by content with `bb search`
- Finding the latest version of a function by name with `bb latest`
- Finding reverse dependencies with `bb caller`

These commands help answer questions like "Who am I?", "What's in this pool?", "Where's the latest version of X?", and "What depends on Y?"

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)
- Clean pool for testing

---

# Chapter 1: Setting Your Identity

Before exploring the pool, let's configure user preferences. The `bb whoami` command manages your identity and language preferences, which affect how other commands display information.

### Step 1: Check current identity (empty when not set)
```bash
$ bb whoami name

```

### Step 2: Set user name
```bash
$ bb whoami name "Alice Developer"
Set name: Alice Developer
```

### Step 3: Set preferred languages
```bash
$ bb whoami language eng fra
Set language: eng fra
```

### Step 4: Verify configuration
```bash
$ bb whoami name
Alice Developer
```

The language preference affects commands like `bb tree` and `bb latest`, which use the first language in your preference list when displaying function names.

---

# Chapter 2: Building a Text Processing Library

Let's build a small library with three functions that form a dependency chain:
1. **tokenize** - Splits text into words (leaf function, no dependencies)
2. **count_words** - Counts word frequency (depends on tokenize)
3. **summarize_text** - Generates text statistics (depends on count_words)

## Source Files

### Text Tokenizer `tokenize_eng.py`
```python
def tokenize(text):
    """Split text into words, removing punctuation."""
    import string
    words = []
    for word in text.split():
        cleaned = word.strip(string.punctuation).lower()
        if cleaned:
            words.append(cleaned)
    return words
```

### Word Counter `count_words_eng.py`
```python
from bb.pool import object_9a530147f8590dfd97545db79ec2ef5f8add090ecaa4a28219d84d793770a7eb as tokenize

def count_words(text):
    """Count word frequency in text."""
    words = tokenize._bb_v_0(text)
    counts = {}
    for word in words:
        counts[word] = counts.get(word, 0) + 1
    return counts
```

### Text Summarizer `summarize_text_eng.py`
```python
from bb.pool import object_077dab1cf785d4346936240332941a160e96e8a3e35dea417229c125d08b61f7 as count_words

def summarize_text(text):
    """Generate text statistics: total words, unique words, top word."""
    counts = count_words._bb_v_0(text)
    total = sum(counts.values())
    unique = len(counts)
    top_word = max(counts.items(), key=lambda x: x[1])[0] if counts else ""
    return total, unique, top_word
```

## Workflow

### Step 5: Add the tokenizer (leaf function)
```bash
$ TOK_HASH=$(bb add tokenize_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 6: Verify it's in the pool
```bash
$ bb log | grep "^Hash:" | wc -l
1
```

### Step 7: Add word counter (depends on tokenize)
```bash
$ COUNT_HASH=$(bb add count_words_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 8: Add text summarizer (depends on count_words)
```bash
$ SUMM_HASH=$(bb add summarize_text_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 9: Verify all three functions are in the pool
```bash
$ bb log | grep "^Hash:" | wc -l
3
```

---

# Chapter 3: Searching the Pool

Now that we have functions in the pool, let's use `bb search` to find them by content. The search command looks through function names, docstrings, and code content.

### Step 10: Search for "text" functions
```bash
$ bb search text | grep "^Hash:" | wc -l
3
```

All three functions match "text"! Even `tokenize`'s docstring mentions "Split text into words".

### Step 11: Search for "frequency"
```bash
$ bb search frequency | grep "^Hash:"
Hash: $COUNT_HASH
```

Only `count_words` mentions "frequency" in its docstring.

### Step 12: Search for "tokenize" (exact name)
```bash
$ bb search tokenize | grep "^Hash:"
Hash: $TOK_HASH
```

### Step 13: Search with multiple terms
```bash
$ bb search word count | grep "^Hash:" | wc -l
3
```

All three functions contain both "word" (in various docstrings) and "count" somewhere in their content. Search uses AND logic across terms.

---

# Chapter 4: Finding the Latest Version

As projects evolve, you might have multiple versions of a function with the same name. The `bb latest` command finds the most recent version by modification time.

### Step 14: Find latest "tokenize"
```bash
$ bb latest tokenize | grep "^Hash:"
Hash: $TOK_HASH
```

### Step 15: Find latest "count_words"
```bash
$ bb latest count_words | grep "^Hash:"
Hash: $COUNT_HASH
```

### Step 16: Search for non-existent function
```bash
$ (bb latest nonexistent 2>&1 || true) | grep "No function found"
No function found with name: nonexistent
```

The command tells us clearly when a function doesn't exist.

---

# Chapter 5: Finding Who Uses What

The `bb caller` command answers "What depends on this function?" It scans the entire pool to find reverse dependencies—critical for understanding impact when modifying a function.

### Step 17: Find callers of tokenize (should be count_words)
```bash
$ bb caller $TOK_HASH | grep "^bb show" | wc -l
1
```

One function depends on `tokenize`: the `count_words` function.

### Step 18: Find callers of count_words (should be summarize_text)
```bash
$ bb caller $COUNT_HASH | grep "^bb show" | wc -l
1
```

One function depends on `count_words`: the `summarize_text` function.

### Step 19: Find callers of summarize_text (leaf node, no callers)
```bash
$ bb caller $SUMM_HASH | wc -l
0
```

No functions depend on `summarize_text`—it's at the top of the dependency tree.

### Step 20: Verify caller output format
```bash
$ bb caller $TOK_HASH | head -1 | grep -o "^bb show"
bb show
```

The output provides ready-to-run commands to inspect each caller.

---

## Summary

| Command | Purpose | What We Discovered |
|---------|---------|-------------------|
| `bb whoami name <value>` | Set user name | Identity for commits |
| `bb whoami language <langs>` | Set language preferences | Affects display in other commands |
| `bb search <terms>` | Find functions by content | Found 2 "text" functions, 1 "frequency" function |
| `bb latest <name>` | Get most recent version | Found latest `tokenize`, `count_words` |
| `bb caller <hash>` | Find reverse dependencies | `tokenize` → 1 caller, `count_words` → 1 caller, `summarize_text` → 0 callers |

## Key Insights

1. **`bb whoami` stores preferences**: The configuration affects how other commands display information. Setting preferred languages determines which language mapping is shown in `bb tree` and `bb latest`.

2. **`bb search` is content-aware**: It searches through function names, docstrings, AND code content. Multiple search terms narrow results (AND logic).

3. **`bb latest` uses modification time**: When multiple versions of a function exist with the same name (but different hashes due to logic changes), `latest` returns the most recently added one.

4. **`bb caller` provides actionable output**: The output is formatted as ready-to-run `bb show` commands, making it easy to inspect each caller.

5. **Dependency chains are discoverable**: By combining `bb caller` with knowledge of the dependency structure, you can trace impact: modifying `tokenize` affects `count_words`, which affects `summarize_text`.

## Use Cases

1. **Exploring inherited pools**: Join a team and use `bb search` to find relevant functions
2. **Impact analysis**: Use `bb caller` before modifying a function to understand downstream effects
3. **Version management**: Use `bb latest` when multiple implementations exist
4. **Identity tracking**: Use `bb whoami` for commit attribution

## Notes

- Search is case-insensitive and matches partial words
- `bb caller` scans the entire pool, which may be slow on large pools
- Language preferences cascade: first language in `whoami language` is used as default
- `bb latest` compares modification times, not semantic versions
