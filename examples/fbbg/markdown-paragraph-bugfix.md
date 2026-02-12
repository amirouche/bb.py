# Markdown Paragraph Bug Fix - Case Study

**Date**: 2026-02-12
**Project**: fbbg (Fast Blog Generator)
**Bug**: Paragraphs not rendering - all text appears as continuous block

---

## Bug Symptoms

The fbbg blog generator was converting markdown to HTML but failing to wrap separate paragraphs in `<p>` tags. Text blocks separated by blank lines in markdown were rendered as plain text without proper HTML paragraph structure.

**Example:**

Markdown input:
```markdown
# Hello World

First paragraph with some text.

Second paragraph with more text.
```

Actual output:
```html
<h1>Hello World</h1>

First paragraph with some text.

Second paragraph with more text.
```

Expected output:
```html
<h1>Hello World</h1>
<p>First paragraph with some text.</p>
<p>Second paragraph with more text.</p>
```

---

## Investigation Process

### Step 1: Identify the Dependency Tree

Used `bb tree` to understand which functions were involved:

```bash
bb tree fbbg
```

Result:
```
fbbg
├── html_markup_fragment (markdown to HTML converter) ← SUSPECT
├── html_new (HTML5 document builder)
└── rss (RSS feed generator)
```

The `html_markup_fragment` function was responsible for markdown-to-HTML conversion.

### Step 2: Examine the Buggy Function

Used `bb show` to inspect the implementation:

```bash
bb show html_markup_fragment@eng
```

Hash: `449393883d05f2bcafc5d3597c5d83805059de97f10233ac86f9804d47780f27`

---

## Root Cause Analysis

The buggy function had this approach:

```python
def html_markup_fragment(text):
    lines = text.split('\n')  # Split by SINGLE newline
    result = []
    for line in lines:        # Process each line individually
        # ... apply formatting to each line ...
        result.append(line)
    return '\n'.join(result)
```

**Problems identified:**

1. **Line-by-line processing**: Split text by single newlines (`\n`), treating each line as an independent unit
2. **No paragraph detection**: Never detected blank lines (double newlines `\n\n`) that separate paragraphs
3. **No paragraph wrapping**: Never added `<p>` tags around regular text blocks

The function only handled:
- Headings: Lines starting with `#` → converted to `<h1>`, `<h2>`, etc.
- Inline formatting: `**bold**`, `*italic*`, `[links](url)`

It completely missed the concept of paragraphs as blocks of text separated by blank lines.

---

## Solution Design

### Algorithm Change

**From:** Line-by-line processing
**To:** Block-by-block processing

```python
def html_markup_fragment(text):
    # Split by DOUBLE newlines (paragraph boundaries)
    blocks = text.split('\n\n')
    result = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # Check if block is a heading
        if block.startswith('#'):
            # Process as heading (no <p> wrapper)
            # ... apply inline formatting ...
        else:
            # Process as paragraph (wrap in <p> tags)
            block = block.replace('\n', ' ')  # Single newlines within paragraph → spaces
            # ... apply inline formatting ...
            result.append(f'<p>{block}</p>')

    return '\n'.join(result)
```

**Key improvements:**

1. **Block detection**: Split by `\n\n` to find paragraph boundaries
2. **Heading vs paragraph**: Headings stay unwrapped, regular text gets `<p>` tags
3. **Inline newlines**: Within a paragraph, single newlines become spaces
4. **Inline formatting**: Still applied to both headings and paragraphs

---

## Implementation Steps

### 1. Create Fixed Function

Created: `/src/bb.py/examples/fbbg/eng/html_markup_fragment_fixed.py`

The fixed version implements the block-by-block algorithm with proper paragraph detection.

### 2. Add to Pool

```bash
bb add html_markup_fragment_fixed.py@eng
```

Result: New hash `d3f87c4ad1f58b2ff4eafd3388f27f56ab809a894de733694281c418689ed23b`

### 3. Refactor Parent Function

Used `bb refactor` to replace the buggy function with the fixed one in fbbg:

```bash
bb refactor <fbbg_hash> <old_html_markup_fragment_hash> <new_html_markup_fragment_hash>
```

Specifically:
```bash
bb refactor da843903dfcc29364dfbb70bd4a41d145757989d9d86936ff39cc3bcbcce97a0 \
            449393883d05f2bcafc5d3597c5d83805059de97f10233ac86f9804d47780f27 \
            d3f87c4ad1f58b2ff4eafd3388f27f56ab809a894de733694281c418689ed23b
```

Result: New fbbg hash `239e1a42e5653fb780211f0df5b27df1488c891807b1c7cdad5c64194627b76c`

### 4. Regenerate Blog

```bash
bb run 239e1a42e5653fb780211f0df5b27df1488c891807b1c7cdad5c64194627b76c@eng /src/bb.py/examples/fbbg
```

Generated 9 files (HTML articles + RSS feeds + index pages) with no errors.

---

## Verification Results

### Test Case 1: English "Hello World" Article

**Markdown source:**
```markdown
# Hello world from bb (also known as beyond babel)

Beyond Babel (bb, also known as beyond babel) is a function pool manager...

The reason for bb (also known as beyond babel)'s existence is to allow...

This project breaks down language barriers in software development...
```

**Generated HTML:**
```html
<h1>Hello world from bb (also known as beyond babel)</h1>
<p>Beyond Babel (bb, also known as beyond babel) is a function pool manager...</p>
<p>The reason for bb (also known as beyond babel)'s existence is to allow...</p>
<p>This project breaks down language barriers in software development...</p>
```

✅ **Pass**: Heading unwrapped, each paragraph properly wrapped in `<p>` tags

### Test Case 2: English "Refusing Fog" Article

**Generated HTML:**
```html
<h1>refusing fog</h1>
<p>bb is a mirror that projects the math behind the the society...</p>
<p>bb.py is a step toward that goal. It's a function pool...</p>
<p>What comes next is möbius — a clean-slate language...</p>
<p>Large language models absorbed the commons without preserving...</p>
```

✅ **Pass**: Multiple paragraphs correctly separated and wrapped

### Test Case 3: French "Bonjour le monde" Article

**Generated HTML:**
```html
<h1>Bonjour le monde depuis bb (aussi connu sous le nom de beyond babel)</h1>
<p>Beyond Babel (bb, aussi connu sous le nom de beyond babel) est un gestionnaire...</p>
<p>La raison d'être de bb (aussi connu sous le nom de beyond babel) est de permettre...</p>
<p>Ce projet brise les barrières linguistiques dans le développement logiciel...</p>
```

✅ **Pass**: Works correctly across languages (French)

### Comprehensive Checks

- ✅ Headings (`#`, `##`, etc.) render without `<p>` wrapper
- ✅ Regular text blocks wrapped in `<p>` tags
- ✅ Multiple paragraphs properly separated
- ✅ Inline formatting (bold, italic, links) still functional
- ✅ Works for both English and French content
- ✅ No double `<p>` wrapping on headings
- ✅ Empty blocks properly skipped

---

## Key Lessons

### 1. Beyond Babel's `bb tree` is Invaluable for Debugging

Instead of grepping through source files, `bb tree` immediately showed the dependency structure and identified which function was responsible for markdown conversion.

### 2. Content-Addressed Storage Enables Safe Refactoring

The old buggy function still exists in the pool (hash `449393...`). The new fixed version coexists (hash `d3f87c4...`). Functions using the old version can be individually updated via `bb refactor` without breaking anything.

### 3. Block-Based Text Processing vs Line-Based

The fundamental issue was a mismatch between the data structure (line-by-line array) and the semantic structure (block-based paragraphs). Markdown paragraphs are defined by blank lines, not single newlines.

**Wrong mental model:** Text = array of lines
**Correct mental model:** Text = array of blocks (separated by blank lines)

### 4. The Fix Was Surgical

Only one function needed to change. The `bb refactor` command automatically updated `fbbg` to use the new version. No manual editing of `fbbg` required. This is the power of compositional, content-addressed functions.

---

## Hash Reference

| Function | Status | Hash |
|----------|--------|------|
| `html_markup_fragment` (buggy) | Deprecated | `449393883d05f2bcafc5d3597c5d83805059de97f10233ac86f9804d47780f27` |
| `html_markup_fragment` (fixed) | Active | `d3f87c4ad1f58b2ff4eafd3388f27f56ab809a894de733694281c418689ed23b` |
| `fbbg` (old) | Deprecated | `da843903dfcc29364dfbb70bd4a41d145757989d9d86936ff39cc3bcbcce97a0` |
| `fbbg` (new) | Active | `239e1a42e5653fb780211f0df5b27df1488c891807b1c7cdad5c64194627b76c` |

---

## Conclusion

The bug was caused by processing markdown line-by-line instead of block-by-block. Changing the algorithm to detect paragraph boundaries (double newlines) and wrap regular text in `<p>` tags fixed the issue.

The fix demonstrates:
- How `bb tree` helps identify buggy dependencies
- How `bb refactor` enables surgical updates to composed functions
- How content-addressed storage preserves all versions (old and new)
- How the same fix propagates across multiple languages (English, French)

**Total time from bug identification to verified fix:** ~10 minutes

The blog now renders paragraphs correctly! 🎉
