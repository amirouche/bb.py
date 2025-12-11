# Transcript: La Langue du Feu - Multilingual Code vs i18n

## Metadata
- ID: 00
- Feature: multilingual code vs internationalization
- Date: 2025-11-23
- Status: draft
- Complexity: nerd

## Overview
Demonstrates the difference between **multilingual code** (what `bb.py` enables) and **internationalization (i18n)**. The algorithm implements "la langue du feu" (the fire language), a French word game that inserts "fa/fe/fi/fo/fu" after each vowel.

Even when the code identifiers are in Japanese or Arabic, the **strings remain in French** - because translating strings is i18n, not multilingual programming.

## Prerequisites
- `bb.py` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

## The Algorithm
"La langue du feu" transforms words by inserting "f" + vowel after each vowel:
- "bonjour" → "bofonjoufur"
- "salut" → "safalufut"
- "merci" → "mefercifia"

## Source Files

### Fire Language Encoder `feu_fra.py`
```python
def encoder_langue_du_feu(texte):
    """Encode un texte en langue du feu."""
    voyelles = "aeiouyAEIOUY"
    resultat = ""
    for caractere in texte:
        resultat += caractere
        if caractere in voyelles:
            if caractere.isupper():
                resultat += "F" + caractere.lower()
            else:
                resultat += "f" + caractere
    return resultat
```

### Fire Language Encoder `feu_jpn.py`
```python
def 火の言葉に変換(文章):
    """Encode un texte en langue du feu."""
    母音 = "aeiouyAEIOUY"
    結果 = ""
    for 文字 in 文章:
        結果 += 文字
        if 文字 in 母音:
            if 文字.isupper():
                結果 += "F" + 文字.lower()
            else:
                結果 += "f" + 文字
    return 結果
```

### Fire Language Encoder `feu_ara.py`
```python
def تشفير_لغة_النار(نص):
    """Encode un texte en langue du feu."""
    حروف_علة = "aeiouyAEIOUY"
    نتيجة = ""
    for حرف in نص:
        نتيجة += حرف
        if حرف in حروف_علة:
            if حرف.isupper():
                نتيجة += "F" + حرف.lower()
            else:
                نتيجة += "f" + حرف
    return نتيجة
```

## Workflow

### Step 1: Add French version
```bash
$ bb.py add feu_fra.py@fra
Added function: <HASH>
```

### Step 2: Add Japanese version
```bash
$ bb.py add feu_jpn.py@jpn
Added function: <HASH>
```
Expected: Same hash - the logic is identical, only identifiers differ.

### Step 3: Add Arabic version
```bash
$ bb.py add feu_ara.py@ara
Added function: <HASH>
```
Expected: Same hash.

### Step 4: Verify all three share the same hash
```bash
$ bb.py show <HASH>
Available languages for <HASH>:
  ara - 1 mapping(s)
  fra - 1 mapping(s)
  jpn - 1 mapping(s)
```

### Step 5: Run with French identifiers
```bash
$ bb.py run <HASH>@fra "bonjour"
"bofonjofoufur"

$ bb.py run <HASH>@fra "merci beaucoup"
"mefércifia befeafucofoufup"
```

### Step 6: Run with Japanese identifiers (same French input!)
```bash
$ bb.py run <HASH>@jpn "bonjour"
"bofonjofoufur"

$ bb.py run <HASH>@jpn "je t'aime"
"jefe t'afaifimefe"
```

### Step 7: Run with Arabic identifiers (same French input!)
```bash
$ bb.py run <HASH>@ara "salut les amis"
"safalufut lefes afamifia"
```

### Step 8: Time execution across languages
```bash
$ /usr/bin/time -v bb.py run <HASH>@fra "Vive la France"
"Vivifeve lafa Frafancefe"
        User time (seconds): 0.11
        System time (seconds): 0.02
        Elapsed (wall clock) time: 0.14
        Maximum resident set size (kbytes): 18200

$ /usr/bin/time -v bb.py run <HASH>@jpn "Vive la France"
"Vivifeve lafa Frafancefe"
        User time (seconds): 0.11
        System time (seconds): 0.02
        Elapsed (wall clock) time: 0.14
        Maximum resident set size (kbytes): 18210

$ /usr/bin/time -v bb.py run <HASH>@ara "Vive la France"
"Vivifeve lafa Frafancefe"
        User time (seconds): 0.11
        System time (seconds): 0.02
        Elapsed (wall clock) time: 0.14
        Maximum resident set size (kbytes): 18195
```

## Expected Results
- All three language versions produce the **same hash**
- All three produce **identical output** for the same French input
- The docstring remains in French across all versions
- Execution time is identical regardless of identifier language

## Key Insight: Multilingual Code ≠ i18n

| Aspect | Multilingual Code (bb.py) | i18n |
|--------|---------------------------|------|
| What changes | Variable/function names | User-facing strings |
| What stays same | Logic, string literals | Code structure |
| Hash | Same across languages | N/A |
| Example | `encoder_langue_du_feu` → `火の言葉に変換` | `"bonjour"` → `"hello"` |

In this transcript:
- **Multilingual code**: The function name changes (`encoder_langue_du_feu` / `火の言葉に変換` / `تشفير_لغة_النار`)
- **NOT i18n**: The input strings stay French (`"bonjour"`, `"merci"`, etc.)

## Timing Summary
| Language | User Time | System Time | Wall Clock | Max RSS |
|----------|-----------|-------------|------------|---------|
| French | 0.11s | 0.02s | 0.14s | 18 MB |
| Japanese | 0.11s | 0.02s | 0.14s | 18 MB |
| Arabic | 0.11s | 0.02s | 0.14s | 18 MB |

## Notes
- "La langue du feu" is a French children's word game (also called "javanais" variants)
- RTL scripts (Arabic) work correctly in Python 3 identifiers
- The docstring could be translated for true i18n, but that's a separate concern
- This example proves: **same algorithm, different cultures, one hash**
- La prochaine fois, on apprend la langue du meu 🐮
