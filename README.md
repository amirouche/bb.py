# mobius.py

![Tests](https://github.com/amirouche/mobius.py/actions/workflows/test.yml/badge.svg)

**Beyond Babel, Python all around the world, one function at a time**

Write functions in your language. Share logic universally. Mobius creates bridges through shared logic—not by erasing differences, but by recognizing equivalence where it naturally emerges.

Mobius is a function pool where the same code written in different human languages produces the same hash.

## The Idea

What if you could write `calculer_somme` in French, `calcular_suma` in Spanish, or `calculate_sum` in English—and they all map to the same function in a shared pool? What if code could be language-agnostic for machines while remaining native for humans?

Mobius is a function pool where **the same logic written in different human languages shares the same hash**. A French developer can write:

```python
def calculer_moyenne(nombres):
    """Calcule la moyenne d'une liste de nombres"""
    return sum(nombres) / len(nombres)
```

While a Spanish developer writes:

```python
def calcular_promedio(numeros):
    """Calcula el promedio de una lista de números"""
    return sum(numeros) / len(numeros)
```

And an English developer writes:

```python
def calculate_average(numbers):
    """Calculate the average of a list of numbers"""
    return sum(numbers) / len(numbers)
```

**These three functions produce the same hash** because they implement identical logic. They're stored together in a content-addressed pool, preserving each language's perspective while recognizing their logical equivalence.

## How It Works

Mobius normalizes Python functions by:
1. Parsing code to an Abstract Syntax Tree (AST)
2. Extracting docstrings (language-specific)
3. Renaming variables to canonical forms (`_mobius_v_0`, `_mobius_v_1`, etc.)
4. Computing a hash on the **logic only** (excluding docstrings)
5. Storing both the normalized code and language-specific name mappings

When you retrieve a function, it's reconstructed in your target language:

```bash
# Add functions in different languages
python3 mobius.py add examples/example_simple.py@eng
python3 mobius.py add examples/example_simple_french.py@fra
python3 mobius.py add examples/example_simple_spanish.py@spa

# All three produce the same hash!
# Retrieve in any language
python3 mobius.py get <HASH>@fra  # Returns French version
python3 mobius.py get <HASH>@spa  # Returns Spanish version
```

## Why This Matters

**Universal logic, local expression**: Functions are stored by what they do, not what they're called. A developer in Seoul can use a function written in São Paulo without translation loss.

**LLM-compatible, human-friendly**: LLMs work with normalized forms while developers work in their native languages. Both perspectives coexist.

**Choice over convention**: You can write in English if you prefer. You can also write in Tagalog, Arabic, or Swahili. The system treats all perspectives as equally valid.

## Status: Experimental Infrastructure

This is research software. The current implementation:
- ✅ Normalizes Python ASTs
- ✅ Generates deterministic hashes for equivalent logic
- ✅ Stores multilingual variants in content-addressed pool
- ✅ Reconstructs code in target language
- ⚠️ Has known bugs (e.g., typos in imports)
- ⚠️ Limited to Python (for now)
- ⚠️ No semantic understanding (purely syntactic)

## Quick Start

```bash
# View examples
cat examples/example_simple.py          # English
cat examples/example_simple_french.py   # French
cat examples/example_simple_spanish.py  # Spanish

# Add a function to the pool
python3 mobius.py add examples/example_simple.py@eng

# Get the hash (stored in $HOME/.local/mobius/objects/ by default)
# Note: Use $MOBIUS_DIRECTORY to customize the location
find ~/.local/mobius/objects -name "*.json"

# Retrieve in different language
python3 mobius.py get <HASH>@fra
```

## Examples

### Simple Function (No Imports)

**English** (`examples/example_simple.py`):
```python
def sum_list(items):
    """Sum a list of numbers"""
    total = 0
    for item in items:
        total += item
    return total
```

**French** (`examples/example_simple_french.py`):
```python
def somme_liste(elements):
    """Somme une liste de nombres"""
    total = 0
    for element in elements:
        total += element
    return total
```

These hash to the same value.

### With Standard Library Imports

**English** (`examples/example_with_import.py`):
```python
from collections import Counter

def count_frequency(items):
    """Count frequency of items"""
    return Counter(items)
```

Import names (`Counter`) are preserved, variable names (`items`) are normalized.

### Compositional Functions

Functions can reference other functions from the pool:

```python
from mobius.pool import abc123def as helper

def process_data(values):
    """Process data using helper function"""
    return helper(values)
```

The import is normalized to `from mobius.pool import abc123def`, making it language-agnostic.

## Why "Mobius"?

Mobius refers to the Mobius strip - a surface with only one side, representing the continuous transformation between languages and the unity of code logic regardless of linguistic expression. Just as the Mobius strip has no boundary between its "sides," Mobius code has no boundary between languages: the same logic flows seamlessly from French to English to Spanish and back.

## Origins, Vision & Philosophy

This idea has been brewing for over a decade, long before the current LLM revolution. The core goals were:

1. **Code as a reusable resource**: Write a function, store it, forget it, and retrieve it later—dependencies and all—without the hassle of reinventing wheels (e.g., the infamous leftpad incident or countless buried helper functions).

2. **Lowering barriers**: Enable people to contribute to code in ways that feel natural to them, reducing friction between thought and expression.

### The Bigger Picture

If Mobius succeeds, it could become infrastructure like npmjs—but with **less friction, less drama, and fewer barriers**. The irony? The vision remains relevant even without LLMs. The core idea—content-addressable, multilingual code—stands on its own.

This explains why the hash-on-logic-not-names design is so critical—it's not just a technical detail, it's the **enabler of the entire multilingual vision**. A French developer writing `calculer_somme(nombres)` and an English developer writing `calculate_sum(numbers)` are contributing the **same** function to the same pool.

### Why This Matters

Code is thought made explicit. Language shapes thought. If code can only be "readable" in one language, we're limiting who can think clearly in code.

As AI systems trained predominantly on English codebases become ubiquitous, there's a subtle risk: we might optimize code for machine readability while narrowing the range of human expression. Mobius explores whether we can have both—tools that work *with* multilingual thinking instead of requiring everyone to think the same way.

That's the Mobius transformation: code that flows continuously between linguistic perspectives, recognizing their unity.

## Architecture

- **Single file**: `mobius.py` (~600 lines)
- **Storage**: `$HOME/.local/mobius/objects/sha256/XX/YYYYYY.../object.json` (content-addressed, configurable via `MOBIUS_DIRECTORY`)
- **Language codes**: Any string up to 256 characters (e.g., eng, fra, español, 日本語)
- **Hash algorithm**: SHA256 on normalized AST

See `CLAUDE.md` for detailed technical documentation.

## Research Questions

1. **Code comprehension**: Does writing in one's native language improve understanding and reduce bugs?
2. **LLM training**: Could multilingual code pools improve LLM performance on non-English code?
3. **Semantic equivalence**: When do syntactic differences reflect semantic distinctions vs. mere translation?
4. **Community building**: Can language-diverse function pools foster more inclusive open source communities?

## Contributions Welcome

We especially welcome:
- **Non-English examples**: Add functions in your native language
- **Bug reports**: The code is messy, we know
- **Linguistic insights**: Are there language structures Python's AST can't normalize?
- **Alternative implementations**: Try this in other languages (Rust? JavaScript?)
- **Criticism**: Is this solving a real problem or creating new ones?

## Known Issues

- **Only functions are supported**: Classes and methods cannot be stored by mobius (only standalone functions)
- Only supports Python 3.9+ (requires `ast.unparse()`)
- Limited testing on edge cases
- No package distribution yet

## License

MIT (see LICENSE file)

## Related Work

### Key Inspirations

- **[Unison](https://www.unison-lang.org/)**: Content-addressable code where the hash is the identity
- **[Abstract Wikipedia](https://meta.wikimedia.org/wiki/Abstract_Wikipedia)**: Multilingual knowledge representation that separates meaning from language
- **[Situational application](https://en.wikipedia.org/wiki/Situational_application)**: Also known as **Situated Software**: Local, contextual solutions

### Similar Projects & Research Areas

- **Non-English-based programming languages**: [Wikipedia overview](https://en.wikipedia.org/wiki/Non-English-based_programming_languages) of programming languages designed for non-English speakers
- **Content-addressed storage**: Git, IPFS, Nix
- **AST-based code similarity**: Moss, JPlag
- **Multilingual programming**: Racket's #lang system, Babylonian programming
- **Code normalization**: Abstract interpretation, program synthesis

## Contact

File issues on GitHub. We're learning in public.

---

*"The limits of my language mean the limits of my world."* – Ludwig Wittgenstein
