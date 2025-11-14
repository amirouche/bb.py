# Ouverture

**Infrastructure for cognitive diversity in the post-LLM era**

This is draft infrastructure exploring whether cognitive diversity enables LLM/non-LLM compatibility. The code is messy because we're learning. Contributions welcome, especially from non-English-dominant perspectives.

## The Problem

Large Language Models have amplified a subtle form of linguistic imperialism: code becomes more readable to English-speaking LLMs but potentially less accessible to programmers who think in other languages. When you write `calculate_sum` instead of `calculer_somme` or `calcular_suma`, you're optimizing for Silicon Valley's training data, not for human cognitive diversity.

What if we could have both?

## The Idea

Ouverture is a function pool where **the same logic written in different human languages shares the same hash**. A French developer can write:

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

Ouverture normalizes Python functions by:
1. Parsing code to an Abstract Syntax Tree (AST)
2. Extracting docstrings (language-specific)
3. Renaming variables to canonical forms (`_ouverture_v_0`, `_ouverture_v_1`, etc.)
4. Computing a hash on the **logic only** (excluding docstrings)
5. Storing both the normalized code and language-specific name mappings

When you retrieve a function, it's reconstructed in your target language:

```bash
# Add functions in different languages
python3 ouverture.py add example_simple.py@eng
python3 ouverture.py add example_simple_french.py@fra
python3 ouverture.py add example_simple_spanish.py@spa

# All three produce the same hash!
# Retrieve in any language
python3 ouverture.py get <HASH>@fra  # Returns French version
python3 ouverture.py get <HASH>@spa  # Returns Spanish version
```

## Post-LLM Implications

**For LLMs**: They can work with canonical normalized forms, making code search and reuse language-agnostic.

**For Humans**: Developers maintain their cognitive workspace in their native language while accessing a global function pool.

**For Collaboration**: A French developer can use a function originally written in Korean without translation loss, because the system preserves both perspectives.

**For Diversity**: We challenge the assumption that "English variable names = universal readability."

## Status: Experimental Infrastructure

This is research software. The current implementation:
- ✅ Normalizes Python ASTs
- ✅ Generates deterministic hashes for equivalent logic
- ✅ Stores multilingual variants in content-addressed pool
- ✅ Reconstructs code in target language
- ⚠️ Has known bugs (e.g., `couverture` typo in imports)
- ⚠️ Limited to Python (for now)
- ⚠️ No semantic understanding (purely syntactic)

## Quick Start

```bash
# View examples
cat example_simple.py          # English
cat example_simple_french.py   # French
cat example_simple_spanish.py  # Spanish

# Add a function to the pool
python3 ouverture.py add example_simple.py@eng

# Get the hash (stored in .ouverture/objects/)
find .ouverture/objects -name "*.json"

# Retrieve in different language
python3 ouverture.py get <HASH>@fra
```

## Examples

### Simple Function (No Imports)

**English** (`example_simple.py`):
```python
def sum_list(items):
    """Sum a list of numbers"""
    total = 0
    for item in items:
        total += item
    return total
```

**French** (`example_simple_french.py`):
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

**English** (`example_with_import.py`):
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
from ouverture import abc123def as helper

def process_data(values):
    """Process data using helper function"""
    return helper(values)
```

The import is normalized to `from ouverture import abc123def`, making it language-agnostic.

## Why "Ouverture"?

French for "opening" or "overture" - the beginning of something larger. Also a nod to the multilingual nature of the project.

## Architecture

- **Single file**: `ouverture.py` (~600 lines)
- **Storage**: `.ouverture/objects/XX/YYYYYY.json` (content-addressed)
- **Language codes**: ISO 639-3 (eng, fra, spa, etc.)
- **Hash algorithm**: SHA256 on normalized AST

See `CLAUDE.md` for detailed technical documentation.

## Research Questions

1. **Cognitive compatibility**: Does writing in one's native language improve code comprehension and reduce bugs?
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

- Import normalization has a typo (`couverture` instead of `ouverture`)
- Only supports Python 3.9+ (requires `ast.unparse()`)
- No semantic analysis (purely syntactic)
- Limited testing on edge cases
- No package distribution yet

## Philosophy

This project starts from a simple premise: **linguistic diversity is cognitive diversity, and cognitive diversity is valuable**. In a post-LLM world where AI systems are trained predominantly on English codebases, we risk optimizing for machine readability at the expense of human diversity.

Ouverture asks: what if we built tools that worked *with* multilingual thinking instead of around it?

## License

MIT (see LICENSE file)

## Related Work

- Content-addressed storage: Git, IPFS
- AST-based code similarity: Moss, JPlag
- Multilingual programming: Racket's #lang system, Babylonian programming
- Code normalization: Abstract interpretation, program synthesis

## Contact

File issues on GitHub. We're learning in public.

---

*"The limits of my language mean the limits of my world."* – Ludwig Wittgenstein

*Ouverture: What if we had more languages, not fewer?*
