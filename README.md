# bb.py

![Tests](https://github.com/amirouche/bb.py/actions/workflows/test.yml/badge.svg)

**Nurturing a society that recognizes diversity as strength through verifiable knowledge sharing.**

> **Experimental**: This is research software under active development.

Every programmer who thinks in Wolof, Tamil, Vietnamese, or Tamazight and codes in English pays a cognitive tax. Every variable named in a second language is a thought translated before it's expressed. This overhead is invisible to the people who don't pay it — and universal for everyone who does.

bb.py makes that tax optional. Write functions in your language — name variables, write documentation, think natively. The tool separates what your code *does* from what you *called* things. Same logic, same hash, regardless of tongue.

Content-addressing gives every function a unique fingerprint. Authorship is preserved. Lineage is traceable. Knowledge is shared without losing track of who made what.

## Same logic, three languages, one hash

```python
# English
def calculate_sum(first_number, second_number):
    """Calculate the sum of two numbers."""
    result = first_number + second_number
    return result
```

```python
# Français
def calculer_somme(premier_nombre, deuxieme_nombre):
    """Calculer la somme de deux nombres."""
    sortie = premier_nombre + deuxieme_nombre
    return sortie
```

```python
# Español
def calcular_suma(primer_numero, segundo_numero):
    """Calcular la suma de dos números."""
    resultado = primer_numero + segundo_numero
    return resultado
```

```bash
bb init
bb add example_simple.py@eng      # → 9f86d0...
bb add example_simple_french.py@fra   # → 9f86d0...  ← same hash
bb add example_simple_spanish.py@spa  # → 9f86d0...  ← same hash again
```

Three languages. One identity. No one translated — they wrote originals.

## What it enables

bb.py is a tool for sharing content-addressed knowledge that is verifiable, maintainable, and preserves authorship and lineage.

- **Think in your language** — variable names and documentation in your native tongue, without penalty
- **Share across languages** — retrieve any function in any language with `bb show hash@lang`
- **Verify identity** — same logic always produces the same hash, no matter who wrote it or in what language
- **Preserve lineage** — every function is traceable; who made what, who built on whom
- **Compose and build** — functions import other pool functions; dependencies are tracked, compiled, and runnable
- **Single file, zero dependencies** — the entire tool is one Python file

## How it works

```
Source code → Parse to AST → Normalize → Hash → Store
```

Normalization renames all local variables to a canonical form, sorts imports, and strips docstrings before hashing. Built-in names and imports are never renamed. The result: any function with the same logical structure produces the same SHA-256 hash, regardless of the names chosen by the author.

The original names, docstrings, and language metadata are stored alongside the hash — one mapping per language. This separates identity (the logic) from presentation (the language).

Exact matching is the foundation — the clean case where two people write the same logic independently and the hash proves it. For the realistic case where two people solve the same problem differently, semantic search surfaces near-matches: similar structure, different choices. Convergence isn't forced. It's discovered. The hash is the meeting point — independent teams who solve the same problem find each other through identity, not coordination.

## Install

```bash
# Using pip
pip install git+https://github.com/amirouche/bb.py.git

# Using uv
uv tool install git+https://github.com/amirouche/bb.py.git
```

Requires Python 3.11+. No runtime dependencies.

## Vision

bb.py is a step toward Möbius — a content-addressed language where timestamps make lineage visible, and names are views into a multilingual registry. Who made what, who built on whom, who absorbed whose work without credit. The mirror doesn't prescribe norms or enforce justice. It refuses amnesia.

## Related Work

- **[Unison](https://www.unison-lang.org/)** — content-addressable code where the hash is the identity
- **[Abstract Wikipedia](https://meta.wikimedia.org/wiki/Abstract_Wikipedia)** — multilingual knowledge representation that separates meaning from language
- **[Situational application](https://en.wikipedia.org/wiki/Situational_application)** — local, contextual solutions (also known as Situated Software)
- **Non-English-based programming languages** — [Wikipedia overview](https://en.wikipedia.org/wiki/Non-English-based_programming_languages)
- **Content-addressed storage** — Git, IPFS, Nix
- **Multilingual programming** — Racket's #lang system, Babylonian programming

## See Also

- [`transcripts/`](transcripts/) — walkthrough sessions showing Beyond Babel in action
- [`LIMITS.md`](LIMITS.md) — known limitations and research questions

---

*"The limits of my language mean the limits of my world."* — Ludwig Wittgenstein
