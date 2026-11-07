# bb.py

![Tests](https://github.com/amirouche/bb.py/actions/workflows/test.yml/badge.svg)

**Diversity as strength, made verifiable.**

Write a function in your language; if someone else, human or machine, had the same thought, the hash will show it.

> **Experimental**: This is research software under active development.

Convergence is detected, divergence is preserved. When two minds write the same logic, they get the same hash, and no shared language was needed to find each other. When they write different logic, they get different hashes, and both are kept. Nothing is flattened into a single English version.

```python
# Français
def additionner(premier, second):
    """Additionner deux nombres."""
    somme = premier + second
    return somme
```

```python
# العربية
def اجمع(الأول, الثاني):
    """اجمع عددين."""
    الناتج = الأول + الثاني
    return الناتج
```

```python
# Español
def sumar(primero, segundo):
    """Sumar dos números."""
    suma = primero + segundo
    return suma
```

```bash
bb add additionner.py@fra   # → one hash
bb add اجمع.py@ara          # → the same hash
bb add sumar.py@spa         # → the same hash again
```

Three people, or two people and a model prompted in Arabic. Nobody translated; each wrote an original. The names and the docstring stay in their own language, and the hash is computed from the logic alone.

## Why

Students learn to code in English while thinking in their own language, and every name they choose is a thought translated before it is written. Languages in Arabic script also tend to cost more tokens on LLMs for the same meaning, a quiet tax on the people who already pay one.

The machine side is the same question seen from outside. When several LLM agents work on one problem, someone has to know whether two of them really agree, or only sound alike. A shared hash is a cheap, honest answer for the case where the logic is identical, and an honest "no" when it is not.

The project is early. Exact matching is the foundation; near-matches are still a research question (see `LIMITS.md`).

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

## About

Nurturing a society that recognizes diversity as strength through verifiable knowledge sharing.

Content-addressing gives every function a fingerprint. Authorship is preserved. Lineage is traceable. Knowledge is shared without losing track of who made what.

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
