# bb.py

![Tests](https://github.com/amirouche/bb.py/actions/workflows/test.yml/badge.svg)

**Beyond Babel, Python all around the world, one function at a time**

> **Experimental**: This is research software under active development.

Beyond Babel is a function pool for Python where logic is identity. Functions with the same structure produce the same hash — regardless of variable names, docstrings, or the human language they were written in. Write in your language. Share logic universally.

## Install

```bash
pip install bb.py
```

Requires Python 3.11+. No runtime dependencies.

## Quick Start

Consider three files — the same function, in three languages:

```python
# example_simple.py
def calculate_sum(first_number, second_number):
    """Calculate the sum of two numbers."""
    result = first_number + second_number
    return result
```

```python
# example_simple_french.py
def calculer_somme(premier_nombre, deuxieme_nombre):
    """Calculer la somme de deux nombres."""
    sortie = premier_nombre + deuxieme_nombre
    return sortie
```

```python
# example_simple_spanish.py
def calcular_suma(primer_numero, segundo_numero):
    """Calcular la suma de dos números."""
    resultado = primer_numero + segundo_numero
    return resultado
```

Add them to the pool:

```bash
bb init
bb add example_simple.py@eng
# → 9f86d0...

bb add example_simple_french.py@fra
# → 9f86d0...  ← same hash

bb add example_simple_spanish.py@spa
# → 9f86d0...  ← same hash again
```

Same logic, same hash. Three languages, one identity. Now retrieve it in any language:

```bash
bb show 9f86d0...@eng    # English names and docstring
bb show 9f86d0...@fra    # French names and docstring
bb show 9f86d0...@spa    # Spanish names and docstring
```

## Features

- **Same logic = same hash** — variable names, docstrings, and human language don't affect identity
- **Content-addressed storage** — functions stored by hash, deduplicated by design
- **Translate, don't rewrite** — add new language mappings to existing functions with `bb translate`
- **Run and test** — execute functions directly with `bb run`, verify with `bb check`
- **Compose** — functions can import other pool functions; dependencies are tracked
- **Compile** — flatten a function and its dependencies into a standalone Python script
- **Search and navigate** — find functions with `bb search`, trace dependencies with `bb tree` and `bb caller`
- **Refactor** — swap dependencies across the pool with `bb refactor`
- **Remote sync** — push, pull, and sync pools across repositories
- **Single file, zero dependencies** — the entire tool is one Python file with no runtime dependencies

## How It Works

```
Source code → Parse to AST → Normalize → Hash → Store
```

Normalization renames all local variables to a canonical form (`_bb_v_0`, `_bb_v_1`, ...), sorts imports, and strips docstrings before hashing. Built-in names and imports are never renamed. The result: any function with the same logical structure produces the same SHA-256 hash, regardless of the names chosen by the author.

The original names, docstrings, and language metadata are stored alongside the hash — one mapping per language. This separates identity (the logic) from presentation (the language).

## Why "Beyond Babel"?

The Tower of Babel story is about humanity divided by language barriers. Beyond Babel transcends those barriers — not by erasing differences, but by recognizing equivalence where it naturally emerges. The same logic flows from French to English to Spanish and back. Different words, same meaning.

## Vision

Beyond Babel is a step toward Möbius — a language where content-addressing and timestamps make lineage visible. Who made what, who built on whom, who absorbed whose work. The math mirror doesn't prescribe norms or enforce justice. It steers toward a society that can see the structure behind itself. Refusing amnesia.

## Related Work

- **[Unison](https://www.unison-lang.org/)** — content-addressable code where the hash is the identity
- **[Abstract Wikipedia](https://meta.wikimedia.org/wiki/Abstract_Wikipedia)** — multilingual knowledge representation that separates meaning from language
- **[Situational application](https://en.wikipedia.org/wiki/Situational_application)** — local, contextual solutions (also known as Situated Software)
- **Non-English-based programming languages** — [Wikipedia overview](https://en.wikipedia.org/wiki/Non-English-based_programming_languages)
- **Content-addressed storage** — Git, IPFS, Nix
- **AST-based code similarity** — Moss, JPlag
- **Multilingual programming** — Racket's #lang system, Babylonian programming
- **Code normalization** — abstract interpretation, program synthesis

## See Also

- [`transcripts/`](transcripts/) — walkthrough sessions showing Beyond Babel in action
- [`CLAUDE.md`](CLAUDE.md) — technical architecture and development guide
- [`SKILLS.md`](SKILLS.md) — development workflows, testing, and conventions
- [`LIMITS.md`](LIMITS.md) — known limitations and research questions

---

*"The limits of my language mean the limits of my world."* – Ludwig Wittgenstein
