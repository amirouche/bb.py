# ♻️ mobius.py

![Tests](https://github.com/amirouche/mobius.py/actions/workflows/test.yml/badge.svg)

**Beyond Babel, Python all around the world, one function at a time**

> ⚠️ **Experimental**: This is research software under active development.

Write functions in your language. Share logic universally. Mobius creates bridges through shared logic—not by erasing differences, but by recognizing equivalence where it naturally emerges.

Mobius is a function pool where the same code written in different human languages produces the same hash.

## Quick Start

```bash
git clone https://github.com/amirouche/mobius.py
cd mobius.py

# View examples
cat examples/example_simple.py          # English
cat examples/example_simple_french.py   # French
cat examples/example_simple_spanish.py  # Spanish

# Add a function to the pool
python3 mobius.py add examples/example_simple.py@eng

# Get the hash (stored in $HOME/.local/mobius/pool/ by default)
# Note: Use $MOBIUS_DIRECTORY to customize the location
find ~/.local/mobius/pool -name "*.json"

# Retrieve in different language
python3 mobius.py get <HASH>@fra
```

## Why "Mobius"?

Mobius refers to the Mobius strip - a surface with only one side, representing the continuous transformation between languages and the unity of code logic regardless of linguistic expression. Just as the Mobius strip has no boundary between its "sides," Mobius code has no boundary between languages: the same logic flows seamlessly from French to English to Spanish and back.

## Related Work

- **[Unison](https://www.unison-lang.org/)**: Content-addressable code where the hash is the identity
- **[Abstract Wikipedia](https://meta.wikimedia.org/wiki/Abstract_Wikipedia)**: Multilingual knowledge representation that separates meaning from language
- **[Situational application](https://en.wikipedia.org/wiki/Situational_application)**: Also known as **Situated Software**: Local, contextual solutions
- **Non-English-based programming languages**: [Wikipedia overview](https://en.wikipedia.org/wiki/Non-English-based_programming_languages) of programming languages designed for non-English speakers
- **Content-addressed storage**: Git, IPFS, Nix
- **AST-based code similarity**: Moss, JPlag
- **Multilingual programming**: Racket's #lang system, Babylonian programming
- **Code normalization**: Abstract interpretation, program synthesis

## See Also

`USAGE.md` for CLI commands, `CLAUDE.md` for technical details, `ROADMAP.md` for what's next, `LIMITS.md` for known limitations, and `CONTRIBUTING.md` to get involved.

---

*"The limits of my language mean the limits of my world."* – Ludwig Wittgenstein
