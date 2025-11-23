<p align="center">
  <img src="logo.svg" alt="Beyond Babel Logo" width="200" />
</p>

# bb.py

![Tests](https://github.com/amirouche/bb.py/actions/workflows/test.yml/badge.svg)

**Beyond Babel, Python all around the world, one function at a time**

> ⚠️ **Experimental**: This is research software under active development.

> 🐮 **Curious?** Check out the [transcripts](transcripts/end2end/) first, especially [La Langue du Feu](https://github.com/amirouche/bb.py/blob/main/transcripts/end2end/00.md) — a multilingual coding session that shows Beyond Babel in action.

Write functions in your language. Share logic universally. Beyond Babel creates bridges through shared logic—not by erasing differences, but by recognizing equivalence where it naturally emerges.

Beyond Babel is a function pool where the same code written in different human languages produces the same hash.

## Quick Start

```bash
git clone https://github.com/amirouche/bb.py
cd bb.py

# View examples
cat examples/example_simple.py          # English
cat examples/example_simple_french.py   # French
cat examples/example_simple_spanish.py  # Spanish

# Add a function to the pool
python3 bb.py add examples/example_simple.py@eng

# Get the hash (stored in $HOME/.local/bb/pool/ by default)
# Note: Use $BB_DIRECTORY to customize the location
find ~/.local/bb/pool -name "*.json"

# Retrieve in different language
python3 bb.py get <HASH>@fra
```

## Why "Beyond Babel"?

Beyond Babel refers to the Tower of Babel story - where humanity was divided by language barriers. Beyond Babel transcends those barriers, enabling code logic to flow seamlessly across human languages. Just as people once shared a common language, Beyond Babel code has no boundary between languages: the same logic flows seamlessly from French to English to Spanish and back.

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
