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

**Universal logic, local expression**: Functions are stored by what they do, not what they're called. A developer in Seoul can use a function written in São Paulo without translation loss.

**LLM-compatible, human-friendly**: LLMs work with normalized forms while developers work in their native languages. Both perspectives coexist.

**Choice over convention**: You can write in English if you prefer. You can also write in Tagalog, Arabic, or Swahili. The system treats all perspectives as equally valid.

Code is thought made explicit. Language shapes thought. If code can only be "readable" in one language, we're limiting who can think clearly in code.

As AI systems trained predominantly on English codebases become ubiquitous, there's a subtle risk: we might optimize code for machine readability while narrowing the range of human expression. Mobius explores whether we can have both—tools that work *with* multilingual thinking instead of requiring everyone to think the same way.

That's the Mobius transformation: code that flows continuously between linguistic perspectives, recognizing their unity.

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

## See Also

`USAGE.md` for CLI commands, `CLAUDE.md` for technical details, `ROADMAP.md` for what's next, `LIMITS.md` for known limitations, and `CONTRIBUTING.md` to get involved.

---

*"The limits of my language mean the limits of my world."* – Ludwig Wittgenstein
