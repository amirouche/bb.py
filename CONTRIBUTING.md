# Contributing to Beyond Babel

Thank you for your interest in contributing to Beyond Babel! This project welcomes all kinds of contributions, and we're excited to have you here.

## Philosophy: Vibe Coding Encouraged

This project embraces **vibe coding** - a creative, exploratory approach to development where intuition and experimentation are valued alongside rigorous planning. We encourage you to:

- Explore ideas freely and propose creative solutions
- Experiment with new approaches to multilingual code sharing
- Follow your instincts when designing features
- Share incomplete ideas - they often spark the best discussions

For a comprehensive understanding of the project architecture, design decisions, and implementation details, please read **[CLAUDE.md](CLAUDE.md)**. This document is your guide to understanding how Beyond Babel works under the hood.

## All Contributions Are Welcome

While code contributions are wonderful, they're not the only way to help! Valuable contributions include:

- **Documentation**: Improve README, add tutorials, translate docs
- **Examples**: Create example functions in different languages (check out the `examples/` directory!)
- **Testing**: Write tests, report bugs, verify multilingual behavior
- **Ideas**: Propose features, suggest improvements, discuss use cases
- **Community**: Answer questions, help other contributors, share your experiences
- **Translations**: Add function translations in your native language
- **Outreach**: Write blog posts, give talks, spread the word

## Getting Started

### Examples Directory

The `examples/` directory contains sample functions demonstrating Beyond Babel's capabilities:

- `example_simple.py` - Basic function (English)
- `example_simple_french.py` - Same logic in French
- `example_simple_spanish.py` - Same logic in Spanish
- `example_with_import.py` - Function with standard library imports
- `example_with_bb.py` - Function calling other pool functions

These examples are great for:
- Understanding how multilingual functions work
- Testing your changes
- Creating new demonstrations
- Learning the normalization process

Try adding your own examples in different human languages!

### Quick Test

```bash
# Add an example function
python3 bb.py add examples/example_simple.py@eng

# Verify it was stored (default location: $HOME/.local/bb)
find $HOME/.local/bb/pool -name "object.json"

# Clean up
rm -rf $HOME/.local/bb
```

## Roadmap: Creative Coding for User Reach

Beyond Babel aims to become a valuable tool for the **creative coding community**, enabling artists and designers to share algorithms across language barriers. Our vision includes:

- **p5.js integration**: Bridge Python functions with p5.js sketches for web-based creative coding
- **Processing compatibility**: Enable Python Mode for Processing to use BB functions
- **Creative coding testbed**: Build a gallery of generative art, visual algorithms, and interactive demos
- **Educational outreach**: Provide multilingual examples for teaching computational creativity
- **Community building**: Foster collaboration between creative coders from different linguistic backgrounds

By targeting creative coding communities (p5.js, Processing, openFrameworks, etc.), we can demonstrate Beyond Babel's value while reaching artists, educators, and students worldwide. If you're interested in creative coding, consider contributing examples that showcase visual algorithms, generative patterns, or interactive systems!

---

# Testing Guide

This section includes comprehensive information about testing Beyond Babel.

## Installation

To run the tests, you need to install pytest:

```bash
pip install -r requirements-dev.txt
```

Or install pytest directly:

```bash
pip install pytest pytest-cov
```

## Running Tests

### Run all tests:

```bash
pytest
```

### Run with verbose output:

```bash
pytest -v
```

### Run with coverage report:

```bash
pytest --cov=bb --cov-report=html
```

### Run specific test by name pattern:

```bash
pytest -k "ast_normalizer"
```

### Run specific test:

```bash
pytest test_bb.py::test_ast_normalizer_visit_name_with_mapping
```

## Common Issues

### Import Errors

If you get import errors, make sure bb.py is in the Python path:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest
```

### Temporary Directory Issues

Tests use temporary directories for isolation. If tests fail with path issues, ensure the tmp_path fixture is working correctly.

### Git Ignores

The following directories/files should be in .gitignore:
- `.bb/` - Local function pool (if used for testing; default is $HOME/.local/bb)
- `__pycache__/` - Python bytecode
- `.pytest_cache/` - Pytest cache
- `htmlcov/` - Coverage reports
- `.coverage` - Coverage data

## Performance

The full test suite should complete in under 10 seconds on modern hardware. If tests are slow:

1. Check for large bb pools in $HOME/.local/bb or local .bb directories
2. Ensure tmp_path fixtures are being used correctly
3. Consider using pytest-xdist for parallel execution:
   ```bash
   pip install pytest-xdist
   pytest -n auto
   ```

## Further Reading

- [pytest documentation](https://docs.pytest.org/)
- [pytest fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [Python AST documentation](https://docs.python.org/3/library/ast.html)

---

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Write descriptive docstrings
- Use the `type_name_verb_complement` naming convention (see CLAUDE.md)

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit your changes with clear messages
6. Push to your branch
7. Open a Pull Request

## Questions?

If you have questions or need help:
- Open an issue for discussion
- Check CLAUDE.md for technical details
- Reach out to the maintainers

Thank you for contributing to Beyond Babel!
