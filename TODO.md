# TODO

This document tracks actionable items for Ouverture development.

Context sources:
- `FEATURES_LIMITATIONS.md` - Current capabilities and known limitations
- `CLAUDE.md` - Technical architecture and development conventions
- `README.md` - Project vision and philosophy

## Priority 1: Critical Bugs

- Fix import rewriting typo: change 'couverture' to 'ouverture' in ouverture.py:205

## Priority 2: Code Quality (Refactoring)

### Phase 1: Modularization (Week 1-2)
- Extract `ASTNormalizer` class to `ouverture/ast_normalization.py`
- Extract name mapping functions to `ouverture/name_mapping.py`
- Extract import handling to `ouverture/import_handling.py`
- Extract hash computation to `ouverture/hash_computation.py`
- Extract storage functions to `ouverture/storage.py`
- Extract denormalization to `ouverture/denormalization.py`
- Extract CLI to `ouverture/cli.py`
- Create `ouverture/config.py` for constants
- Create `ouverture/exceptions.py` for custom exceptions
- Create `ouverture/__init__.py` with public API exports

### Phase 2: Packaging (Week 3-4)
- Create `pyproject.toml` for modern Python packaging
- Add entry point: `ouverture` command
- Test `pip install -e .` works correctly
- Add mypy type checking configuration
- Add ruff linting configuration

### Phase 3: Documentation (Week 5-6)
- Create `docs/API.md` with Python API documentation
- Create `docs/INTERNALS.md` with architecture guide
- Create `docs/CONTRIBUTING.md` for contributors
- Add docstrings to all public functions
- Deprecate `ouverture.py` as standalone script (keep as wrapper)

## Priority 3: Testing Improvements

### Property-Based Testing
- Implement normalization idempotence tests with Hypothesis
- Implement multilingual equivalence property tests
- Implement roundtrip preservation tests
- Implement hash determinism tests

### Multilingual Test Corpus
- Create `tests/corpus/simple_functions/` with parallel implementations
- Create `tests/corpus/with_imports/` with import examples
- Create `tests/corpus/compositional/` with ouverture imports
- Implement corpus discovery and equivalence testing
- Add test cases in 5+ languages (eng, fra, spa, ara, zho)

### Advanced Testing
- Add fuzzing tests with random AST generation
- Add mutation testing for edge case discovery
- Add performance benchmarks for normalization
- Add performance benchmarks for storage operations
- Add scalability tests with 10,000+ functions
- Add integration tests for CLI workflows

### Regression Testing
- Create snapshot tests for expected normalizations
- Mark known issues with `@pytest.mark.xfail`
- Add regression test suite for bug fixes

## Priority 4: Semantic Understanding

### Short Term (6 months)
- Implement basic pattern matching for top 10 equivalent patterns (sum() vs loop, etc.)
- Add execution-based testing with property tests
- Implement `--semantic-level=basic` flag for optional semantic analysis
- Add warning for possible duplicate functions during `add`

### Medium Term (1 year)
- Expand pattern library to top 100 equivalent patterns
- Implement user feedback system for marking functions as equivalent
- Store equivalence relationships in pool metadata
- Add `ouverture search --similar <hash>` command

### Long Term (2+ years)
- Experiment with ML-based code embeddings (CodeBERT)
- Train semantic equivalence model on collected data
- Implement hybrid syntactic → pattern → execution → ML pipeline
- Add cross-language semantic matching (Python ≡ JavaScript)

## Priority 5: Core Features

### Language Support
- Add support for async functions (ast.AsyncFunctionDef)
- Document async function behavior and limitations
- Add support for class storage and normalization (if useful)
- Add support for multiple functions per file with dependency tracking

### Type Hints
- Add optional type hint normalization for consistent hashing
- Implement `--normalize-types` flag
- Document impact on multilingual equivalence

### Import Validation
- Implement validation that ouverture imports exist in pool
- Add `--validate-imports` flag to `add` command
- Warn (don't error) when imports are missing

### CLI Improvements
- Add `ouverture list` command to show pool contents
- Add `ouverture list --hash <HASH>` to show languages for hash
- Add `ouverture stats <HASH>` to show function statistics
- Add `--verbose` flag for detailed output
- Add `--version` flag
- Improve error messages with suggestions

## Priority 6: Infrastructure (Microlibrary Vision)

### Phase 1: Centralized Registry (Months 4-6)
- Design HTTP API for function registry
- Implement PostgreSQL schema for metadata
- Implement S3/blob storage for function JSON
- Implement Redis caching layer
- Implement search by hash, signature, description
- Add `ouverture publish` command
- Add `ouverture pull` command from registry
- Add `ouverture search` command
- Create basic web UI for browsing

### Phase 2: Community Features (Months 7-9)
- Implement user accounts and authentication
- Add ratings and reviews system
- Track download statistics
- Create multilingual landing pages
- Implement translation contribution system
- Add reputation system for contributors

### Phase 3: Developer Tools (Months 10-12)
- Create VS Code extension for ouverture
- Implement GitHub Actions integration
- Create pre-commit hooks template
- Build documentation website
- Create example gallery

### Phase 4: Federation (Year 2)
- Design federated registry protocol
- Implement private registry support
- Add registry configuration in `~/.ouverture/config.yaml`
- Implement registry priority and fallback
- Add semantic search with ML embeddings

## Priority 7: Research

### Experiments to Run
- Benchmark Top 100 equivalent patterns in real Python codebases
- Measure property testing effectiveness (false positive/negative rates)
- Study user preference for programming styles by language/culture
- Measure performance impact of function composition at depth 10, 100, 1000

### User Studies
- Code comprehension: native language vs English (measure time, bug detection)
- Cultural coding patterns: analyze function structures by language community
- LLM performance: train on multilingual corpus, measure improvement

### Publications
- Publish multilingual function corpus as research dataset
- Write paper on semantic equivalence detection approaches
- Write paper on impact of native-language programming on comprehension
- Write paper on multilingual code sharing infrastructure

## Priority 8: Documentation

### User Documentation
- Document workarounds for unsupported features (classes, globals, etc.)
- Create migration guide for future schema version changes
- Add more examples of compositional functions with ouverture imports
- Create quickstart tutorial
- Create video walkthrough

### Developer Documentation
- Document AST normalization algorithm in detail
- Document hash computation strategy and rationale
- Document storage format and versioning
- Document plugin system design (future)
- Create architecture decision records (ADRs)

## Priority 9: Cross-Language Support

### JavaScript Support
- Implement JavaScript AST normalization
- Map JavaScript constructs to canonical form
- Handle differences in type systems
- Test multilingual equivalence: Python ↔ JavaScript

### Rust Support
- Implement Rust AST normalization (harder due to type system)
- Map Rust constructs to canonical form
- Handle lifetime annotations
- Test multilingual equivalence: Python ↔ Rust

### Universal IR (Long Term)
- Design language-independent intermediate representation
- Map Python, JavaScript, Rust to IR
- Compute hash on IR (true cross-language equivalence)

## Priority 10: Production Readiness

### Security
- Implement static analysis for dangerous patterns (eval, exec, os.system)
- Add sandboxed execution for function testing
- Implement code signing for trusted contributors
- Add malware scanning integration

### Performance
- Profile normalization pipeline for bottlenecks
- Optimize storage format (compression, deduplication)
- Implement caching for frequently accessed functions
- Add lazy loading for metadata

### Reliability
- Add comprehensive error handling throughout codebase
- Implement retries for network operations
- Add circuit breaker for registry access
- Implement graceful degradation

### Monitoring
- Add telemetry for usage tracking (opt-in)
- Implement logging with appropriate levels
- Add health check endpoints for registry
- Create dashboard for pool statistics

## Completed

- ✅ Create SEMANTIC_UNDERSTANDING.md analysis document
- ✅ Create REFACTORING.md analysis document
- ✅ Create TESTING.md analysis document
- ✅ Create MICROLIBRARY.md analysis document
