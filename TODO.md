# TODO

This document tracks actionable items for Ouverture development.

Context sources:
- `FEATURES_LIMITATIONS.md` - Current capabilities and known limitations
- `CLAUDE.md` - Technical architecture and development conventions (single-file design)
- `README.md` - Project vision and philosophy

## Priority 1: User Identity and Configuration

### Configuration System
- Implement `ouverture.py init` command to create `.ouverture/` config
- Create `~/.ouverture/config.yaml` for user settings
- Implement `ouverture.py whoami username [USERNAME]` to set/get username
- Implement `ouverture.py whoami email [EMAIL]` to set/get email
- Implement `ouverture.py whoami public-key [URL]` to set/get public key location
- Implement `ouverture.py whoami language [LANG...]` to set/get preferred languages
- Store user identity in config for attribution

## Priority 2: Remote Repository System

### Remote Management
- Implement `ouverture.py remote add NAME URL` for HTTP/HTTPS remotes
- Implement `ouverture.py remote add NAME file:///path/to/win.sqlite` for SQLite remotes
- Implement `ouverture.py remote remove NAME` to remove remotes
- Implement `ouverture.py remote pull NAME` to fetch functions from remote
- Implement `ouverture.py remote push NAME` to publish functions to remote
- Store remote configuration in `.ouverture/config.yaml`
- Support multiple remotes with priority/fallback

### Remote Storage Backends
- Design SQLite schema for local/file-based remotes
- Implement HTTP API client for HTTP/HTTPS remotes
- Add authentication/authorization for push operations
- Implement conflict resolution for remote operations
- Add caching layer for remote fetches

## Priority 3: Enhanced CLI Commands

### Function Discovery
- Implement `ouverture.py log [NAME | URL]` to show available functions
- Implement `ouverture.py search [NAME | URL] [QUERY...]` to search functions
- Add filtering by language, author, date
- Display function statistics (downloads, ratings if available)

### Function Operations
- Implement `ouverture.py translate HASH@LANG LANG` to add translation to existing function
- Implement `ouverture.py review HASH` to review function details
- Implement `ouverture.py run HASH@lang` to execute function interactively
- Keep existing `ouverture.py add FILENAME.py@LANG` command
- Keep existing `ouverture.py get HASH` command (enhance to accept `HASH@LANG`)

## Priority 4: Native Language Debugging

### Traceback Localization
- Implement traceback rewriting to show native language variable names
- When exception occurs, map `_ouverture_v_X` back to original names
- Show both normalized and native language versions of traceback
- Preserve line numbers from original source

### Interactive Debugger Integration
- Integrate with Python debugger (pdb)
- Show variables in native language during debugging
- Allow setting breakpoints using native language names
- Implement `ouverture.py run HASH@lang --debug` for interactive debugging
- Support stepping through code with native language context

## Priority 5: Code Quality

### Documentation
- Create `docs/API.md` with Python API documentation
- Create `docs/INTERNALS.md` with architecture guide
- Create `docs/CONTRIBUTING.md` for contributors
- Add docstrings to all public functions
- Document the single-file architecture philosophy in CLAUDE.md

### Packaging
- Create `pyproject.toml` for modern Python packaging
- Add entry point: `ouverture` command
- Test `pip install -e .` works correctly
- Add mypy type checking configuration
- Add ruff linting configuration

## Priority 6: Testing Improvements

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

## Priority 7: Semantic Understanding

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

## Priority 8: Core Features

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

## Priority 9: Infrastructure (Microlibrary Vision)

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

## Priority 10: Research

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

## Priority 11: Documentation

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

## Priority 12: Cross-Language Support

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

## Priority 13: Production Readiness

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
