# TODO

This document tracks actionable items for Ouverture development.

Context sources:
- `FEATURES_LIMITATIONS.md` - Current capabilities and known limitations
- `CLAUDE.md` - Technical architecture and development conventions (single-file design)
- `README.md` - Project vision and philosophy

## Priority 0: Future-Proof On-Disk Schema

**Current schema issues**:
- Language codes limited to 3 characters (ISO 639-3)
- Only one name/alias mapping per language
- Mappings stored inline (no deduplication)
- Limited extensibility for metadata

**Schema redesign requirements**:

### Language Support
- Support language identifiers up to 256 characters
- Enable custom language tags (e.g., "eng-formal", "fra-canadian", "python-3.12")
- Allow arbitrary metadata per language variant

### Multiple Mappings Per Language
- Support multiple name mappings for same function in same language
- Use case: Different naming conventions (camelCase vs snake_case)
- Use case: Formal vs informal variable names
- Store array of mappings rather than single mapping per language

### Content-Addressed Mappings
- Hash the content of each name/alias mapping (docstring + name_mapping + alias_mapping)
- Store mappings within function directory: `$OUVERTURE_DIRECTORY/objects/ab/c123.../lang-code/XX/YYYYYY.json`
  (default: `$HOME/.local/ouverture/objects/ab/c123.../lang-code/XX/YYYYYY.json`)
- No hash references in object.json - mappings discovered by scanning language directories
- Deduplication: identical mappings share same hash/file within language directory
- Structure: `{docstring, name_mapping, alias_mapping}`
- All function data grouped in single directory for easy management

### Schema Versioning and Migration
- Implement schema version migration strategy
- Support reading old schema versions
- Provide migration tool: `ouverture.py migrate`
- Document migration path from v0 to v1+

### Extensible Metadata
- Add `metadata` field for extensible key-value pairs
- Support: author, timestamp, tags, description
- Support: dependencies (list of function hashes this depends on)
- Support: test_cases, examples
- Support: performance_characteristics (time/space complexity)
- Keep backward compatibility (optional fields)

### Alternative Hash Algorithms
- Support multiple hash algorithms (SHA256, BLAKE2b, etc.)
- Store hash algorithm in metadata: `hash_algorithm: "sha256"`
- Enable migration to stronger algorithms in future
- Verify hash on load using specified algorithm

### Compression and Encoding
- Add optional compression for large normalized_code
- Store encoding type in metadata: `encoding: "gzip" | "none"`
- Transparent decompression on load

### Proposed Schema v1 - Directory Structure

```
$OUVERTURE_DIRECTORY/objects/          # Default: $HOME/.local/ouverture/objects/
  ab/                                    # First 2 chars of function hash
    c123def456.../                       # Function directory (remaining hash chars)
      object.json                        # Core function data (no language data)
      eng/                               # Language code directory
        xy/                              # First 2 chars of mapping hash
          z789.../mapping.json          # Complete mapping for this variant
        ab/
          cdef.../mapping.json          # Another variant for eng
      fra-canadian/                      # Another language (up to 256 chars)
        mn/
          opqr.../mapping.json
```

### object.json (minimal - no duplication)

```json
{
  "schema_version": 1,
  "hash": "abc123def456...",
  "hash_algorithm": "sha256",
  "normalized_code": "def _ouverture_v_0(...):\n    ...",
  "encoding": "none",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "username",
    "tags": ["math", "statistics"],
    "dependencies": ["def456...", "ghi789..."]
  }
}
```

**No language-specific data** - docstrings, name_mappings, alias_mappings live only in mapping.json files

### mapping.json (in lang-code/XX/YYY.../mapping.json)

```json
{
  "docstring": "Calculate the average of a list of numbers",
  "name_mapping": {"_ouverture_v_0": "calculate_average", "_ouverture_v_1": "numbers"},
  "alias_mapping": {"abc123": "helper"}
}
```

Mapping files are content-addressed by hashing their content, enabling deduplication across functions.

### Implementation Plan
- Design schema v1 with all future-proofing features
- Implement backward compatibility: read v0, write v1
- Create migration tool for existing pools
- Add validation for schema integrity
- Document schema in detail for external implementations

## Priority 1: User Identity and Configuration

### Configuration System
- Implement `ouverture.py init` command to initialize ouverture directory (default: `$HOME/.local/ouverture/`)
- Create `~/.config/ouverture/config.yaml` for user settings (follows XDG Base Directory spec)
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
- Store remote configuration in `~/.config/ouverture/config.yaml`
- Support multiple remotes with priority/fallback

### Remote Storage Backends
- Design SQLite schema for local/file-based remotes
- Implement HTTP API client for HTTP/HTTPS remotes
- Add authentication/authorization for push operations
- Implement conflict resolution for remote operations
- Add caching layer for remote fetches

## Priority 3: Enhanced CLI Commands

### Function Discovery
- Implement `ouverture.py log [NAME | URL]` to show git-like commit log of pool/remote
- Implement `ouverture.py search [NAME | URL] [QUERY...]` to search and list functions
- Add filtering by language, author, date
- Display function statistics (downloads, ratings if available)

### Function Operations
- Implement `ouverture.py translate HASH@LANG LANG` to add translation to existing function
- Implement `ouverture.py review HASH` to recursively review function and dependencies (in user's preferred languages)
- Implement `ouverture.py run HASH@lang` to execute function interactively
- Keep existing `ouverture.py add FILENAME.py@LANG` command
- Update `ouverture.py get HASH[@LANG] FILENAME.py` to save retrieved function to file

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
- Extend language codes beyond 3 characters to support any string <256 chars
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
- Add registry configuration in `~/.config/ouverture/config.yaml`
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
