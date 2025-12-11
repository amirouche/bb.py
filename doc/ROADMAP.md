# ROADMAP

This document tracks the development roadmap for Beyond Babel.

Context sources:
- `LIMITS.md` - Current capabilities and known limitations
- `CLAUDE.md` - Technical architecture and development conventions (single-file design)
- `README.md` - Project vision and philosophy

## Priority 0: LMDB Storage with zlmdb (nstore/vnstore)

### Background

Replace the current filesystem-based storage (git-like content-addressed directories) with LMDB-backed storage using [zlmdb](https://github.com/crossbario/zlmdb).

**Key concepts:**
- **nstore**: Generic tuple store abstraction ([SRFI 168](https://srfi.schemers.org/srfi-168/srfi-168.html)) - generalizes triplestores/quadstores to n-tuples
- **vnstore**: Versioned nstore with history tracking (alive/dead tuples, temporal queries)

**Benefits:**
- 🚀 **Performance**: LMDB is extremely fast with memory-mapped I/O
- 🔒 **ACID transactions**: Atomic writes, consistent reads
- 📦 **Single-file database**: Replace directory tree with compact `.lmdb` file
- 📜 **Built-in versioning**: vnstore tracks function evolution over time
- 🔍 **Efficient queries**: Tuple-based queries for dependencies, languages, mappings
- 🧹 **No external dependencies**: Self-contained, no git required

### Implementation Plan

#### Phase 1: Core nstore Implementation (Weeks 1-2)

**Tasks:**
- Add zlmdb dependency to `pyproject.toml`
- Implement `nstore` abstraction layer:
  - `nstore_open(path)` → initialize LMDB database
  - `nstore_add(store, tuple)` → add n-tuple to store
  - `nstore_ask(store, pattern)` → check if tuple exists
  - `nstore_query(store, pattern)` → query tuples matching pattern
  - `nstore_remove(store, tuple)` → remove tuple (soft delete for vnstore)
  - `nstore_close(store)` → close database

**Tuple schema for bb.py:**
```python
# Function storage (normalized code)
('function', hash, 'code', normalized_code)
('function', hash, 'schema_version', 1)
('function', hash, 'created', timestamp)
('function', hash, 'author', author_name)

# Language mapping storage
('mapping', func_hash, lang, mapping_hash, 'docstring', docstring_text)
('mapping', func_hash, lang, mapping_hash, 'name_mapping', json_blob)
('mapping', func_hash, lang, mapping_hash, 'alias_mapping', json_blob)
('mapping', func_hash, lang, mapping_hash, 'comment', comment_text)

# Index tuples for queries
('index', 'by_author', author_name, func_hash)
('index', 'by_lang', lang, func_hash)
('index', 'by_created', timestamp, func_hash)
```

**Migration strategy:**
- Keep filesystem storage functions for backwards compatibility
- Add `--storage-backend` flag: `filesystem` (default) or `lmdb`
- Implement `bb.py migrate filesystem-to-lmdb` command

#### Phase 2: vnstore for Versioning (Weeks 3-4)

**Tasks:**
- Implement `vnstore` layer on top of nstore:
  - `vnstore_add(store, tuple, change_id)` → add versioned tuple
  - `vnstore_query(store, pattern, as_of=None)` → time-travel queries
  - `vnstore_history(store, tuple)` → get tuple history
  - `vnstore_diff(store, change_id_a, change_id_b)` → compare versions

**Use cases:**
- Track function evolution: `bb.py history HASH@lang`
- Compare versions: `bb.py diff HASH@lang@v1 HASH@lang@v2`
- Revert to previous version: `bb.py revert HASH@lang --to CHANGE_ID`
- List all changes: `bb.py log --author USERNAME --since DATE`

**Change tracking schema:**
```python
# Change log
('change', change_id, 'timestamp', timestamp)
('change', change_id, 'author', author_name)
('change', change_id, 'message', commit_message)
('change', change_id, 'parent', parent_change_id)  # for history graph

# Tuple liveness tracking
('vnstore', tuple_hash, change_id, 'alive', True/False)
```

#### Phase 3: Query & Index Optimization (Week 5)

**Tasks:**
- Build efficient indexes for common queries:
  - Find all functions by author
  - Find all languages for a function hash
  - Find all mappings for (hash, lang) pair
  - Full-text search on docstrings
- Implement query planner for complex queries
- Add statistics tracking (usage counts, popular functions)

#### Phase 4: Testing & Documentation (Week 6)

**Tasks:**
- Write comprehensive tests for nstore/vnstore
- Property-based tests for tuple store invariants
- Performance benchmarks vs filesystem storage
- Migration guide for existing pools
- Document tuple schema and query patterns

### Future Extensions

**Advanced vnstore features:**
- Branch support (multiple timelines)
- Merge resolution for conflicting changes
- Garbage collection for dead tuples
- Compact representation of history

**Federation support:**
- Replicate vnstore across remotes
- Pull/push changes by change_id
- Conflict resolution strategies

## Priority 1: Real-World Application Development & Code Agent Loops

### Motivation

For bb.py to be truly useful in real-world software development—especially for AI code agents—we need to address critical gaps in **discovery**, **evolution**, **testing**, and **composition**.

### Current State vs. Real-World Needs

#### ✅ What bb.py Does Well Today

**Coding from scratch:**
- Agent can add functions to pool as they're written
- Multilingual support means the pool grows across language communities
- Content-addressed storage ensures deduplication

**Exploration:**
- Can check if a function already exists by normalizing and hashing
- `show` command lets you inspect pool functions

#### 🚧 Critical Gaps for Agent Workflows

**1. Discovery Problem**
```bash
# Agent wants to: "Find a function that calculates averages"
# Today: No way to search the pool semantically
# Need: bb.py search "calculate average" @eng
```

**2. Versioning/Evolution**
```bash
# Bug found in pool function abc123
# Today: Content-addressed = immutable
# Need: bb.py evolve abc123 --fix "handle empty list" --creates def456
#       bb.py deprecate abc123 --replaced-by def456
```

**3. Metadata & Context**
```bash
# Agent needs: tags, categories, use cases, dependencies
# Today: Only author + timestamp
# Need: bb.py tag abc123 "math" "statistics" "pure"
#       bb.py add example.py@eng --category "data-processing" --pure
```

**4. Testing & Validation**
```bash
# Agent needs: confidence that pool functions work
# Today: No testing framework integration
# Need: bb.py test abc123 --with tests/test_abc123.py
#       bb.py verify abc123 --type-check --lint
```

### Code Agent Loop Design

#### Scenario 1: Writing New Code

```python
# Agent's inner loop:
1. Understand task → break into sub-functions
2. For each sub-function:
   a. Search pool: bb.py search "description" @eng
   b. If found: import from bb.pool
   c. If not: write function, add to pool
3. Compose solution from pool functions + new functions
4. Add main function to pool
```

**Missing pieces:**
- Semantic search (embedding-based or tag-based)
- Confidence scoring (how good is this match?)
- Usage statistics (how popular/reliable?)

#### Scenario 2: Bug Fixing

```python
# Agent discovers bug in pool function
1. Load function: bb.py show abc123@eng
2. Fix bug locally
3. Normalize + hash fixed version → def456
4. bb.py evolve abc123 --to def456 --reason "Fix: handle edge case X"
5. Update dependents (need dependency graph!)
```

**Missing pieces:**
- Evolution tracking (abc123 → def456 relationship)
- Deprecation warnings
- Automated dependency updates
- Migration tools

#### Scenario 3: Refactoring

```python
# Agent detects duplicate logic in codebase
1. Extract common code to function
2. Normalize + hash → abc123
3. bb.py show abc123@eng → already exists!
4. Replace local code with: from bb.pool import object_abc123 as helper
5. bb.py usage abc123 → see where else it's used
```

**Missing pieces:**
- Duplicate detection across codebase
- Automated refactoring suggestions
- Impact analysis

### Proposed Extensions for Agent Workflows

#### 1. Enhanced Metadata Schema

```json
{
  "schema_version": 2,
  "hash": "abc123...",
  "normalized_code": "...",
  "metadata": {
    "created": "2025-11-27T10:00:00Z",
    "author": "agent-claude",
    "tags": ["math", "statistics", "pure"],
    "category": "data-processing",
    "dependencies": ["def456", "ghi789"],
    "dependents": ["jkl012"],
    "tests": ["test_abc123.py"],
    "quality": {
      "usage_count": 42,
      "test_coverage": 95,
      "type_checked": true
    },
    "evolution": {
      "replaces": null,
      "replaced_by": null,
      "related": ["xyz999"]
    }
  }
}
```

#### 2. Search & Discovery Commands

```bash
# Semantic search
bb.py search "calculate list average" @eng --limit 5

# Tag-based search
bb.py list --tag "math" --tag "pure" @eng

# Dependency queries
bb.py deps abc123 --recursive
bb.py dependents abc123
bb.py impact abc123  # what breaks if we change this?
```

#### 3. Evolution & Versioning

```bash
# Track evolution
bb.py evolve abc123 new_version.py@eng --reason "Performance improvement"

# Deprecation
bb.py deprecate abc123 --replaced-by def456

# Migration
bb.py migrate abc123 def456 --dry-run
bb.py migrate abc123 def456 --in project/  # update all imports
```

#### 4. Quality & Testing

```bash
# Add tests
bb.py test add abc123 tests/test_average.py

# Validate
bb.py validate abc123 --type-check --lint --test

# Stats
bb.py stats abc123  # usage, quality metrics
```

### Implementation Roadmap

**Phase 1: Basic Metadata (Weeks 1-2)** ✅ Easy
- Add `tags` and `category` fields to metadata
- Implement `bb.py tag` command
- Store tags in nstore tuples
- **Documentation storage**: Store markdown files and relate them to function hashes
- **Hash labeling/naming**: Implement human-readable labels/aliases for hashes

**Phase 2: Tag-Based Search (Weeks 3-4)** - Medium
- Implement `bb.py search --tag TAG`
- Build tag index in nstore
- Support multi-tag queries with AND/OR

**Phase 3: Dependency Tracking (Weeks 5-6)** - Medium
- Parse `from bb.pool import ...` statements
- Build dependency graph in nstore
- Implement `bb.py deps` and `bb.py dependents`
- **Call stack traversal**: Implement to figure out all paths to reach a function from another

**Phase 4: Evolution Tracking (Weeks 7-8)** - Medium
- Link related functions via evolution metadata
- Implement `bb.py evolve` command
- Track deprecation chains

**Phase 5: Testing Integration (Weeks 9-10)** - Medium
- Link test files to functions
- Implement `bb.py test add/run`
- Track test coverage metadata

**Phase 6: Statistics & Quality (Weeks 11-12)** - Harder
- Usage tracking (import counts)
- Quality metrics (tests, types, lint)
- Validation framework

**Phase 7: Semantic Search (Future)** - Harder
- Embedding-based similarity search
- Integration with vector databases
- LLM-powered code understanding

**Phase 8: Whole Program Synthesis (Future)** - Harder
- Design schemas and architecture for whole program synthesis
- Planning framework for composing functions from pool into complete programs
- Code generation strategies for multi-function applications

### Open Questions

1. **Discovery priority**: Tag-based search first (simpler) or semantic embeddings (more powerful)?
   - **Recommendation**: Start with tags, add semantic later

2. **Mutability stance**: Immutable (functional) or allow evolution/updates?
   - **Recommendation**: Immutable hashes + evolution tracking (best of both worlds)

3. **Agent integration**: Library (Python API) or CLI tool?
   - **Recommendation**: Both - CLI wraps Python API

4. **Scope**: Just functions, or also classes, modules, fixtures?
   - **Recommendation**: Start with functions, extend later

## Priority 2: Applications

- **ffff**: CLI zettelkasten for linked ideas with a Forth-like interpreter
- **bb**: Rewrite of bb in bb (self-hosting)
- **p5py**: Port p5.js creative coding library to Python using bb for function sharing
- **asyncify**: Tool for on-the-fly rewriting of synchronous Python code to async/await style
- **todo-flask**: Reference todo application built with Flask demonstrating bb integration
- **ing0**: Bubblewrap wrapper to run bb.py across Linux distributions and assist with cross-compiling
- **chez-scheme-port**: Port Beyond Babel to Chez Scheme
- **beyond-babel**: Rename bb.py to beyond-babel with CLI and module called bb.py across Linux

## Priority 3: Remote HTTP/HTTPS Support

### HTTP/HTTPS Remotes
- Implement HTTP API client for HTTP/HTTPS remotes
- Add `bb.py remote add NAME URL` support for HTTP/HTTPS URLs
- Add authentication/authorization for push operations
- Implement conflict resolution for remote operations
- Add caching layer for remote fetches

### Remote Storage Backends (Advanced)
- Design SQLite schema for local/file-based remotes
- Support multiple remotes with priority/fallback

## Priority 4: Search and Discovery Improvements

### Search Filtering
- Add filtering by language, author, date to `search` command
- Display function statistics (downloads, ratings if available)
- Implement `bb.py log [NAME | URL]` to show log of specific remote

### Search Indexing (Performance Enhancement)
- Implement local index for faster search operations
- Index structure: metadata cache (function hashes, docstrings, tags, dependencies)
- Index file: `$BB_DIRECTORY/index.db` (SQLite or JSON-based)
- Automatically update index on `add`, `translate`, and `get` operations
- Implement `bb.py index rebuild` command to rebuild index from objects directory
- Implement `bb.py index verify` command to check index consistency
- Support incremental indexing (only reindex changed functions)
- Search query optimization:
  - Full-text search on docstrings across all languages
  - Prefix/substring matching on function names
  - Tag-based filtering
  - Dependency graph traversal
- Fallback to direct filesystem scan if index is missing or corrupted
- Design considerations:
  - Keep index schema compatible with future v1 storage format
  - Index should be optional (search works without it, just slower)
  - Consider memory-mapped index for large repositories (10,000+ functions)

## Priority 5: Code Quality

### Documentation
- Create `docs/API.md` with Python API documentation
- Create `docs/INTERNALS.md` with architecture guide
- Create `docs/CONTRIBUTING.md` for contributors
- Add docstrings to all public functions
- Document the single-file architecture philosophy in CLAUDE.md

### Packaging
- Create `pyproject.toml` for modern Python packaging
- Add entry point: `bb` command
- Test `pip install -e .` works correctly
- Publish to pypi.org
- Add mypy type checking configuration
- Add ruff linting configuration

### Distribution
- Integrate with PyOxidizer for standalone binary distribution
- Create platform-specific binaries (Linux, macOS, Windows)
- Set up automated release pipeline

## Priority 6: Testing Improvements

### Property-Based Testing
- Implement normalization idempotence tests with Hypothesis
- Implement multilingual equivalence property tests
- Implement roundtrip preservation tests
- Implement hash determinism tests

### Multilingual Test Corpus
- Create `tests/corpus/simple_functions/` with parallel implementations
- Create `tests/corpus/with_imports/` with import examples
- Create `tests/corpus/compositional/` with bb imports
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

### Transcript-Based Testing

Transcript testing uses markdown files to define CLI test scenarios declaratively.

**Why it works for Beyond Babel**:
- CLI commands produce deterministic, predictable output
- Tests follow a consistent setup → command → assert pattern
- Non-programmers can contribute test cases
- Tests serve as self-documenting examples

**Example transcript format** (`tests/transcripts/add_show_roundtrip.md`):

~~~markdown
# Test: Add then Show Roundtrip

## Setup

Create file `greet.py`:

```python
def greet(name):
    """Greet someone by name"""
    return f"Hello, {name}!"
```

## Transcript

```console
$ bb.py add greet.py@eng
Hash: {HASH}

$ bb.py show {HASH}@eng
def greet(name):
    """Greet someone by name"""
    return f'Hello, {name}!'
```

## Assertions

- `{HASH}` is a 64-character hex string
- Show output contains `def greet`
- Show output contains `Hello`
~~~

**Implementation tasks**:
- Create transcript parser for markdown format
- Implement variable capture (`{HASH}` patterns)
- Build transcript runner with temp directory isolation
- Support pattern matching for non-deterministic output
- Integrate with pytest as custom test collector

**Hybrid approach**:
- Use transcripts for happy-path CLI tests
- Keep Python tests for:
  - Complex algorithm unit tests (`test_internals.py`)
  - Grey-box storage validation
  - Error condition testing with specific exit codes

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
- Add `bb search --similar <hash>` command

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
- Implement validation that bb imports exist in pool
- Add `--validate-imports` flag to `add` command
- Warn (don't error) when imports are missing

### CLI Improvements
- Add `bb list` command to show pool contents
- Add `bb list --hash <HASH>` to show languages for hash
- Add `bb stats <HASH>` to show function statistics
- Add `--verbose` flag for detailed output
- Add `--version` flag
- Improve error messages with suggestions

## Priority 9: Native Language Debugging

### Traceback Localization
- Implement traceback rewriting to show native language variable names
- When exception occurs, map `_bb_v_X` back to original names
- Show both normalized and native language versions of traceback
- Preserve line numbers from original source

### Interactive Debugger Integration
- Integrate with Python debugger (pdb)
- Show variables in native language during debugging
- Allow setting breakpoints using native language names
- Implement `bb.py run HASH@lang --debug` for interactive debugging
- Support stepping through code with native language context

## Priority 10: Infrastructure (Microlibrary Vision)

### Phase 1: Centralized Registry (Months 4-6)
- Design HTTP API for function registry
- Implement PostgreSQL schema for metadata
- Implement S3/blob storage for function JSON
- Implement Redis caching layer
- Implement search by hash, signature, description
- Add `bb publish` command
- Add `bb pull` command from registry
- Create basic web UI for browsing

### Phase 2: Community Features (Months 7-9)
- Implement user accounts and authentication
- Add ratings and reviews system
- Track download statistics
- Create multilingual landing pages
- Implement translation contribution system
- Add reputation system for contributors

### Phase 3: Developer Tools (Months 10-12)
- Create VS Code extension for bb
- Implement GitHub Actions integration
- Create pre-commit hooks template
- Build documentation website
- Create example gallery

### Phase 4: Federation (Year 2)
- Design federated registry protocol
- Implement private registry support
- Add registry configuration in `~/.config.bb/config.yaml`
- Implement registry priority and fallback
- Add semantic search with ML embeddings

## Priority 11: Research

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

## Priority 12: Documentation

### User Documentation
- Document workarounds for unsupported features (classes, globals, etc.)
- Create migration guide for future schema version changes
- Add more examples of compositional functions with bb imports
- Create quickstart tutorial
- Create video walkthrough

### Developer Documentation
- Document AST normalization algorithm in detail
- Document hash computation strategy and rationale
- Document storage format and versioning
- Document plugin system design (future)
- Create architecture decision records (ADRs)

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
