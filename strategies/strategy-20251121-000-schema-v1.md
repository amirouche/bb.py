# Schema v1 Implementation Strategy

## Executive Summary

This document outlines the strategy for migrating from schema v0 (single JSON file) to schema v1 (directory-based, content-addressed mappings). The migration adopts **immediate v1 as the default write format** with read-only backward compatibility for v0.

**Key decisions**:
- ✅ V1 becomes the only write format (no v0 write support)
- ✅ V0 read support maintained indefinitely
- ✅ Migration deletes v0 files by default (with --keep-v0 safety option)
- ✅ Multiple mappings shown as selection menu with explanatory comments
- ✅ Comment field included in mapping.json for variant identification

**Estimated effort**: 5 days implementation + 3-5 days testing/documentation = **8-10 days total**

---

## 1. Current State Analysis (Schema v0)

### Storage Structure
```
$BB_DIRECTORY/objects/
  XX/
    YYYYYY.json  # Single file contains everything
```

### File Format (v0)
```json
{
  "version": 0,
  "hash": "abc123def456...",
  "normalized_code": "def _bb_v_0(...):\n    ...",
  "docstrings": {"eng": "...", "fra": "..."},
  "name_mappings": {"eng": {...}, "fra": {...}},
  "alias_mappings": {"eng": {...}, "fra": {...}}
}
```

### Affected Functions (bb.py)

| Function | Lines | Purpose | Modification Needed |
|----------|-------|---------|-------------------|
| `directory_get_bb()` | 326-336 | Get bb directory | **No change** |
| `hash_compute()` | 321-323 | Compute SHA256 | **Extend** for algorithm support |
| `function_save()` | 339-376 | Save function (v0) | **Major rewrite** |
| `function_load()` | 530-567 | Load function (v0) | **Major rewrite** |
| `function_add()` | 446-486 | CLI add command | **Modify** to use new save |
| `function_get()` | 570-607 | CLI get command | **Modify** to use new load |
| `ast_normalize()` | 272-318 | Normalize AST | **No change** |
| `docstring_replace()` | 489-527 | Replace docstring | **No change** |
| `code_denormalize()` | 378-443 | Denormalize code | **No change** |

**7 functions need modification**, 3 remain unchanged

---

## 2. Target State (Schema v1)

### Storage Structure
```
$BB_DIRECTORY/objects/
  sha256/                       # Hash algorithm name
    ab/                         # First 2 chars of function hash
      c123def456.../            # Function directory (remaining hash chars)
        object.json             # Core function data (no language data)
        eng/                    # Language code directory
          sha256/               # Hash algorithm for mapping
            xy/                 # First 2 chars of mapping hash
              z789abc.../       # Mapping directory (remaining hash chars)
                mapping.json    # Mapping file
            de/
              f012ghi.../
                mapping.json    # Another variant
        fra-canadian/           # Extended language codes (up to 256 chars)
          sha256/
            mn/
              opqr.../
                mapping.json
```

### object.json (v1)
```json
{
  "schema_version": 1,
  "hash": "abc123def456...",
  "hash_algorithm": "sha256",
  "normalized_code": "def _bb_v_0(...):\n    ...",
  "encoding": "none",
  "metadata": {
    "created": "2025-11-21T10:00:00Z",
    "author": "username",
    "tags": ["math", "statistics"],
    "dependencies": ["def456...", "ghi789..."]
  }
}
```

**Key change**: No `docstrings`, `name_mappings`, or `alias_mappings` in object.json

### mapping.json (v1)
```json
{
  "docstring": "Calculate the average of a list of numbers",
  "name_mapping": {"_bb_v_0": "calculate_average", "_bb_v_1": "numbers"},
  "alias_mapping": {"abc123": "helper"},
  "comment": "Formal mathematical terminology variant"
}
```

**Content-addressed**: Hash of this JSON (including comment field) determines the mapping file path

**Note**: The `comment` field provides rationale for this mapping variant (e.g., "formal vs informal", "camelCase style", "domain-specific terminology"). It's **included in the hash** so different variants are properly distinguished, and displayed to help users choose between multiple mappings.

---

## 3. Implementation Phases

### Phase 1: Foundation (Day 1)
**Goal**: Add v1 infrastructure without breaking v0

**New Functions**:
1. `mapping_compute_hash(docstring, name_mapping, alias_mapping) -> str`
   - Create mapping dict, serialize to canonical JSON, compute hash
   - Returns 64-char hex hash

2. `schema_detect_version(func_hash) -> int`
   - Check if function directory exists (v1) or JSON file exists (v0)
   - Returns 0 or 1

3. `metadata_create() -> dict`
   - Generate default metadata (timestamp, author from env, empty tags/deps)

**Modifications**:
- `hash_compute()`: Add optional `algorithm='sha256'` parameter

**Testing**:
- Unit tests for `mapping_compute_hash()` determinism
- Unit tests for `schema_detect_version()` with test fixtures

**Risks**: None (additive changes only)

---

### Phase 2: V1 Write Path (Day 2)
**Goal**: Implement writing v1 format as the default (no v0 write support)

**New Functions**:
1. `function_save_v1(hash_value, normalized_code, metadata)`
   - Create function directory: `objects/XX/Y.../`
   - Write `object.json` with schema_version=1
   - Does NOT write language data

2. `mapping_save_v1(func_hash, lang, docstring, name_mapping, alias_mapping, comment="")`
   - Create mapping dict with comment field
   - Compute mapping hash (including comment)
   - Create language directory: `objects/XX/Y.../lang/`
   - Create mapping directory: `objects/XX/Y.../lang/ZZ/W.../`
   - Write `mapping.json`
   - Return mapping hash for confirmation

**Modifications**:
- Rename `function_save()` to `function_save_v0()` (keep for migration tool only)
- Replace `function_save()` with direct call to `function_save_v1()` + `mapping_save_v1()`
- **No v0 write support** - all new functions written in v1 format

**Testing**:
- Integration test: add function, verify directory structure
- Integration test: add same function in 2 languages, verify deduplication
- Integration test: add same mapping to 2 functions, verify file reuse
- Integration test: add with comment field

**Risks**: Medium (breaking change for write path, but read remains compatible)

---

### Phase 3: V1 Read Path (Day 3)
**Goal**: Implement reading v1 format with v0 backward compatibility (read-only)

**New Functions**:
1. `function_load_v1(hash_value) -> dict`
   - Load `object.json`
   - Return dict with normalized_code, metadata, etc.

2. `mappings_list_v1(func_hash, lang) -> List[tuple]`
   - Scan `objects/XX/Y.../lang/` directory
   - Return list of (mapping_hash, comment) tuples
   - Comment extracted from each mapping.json for display

3. `mapping_load_v1(func_hash, lang, mapping_hash) -> tuple`
   - Load specific mapping file
   - Return (docstring, name_mapping, alias_mapping, comment)

4. `function_load_dispatch(hash_value, lang, mapping_hash=None) -> tuple`
   - Detect schema version using `schema_detect_version()`
   - If v0: Call v0 loader (read-only, for backward compatibility)
   - If v1: Call v1 loader with optional mapping_hash
   - Return unified format: (normalized_code, name_mapping, alias_mapping, docstring, metadata)

**Modifications**:
- Rename `function_load()` to `function_load_v0()` (keep for v0 read support)
- Replace `function_load()` with `function_load_dispatch()`

**Testing**:
- Integration test: write v1, read v1, verify correctness
- Integration test: read v0 file, verify backward compatibility
- Integration test: list mappings for language
- Integration test: load specific mapping by hash

**Risks**: High (must maintain backward compatibility with v0 read)

---

### Phase 4: Migration Tool (Day 4)
**Goal**: Provide migration from v0 to v1 with automatic v0 file deletion

**New Functions**:
1. `schema_migrate_function_v0_to_v1(hash_value, keep_v0=False) -> bool`
   - Load v0 JSON
   - Extract object data and create v1 object.json
   - For each language, create mapping files with empty comment field
   - Validate v1 structure was created correctly
   - Delete v0 file (unless keep_v0=True for safety)
   - Return success/failure

2. `schema_migrate_all_v0_to_v1(keep_v0=False, dry_run=False)`
   - Scan all v0 files in objects/ directory
   - Migrate each function
   - Print statistics (migrated, failed, skipped)
   - Support dry-run mode (no changes)
   - Delete v0 files after successful migration (unless keep_v0=True)

3. `schema_validate_v1(func_hash) -> bool`
   - Verify object.json exists and is valid
   - Verify at least one mapping exists for at least one language
   - Check hash integrity (recompute and compare)
   - Return validation result

**CLI Commands**:
```bash
bb.py migrate               # Migrate all v0 -> v1 (delete v0 after success)
bb.py migrate --keep-v0     # Migrate but keep v0 files (safe mode)
bb.py migrate --dry-run     # Show what would be migrated
bb.py migrate HASH          # Migrate specific function
bb.py validate              # Validate entire pool
bb.py validate HASH         # Validate specific function
```

**Testing**:
- Integration test: migrate simple v0 function, verify v0 deleted
- Integration test: migrate function with multiple languages
- Integration test: migrate function with bb imports
- Integration test: migrate with --keep-v0 flag, verify v0 preserved
- Integration test: dry-run doesn't modify files
- Integration test: validate detects corruption
- Integration test: failed migration doesn't delete v0 file

**Risks**: High (data migration, potential data loss)

**Mitigation**:
- Validate v1 structure before deleting v0 file
- Support --keep-v0 flag for cautious users
- Extensive validation after migration
- Never delete v0 on migration failure

---

### Phase 5: Mapping Exploration (Day 5)
**Goal**: Make it easy to explore and select mappings

**New Command: `show`**

The `show` command replaces and unifies the `get` command with better support for multiple mappings:

```bash
bb.py show HASH@LANG             # Auto-select behavior
bb.py show HASH@LANG@LANGHASH    # Explicit mapping selection
```

**Behavior**:

1. **Single mapping exists**: Print the denormalized function code directly
   ```bash
   $ bb.py show abc123...@eng
   def calculate_average(numbers):
       """Calculate the average of a list of numbers"""
       return sum(numbers) / len(numbers)
   ```

2. **Multiple mappings exist**: Show selection menu with commands
   ```bash
   $ bb.py show abc123...@eng
   Multiple mappings found for 'eng'. Please choose one:

   bb.py show abc123...@eng@xyz789...  # Formal mathematical terminology variant
   bb.py show abc123...@eng@def456...  # Casual style with informal names
   bb.py show abc123...@eng@mno012...  # Domain-specific scientific terminology
   ```
   Each line shows the full command with the mapping hash and the comment explaining the variant.

3. **Explicit mapping hash**: Print the specific mapping
   ```bash
   $ bb.py show abc123...@eng@xyz789...
   def calculate_average(numbers):
       """Calculate the average of a list of numbers"""
       return sum(numbers) / len(numbers)
   ```

**New Functions**:
1. `function_show(hash_with_lang_and_mapping: str)`
   - Parse format: `HASH@LANG[@LANGHASH]`
   - If LANGHASH provided: Load and print that specific mapping
   - If LANGHASH omitted:
     - List all mappings for that language
     - If exactly 1: Print it
     - If multiple: Print selection menu with commands and comments

**Modifications**:
1. Keep `function_get()` for backward compatibility (eventually deprecate)
2. Add `show` subcommand to CLI parser
3. Update `function_add()` to accept optional `--comment` parameter:
   ```bash
   bb.py add example.py@eng --comment "Formal mathematical terminology"
   ```

**Testing**:
- CLI test: show with single mapping, verify output
- CLI test: show with multiple mappings, verify menu format
- CLI test: show with explicit mapping hash
- CLI test: add with comment parameter
- CLI test: show with v0 function (backward compatibility)

**Risks**: Low (additive feature, backward compatible)

---

## 4. Backward Compatibility Strategy

### Immediate V1 Adoption (Clean Break)

**Design Decision**: V1 becomes the default immediately upon release. No v0 write support.

**Write Path**:
- ✅ V1: Default and only write format
- ❌ V0: No write support (removed)

**Read Path**:
- ✅ V1: Native format
- ✅ V0: Read-only support (maintained indefinitely for backward compatibility)

### Migration Strategy

**On encountering v0 files**:
1. CLI commands automatically detect v0 format
2. Display warning with migration instructions
3. Read v0 data successfully (backward compatible)
4. Suggest running migration tool

**Example warning**:
```bash
$ bb.py show abc123...@eng
Warning: Function 'abc123...' uses deprecated schema v0.
Please migrate: bb.py migrate abc123...
Reading v0 format...

def calculate_average(numbers):
    ...
```

### User Guidance

**For users with existing v0 pools**:
1. Run migration tool: `bb.py migrate`
2. Validate results: `bb.py validate`
3. All v0 files automatically deleted after successful migration
4. Use `--keep-v0` flag if cautious

**For new users**:
- No action needed
- All functions stored in v1 format by default

### No Environment Variables Needed

Since v1 is the only write format, no configuration needed. The code automatically:
- Writes v1 for all new functions
- Reads v0 or v1 transparently based on detection

---

## 5. Risk Assessment

### High Risk Areas

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Data loss during migration | **CRITICAL** | Low | Backups, keep v0 files, extensive testing |
| Incompatible with existing tools | High | Medium | Maintain v0 read support, document changes |
| Performance degradation | Medium | Low | Benchmark before/after, optimize file I/O |
| Hash collisions in mappings | Medium | Very Low | Use SHA256 (same as function hashes) |
| Language code validation | Low | Medium | Accept any string <256 chars, document conventions |

### Testing Strategy

1. **Unit tests** (50+ tests):
   - All new functions
   - Edge cases (empty mappings, long language codes, special chars)
   - Hash determinism

2. **Integration tests** (20+ tests):
   - Round-trip: add v1 → get v1
   - Backward compat: add v0 → get with v1 code
   - Migration: v0 → v1 → validate
   - Multi-language: same function, 3 languages

3. **Property-based tests** (Hypothesis):
   - Mapping hash determinism
   - Migration preserves data
   - v0 and v1 produce same denormalized output

4. **Manual testing**:
   - Migrate real-world pool (if available)
   - Test with examples in `examples/`
   - Performance testing with 100+ functions

---

## 6. Decisions Made

### Q1: When to enable v1 by default?
**Decision**: ✅ **Immediately** - V1 becomes the default and only write format upon release.

**Rationale**: Clean break simplifies implementation. No need for dispatch logic or configuration. Users with v0 pools use migration tool once.

### Q2: What to do with v0 files after migration?
**Decision**: ✅ **Delete after successful migration** - V0 files removed automatically.

**Rationale**: Clean storage, no clutter. Safe because:
- Validation runs before deletion
- `--keep-v0` flag available for cautious users
- Migration never deletes on failure

### Q3: How to select mapping when multiple exist?
**Decision**: ✅ **Show selection menu with commands** - When multiple mappings exist, display all options with full commands and comments.

**Example**:
```bash
$ bb.py show HASH@eng
Multiple mappings found for 'eng'. Please choose one:

bb.py show HASH@eng@xyz789...  # Formal mathematical terminology
bb.py show HASH@eng@def456...  # Casual informal style
```

**Rationale**: Explicit is better than implicit. Users see all options and can copy-paste the command they want.

### Q4: Should mapping hash include timestamp/author?
**Decision**: ✅ **No timestamp/author in hash** - Hash only docstring, name_mapping, alias_mapping, and comment.

**Rationale**: Enables deduplication. Identical mappings (even with same comment) share storage across functions.

**Note**: Timestamp/author can be added as metadata fields later if needed (not part of hash).

### Q5: Support compression in initial v1 release?
**Decision**: ✅ **No compression** - Keep it simple for initial release.

**Rationale**: Add complexity only when needed. Can add compression later without breaking changes (use `encoding` field in object.json).

---

## 7. Implementation Checklist

### Phase 1: Foundation ✅ COMPLETED
- [x] Implement `mapping_compute_hash()`
- [x] Implement `schema_detect_version()`
- [x] Implement `metadata_create()`
- [x] Extend `hash_compute()` with algorithm parameter
- [x] Add unit tests for new functions
- [x] Update CLAUDE.md with new functions

### Phase 2: V1 Write Path ✅ COMPLETED
- [x] Rename `function_save()` to `function_save_v0()` (keep for migration tool)
- [x] Implement `function_save_v1()` (object.json creation)
- [x] Implement `mapping_save_v1()` (with comment field support)
- [x] Replace `function_save()` with v1 implementation (no dispatch needed)
- [x] Update `function_add()` to use v1 save functions
- [x] Add `--comment` parameter to `function_add()` CLI
- [x] Add integration tests for v1 writing
- [x] Test deduplication of identical mappings
- [x] Test comment field in mappings

### Phase 3: V1 Read Path ✅ COMPLETED
- [x] Rename `function_load()` to `function_load_v0()` (keep for v0 read support)
- [x] Implement `function_load_v1()` (load object.json)
- [x] Implement `mappings_list_v1()` (return list of mapping_hash, comment tuples)
- [x] Implement `mapping_load_v1()` (load specific mapping by hash)
- [x] Implement `function_load_dispatch()` (detect v0/v1 and route)
- [x] Replace `function_load()` with dispatch implementation
- [x] Add integration tests for v1 reading
- [x] Test backward compatibility with v0 files (read-only)
- [x] Test loading specific mapping by hash

### Phase 4: Migration Tool ✅ COMPLETED
- [x] Implement `schema_migrate_function_v0_to_v1()` (with keep_v0 parameter)
- [x] Implement `schema_migrate_all_v0_to_v1()` (delete v0 by default)
- [x] Implement `schema_validate_v1()` (verify object.json and mappings)
- [x] Add `migrate` CLI command with --keep-v0 flag
- [x] Add `validate` CLI command
- [x] Add integration tests for migration with v0 deletion
- [x] Test --keep-v0 flag preserves v0 files
- [x] Test dry-run mode
- [x] Test migration failure doesn't delete v0 file
- [x] Add validation before v0 deletion

### Phase 5: Mapping Exploration ✅ COMPLETED
- [x] Implement `function_show()` with HASH@LANG[@LANGHASH] parsing
- [x] Add `show` subcommand to CLI parser
- [x] Implement single mapping display (direct code output)
- [x] Implement multiple mapping menu (with commands and comments)
- [x] Implement explicit mapping hash selection
- [x] Update `function_add()` to accept `--comment` parameter
- [x] Keep `function_get()` for backward compatibility (with deprecation note)
- [x] Add CLI tests for show with single mapping
- [x] Add CLI tests for show with multiple mappings
- [x] Add CLI tests for show with explicit mapping hash
- [x] Add CLI tests for add with comment
- [x] Test show with v0 functions (backward compatibility)
- [x] Update documentation (README.md, CLAUDE.md)

### Documentation (Partial)
- [ ] Update TODO.md (mark Priority 0 as completed)
- [x] Update CLAUDE.md with v1 schema details
- [ ] Create migration guide (MIGRATION_V0_TO_V1.md)
- [ ] Update README.md with v1 examples
- [x] Add inline code documentation for all new functions
- [ ] Create schema specification document

### Testing ✅ COMPLETED
- [x] 50+ unit tests for new functions (105 total tests)
- [x] 20+ integration tests for v1 workflow
- [x] 10+ migration tests
- [ ] Property-based tests with Hypothesis
- [ ] Manual testing with examples/
- [ ] Performance benchmarks (v0 vs v1)

---

## 8. Alternatives Considered

### Alternative 1: Hybrid Approach (Single File with References)
**Idea**: Keep single JSON file, but reference external mapping files

```json
{
  "version": 1,
  "hash": "abc123...",
  "normalized_code": "...",
  "mappings": {
    "eng": ["xyz789...", "abc123..."],
    "fra": ["def456..."]
  }
}
```

**Pros**: Easier to implement, less disruptive
**Cons**: Doesn't solve deduplication, mixing concerns

**Decision**: Rejected - Doesn't achieve the goal of content-addressed mappings

### Alternative 2: Database Backend (SQLite)
**Idea**: Store everything in SQLite database instead of filesystem

**Pros**: Easier queries, atomic transactions, better performance
**Cons**: Less git-friendly, harder to inspect, breaks current architecture

**Decision**: Rejected for now - Consider for Priority 2 (remotes)

### Alternative 3: Immediate V1 Adoption (No v0 Write)
**Idea**: Make v1 the only write format, maintain v0 read support

**Pros**: Simpler code (no dispatch), cleaner architecture, forces users to migrate once
**Cons**: Breaking change for write operations

**Decision**: ✅ **ACCEPTED** - Best balance of simplicity and safety. V0 read support provides backward compatibility.

---

## 9. Success Criteria

**Phase completion criteria**:
- [ ] All tests pass (unit + integration)
- [ ] No regression in v0 functionality
- [ ] Migration tool successfully migrates all examples
- [ ] Documentation updated
- [ ] Code review approved

**Release criteria**:
- [ ] All phases completed
- [ ] 90%+ test coverage for new code
- [ ] Manual testing on Linux, macOS, Windows
- [ ] Performance: v1 operations within 20% of v0 speed
- [ ] Migration: 100% success rate on test corpus

---

## 10. Timeline

| Phase | Days | Dependencies | Risk |
|-------|------|--------------|------|
| Phase 1: Foundation | 1 | None | Low |
| Phase 2: V1 Write | 1 | Phase 1 | Medium |
| Phase 3: V1 Read | 1 | Phase 2 | High |
| Phase 4: Migration | 1 | Phase 2+3 | High |
| Phase 5: CLI | 1 | Phase 2+3+4 | Medium |
| **Total** | **5 days** | - | - |

**Additional time**:
- Testing & bug fixes: +2 days
- Documentation: +1 day
- Code review & revisions: +1 day

**Total estimated time**: 8-10 days

---

## 11. Next Steps

**Immediate actions**:
1. ✅ **Review this strategy** - Complete
2. ✅ **Answer open questions** - All decisions made (see Section 6)
3. **Create feature branch**: `feature/schema-v1`
4. **Start Phase 1** implementation
5. **Set up CI/CD** for automated testing

**Before starting implementation**:
- [x] Approval on overall strategy ✅
- [x] Decisions on open questions ✅
- [ ] Agreement on timeline
- [ ] Backup plan if issues arise

---

## Conclusion

The migration from schema v0 to v1 is a significant undertaking that will **future-proof** the bb storage format. The proposed **5-phase approach** with **immediate v1 adoption** provides:

- **Simplicity**: No dispatch logic, no configuration - v1 is the only write format
- **Safety**: Read-only v0 support for backward compatibility, validation before migration
- **Flexibility**: Content-addressed mappings, extended language codes (up to 256 chars), extensible metadata
- **Clarity**: Clean separation between code (object.json) and language variants (mapping.json)
- **Discoverability**: Multiple mappings per language with explanatory comments

**Key design decisions**:
- ✅ V1 becomes default immediately (no gradual rollout)
- ✅ V0 read-only support maintained indefinitely
- ✅ Migration deletes v0 files by default (with --keep-v0 safety flag)
- ✅ Multiple mappings shown as selection menu with comments
- ✅ No compression or timestamp in initial release (YAGNI principle)

**Recommendation**: Proceed with implementation, starting with Phase 1 (Foundation), with close attention to testing and backward compatibility for v0 reading.

The key to success is **incremental progress** with **continuous validation** at each phase.

**Total estimated time**: 8-10 days (5 days implementation + 3-5 days testing/documentation)
