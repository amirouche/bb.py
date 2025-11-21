# TODO

## Known Bugs

- Fix import rewriting typo: change 'couverture' to 'ouverture' in ouverture.py:205

## Deferred Analysis Documents

- Restore SEMANTIC_UNDERSTANDING.md from git tag analysis-documents-draft
- Restore REFACTORING.md from git tag analysis-documents-draft
- Restore TESTING.md from git tag analysis-documents-draft
- Restore MICROLIBRARY.md from git tag analysis-documents-draft

## Feature Enhancements

- Add support for class storage and normalization
- Add support for multiple functions per file with dependency tracking
- Add support for async functions (ast.AsyncFunctionDef)
- Implement semantic equivalence detection for functionally identical code
- Add optional type hint normalization for consistent hashing
- Implement import validation to verify ouverture imports exist in pool
- Add cross-language support beyond Python (JavaScript, Rust, etc.)
- Implement version migration system for stored JSON format changes

## Testing & Validation

- Test async function support and document behavior
- Add test cases for edge cases in FEATURES_LIMITATIONS.md
- Expand pytest coverage for error handling scenarios

## Research Questions

- Measure performance impact of deep function composition in ouverture pool
- Study cognitive impact of native language variable names on comprehension and bug reduction
- Investigate whether multilingual function pools improve LLM performance on non-English code
- Benchmark function pool scalability and search/retrieval performance at large scale

## Documentation

- Document workarounds for unsupported features in user guide
- Create migration guide for future schema version changes
- Add examples of compositional functions with ouverture imports
