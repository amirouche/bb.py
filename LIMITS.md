# LIMITS

## Positional arguments only

bb stores parameters under positional placeholders (`_bb_v_N`) so that the same
logic in any human language has one hash. A keyword names a parameter in one
language at the call site, so it cannot reach them: calls to pool functions, to
the function itself and to functions defined inside it take positional arguments
only, and definitions have no keyword-only parameters or `**kwargs`. `bb add` and
`bb edit` refuse code that breaks this rule. Keywords passed indirectly (e.g.
`functools.partial(helper, height=1)`) cannot be detected and fail at run time.

## Potential Enhancements

1. **Class support**: Would require normalizing method names, class hierarchy, etc.
2. **Multiple functions per file**: Would require dependency tracking within the file
3. **Semantic equivalence**: Detect functionally equivalent but syntactically different code
4. **Type hint normalization**: Optionally normalize type hints for consistent hashing
5. **Import validation**: Verify bb imports exist in the pool
6. **Cross-language support**: Extend beyond Python (JavaScript, Rust, etc.)
7. **Version migration**: Handle schema changes in stored JSON format

## Research Questions

1. **Composability**: How deep can function composition go before performance degrades?
2. **Cognitive impact**: Does coding in native language improve comprehension and reduce bugs?
3. **LLM training**: Could multilingual function pools improve LLM performance on non-English code?
4. **Scale**: How large can the function pool grow before search/retrieval becomes slow?
