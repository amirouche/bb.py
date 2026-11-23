# LIMITS

## Positional arguments only

bb stores parameters under positional placeholders (`_bb_v_N`) so that the same
logic in any human language has one hash. A keyword names a parameter in one
language at the call site, so it cannot reach them: calls to pool functions, to
the function itself and to functions defined inside it take positional arguments
only, and definitions have no keyword-only parameters or `**kwargs`. `bb add` and
`bb edit` refuse code that breaks this rule. Keywords passed indirectly (e.g.
`functools.partial(helper, height=1)`) cannot be detected and fail at run time.

## Anchoring proves a file existed, nothing more

`bb anchor` proves that a file's bytes existed by the time of a Bitcoin block. It
does not prove who wrote them, that they are not older, or that nothing was
removed since: a deleted record leaves no trace but git's. The block height bb
prints (`bb log`, `bb anchor --status`) is read from the proof and not checked
against Bitcoin; `bb anchor --verify` asks the OpenTimestamps client, whose Python
version needs a Bitcoin node for that. Proofs stay pending for a few hours, and a
pending proof is a calendar's promise, not yet evidence.

## A signed step says who vouches, not who wrote

With a signing key, the lineage steps you save are signed (`Signed:` in `bb log`).
The signature shows that the holder of a key saved that code under that name after
that step. It does not show they wrote it: anyone can add, and sign, a copy of
someone else's function. Who came first is a question for `bb anchor`, and only
between those who anchored. The author recorded in a function's own `object.json`
is not signed, nor are worklogs or steps saved before signing existed.

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
