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
does not prove who wrote them or that they are not older. The block height bb
prints (`bb log`, `bb show`, `bb anchor --status`) is read from the proof and not
checked against Bitcoin; `bb anchor --verify` asks the OpenTimestamps client, whose
Python version needs a Bitcoin node for that. Proofs stay pending for a few hours,
and a pending proof is a calendar's promise, not yet evidence.

## A snapshot remembers what was anchored, not what was never anchored

Each `bb anchor` anchors the list of every file, so a record removed afterwards is
seen (`bb anchor --status`, `bb anchor --snapshots`). A record removed before any
snapshot listed it leaves no trace but git's. Someone able to rewrite the whole
`anchor/` directory can replace the chain of snapshots with a shorter one: its
proofs will be younger than the blocks of the chain it replaced, which only shows
to someone who kept an old snapshot id or its proof. Snapshots are not signed.

## A signature says who vouches, not who wrote

With a signing key, what you save is signed: functions, mappings, lineage steps and
worklogs (`Signed:` in `bb log`, `# bb:` lines in `bb show`). A signature shows that
the holder of a key saved that record. It does not show they wrote the code: anyone
can add, and sign, a copy of someone else's function. Who came first is a question
for `bb anchor`, and only between those who anchored.

A function's `object.json` is written once, by whoever adds the code first, and
keeps that author and signature: adding the same code later signs only your own
lineage step. A mapping and a worklog message name no author, so removing their
signature leaves them looking unsigned; what vouches for them is the signed step,
or worklog, naming their hash. A worklog names its original message only, not its
translations. Records saved before signing existed stay unsigned.

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
