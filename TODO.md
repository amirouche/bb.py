# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

## Unix Tooling MVP

- `cat`: command to output raw normalized code to stdout (no denormalization)
- `pipe`: command to read stdin, apply function, write result to stdout
- `diff`: command to diff two functions by hash
- `edit`: command to open function in $EDITOR, save as new hash on exit
- `add`: support stdin input with `-` placeholder (e.g., `echo "def f(x): return x+1" | mobius.py add -@eng`)

## Paco MVP (Parser Combinators)

- `paco`: function-based parser combinator library built on Mobius pool
- `paco`: predicate-based type checks with asserts for runtime validation
- `paco`: linter integration to catch type/parse errors at edit time
- `paco`: composable parsers as content-addressed functions (share grammars across projects)

## Compiler MVP (Python to WASM)

- `compile`: extend to emit WebAssembly via Python-to-WASM transpilation
- `compile`: bundle function + dependencies into standalone .wasm module
- `compile`: browser-runnable functions from the pool

## Couch MVP (Visual/Casual Programming)

- `couch`: browser-based visual environment for composing Mobius functions
- `couch`: drag-and-drop function composition without writing code
- `couch`: live preview of function outputs as you connect blocks
- `couch`: export compositions back to Python/Mobius pool

## Monetization Ideas

- Family search engine: genealogy-focused function library (niche with paying users)
- Data pipeline marketplace: sell/license curated function collections
- Enterprise pool hosting: managed remote with access control and audit logs
- Educational platform: learn programming through multilingual function exploration

## Backlog

- `fork`: command to create a modified function with parent lineage tracking
- `init`: test, QA, and bulletproof error handling for edge cases
- `whoami`: test, QA, and bulletproof config validation and error messages
- `add`: test, QA, and bulletproof parsing, normalization, and hash stability
- `get`: test, QA, and bulletproof (deprecated, ensure graceful migration to show)
- `show`: test, QA, and bulletproof mapping selection and output formatting
- `translate`: test, QA, and bulletproof interactive prompts and validation
- `run`: test, QA, and bulletproof execution sandbox and argument handling
- `review`: test, QA, and bulletproof recursive dependency resolution
- `log`: test, QA, and bulletproof output formatting and empty pool handling
- `search`: test, QA, and bulletproof query parsing and result ranking
- `remote`: test, QA, and bulletproof URL validation and network error handling
- `validate`: test, QA, and bulletproof schema checks and error reporting
- `caller`: test, QA, and bulletproof reverse dependency discovery
- `refactor`: test, QA, and bulletproof hash replacement and integrity checks
- `compile`: test, QA, and bulletproof build process and dependency bundling
