# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

## Unix Tooling MVP

- `cat`: command to output raw normalized code to stdout (no denormalization)
- `pipe`: command to read stdin, apply function, write result to stdout
- `diff`: command to diff two functions by hash
- `edit`: command to open function in $EDITOR, save as new hash on exit
- `add`: support stdin input with `-` placeholder (e.g., `echo "def f(x): return x+1" | mobius.py add -@eng`)

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
