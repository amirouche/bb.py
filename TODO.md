# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `pipe`: command to read stdin, apply function, write result to stdout
- `edit`: command to open function in $EDITOR, save as new hash on exit
- `cat`: command to output raw normalized code to stdout (no denormalization)
- `add`: support stdin input with `-` placeholder (e.g., `echo "def f(x): return x+1" | mobius.py add -@eng`)
- `diff`: command to diff two functions by hash
- `git`: command to generate commit message from pool changes
- `comment`: command to add/view comments on a function hash
- `html`: command to render pool as static HTML site for browsing
- `fork`: command to create a modified function with parent lineage tracking
- `validate`: test, QA, and bulletproof schema checks and error reporting
- `caller`: test, QA, and bulletproof reverse dependency discovery
- `refactor`: test, QA, and bulletproof hash replacement and integrity checks
- `compile`: test, QA, and bulletproof build process and dependency bundling
- `draw`: command to render function output as image (SVG/PNG) for generative art
- `sandbox`: command to run function in restricted mode (no I/O, no network)
- `approve`: command for parent/moderator to mark function as reviewed
- `family`: command to list pool contributors and their functions
- generative art: curate starter pool of classic 2D algorithms (Perlin, spirals, L-systems)
- kid-safe: subset interpreter with visual output and parental approval workflow
- `mutate`: command to port mutation testing library for function-level mutation analysis
