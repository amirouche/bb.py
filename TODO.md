# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `run`: it is not necessary to pass a @lang when the --debug flag is set
- `pipe`: command to read stdin, apply function, write result to stdout
- `html`: command to render pool as static HTML site for browsing
- `run`: thoroughly review including --debug and arguments passing (no implicit coercion)
- `log`: thoroughly review output formatting and metadata display
- `search`: thoroughly review query parsing and result ranking
- `compile`: thoroughly review build process and dependency bundling
- `compile`: drop --output flag, use a.out or a.out.exe as default output
- `translate`: add Spanish translation for add function (d6ecfc90...)
- `translate`: add Spanish translation for twice function (28cdad41...)
- `review`: make interactive, request explicit ack for security/correctness, one function at a time starting from lowest level, remember reviewed functions
- `validate`: validate whole mobius directory including pool and config
