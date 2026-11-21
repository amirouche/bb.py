# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `review`: make interactive, request explicit ack for security/correctness, one function at a time starting from lowest level, remember reviewed functions
- `add`: support adding a module with several definitions
- `identity`: support cryptographic identity management and signing function hash with mapping upon review
- `resolve`: when several functions answer to a name, remember your pick in private state so `bb.resolve` follows it
- `search`: tell namesakes apart by running one against the other's checks (interchangeable, covers, incompatible)
