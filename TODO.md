# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `review`: make interactive, request explicit ack for security/correctness, one function at a time starting from lowest level, remember reviewed functions
- `add`: support adding a module with several definitions
- `search`: tell namesakes apart by running one against the other's checks (interchangeable, covers, incompatible)
- `update`, `refactor`: hash exactly the stored `normalized_code` so a body with two leading strings does not save a version `object_hash_matches` refuses
- `object_hash_matches`: return False unless `checks` is absent or a list of strings, so a tampered object is reported instead of crashing `bb validate`
- `code_denormalize`: reject mapping names and aliases that are not non-keyword identifiers, which can inject code through `bb compile --debug` and forge `bb show`
- `update_check_rewritten`: require `base` and its closure to be approved or authored before `bb update` runs its checks
- `update`, `refactor`, `code_load_normalized`: call `object_check_hash` on every base so swapped code never becomes a new, correctly hashed version
- `review_approvals`: verify a review against an allowed-signers entry holding exactly its email and `public_key`, so one peer cannot supersede another's rejection
- `code_restore_inner_imports`: stop the unhashed `normalization` field and unverified mappings from changing how approved code behaves
- `review`: verify every hash in `order` before partitioning, so an approved but swapped function exits 1 instead of "already reviewed"
- `review_render`, `translate_render_file`: call `object_check_hash` on each check and translation source before showing it, and refuse approval of the parent on failure
- `review`: validate key and email with `re.fullmatch` so the one-line invariant of allowed_signers also rejects newlines
