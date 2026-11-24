# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `object_hash_matches`: return False unless `checks` is absent or a list of strings, so a tampered object is reported instead of crashing `bb validate`
- `code_denormalize`: reject mapping names and aliases that are not non-keyword identifiers, which can inject code through `bb compile --debug` and forge `bb show`
- `update_check_rewritten`: require `base` and its closure to be approved or authored before `bb update` runs its checks
- `update`, `refactor`, `code_load_normalized`: call `object_check_hash` on every base so swapped code never becomes a new, correctly hashed version
- `review_approvals`: verify a review against an allowed-signers entry holding exactly its email and `public_key`, so one peer cannot supersede another's rejection (introduced on hello-beyonder: before, a reject always won)
- `code_restore_inner_imports`: stop the unhashed `normalization` field and unverified mappings from changing how approved code behaves
- `review`: verify every hash in `order` before partitioning, so an approved but swapped function exits 1 instead of "already reviewed"
- `review_render`, `translate_render_file`: call `object_check_hash` on each check and translation source before showing it, and refuse approval of the parent on failure
- `review`: validate key and email with `re.fullmatch` so the one-line invariant of allowed_signers also rejects newlines
- `edit`: skip the "one name, one code" guard when `prep['hash']` is the edited function, so a doc-only edit of a name with two current steps is not refused
- `helper_previous_version`: keep a `seen` set like the other lineage walks, so a step whose `from` loops back cannot hang `bb search` and `bb latest`
- `signature_sign`: treat any `OSError` or decode error from `ssh-keygen` as a signing failure and save unsigned without `public_key`, never half a save
- `review_publish`, `review_verify`, `review_list_shared`: use `signature_sign`, `signature_verify` and `signature_status` instead of their own copies of the rules
- `signature_status`: load peers and your identity once per command and verify only when a `Signed:` line is shown, so `bb log` does not spawn N `ssh-keygen`
- `search_describe`: compute `search_uses` and `search_pinned_uses` once per function instead of once per field and per outdated dependency
