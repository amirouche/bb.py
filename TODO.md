# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `edit`: skip the "one name, one code" guard when `prep['hash']` is the edited function, so a doc-only edit of a name with two current steps is not refused
- `helper_previous_version`: keep a `seen` set like the other lineage walks, so a step whose `from` loops back cannot hang `bb search` and `bb latest`
- `signature_sign`: treat any `OSError` or decode error from `ssh-keygen` as a signing failure and save unsigned without `public_key`, never half a save
- `review_publish`, `review_verify`, `review_list_shared`: use `signature_sign`, `signature_verify` and `signature_status` instead of their own copies of the rules
- `signature_status`: load peers and your identity once per command and verify only when a `Signed:` line is shown, so `bb log` does not spawn N `ssh-keygen`
- `search_describe`: compute `search_uses` and `search_pinned_uses` once per function instead of once per field and per outdated dependency
