# TODO

<!-- Format: bullet list with topic, type names, intended feature - one sentence max -->
<!-- Remove implemented entries in atomic commits (separate from feature commits) -->

- `signature_status`: load peers and your identity once per command and verify only when a `Signed:` line is shown, so `bb log` does not spawn N `ssh-keygen`
