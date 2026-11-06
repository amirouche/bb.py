# Transcript: Shared reviews and trusted peers

## Metadata
- ID: 10
- Feature: bb whoami signing-key, bb review (signed), bb reviews, bb trust publish/import
- Date: 2026-11-05
- Status: draft
- Complexity: intermediate

## Overview
bb only runs code you approved. Reading everything yourself does not scale, so a review can be shared: with a signing key, every verdict you give in `bb review` is also written to `$BB_DIRECTORY/review/` and signed with `ssh-keygen`. Anyone able to commit can write files there, so a shared review only counts for you when its signature checks out against a peer in your private list of trusted peers. This transcript has Alice review a function and Bob come to trust her review, then shows a tampered review being rejected.

Alice and Bob share one `$BB_DIRECTORY` here, as they would through git. Each has their own private state (config, approvals, trusted peers), selected here with `BB_STATE_DIRECTORY`.

## Prerequisites
- `bb` installed and in PATH
- `ssh-keygen` (OpenSSH 8.0 or newer)

---

# Chapter 1: Alice Signs Her Review

## Source Files

### Show double `show_double_eng.py`
```python
def show_double(x):
    """Print twice x."""
    print(int(x) * 2)
```

## Workflow

### Step 1: Alice gets an SSH key and tells bb to sign with it
```bash
$ ssh-keygen -q -t ed25519 -N "" -C alice -f alice_key
$ env BB_STATE_DIRECTORY=alice bb whoami name Alice
Set name: Alice
$ env BB_STATE_DIRECTORY=alice bb whoami email alice@example.org
Set email: alice@example.org
$ env BB_STATE_DIRECTORY=alice bb whoami signing-key alice_key
Set signing-key: alice_key
```

### Step 2: Alice adds the function and reviews it
Her approval is saved privately, and published as a signed review.

```bash
$ env BB_STATE_DIRECTORY=alice bb add show_double_eng.py@eng
Hash: 622a6c87530d8dac589db37c98dc6608cff82ae935ef0805d5d354ac4b731ee8
Mapping hash: 53a63e33c5dfc91d5f7a1d4bc3bb10f0a9dec54b021510ee8ccb453e9481f92f
$ yes | env BB_STATE_DIRECTORY=alice bb review show_double@eng -m "Read it" | grep "✓"
Approve this function? (r to reject) [y/n/r/q]: ✓ Function approved and signed review published.
```

### Step 3: Alice adds herself to the shared list of peers
The shared list is a directory of keys, nothing more: nobody trusts it
as a whole.

```bash
$ env BB_STATE_DIRECTORY=alice bb trust publish | cut -d' ' -f1-2
Published: alice@example.org
```

---

# Chapter 2: Bob Decides Whom to Trust

## Workflow

### Step 4: Bob sees the review, but does not trust Alice yet
```bash
$ env BB_STATE_DIRECTORY=bob bb reviews show_double@eng | tail -1 | awk '{print $1, $2, $3}'
untrusted approve Alice
$ (env BB_STATE_DIRECTORY=bob bb run show_double@eng 3 2>&1 || true) | head -1
Error: Refusing to run code you have not reviewed (1 function(s) to approve).
```

### Step 5: Bob imports Alice from the shared list
`bb trust import` shows each entry and asks; here Bob answers `y`.

```bash
$ echo y | env BB_STATE_DIRECTORY=bob bb trust import | tail -1
Trusted peers: 1
```

### Step 6: Alice's review now counts for Bob
```bash
$ env BB_STATE_DIRECTORY=bob bb reviews show_double@eng | tail -1 | awk '{print $1, $2, $3}'
trusted approve Alice
$ env BB_STATE_DIRECTORY=bob bb run show_double@eng 3
6
```

---

# Chapter 3: A Tampered Review

Someone with commit access edits Alice's review. The signature no longer
matches, so the review stops counting.

## Workflow

### Step 7: Change one word of the signed review
```bash
$ sed -i 's/Read it/Never read it/' $BB_DIRECTORY/review/*/*/*.json
$ env BB_STATE_DIRECTORY=bob bb reviews show_double@eng | tail -1 | awk '{print $1, $2, $3}'
invalid approve Alice
$ (env BB_STATE_DIRECTORY=bob bb run show_double@eng 3 2>&1 || true) | head -1
Error: Refusing to run code you have not reviewed (1 function(s) to approve).
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb whoami signing-key PATH` | Sign your reviews with this SSH key (PATH.pub next to it) |
| `bb review X -m COMMENT` | Approve (`y`) or reject (`r`); with a key, publishes a signed review |
| `bb reviews X` | Who reviewed X: trusted, untrusted or invalid |
| `bb trust publish` | Add yourself to the shared list of peers |
| `bb trust import` | Copy peers from the shared list into yours, one by one |
| `bb trust add/remove/list` | Manage your trusted peers directly |

## Key Insights

1. **Only your private list is trusted**: it lives in your state directory, outside the shared `$BB_DIRECTORY`. The shared list is only a place to find keys.

2. **Signatures, not files, carry trust**: a review counts when it is signed by a key you trust, for the function it names. An edited, moved or forged review is invalid.

3. **Rejections count too**: a trusted peer's rejection cancels trusted approvals of that hash. Your own verdict always wins.
