# Transcript: Anchoring the records in Bitcoin

## Metadata
- ID: 14
- Feature: bb anchor, bb anchor --status/--upgrade/--verify, Anchored: in bb log, anchored: in bb show
- Date: 2026-11-23
- Status: draft
- Complexity: intermediate

## Overview
The date of a record is what its author says. `bb anchor` has it attested: every JSON file of the pool, the worklogs and the reviews gets an OpenTimestamps proof next to it, `FILE.ots`, that these exact bytes existed when a given Bitcoin block was mined. The proofs are tracked with git like everything else, and `ots verify FILE.ots` checks one without bb.

bb runs the OpenTimestamps client for this: `ots`, or the command in `$BB_OTS`. So that this transcript needs neither the network nor a few hours of waiting, `$BB_OTS` names a stand-in that writes the same files as the client and answers at once.

## Prerequisites
- `bb` installed and in PATH
- For real use: the OpenTimestamps client (`pip install opentimestamps-client`)

---

# Chapter 1: Stamping

## Source Files

### Greeting `greet_eng.py`
```python
def greet(name):
    """Greet someone by name."""
    return f"Hello, {name}!"
```

### Stand-in for the OpenTimestamps client `ots_stand_in.py`
```python
"""A stand-in for the OpenTimestamps client: the same files, no network."""
import hashlib
import os
import sys

HEADER = b'\x00OpenTimestamps\x00\x00Proof\x00\xbf\x89\xe2\xe8\x84\xe8\x92\x94\x01\x08'
CALENDAR = b'https://calendar.example'
PENDING = bytes.fromhex('83dfe30d2ef90c8e') + bytes([len(CALENDAR) + 1, len(CALENDAR)]) + CALENDAR
BLOCK_870000 = bytes.fromhex('0588960d73d71901') + b'\x03\xf0\x8c\x35'


def proof(path, attestation):
    with open(path, 'rb') as f:
        return HEADER + hashlib.sha256(f.read()).digest() + b'\x00' + attestation


command, paths = sys.argv[1], [a for a in sys.argv[2:] if not a.startswith('-')]
if command == 'stamp':
    for path in paths:
        with open(path + '.ots', 'xb') as f:
            f.write(proof(path, PENDING))
elif command == 'upgrade':
    for path in paths:
        os.replace(path, path + '.bak')
        with open(path, 'wb') as f:
            f.write(proof(path[:-len('.ots')], BLOCK_870000))
elif command == 'verify':
    print("Success! Bitcoin block 870000 attests existence as of 2026-11-23 UTC")
```

## Workflow

### Step 1: Alice adds a function
Three files are written: the function, its English mapping and the
lineage step saying `greet@eng` starts here. Saving never reaches the
network, so nothing is anchored yet.

```bash
$ bb whoami name Alice
Set name: Alice
$ bb whoami language eng
Set language: eng
$ bb add greet_eng.py@eng
Hash: 5de04055d51c70e8e499adcbebf43621e39a43c6579fe7175639f56e5752e1e1
Mapping hash: 0df0d09f8c9769983af92b4762bb81879d3c17e7ef3b08181073f6b43fc21fe2
$ bb anchor --status
Complete: 0
Pending: 0
Not anchored: 3
```

### Step 2: Anchor them
One request to the calendars covers every file. Each gets its proof next
to it (hashes left out here).

```bash
$ env BB_OTS="python3 ots_stand_in.py" bb anchor
Anchored: 3 files, pending until a Bitcoin block holds them (in a few hours: bb anchor --upgrade)
$ (cd $BB_DIRECTORY/pool && find . -name '*.ots' | sed -E 's/[0-9a-f]{2}\/[0-9a-f]{62}/<hash>/; s/[0-9a-f]{64}/<hash>/' | sort)
./functions/<hash>/object.json.ots
./lineage/<hash>.json.ots
./mappings/<hash>/eng/<hash>.json.ots
```

### Step 3: Only what is new is stamped
A proof is made once. Later records are anchored when you next run
`bb anchor`: here a worklog and its message.

```bash
$ env BB_OTS="python3 ots_stand_in.py" bb anchor
Nothing to anchor.
$ bb worklog new greet@eng -m "Wrote a first greeting" | grep -c '^Worklog:'
1
$ env BB_OTS="python3 ots_stand_in.py" bb anchor
Anchored: 2 files, pending until a Bitcoin block holds them (in a few hours: bb anchor --upgrade)
```

---

# Chapter 2: From Pending to a Block

A calendar answers at once with a promise; the proof is complete when a
Bitcoin block holds it, a few hours later.

## Workflow

### Step 4: The log says where each record stands
The function, its lineage step and the worklog each carry an `Anchored:`
line under their date.

```bash
$ bb anchor --status
Complete: 0
Pending: 5
Not anchored: 0
$ bb log greet | grep -c '^Anchored: pending'
3
```

### Step 5: Complete the proofs
```bash
$ env BB_OTS="python3 ots_stand_in.py" bb anchor --upgrade
Completed: 5
Still pending: 0
$ bb anchor --status
Complete: 5 (Bitcoin block 870000)
Pending: 0
Not anchored: 0
$ bb log greet | grep '^Anchored' | sort -u
Anchored: Bitcoin block 870000
```

`bb show` says it too, in `# bb:` comment lines above the code: of the
function, of the mapping shown and of the step that made it current (the
leading `# ` is cut here).

```bash
$ bb show greet@eng | head -3 | sed -E 's/[0-9a-f]{12}\.\.\./<id>/; s/^# //'
bb: function <id> anchored: Bitcoin block 870000
bb: mapping <id> anchored: Bitcoin block 870000
bb: step <id> anchored: Bitcoin block 870000
```

### Step 6: Verify
bb reads the block's height from the proof; whether that block really
commits to it is for the client to check against Bitcoin (the real
client asks a Bitcoin node).

```bash
$ env BB_OTS="python3 ots_stand_in.py" bb anchor --verify greet@eng | cut -d: -f2- | sort -u
 Bitcoin block 870000: Success! Bitcoin block 870000 attests existence as of 2026-11-23 UTC
```

---

# Chapter 3: A Record Changed After the Fact

A proof is about bytes. Someone rewrites the documentation of `greet` in
place: the proof next to it is now about other content, and bb says so
without asking anyone.

## Workflow

### Step 7: Edit an anchored file
```bash
$ sed -i 's/Greet someone/Greet anyone/' $BB_DIRECTORY/pool/mappings/*/*/eng/*.json
$ (bb anchor --status 2>&1 || true) | grep -c 'the file changed since it was anchored'
1
$ bb anchor --status > /dev/null 2>&1 || echo "exit status 1"
exit status 1
```

---

## Summary

| Command | Purpose |
|---------|---------|
| `bb anchor [X]` | Stamp every file with no proof yet (of function X only) |
| `bb anchor --status` | Count complete, pending and missing proofs; no network |
| `bb anchor --upgrade` | Complete the pending proofs once a block holds them |
| `bb anchor --verify [X]` | Have the client check each complete proof against Bitcoin |
| `bb log` | `Anchored:` under the date of each anchored record |
| `bb show X@lang` | `# bb:` lines above the code: what is anchored (and signed) |

## Key Insights

1. **`created` is a claim, a proof is evidence**: an anchored record existed, byte for byte, by the time of its block. It may be older; it cannot be newer.

2. **Only `bb anchor` reaches the network**: saving, editing and reviewing stay offline. What the calendars receive is one hash for the whole batch, never the records.

3. **Proofs are ordinary OpenTimestamps files**: `FILE.ots` next to `FILE`, kept in git with it, checked by `ots verify` without bb.

4. **Records are written once, so a proof is forever**: a file that no longer matches its proof was changed after being anchored, and is reported rather than stamped again.
