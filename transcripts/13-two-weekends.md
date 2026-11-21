# Transcript: Two weekends, one name

## Metadata
- ID: 13
- Feature: bb edit --fork, bb.follow, relations in bb search
- Date: 2026-11-21
- Status: draft
- Complexity: intermediate

## Overview
A pool does not need everyone to agree on what a name means. In Dakar the weekend is Saturday and Sunday; in Algiers it is Friday and Saturday. Both are right, and both are called `est_weekend`. This transcript writes the Dakar function and a shop that uses it, forks it for Algiers under the same name with `bb edit --fork`, and shows what follows: the Dakar function stays current and nothing is out of date, `bb search` says how the two relate, the name alone no longer picks one, and each shop keeps following its own function with `bb.follow("<hash>")`, through later edits, without anybody pinning anything.

Days are numbered as Python does: 0 is Monday, 4 is Friday, 5 is Saturday, 6 is Sunday.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)

---

# Chapter 1: A Shop in Dakar

`est_weekend` says whether a day is a day of rest, and `ouvert` prints whether the shop opens. `bb run` passes its arguments as text, hence `int(jour)`.

## Source Files

### Weekend `est_weekend_fra.py`
```python
def est_weekend(jour):
    """Vrai quand jour (0 pour lundi) est un jour de repos."""
    return int(jour) in (5, 6)
```

### Shop `ouvert_fra.py`
```python
import bb

est_weekend = bb.resolve("est_weekend")


def ouvert(jour):
    """Dit si la boutique ouvre ce jour (0 pour lundi)."""
    print("ouvert" if not est_weekend(jour) else "fermé")
```

## Workflow

### Step 1: Add the weekend, then the shop
One function is called `est_weekend`, so the name finds it.

```bash
$ DAKAR=$(bb add est_weekend_fra.py@fra | grep '^Hash:' | awk '{print $2}')
$ echo $DAKAR
258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469
$ bb add ouvert_fra.py@fra | grep '^Import'
Import: est_weekend -> 258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469
```

### Step 2: Open on Friday, closed on Sunday
```bash
# bb only runs code you have reviewed
$ yes | bb review ouvert@fra > /dev/null
$ bb run ouvert@fra 4
ouvert
$ bb run ouvert@fra 6
fermé
```

---

# Chapter 2: Another Weekend

Algiers rests on Friday and Saturday. That is not a fix of the Dakar function, and not a newer version of it: it is another function, true somewhere else. `bb edit --fork` says so, and its comment says why it leaves.

## Workflow

### Step 3: Fork it, under the same name
```bash
$ bb edit est_weekend@fra alger.md
Wrote alger.md. Change it, then run the same command again to save it.
$ sed -i 's/(5, 6)/(4, 5)/' alger.md
$ bb edit est_weekend@fra alger.md --fork -m "Vendredi et samedi, comme en Algérie"
No checks.
Forked from: 258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469 (still current)
Hash: 6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963
```

### Step 4: Both are current, and nothing is out of date
An edit would have replaced the Dakar function and left `ouvert` behind. A fork replaces nothing: the shop in Dakar uses a current function, as before. A fork is declared, so it is not a lineage that split without anyone saying so either (`--diverged`).

```bash
$ bb latest est_weekend@fra --hash | sort
258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469
6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963
$ bb search --outdated | tail -1
No matches found: nothing outdated
$ bb search --diverged | tail -1
No matches found
```

### Step 5: Search says how they relate
Each function lists the others that answer to its name: which one forked from which, the reason the fork gave, and how alike their code is. Nothing ranks them.

```bash
$ bb search est_weekend | grep '^Also' | sort
Also est_weekend: 258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469, this one forked from it (Vendredi et samedi, comme en Algérie), code 69% alike
Also est_weekend: 6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963, forked from this one (Vendredi et samedi, comme en Algérie), code 69% alike
$ bb log est_weekend@fra | grep -A1 '^Fork:'
Fork: est_weekend@fra 6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963
From: est_weekend@fra 258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469
```

---

# Chapter 3: A Name Is a Search

Two functions answer to `est_weekend`, so the name alone picks neither. bb does not guess: commands take a hash, and code says which function it means with `bb.follow("<hash>")`.

## Source Files

### A shop in Algiers, by name `essai_fra.py`
```python
import bb

est_weekend = bb.resolve("est_weekend")


def ouvert_alger(jour):
    """Dit si la boutique d'Alger ouvre ce jour (0 pour lundi)."""
    print("ouvert" if not est_weekend(jour) else "fermé")
```

### A shop in Algiers, following its weekend `ouvert_alger_fra.py`
```python
import bb

est_weekend = bb.follow("6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963")


def ouvert_alger(jour):
    """Dit si la boutique d'Alger ouvre ce jour (0 pour lundi)."""
    print("ouvert" if not est_weekend(jour) else "fermé")
```

## Workflow

### Step 6: Commands take a hash
```bash
$ (bb show est_weekend@fra 2>&1 || true) | head -1
Error: 2 functions answer to 'est_weekend@fra'. Name one by its hash (bb search est_weekend tells them apart):
$ bb show 6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963@fra
def est_weekend(jour):
    """Vrai quand jour (0 pour lundi) est un jour de repos."""
    return int(jour) in (4, 5)
```

### Step 7: By name, saving is refused, with the lines to paste
Each candidate comes as a line to put in the source, followed by its date, author and comment.

```bash
$ (bb add essai_fra.py@fra 2>&1 || true) | grep -o 'est_weekend = bb.follow("[0-9a-f]*")' | sort
est_weekend = bb.follow("258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469")
est_weekend = bb.follow("6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963")
```

### Step 8: Follow the one you mean
`bb.follow` is resolved when saving, like `bb.resolve`: the stored code holds a hash.

```bash
$ bb add ouvert_alger_fra.py@fra | grep '^Import'
Import: est_weekend -> 6968570ec4b391ca7d42c448a2bb4bb8c06eca72b2cba5a7df5d0be70c418963
$ yes | bb review ouvert_alger@fra > /dev/null
$ bb run ouvert_alger@fra 4
fermé
$ bb run ouvert_alger@fra 6
ouvert
```

---

# Chapter 4: Each Shop Follows Its Own Weekend

The shop in Dakar was written with `bb.resolve("est_weekend")`, when one function had that name. Its stored code holds a hash, and a hash knows which function it means. So its draft now shows `bb.follow`, and it keeps following the Dakar function whatever else is called `est_weekend`.

## Workflow

### Step 9: The draft follows a lineage, not a name
The note is a `# bb:` comment line in the draft; `sed` takes the `# ` off here, only because a line starting with `#` is a comment in a transcript.

```bash
$ bb show ouvert@fra | head -3 | sed 's/^# //'
import bb
bb: one of 2 called est_weekend: its lineage is followed
est_weekend = bb.follow('258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469')
```

### Step 10: Dakar edits its function
A plain edit: a newer version of the Dakar weekend. It is edited by hash, since the name no longer picks one.

```bash
$ bb edit $DAKAR@fra dakar.md
Wrote dakar.md. Change it, then run the same command again to save it.
$ sed -i 's/in (5, 6)/>= 5/' dakar.md
$ bb edit $DAKAR@fra dakar.md -m "Plus court"
No checks.
Parent: 258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469
Hash: d53984577b6d626ecfe381ae2d29a49d1b769a081a707854d2cac56219dc1d1f
```

### Step 11: Only the shop in Dakar is behind
The edit is on the Dakar lineage. The shop in Algiers follows another one, so it is up to date.

```bash
$ bb search --outdated | grep '^Name:\|^Status:'
Name: ouvert (fra)
Status: no checks, reviewed, uses 1 out-of-date version(s): est_weekend
$ bb tree ouvert@fra
ouvert — Dit si la boutique ouvre ce jour (0 pour lundi).
└── est_weekend — Vrai quand jour (0 pour lundi) est un jour de repos.  [out of date]
$ bb tree ouvert_alger@fra
ouvert_alger — Dit si la boutique d'Alger ouvre ce jour (0 pour lundi).
└── est_weekend — Vrai quand jour (0 pour lundi) est un jour de repos.
$ bb show ouvert@fra | head -4 | sed 's/^# //'
import bb
bb: out of date: uses 258117aefda8..., saving picks d53984577b6d...
bb: one of 2 called est_weekend: its lineage is followed
est_weekend = bb.follow('258117aefda83e4c400fb36bb534c7560f7de343101fbd425044932f8534a469')
```

### Step 12: Updating goes to the new Dakar weekend, never to the fork
```bash
$ bb update ouvert@fra -m "Suivre Dakar" | head -1
Updated 1 function(s), deepest first:
$ bb search --outdated | tail -1
No matches found: nothing outdated
$ bb show ouvert@fra | head -3 | sed 's/^# //'
import bb
bb: one of 2 called est_weekend: its lineage is followed
est_weekend = bb.follow('d53984577b6d626ecfe381ae2d29a49d1b769a081a707854d2cac56219dc1d1f')
```

### Step 13: Two shops, two weekends, one name
```bash
$ yes | bb review ouvert@fra > /dev/null
$ bb run ouvert@fra 4
ouvert
$ bb run ouvert_alger@fra 4
fermé
$ bb run ouvert@fra 6
fermé
$ bb run ouvert_alger@fra 6
ouvert
```

---

1. **A fork is another function, not a newer one**: `bb edit --fork` leaves the function it forks current. Nothing that uses it becomes out of date, and the fork starts a lineage of its own. An edit without `--fork` is a newer version: it replaces.

2. **The comment of a fork is its premise**: it is what `bb search` shows next to the relation, so say why the fork leaves.

3. **A name is a search**: when several functions answer to it, bb does not pick. `bb search NAME` tells them apart; commands take a hash.

4. **`bb.follow("<hash>")` refers to a lineage**: any version of it will do, and saving stores where that lineage is now. `bb.ref("<hash>")` stays on one version; `bb.resolve("name")` is the short form for a name only one function answers to.

5. **Callers never lose their way**: a caller's stored hash says which function it means. `bb show` and `bb edit` write the reference as `bb.follow` when the name no longer finds that function alone, so `bb search --outdated` and `bb update` keep following the right lineage.
