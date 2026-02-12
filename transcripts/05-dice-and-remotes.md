# Transcript: Dice Game - Collaborative Development with Remotes

## Metadata
- ID: 05
- Feature: bb log, commit, and remote operations (add/list/push/remove)
- Date: 2026-02-12
- Status: draft
- Complexity: intermediate
- Note: bb remote sync requires git remotes; demonstrated with file:// for simplicity

## Overview

This transcript tells the story of two developers—Alice and Bob—collaborating on a dice game library through Beyond Babel's remote synchronization features. Alice builds the core dice roller in English, Bob adds a high-low game in French, and they use remotes to share their work bidirectionally.

Along the way, we demonstrate:
- The commit model (pool → git → remote)
- Read-only vs writable remotes
- Bidirectional sync with `bb remote sync`
- Collaborative multilingual development
- Error handling (forgot to commit, read-only push attempts)

The dice game is simple but demonstrates compositional functions: `play_high_low` depends on `roll_dice`, so committing the game automatically includes its dependencies.

## Prerequisites
- `bb` installed and in PATH
- `BB_DIRECTORY` configured (or using default `~/.local/bb/`)
- Clean pool for testing

---

# Chapter 1: Alice Builds the Dice Roller

Alice wants to create a reusable dice rolling function that others can import. She'll write it in English and add it to her local pool. The function will accept a number of dice and the number of sides per die, returning the sum of the rolls.

## Source Files

### Dice Roller `dice_eng.py`
```python
def roll_dice(num_dice, sides=6):
    """Roll num_dice dice with the given number of sides."""
    import random
    total = 0
    for i in range(num_dice):
        total += random.randint(1, sides)
    return total
```

### Dice Roller `dice_fra.py`
```python
def lancer_des(nombre_des, faces=6):
    """Lance nombre_des dés avec le nombre de faces donné."""
    import random
    total = 0
    for i in range(nombre_des):
        total += random.randint(1, faces)
    return total
```

### High-Low Game `game_eng.py`
```python
from bb.pool import object_1b82ac0b8353f028b4df562faaf36cbeacd1a81b085a62cedc7d08e92888999d as roll_dice

def play_high_low(bet):
    """Play a high-low dice game. Returns True if you win."""
    result = roll_dice.roll_dice(2)
    if bet == "high":
        return result >= 8
    else:
        return result < 8
```

### High-Low Game `game_fra.py`
```python
from bb.pool import object_1b82ac0b8353f028b4df562faaf36cbeacd1a81b085a62cedc7d08e92888999d as lancer_des

def jouer_haut_bas(pari):
    """Joue au jeu de dés haut-bas. Retourne True si vous gagnez."""
    resultat = lancer_des.lancer_des(2)
    if pari == "haut":
        return resultat >= 8
    else:
        return resultat < 8
```

## Workflow

### Step 1: Alice starts with an empty pool
```bash
$ bb log
Function Pool Log (0 functions)
================================================================================

```

### Step 2: Alice adds the dice roller in English
```bash
$ DICE_HASH=$(bb add dice_eng.py@eng | grep '^Hash:' | awk '{print $2}')
```

### Step 3: Alice verifies it's in her pool
```bash
$ bb log | grep "^Hash:" | awk '{print $2}'
$DICE_HASH
```

---

# Chapter 2: Sharing Through Remotes

Alice wants to share her dice roller with Bob. She'll set up a "central" remote (simulated with a local directory), commit her function, and push it. This demonstrates the three-stage model: pool → git → remote.

Before pushing, Alice needs to **commit** the function. The commit step marks functions as ready to share—only committed functions can be pushed to remotes.

### Step 4: Alice tries to push without committing (error case)
```bash
$ mkdir -p /tmp/bb-remote-central
$ bb remote add origin file:///tmp/bb-remote-central
Added remote 'origin': file:///tmp/bb-remote-central (type: file)
```

```bash
$ (bb remote push origin 2>&1 || true) | grep "No committed functions"
Error: No committed functions. Use 'bb commit HASH' first.
```

Right! Alice forgot to commit. The error message reminds us that functions must be committed before they can be pushed.

### Step 5: Alice commits the dice roller
```bash
$ bb commit $DICE_HASH -c "Add dice roller" | grep "Committed"
Committed 1 function(s)
```

### Step 6: Now push succeeds
```bash
$ bb remote push origin | head -1
Pushing to remote 'origin': file:///tmp/bb-remote-central
```

### Step 7: Verify remotes are configured
```bash
$ bb remote list
Configured remotes:
  origin: file:///tmp/bb-remote-central
```

---

# Chapter 3: Adding French Translation

Alice adds a French translation of her dice roller. This demonstrates that Beyond Babel tracks multiple language mappings for the same logical function—all sharing one hash.

### Step 8: Alice adds French translation of dice roller
```bash
$ DICE_HASH_2=$(bb add dice_fra.py@fra | grep '^Hash:' | awk '{print $2}')
$ test "$DICE_HASH" = "$DICE_HASH_2" && echo "Same hash - same logic!"
Same hash - same logic!
```

### Step 9: Verify log shows both English and French
```bash
$ bb log | grep "^Languages:" | head -1
Languages: eng, fra
```

Both language versions coexist with the same hash because the underlying logic is identical.

---

# Chapter 4: Building a Dependent Function

Now Alice builds a high-low dice game that depends on her `roll_dice` function. She'll write the game in French to demonstrate that function composition works across languages. When she commits the game, the commit system automatically includes its dependencies.

### Step 10: Alice adds the high-low game in French
```bash
$ GAME_HASH=$(bb add game_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 11: Alice commits the game (includes dice roller dependency)
```bash
$ bb commit $GAME_HASH -c "Add high-low dice game" | grep -E "(Resolving|Found|Committed)"
Resolving dependencies for $GAME_HASH...
Found 2 function(s) to commit
Committed 2 function(s)
```

Notice: Committed **2 functions** even though Alice only specified the game. The commit system followed the dependency graph and included the dice roller.

---

# Chapter 5: Pulling from Remotes

Before setting up additional remotes, let's verify that pulling from an existing remote works correctly. This demonstrates how `bb remote pull` fetches functions from shared repositories.

### Step 12: Alice pulls from origin (verifying sync)
```bash
$ bb remote pull origin | head -3
Pulling from remote 'origin': file:///tmp/bb-remote-central

Validating remote pool structure...
```

### Step 13: Verify both functions are still in the log
```bash
$ bb log | grep "^Hash:" | wc -l
2
```

Perfect! Two functions in the pool after the pull operation.

---

# Chapter 6: Read-Only Remotes and Safety

Alice demonstrates read-only remotes, which prevent accidental pushes. This is essential for public registries or upstream sources where you want to pull but not push directly.

### Step 14: Alice adds a second remote (read-only)
```bash
$ mkdir -p /tmp/bb-remote-upstream
$ bb remote add upstream file:///tmp/bb-remote-upstream --read-only
Added remote 'upstream': file:///tmp/bb-remote-upstream (type: file) (read-only)
```

### Step 15: Try to push to read-only remote (error case)
```bash
$ (bb remote push upstream 2>&1 || true) | grep "read-only"
Error: Remote 'upstream' is read-only
```

Perfect! The read-only flag prevented an accidental push.

### Step 16: Verify remotes list
```bash
$ bb remote list
Configured remotes:
  origin: file:///tmp/bb-remote-central
  upstream: file:///tmp/bb-remote-upstream
```

### Step 17: Cleanup test remotes
```bash
$ bb remote remove origin
Removed remote 'origin'

$ bb remote remove upstream
Removed remote 'upstream'

$ rm -rf /tmp/bb-remote-central /tmp/bb-remote-upstream
```

---

## Summary

This transcript demonstrated a complete remote workflow with Beyond Babel:

| Command | Purpose | What We Did |
|---------|---------|-------------|
| `bb add` | Add function to pool | Dice roller (eng+fra), Game (fra) |
| `bb commit` | Mark functions for sharing | Commit with auto-dependency resolution |
| `bb remote add` | Configure remote | origin (writable), upstream (read-only) |
| `bb remote list` | Show remotes | Verify configured remotes |
| `bb remote push` | Share committed functions | Push to origin, error on read-only |
| `bb remote remove` | Cleanup | Remove remotes after demo |

## Key Insights

1. **Three-stage model**: Beyond Babel uses a three-tier architecture:
   - **Pool**: Working directory (where you add/refactor functions)
   - **Git directory**: Staging area (where you commit functions)
   - **Remotes**: Shared repositories (where you push/pull/sync)

   You must commit before pushing. Only committed functions can be shared.

2. **Automatic dependency resolution**: When you commit a function that depends on others (like `play_high_low` → `roll_dice`), the commit system automatically includes all dependencies. You don't manually track the dependency graph.

3. **Read-only remotes prevent mistakes**: Marking a remote as `--read-only` during `bb remote add` prevents accidental pushes. This is essential for upstream/public remotes where you want to pull but not push directly.

4. **Multilingual functions share one hash**: The English and French versions of `roll_dice` produce the same hash because the logic is identical. The pool tracks logic (hash) separately from presentation (language mappings).

5. **Error handling guides you**: When you forget to commit before pushing, the error message tells you exactly what to do: "Use 'bb commit HASH' first." When you try to push to a read-only remote, you get a clear error.

## Error Cases Demonstrated

- **Forgot to commit**: Tried to push before committing → "Error: No committed functions. Use 'bb commit HASH' first."
- **Read-only push**: Tried to push to read-only upstream → "Error: Remote 'upstream' is read-only"

## Use Cases

1. **Team collaboration**: Central remote for sharing functions
2. **Multilingual development**: Multiple language mappings for same logic
3. **Dependency sharing**: Functions compose with auto-resolution
4. **Public registries**: Read-only upstreams prevent accidents

## Notes

- This transcript uses `file://` remotes for simplicity (local directories)
- Real workflows typically use `git@host:` or `git+https://` remotes
- **`bb remote sync`** (bidirectional pull+push) requires git remotes (`git-ssh`, `git-https`, `git-file`) — not demonstrated here to keep the transcript simple
- Dependency resolution is automatic during `bb commit`
