# Transcript: Dice Game - Composing Functions Across Languages

## Metadata
- ID: 05
- Feature: bb log, translations sharing one hash, functions importing functions
- Date: 2026-02-12
- Status: draft
- Complexity: intermediate

## Overview

Alice builds a dice roller in English, adds a French version of the same logic, and writes a high-low game in French that imports the roller from the pool.

Along the way, we demonstrate:
- The pool log
- Multilingual mappings sharing one hash
- Compositional functions: `jouer_haut_bas` depends on `roll_dice`

Sharing is not bb's job: `$BB_DIRECTORY` is a plain directory that you track and share with git yourself.

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
import bb
roll_dice = bb.ref("32bf70674802add86d266677a6b3d2322e8340d4bb9a77ded6272387303568e7")

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
import bb
lancer_des = bb.ref("32bf70674802add86d266677a6b3d2322e8340d4bb9a77ded6272387303568e7")

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
Pool Log (0 functions, 0 worklogs)
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

# Chapter 2: Adding French Translation

Alice adds a French translation of her dice roller. This demonstrates that Beyond Babel tracks multiple language mappings for the same logical function—all sharing one hash.

### Step 4: Alice adds French translation of dice roller
```bash
$ DICE_HASH_2=$(bb add dice_fra.py@fra | grep '^Hash:' | awk '{print $2}')
$ test "$DICE_HASH" = "$DICE_HASH_2" && echo "Same hash - same logic!"
Same hash - same logic!
```

### Step 5: Verify log shows both English and French
```bash
$ bb log | grep "^Languages:" | head -1
Languages: eng, fra
```

Both language versions coexist with the same hash because the underlying logic is identical.

---

# Chapter 3: Building a Dependent Function

Now Alice builds a high-low dice game that depends on her `roll_dice` function. She'll write the game in French to demonstrate that function composition works across languages.

### Step 6: Alice adds the high-low game in French
```bash
$ GAME_HASH=$(bb add game_fra.py@fra | grep '^Hash:' | awk '{print $2}')
```

### Step 7: Both functions are in the pool
```bash
$ bb log | grep "^Hash:" | wc -l
2
```

---

## Summary

| Command | Purpose | What We Did |
|---------|---------|-------------|
| `bb log` | Show the pool history | Empty pool, then two functions |
| `bb add` | Add function to pool | Dice roller (eng+fra), Game (fra) |

## Key Insights

1. **Multilingual functions share one hash**: The English and French versions of `roll_dice` produce the same hash because the logic is identical. The pool tracks logic (hash) separately from presentation (language mappings).

2. **Functions compose across languages**: The French game imports the dice roller by hash, whatever language the roller was written in.

3. **Sharing is git's job**: `$BB_DIRECTORY` is a plain directory holding the pool, worklogs and unfinished edits. Track it with git and use git's own commands (`git remote`, `git push`, `git pull`) to share it.
