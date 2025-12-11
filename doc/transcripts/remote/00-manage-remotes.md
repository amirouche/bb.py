# Transcript 00: Manage remote repositories

**Purpose**: Verify that `bb.py remote` manages remote repository URLs for syncing function pools.

## Setup

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Run `bb.py init` to create config

## Execution

### Add Remote

1. Add remote: `bb.py remote add origin https://example.com/pool.git`

### List Remotes

2. List remotes: `bb.py remote list`

### Remove Remote

3. Remove remote: `bb.py remote remove origin`

## Expected Behavior

### Add Remote Output

```
Added remote 'origin': https://example.com/pool.git
```

### List Remotes Output

```
Configured remotes:
  origin: https://example.com/pool.git
```

Or when empty:
```
No remotes configured
```

### Remove Remote Output

```
Removed remote 'origin'
```

### Config File State

`$BB_DIRECTORY/config.json` updated:

```json
{
  "user": { ... },
  "remotes": {
    "origin": "https://example.com/pool.git"
  }
}
```

**Salient elements to verify**:
- Remotes stored in config.json
- Add/list/remove operations work
- Multiple remotes supported
- Used by sync operations (if implemented)

**Rationale**: Remote management enables collaboration and backup of function pools across systems.
