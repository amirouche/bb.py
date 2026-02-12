# Transcript 00: Get and set user configuration

**Purpose**: Verify that `bb.py whoami` can get and set user configuration fields.

## Setup

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Run `bb.py init` to create initial configuration

## Execution

### Setting Values

1. Set name: `bb.py whoami name testuser`
2. Set email: `bb.py whoami email test@example.com`
3. Set public key: `bb.py whoami public-key https://keys.example.com/key.pub`
4. Set languages: `bb.py whoami language eng fra spa`

### Getting Values

1. Get name: `bb.py whoami name`
2. Get email: `bb.py whoami email`
3. Get public key: `bb.py whoami public-key`
4. Get languages: `bb.py whoami language`

## Expected Behavior

### Setting Values - Output

Each set command shows confirmation:

```
Set name: testuser
Set email: test@example.com
Set public-key: https://keys.example.com/key.pub
Set language: eng fra spa
```

Exit code: 0 (success) for all

### Getting Values - Output

Each get command outputs the value:

```
testuser
test@example.com
https://keys.example.com/key.pub
eng fra spa
```

Exit code: 0 (success) for all

### Config File State

`$BB_DIRECTORY/config.json` is updated:

```json
{
  "user": {
    "username": "testuser",
    "name": "testuser",
    "email": "test@example.com",
    "public_key": "https://keys.example.com/key.pub",
    "languages": ["eng", "fra", "spa"]
  },
  "remotes": {}
}
```

**Salient elements to verify**:
- Set command writes to config.json immediately
- Get command reads from config.json
- Languages can be multiple values (stored as array)
- Setting languages replaces previous values (not append)
- Values persist across commands
- Empty get (no config) returns empty string

**Rationale**: User configuration enables personalization and attribution of functions, and language preferences guide default behavior.
