# Transcript 00: Init creates directory structure

**Purpose**: Verify that `bb.py init` creates the required directory structure and configuration file.

## Setup

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory (e.g., `/tmp/test_bb`)
- Directory does not exist yet

## Execution

1. Run: `bb.py init`

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains:
  - "Created config file"
  - "Initialized bb directory"

### File System State

**BB directory created**: `$BB_DIRECTORY/`

**Pool directory created**: `$BB_DIRECTORY/pool/`

**Config file created**: `$BB_DIRECTORY/config.json`

The `config.json` contains:

```json
{
  "user": {
    "username": "<from-env-USER>",
    "name": "",
    "email": "",
    "public_key": "",
    "languages": ["eng"]
  },
  "remotes": {}
}
```

**Salient elements to verify**:
- Pool directory exists and is empty
- Config file has valid JSON structure
- `user.username` defaults to USER environment variable
- `user.languages` defaults to `["eng"]`
- `remotes` is an empty object
- Other user fields are empty strings by default
- Command is idempotent (safe to run multiple times)

**Rationale**: The init command sets up the bb directory structure and configuration, preparing the system for function management.
