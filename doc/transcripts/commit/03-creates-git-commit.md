# Transcript 03: Commit creates git commit

**Purpose**: Verify that the `commit` command creates an actual git commit with the provided message in the git repository.

## Setup

Create a simple Python function file:

**File**: `/tmp/test.py`
```python
def test():
    """Test function"""
    return 1
```

**Environment**:
- Set `BB_DIRECTORY` to a temporary test directory
- Initialize empty pool directory
- No git repository exists yet in `$BB_DIRECTORY/git/`

## Execution

1. Add the function to the pool with language tag `eng`
2. Capture the returned hash
3. Run the commit command with the hash and comment "Test commit message"

## Expected Behavior

### Command Output
- Exit code: 0 (success)
- Standard output contains: "Committed 1 function(s) to git repository"

### Git Repository State

**Repository initialized**: `$BB_DIRECTORY/git/.git/` directory exists

**Git log contains the commit**:
- At least one commit exists in the repository
- The most recent commit message is "Test commit message"
- The commit message matches exactly what was provided via `--comment` flag

**Files tracked in git**:
- `git ls-files` shows the function's `object.json` and mapping files
- All files in the git directory are tracked (not untracked)

**Working directory clean**:
- `git status` shows no uncommitted changes
- All changes from the commit command are included in the git commit

**Salient elements to verify**:
- Git repository is properly initialized (`.git` directory exists)
- Git log can be queried successfully
- Commit message exactly matches the `--comment` parameter
- All function files are staged and committed (not just copied)
- Repository is in a clean state after commit

### Git Commit Message Format

The commit message should be exactly as provided in the `--comment` flag, without additional formatting or prefixes.

**Example git log output**:
```
abc1234 Test commit message
```
