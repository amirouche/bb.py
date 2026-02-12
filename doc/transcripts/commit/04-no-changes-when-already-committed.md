# Transcript 04: No changes when already committed

**Purpose**: Verify that committing the same function twice reports "No new changes" and does not create a duplicate git commit.

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

## Execution

1. Add the function to the pool with language tag `eng`
2. Capture the returned hash
3. Run the commit command with the hash and comment "First commit"
4. Run the commit command **again** with the same hash and comment "Second commit"

## Expected Behavior

### First Commit Output
- Exit code: 0 (success)
- Standard output contains: "Committed 1 function(s) to git repository"
- A new git commit is created

### Second Commit Output
- Exit code: 0 (success)
- Standard output contains: "No new changes to commit"
- **No new git commit is created**

### Git Repository State

**Single commit exists**:
- `git log` shows exactly **one** commit (not two)
- The commit message is "First commit" (from the first commit command)
- The second commit command did not create a new commit

**Git log verification**:
- The most recent commit message is "First commit"
- There is no commit with message "Second commit"

**Files unchanged**:
- The function files in git directory remain the same
- No duplicate files or directories are created

**Salient elements to verify**:
- Commit command is idempotent (running it multiple times has no additional effect)
- Second commit correctly detects that no changes exist
- Output message clearly indicates "No new changes to commit"
- Git history is not polluted with empty or duplicate commits
- Exit code is still 0 (not an error condition, just informational)

### Rationale

This behavior ensures that:
- The git repository stays clean without duplicate commits
- Users can safely re-run commit commands without worrying about duplication
- The system correctly detects when a function and its dependencies are already committed
