# Ouverture News & Changelog

## Unreleased Changes

### Breaking Changes

#### OUVERTURE_DIRECTORY: New Default Location for Function Pool

**What changed:**
- The default location for the ouverture function pool has moved from `.ouverture/` (in the current working directory) to `$HOME/.local/ouverture/`
- This follows XDG Base Directory conventions for storing user-specific data

**Migration:**
- **Environment Variable:** You can now set `OUVERTURE_DIRECTORY` to customize where the function pool is stored
- **Default behavior:** If `OUVERTURE_DIRECTORY` is not set, ouverture will use `$HOME/.local/ouverture/` as the default location
- **Old behavior:** To maintain the old behavior of using `.ouverture/` in the current directory, set:
  ```bash
  export OUVERTURE_DIRECTORY=.ouverture
  ```

**Why this change:**
- **System-wide pool:** Having a single location in `$HOME/.local/ouverture/` allows you to build one shared function pool across all your projects
- **Standards compliance:** Follows the XDG Base Directory specification for user data
- **Cleaner project directories:** No more `.ouverture/` directories cluttering your project folders
- **Better collaboration:** Makes it clearer when functions are being stored in a global pool vs. project-specific pool

**Example usage:**
```bash
# Use default location ($HOME/.local/ouverture/)
python3 ouverture.py add examples/example_simple.py@eng

# Use custom location
export OUVERTURE_DIRECTORY=/path/to/my/pool
python3 ouverture.py add examples/example_simple.py@eng

# Use current directory (old behavior)
export OUVERTURE_DIRECTORY=.ouverture
python3 ouverture.py add examples/example_simple.py@eng
```

**Impact:**
- Existing `.ouverture/` directories in your projects will no longer be used by default
- To migrate existing functions, you can either:
  1. Copy your old `.ouverture/` directory to `$HOME/.local/ouverture/`
  2. Set `OUVERTURE_DIRECTORY=.ouverture` to continue using the old location
  3. Re-add your functions to the new default location

---

## Version History

This is the first NEWS.md file. Future releases will be documented here.
