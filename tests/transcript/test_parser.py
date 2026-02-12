"""
Unit tests for transcript parser.
"""
import pytest
from pathlib import Path

from .parser import TranscriptParser, Transcript, Step


def test_parser_extracts_metadata():
    """Test that parser extracts metadata from document header."""
    markdown = """# Transcript Test

## Metadata
- ID: 01
- Feature: test feature
- Date: 2025-01-15
- Status: draft

## Content
Some content here.
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert transcript.metadata["ID"] == "01"
    assert transcript.metadata["Feature"] == "test feature"
    assert transcript.metadata["Date"] == "2025-01-15"
    assert transcript.metadata["Status"] == "draft"


def test_parser_extracts_source_files():
    """Test that parser extracts Python code blocks with filenames."""
    markdown = """
### Test File `test.py`
```python
def foo():
    pass
```

Some narrative text.

### Another File `calc.py`
```python
def add(a, b):
    return a + b
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert "test.py" in transcript.source_files
    assert "def foo():" in transcript.source_files["test.py"]
    assert "calc.py" in transcript.source_files
    assert "def add(a, b):" in transcript.source_files["calc.py"]


def test_parser_extracts_workflow_steps():
    """Test that parser extracts bash commands and expected output."""
    markdown = """
```bash
$ bb add test.py@eng
Hash: abc123
Mapping hash: def456
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 1
    assert transcript.workflow[0].command == "bb add test.py@eng"
    assert "abc123" in transcript.workflow[0].expected_output
    assert "def456" in transcript.workflow[0].expected_output


def test_parser_handles_variable_assignments():
    """Test that parser recognizes shell variable assignments."""
    markdown = """
```bash
$ HASH=$(bb add file.py@eng | awk '{print $2}')
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 1
    step = transcript.workflow[0]
    assert step.capture_to == "HASH"
    assert step.command == "bb add file.py@eng | awk '{print $2}'"


def test_parser_skips_marked_blocks():
    """Test that parser skips blocks with SKIP-TEST marker."""
    markdown = """
```bash
$ bb add test.py@eng
Hash: abc123
```

```bash
# SKIP-TEST: Not implemented yet
$ bb experimental-feature
Should not be parsed
```

```bash
$ bb show abc123@eng
def foo(): pass
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    # Should only have 2 steps (first and third blocks)
    assert len(transcript.workflow) == 2
    assert transcript.workflow[0].command == "bb add test.py@eng"
    assert transcript.workflow[1].command == "bb show abc123@eng"


def test_parser_handles_multiple_commands_in_block():
    """Test that parser extracts multiple commands from a single bash block."""
    markdown = """
```bash
$ bb add file1.py@eng
Hash: abc123

$ bb add file2.py@fra
Hash: def456
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 2
    assert transcript.workflow[0].command == "bb add file1.py@eng"
    assert "abc123" in transcript.workflow[0].expected_output
    assert transcript.workflow[1].command == "bb add file2.py@fra"
    assert "def456" in transcript.workflow[1].expected_output


def test_parser_ignores_comments():
    """Test that parser ignores comment lines in bash blocks."""
    markdown = """
```bash
# This is a comment
$ bb add file.py@eng
Hash: abc123
# Another comment
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 1
    assert transcript.workflow[0].command == "bb add file.py@eng"


def test_parser_preserves_step_order():
    """Test that parser preserves step order across multiple bash blocks."""
    markdown = """
## Part 1
```bash
$ bb add file1.py@eng
Hash: abc
```

## Part 2
```bash
$ bb add file2.py@fra
Hash: def
```

## Part 3
```bash
$ bb show abc@eng
def foo(): pass
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 3
    assert transcript.workflow[0].command == "bb add file1.py@eng"
    assert transcript.workflow[1].command == "bb add file2.py@fra"
    assert transcript.workflow[2].command == "bb show abc@eng"
    # Verify step numbers are sequential
    assert transcript.workflow[0].number == 1
    assert transcript.workflow[1].number == 2
    assert transcript.workflow[2].number == 3


def test_parser_handles_empty_expected_output():
    """Test that parser handles commands with no expected output."""
    markdown = """
```bash
$ HASH=$(bb add file.py@eng | awk '{print $2}')
```
"""
    parser = TranscriptParser()
    transcript = parser.parse_string(markdown)

    assert len(transcript.workflow) == 1
    assert transcript.workflow[0].expected_output == ""
    assert transcript.workflow[0].capture_to == "HASH"
