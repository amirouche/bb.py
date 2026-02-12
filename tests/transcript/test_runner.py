"""
Unit tests for transcript runner.
"""
import pytest
from pathlib import Path

from .parser import Transcript, Step
from .runner import TranscriptRunner


def test_runner_substitutes_variables():
    """Test that runner substitutes shell variables in commands."""
    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[]
    )
    runner = TranscriptRunner(transcript, Path("/tmp"), Path("/tmp/bb"), track_coverage=False)

    # Add some variables
    runner.variables["HASH"] = "abc123def456"
    runner.variables["LANG"] = "eng"

    # Test substitution
    command = "bb show $HASH@$LANG"
    result = runner._substitute_variables(command)
    assert result == "bb show abc123def456@eng"


def test_runner_substitutes_multiple_variables():
    """Test that runner handles multiple variable substitutions."""
    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[]
    )
    runner = TranscriptRunner(transcript, Path("/tmp"), Path("/tmp/bb"), track_coverage=False)

    runner.variables["HASH1"] = "aaa"
    runner.variables["HASH2"] = "bbb"

    command = "test $HASH1 = $HASH2"
    result = runner._substitute_variables(command)
    assert result == "test aaa = bbb"


def test_runner_writes_source_files(tmp_path):
    """Test that runner writes all source files during setup."""
    transcript = Transcript(
        metadata={},
        source_files={
            "test1.py": "def foo(): pass",
            "test2.py": "def bar(): return 42"
        },
        workflow=[]
    )
    bb_dir = tmp_path / "bb"
    bb_dir.mkdir()

    runner = TranscriptRunner(transcript, tmp_path, bb_dir, track_coverage=False)
    runner.setup()

    # Verify files were written
    assert (tmp_path / "test1.py").exists()
    assert (tmp_path / "test1.py").read_text() == "def foo(): pass"
    assert (tmp_path / "test2.py").exists()
    assert (tmp_path / "test2.py").read_text() == "def bar(): return 42"


def test_runner_captures_variable_from_output(tmp_path):
    """Test that runner captures shell variables from command output."""
    # Create a simple test file
    test_file = tmp_path / "test.py"
    test_file.write_text("def test(): pass")

    # Create bb directory
    bb_dir = tmp_path / "bb"
    pool_dir = bb_dir / "pool"
    pool_dir.mkdir(parents=True)

    # Create transcript with variable capture step
    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[
            Step(
                command="bb add test.py@eng | grep '^Hash:' | awk '{print $2}'",
                expected_output="",
                number=1,
                capture_to="HASH"
            )
        ]
    )

    runner = TranscriptRunner(transcript, tmp_path, bb_dir, track_coverage=False)
    results = runner.run_workflow()

    # Verify variable was captured
    assert "HASH" in runner.variables
    assert len(runner.variables["HASH"]) == 64  # SHA256 hex digest
    assert all(c in '0123456789abcdef' for c in runner.variables["HASH"])


def test_runner_verifies_output_match(tmp_path):
    """Test that runner correctly verifies output matches expectations."""
    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[]
    )
    runner = TranscriptRunner(transcript, tmp_path, tmp_path, track_coverage=False)

    # Exact match
    assert runner._verify_output("hello world", "hello world")

    # Whitespace normalized
    assert runner._verify_output("  hello world  \n", "hello world")

    # Different content
    assert not runner._verify_output("hello", "world")


def test_runner_stops_on_first_failure(tmp_path):
    """Test that runner stops executing steps after first failure."""
    test_file = tmp_path / "test.py"
    test_file.write_text("def test(): pass")

    bb_dir = tmp_path / "bb"
    pool_dir = bb_dir / "pool"
    pool_dir.mkdir(parents=True)

    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[
            Step(
                command="bb add test.py@eng",
                expected_output="Hash: WRONG_HASH",  # This will fail
                number=1,
                capture_to=None
            ),
            Step(
                command="bb show abc123@eng",  # This should NOT execute
                expected_output="def test(): pass",
                number=2,
                capture_to=None
            )
        ]
    )

    runner = TranscriptRunner(transcript, tmp_path, bb_dir, track_coverage=False)
    results = runner.run_workflow()

    # Should have 1 result (stopped after first failure)
    assert len(results) == 1
    assert not results[0].match


def test_runner_tracks_variables_across_steps(tmp_path):
    """Test that runner maintains shell variables across multiple steps."""
    test_file = tmp_path / "test.py"
    test_file.write_text("def test(): pass")

    bb_dir = tmp_path / "bb"
    pool_dir = bb_dir / "pool"
    pool_dir.mkdir(parents=True)

    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[
            Step(
                command="bb add test.py@eng | grep '^Hash:' | awk '{print $2}'",
                expected_output="",
                number=1,
                capture_to="HASH"
            ),
            Step(
                command="bb show $HASH@eng",
                expected_output="def test():\n    pass",
                number=2,
                capture_to=None
            )
        ]
    )

    runner = TranscriptRunner(transcript, tmp_path, bb_dir, track_coverage=False)
    results = runner.run_workflow()

    # Both steps should succeed
    assert len(results) == 2
    assert results[0].match
    assert results[1].match

    # HASH variable should be set
    assert "HASH" in runner.variables


def test_runner_handles_variable_name_prefixes():
    """Test that runner correctly handles variables where one is a prefix of another.

    This is a regression test for the bug where $HASH and $HASH_JPN would
    incorrectly substitute, causing $HASH_JPN to become <hash_value>_JPN.
    """
    transcript = Transcript(
        metadata={},
        source_files={},
        workflow=[]
    )
    runner = TranscriptRunner(transcript, Path("/tmp"), Path("/tmp/bb"), track_coverage=False)

    # Set up variables where one is a prefix of another
    runner.variables["HASH"] = "abc123"
    runner.variables["HASH_JPN"] = "def456"
    runner.variables["HASH_ARA"] = "ghi789"

    # Test that substitution works correctly
    command = 'test "$HASH" = "$HASH_JPN" && test "$HASH" = "$HASH_ARA"'
    result = runner._substitute_variables(command)

    # Should substitute each variable independently, not treat HASH as part of HASH_JPN
    assert result == 'test "abc123" = "def456" && test "abc123" = "ghi789"'
    assert "_JPN" not in result  # Bug would produce "abc123_JPN"
    assert "_ARA" not in result  # Bug would produce "abc123_ARA"
