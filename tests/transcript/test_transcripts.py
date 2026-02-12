"""
Pytest integration for transcript-based tests.

Discovers all markdown transcripts and runs them as parameterized tests.
Transcripts with ``Status: xfail`` in their metadata are marked as expected
failures so they appear red without breaking CI.
"""
import pytest
from pathlib import Path

from .parser import TranscriptParser
from .runner import TranscriptRunner


# Discover all transcripts
TRANSCRIPT_DIR = Path(__file__).parent.parent.parent / "transcripts"
TRANSCRIPT_FILES = sorted(TRANSCRIPT_DIR.glob("*.md")) if TRANSCRIPT_DIR.exists() else []

# Pre-parse metadata to identify xfail transcripts
_parser = TranscriptParser()
_XFAIL_STEMS = set()
for _tf in TRANSCRIPT_FILES:
    _meta = _parser.parse(_tf).metadata
    if _meta.get("Status") == "xfail":
        _XFAIL_STEMS.add(_tf.stem)


def _transcript_id(path: Path) -> str:
    return path.stem


@pytest.mark.parametrize("transcript_file", TRANSCRIPT_FILES, ids=_transcript_id)
def test_transcript(request, transcript_file: Path, tmp_path: Path, isolated_bb_dir: Path):
    """Execute a transcript file as a test.

    This test:
    1. Parses the markdown transcript
    2. Writes source files to tmpdir
    3. Executes workflow steps in order
    4. Verifies output matches expectations
    5. Tracks shell variables across steps
    6. Runs commands under coverage

    Transcripts whose metadata contains ``Status: xfail`` are automatically
    marked as expected failures.

    Args:
        transcript_file: Path to markdown transcript
        tmp_path: Pytest fixture for temporary directory
        isolated_bb_dir: Isolated BB pool directory
    """
    # 0. Skip if --transcript filter doesn't match
    transcript_filter = request.config.getoption("--transcript")
    if transcript_filter and transcript_filter not in transcript_file.stem:
        pytest.skip(f"Filtered out by --transcript {transcript_filter}")

    # 1. Parse transcript
    parser = TranscriptParser()
    transcript = parser.parse(transcript_file)

    # Mark xfail transcripts so failures are expected
    is_xfail = transcript.metadata.get("Status") == "xfail"
    if is_xfail:
        pytest.xfail(
            f"Transcript {transcript_file.name} is marked Status: xfail"
        )

    # 2. Setup runner with isolated directories
    runner = TranscriptRunner(transcript, tmp_path, isolated_bb_dir)
    runner.setup()

    # 3. Execute workflow
    results = runner.run_workflow()

    # 4. Assert all steps passed
    failures = [r for r in results if not r.match]
    if failures:
        # Build detailed failure message
        msg = f"Transcript {transcript_file.name} failed:\n\n"
        for failure in failures:
            msg += f"Step {failure.step.number}: $ {failure.step.command}\n"
            if failure.step.expected_output:
                msg += f"\nExpected output:\n{failure.step.expected_output}\n"
            msg += f"\nActual output:\n{failure.actual_output}\n"
            msg += "\n" + "="*70 + "\n"
        pytest.fail(msg)
