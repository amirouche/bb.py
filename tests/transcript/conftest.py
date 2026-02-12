"""
Pytest fixtures for transcript-based tests.
"""
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--transcript", default=None,
        help="Run only transcripts matching this substring (e.g. --transcript 01)"
    )


@pytest.fixture
def isolated_bb_dir(tmp_path: Path) -> Path:
    """Create isolated BB_DIRECTORY for transcript tests.

    Each transcript test gets its own isolated pool directory to ensure
    tests don't interfere with each other when run in parallel.

    Returns:
        Path to isolated bb directory (with pool/ subdirectory created)
    """
    bb_dir = tmp_path / "bb_pool"
    pool_dir = bb_dir / "pool"
    pool_dir.mkdir(parents=True)
    return bb_dir
