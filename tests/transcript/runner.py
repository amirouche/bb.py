"""
Transcript workflow runner for Beyond Babel tests.

Executes workflow steps from parsed transcripts, tracking shell variables
and running commands under coverage.
"""
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .parser import Transcript, Step

# Check if coverage is available
try:
    import coverage
    COVERAGE_AVAILABLE = True
except ImportError:
    COVERAGE_AVAILABLE = False


@dataclass
class StepResult:
    """Result of executing a workflow step."""
    step: Step
    actual_output: str
    match: bool


class CommandExecutionError(Exception):
    """Raised when a command fails to execute."""
    pass


class TranscriptRunner:
    """Executes workflow steps from a parsed transcript."""

    def __init__(self, transcript: Transcript, tmpdir: Path, bb_directory: Path,
                 track_coverage: bool = True):
        """Initialize runner.

        Args:
            transcript: Parsed transcript to execute
            tmpdir: Working directory for source files
            bb_directory: Isolated BB pool directory
            track_coverage: Whether to run commands under coverage (default True, auto-disabled if coverage not available)
        """
        self.transcript = transcript
        self.tmpdir = tmpdir
        self.bb_directory = bb_directory
        # Only track coverage if requested AND coverage module is available
        self.track_coverage = track_coverage and COVERAGE_AVAILABLE
        self.variables: Dict[str, str] = {}  # Tracks shell variable values

        # Find bb.py script
        self.bb_py = Path(__file__).parent.parent.parent / 'bb.py'
        if not self.bb_py.exists():
            raise RuntimeError(f"bb.py not found at {self.bb_py}")

        # Point subprocess coverage data at the project root so it merges
        # with pytest-cov's .coverage file instead of landing in tmpdir.
        self.coverage_file = str(self.bb_py.parent / '.coverage')

    def setup(self):
        """Write all source files to tmpdir before running workflow."""
        for filename, code in self.transcript.source_files.items():
            filepath = self.tmpdir / filename
            filepath.write_text(code, encoding='utf-8')

    def run_workflow(self) -> List[StepResult]:
        """Execute all workflow steps, return results."""
        results = []

        for step in self.transcript.workflow:
            # 1. Substitute shell variables in command ($HASH, etc.)
            command = self._substitute_variables(step.command)

            # 2. Execute command (using subprocess + coverage if enabled)
            try:
                actual_output = self._execute_command(command)
            except CommandExecutionError as e:
                # Command failed - create failure result and stop
                results.append(StepResult(
                    step=step,
                    actual_output=str(e),
                    match=False
                ))
                break

            # 3. If this step captures to a variable, extract and store it
            if step.capture_to:
                self.variables[step.capture_to] = actual_output.strip()

            # 4. Verify output matches expectations (if provided)
            if step.expected_output:
                expected = self._substitute_variables(step.expected_output)
                match = self._verify_output(actual_output, expected)

                results.append(StepResult(step, actual_output, match))

                if not match:
                    break  # Stop on first failure
            else:
                # No expected output means this was just a variable assignment
                results.append(StepResult(step, actual_output, True))

        return results

    def _substitute_variables(self, text: str) -> str:
        """Replace $HASH etc. with actual values from self.variables.

        Substitutes variables in order of decreasing length to avoid issues where
        one variable name is a prefix of another (e.g., $HASH and $HASH_JPN).
        """
        # Sort by length (longest first) to handle prefix overlaps
        sorted_vars = sorted(self.variables.items(), key=lambda x: len(x[0]), reverse=True)
        for var_name, value in sorted_vars:
            text = text.replace(f"${var_name}", value)
        return text

    def _execute_command(self, command: str) -> str:
        """Execute command, optionally under coverage for bb commands.

        Handles:
        - bb commands: bb add file@lang
        - Piped commands: bb add file@lang | awk '{print $2}'
        - Shell commands: test "$HASH1" = "$HASH2" && echo "match"
        - Shell features: redirects (2>&1), boolean operators (&&, ||)

        Returns:
            Command stdout

        Raises:
            CommandExecutionError: If command exits non-zero
        """
        # Check if this is a bb command or general shell command
        if command.strip().startswith("bb"):
            # bb command - check which execution path to use
            # Pipes get special handling to maintain coverage tracking
            if ' | ' in command:
                return self._execute_piped_command(command)

            # Other shell features require shell=True but lose coverage
            # Note: This is a limitation - we could improve this later
            shell_features = [
                ' || ',     # OR operator
                ' && ',     # AND operator
                '2>&1',     # stderr redirect
                '2>',       # stderr redirect to file
                '>',        # stdout redirect (but not in quotes)
                '<',        # stdin redirect (but not in comparison operators)
            ]

            needs_shell = any(feature in command for feature in shell_features)
            if needs_shell:
                return self._execute_shell_command(command)
            else:
                return self._execute_simple_command(command)
        else:
            # Non-bb command - always use shell
            return self._execute_shell_command(command)

    def _execute_shell_command(self, command: str) -> str:
        """Execute shell command (test, echo, etc.) without coverage."""
        result = subprocess.run(
            command,
            shell=True,
            cwd=self.tmpdir,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "BB_DIRECTORY": str(self.bb_directory),
                "PYTHONPATH": str(self.bb_py.parent),
                "COVERAGE_FILE": self.coverage_file,
            }
        )

        if result.returncode != 0:
            raise CommandExecutionError(
                f"Command failed: {command}\n"
                f"Exit code: {result.returncode}\n"
                f"Stdout: {result.stdout}\n"
                f"Stderr: {result.stderr}"
            )

        return result.stdout

    def _execute_simple_command(self, command: str) -> str:
        """Execute simple bb command (no pipes)."""
        # Parse command: "bb add file.py@lang" -> ["add", "file.py@lang"]
        args = shlex.split(command.replace("bb", "", 1).strip())

        # Build command with optional coverage
        if self.track_coverage:
            cmd = [
                sys.executable, "-m", "coverage", "run",
                "-a",  # Append to existing .coverage file
                "--source=bb",
                str(self.bb_py),
                *args
            ]
        else:
            cmd = [sys.executable, str(self.bb_py), *args]

        # Execute with isolated BB_DIRECTORY
        result = subprocess.run(
            cmd,
            cwd=self.tmpdir,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "BB_DIRECTORY": str(self.bb_directory),
                "PYTHONPATH": str(self.bb_py.parent),
                "COVERAGE_FILE": self.coverage_file,
            }
        )

        if result.returncode != 0:
            raise CommandExecutionError(
                f"Command failed: {command}\n"
                f"Exit code: {result.returncode}\n"
                f"Stdout: {result.stdout}\n"
                f"Stderr: {result.stderr}"
            )

        return result.stdout

    def _execute_piped_command(self, command: str) -> str:
        """Execute command with shell pipes like: bb add file@lang | awk '{print $3}'

        For piped commands, we wrap only the bb part with coverage, then pipe to
        the rest of the command.
        """
        # For piped commands, wrap only the bb command part
        parts = command.split(" | ", 1)
        bb_command = parts[0].strip()
        pipe_rest = parts[1].strip()

        # Parse bb command
        bb_args = shlex.split(bb_command.replace("bb", "", 1).strip())

        # Build bb command with optional coverage
        if self.track_coverage:
            bb_cmd_parts = [
                sys.executable, "-m", "coverage", "run",
                "-a",
                "--source=bb",
                str(self.bb_py),
                *bb_args
            ]
        else:
            bb_cmd_parts = [sys.executable, str(self.bb_py), *bb_args]

        # Reconstruct full piped command
        bb_cmd_str = " ".join(shlex.quote(str(p)) for p in bb_cmd_parts)
        full_command = f"{bb_cmd_str} | {pipe_rest}"

        # Execute with shell=True to support pipes
        result = subprocess.run(
            full_command,
            shell=True,
            cwd=self.tmpdir,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "BB_DIRECTORY": str(self.bb_directory),
                "PYTHONPATH": str(self.bb_py.parent),
                "COVERAGE_FILE": self.coverage_file,
            }
        )

        if result.returncode != 0:
            raise CommandExecutionError(
                f"Command failed: {command}\n"
                f"Exit code: {result.returncode}\n"
                f"Stdout: {result.stdout}\n"
                f"Stderr: {result.stderr}"
            )

        return result.stdout

    def _verify_output(self, actual: str, expected: str) -> bool:
        """Compare actual vs expected output.

        Strategy:
        1. Normalize whitespace (strip trailing/leading)
        2. Exact match

        Returns:
            True if output matches expectations
        """
        actual_normalized = actual.strip()
        expected_normalized = expected.strip()

        return actual_normalized == expected_normalized
