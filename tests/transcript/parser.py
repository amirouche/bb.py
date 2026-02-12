"""
Markdown transcript parser for Beyond Babel tests.

Parses interleaved markdown transcripts where source files, commands, and narratives
are mixed throughout the document in a natural tutorial flow.
"""
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Step:
    """A single workflow step (command + expected output)."""
    command: str            # CLI command (without "$ " prefix)
    expected_output: str    # Expected stdout
    number: int             # Step number for error reporting
    capture_to: Optional[str] = None  # Shell variable name to capture output (e.g., "HASH")


@dataclass
class Transcript:
    """Parsed transcript structure."""
    metadata: Dict[str, str]         # ID, Feature, Date, Status
    source_files: Dict[str, str]     # filename -> code (e.g., "calc_eng.py" -> Python code)
    workflow: List[Step]             # Sequence of commands to execute (in document order)


class TranscriptParser:
    """Parser for markdown transcript files."""

    def parse(self, markdown_path: Path) -> Transcript:
        """Parse markdown transcript file into structured object."""
        markdown = markdown_path.read_text(encoding='utf-8')
        return self.parse_string(markdown)

    def parse_string(self, markdown: str) -> Transcript:
        """Parse markdown string into structured object."""
        metadata = self._extract_metadata(markdown)
        source_files = self._extract_source_files(markdown)
        workflow = self._extract_workflow_steps(markdown)

        return Transcript(
            metadata=metadata,
            source_files=source_files,
            workflow=workflow
        )

    def _extract_metadata(self, markdown: str) -> Dict[str, str]:
        """Extract metadata from top of document.

        Looks for lines like:
            - ID: 00
            - Feature: multilingual functions
            - Date: 2025-01-15
            - Status: draft
        """
        metadata = {}
        lines = markdown.split('\n')

        # Find metadata section (starts after "## Metadata")
        in_metadata = False
        for line in lines:
            if line.strip() == "## Metadata":
                in_metadata = True
                continue
            elif in_metadata and line.startswith('##'):
                # Next section, stop
                break
            elif in_metadata and line.startswith('- '):
                # Parse "- Key: Value"
                match = re.match(r'^- ([^:]+): (.+)$', line)
                if match:
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    metadata[key] = value

        return metadata

    def _extract_source_files(self, markdown: str) -> Dict[str, str]:
        """Extract Python code blocks with filenames from anywhere in document.

        Scans entire markdown for headings with backtick-wrapped filenames
        followed by Python code blocks. These can appear in any section.

        Example patterns:
            ### Fire Encoder `feu_fra.py`
            ```python
            def encoder(text): ...
            ```

            ## Part 2
            ### Calculator `calc.py`
            ```python
            def add(a, b): ...
            ```
        """
        source_files = {}

        # Pattern: Heading (any level) with `filename.py` followed by ```python block
        # Uses re.DOTALL to match across newlines
        pattern = r'###?\s+.*?`([^`]+\.py)`.*?\n```python\n(.*?)\n```'

        for match in re.finditer(pattern, markdown, re.DOTALL):
            filename = match.group(1)
            code = match.group(2)
            source_files[filename] = code

        return source_files

    def _extract_workflow_steps(self, markdown: str) -> List[Step]:
        """Extract bash code blocks as workflow steps from anywhere in document.

        Scans entire markdown for ```bash blocks in document order. Each block
        can contain multiple commands. Commands execute sequentially, preserving
        order across all blocks.

        Supports:
        - Shell variable assignments: HASH=$(bb add file@lang | awk '{print $3}')
        - Regular commands: bb show $HASH@eng
        - Expected output after each command
        - Skip markers: # SKIP-TEST or # SKIP: in block

        Example:
            ## Part 1
            ```bash
            $ HASH=$(bb add file@eng | awk '{print $3}')
            ```

            ## Part 2
            ```bash
            $ bb show $HASH@eng
            def foo(): pass
            ```

        Both blocks are processed in order, $HASH available in second block.
        """
        steps = []
        step_number = 1

        # Find all bash code blocks IN DOCUMENT ORDER
        bash_blocks = re.finditer(r'```bash\n(.*?)\n```', markdown, re.DOTALL)

        for block in bash_blocks:
            content = block.group(1)

            # Skip blocks marked for skipping
            if "# SKIP-TEST" in content or "# SKIP:" in content:
                continue

            # Parse commands from this block
            lines = content.split('\n')
            output_lines = []
            current_command = None
            current_capture_to = None

            for line in lines:
                if line.startswith('$ '):
                    # Save previous command if exists
                    if current_command:
                        steps.append(Step(
                            command=current_command,
                            expected_output='\n'.join(output_lines).strip(),
                            number=step_number,
                            capture_to=current_capture_to
                        ))
                        step_number += 1
                        output_lines = []

                    # Start new command - check for variable assignment
                    command_line = line[2:].strip()  # Remove "$ "

                    # Pattern: VAR=$(command with | pipes)
                    var_match = re.match(r'^([A-Z_][A-Z0-9_]*)=\$\((.*)\)$', command_line)
                    if var_match:
                        current_capture_to = var_match.group(1)
                        current_command = var_match.group(2).strip()
                    else:
                        current_capture_to = None
                        current_command = command_line

                elif line.startswith('#'):
                    # Comment line - ignore
                    continue
                else:
                    # Output line
                    output_lines.append(line)

            # Save last command in block
            if current_command:
                steps.append(Step(
                    command=current_command,
                    expected_output='\n'.join(output_lines).strip(),
                    number=step_number,
                    capture_to=current_capture_to
                ))
                step_number += 1

        return steps
