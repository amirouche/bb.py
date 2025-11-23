# Transcripts

This directory contains workflow transcripts documenting common usage patterns for Beyond Babel.

## Directory Structure

```
transcripts/
├── README.md           # This file
├── end2end/            # End-to-end workflow transcripts
│   └── 00.md           # First transcript
├── commit/             # Commit command transcripts (future)
└── remote/             # Remote operations transcripts (future)
```

## Numbering Scheme

Files use a two-character base36 identifier: `00`, `01`, ... `09`, `0A`, `0B`, ... `0Z`, `10`, etc.

Characters: `0-9` (10) + `A-Z` (26) = 36 values per digit.

## Transcript Format

Each transcript follows this structure:

```markdown
# Transcript: [Title]

## Metadata
- ID: NN
- Feature: [feature name]
- Date: YYYY-MM-DD
- Status: draft|final

## Overview
Brief description.

## Prerequisites
What's needed before starting.

## Source Files
Code examples (if applicable).

## Workflow
Step-by-step commands with expected output.

## Expected Results
What should happen at the end.

## Notes
Additional information.
```

## Status

- **draft**: Work in progress, may contain placeholders
- **final**: Verified and complete
