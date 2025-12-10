#!/usr/bin/env python3
"""
GitHub Review Comments Fetcher

This script fetches unresolved review comments for the current branch's PR
and prints them as JSON lines to stdout with the specified fields.

Usage: ./bin/github-review-comments.py
"""

import subprocess
import json
import sys
import argparse
from typing import List, Dict, Any


def run_gh_command(command: List[str]) -> Any:
    """Run a gh CLI command and return parsed JSON output."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        print(f"Error running gh command: {' '.join(command)}", file=sys.stderr)
        print(f"Error: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON from command: {' '.join(command)}", file=sys.stderr)
        print(f"Output: {result.stdout}", file=sys.stderr)
        print(f"JSON Decode Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


def get_current_repo_info() -> Dict[str, str]:
    """Get current repository information."""
    repo_info = run_gh_command(["gh", "repo", "view", "--json", "nameWithOwner"])
    owner, repo = repo_info["nameWithOwner"].split("/")
    return {"owner": owner, "repo": repo}


def get_current_branch() -> str:
    """Get current git branch name."""
    try:
        result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting current branch: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def find_pr_for_branch(owner: str, repo: str, branch: str) -> int:
    """Find the PR number for the current branch."""
    # List all PRs and find the one with matching headRefName
    prs = run_gh_command(["gh", "pr", "list", "--json", "number,headRefName", "--state", "open"])

    # Filter PRs to find the one with matching branch
    matching_prs = [pr for pr in prs if pr.get("headRefName") == branch]

    if not matching_prs:
        print(f"No pull request found for branch: {branch}", file=sys.stderr)
        sys.exit(1)

    # If multiple PRs found (unlikely), take the first one
    return matching_prs[0]["number"]


def get_unresolved_review_comments(owner: str, repo: str, pr_number: int) -> List[Dict[str, Any]]:
    """Get unresolved review comments for a PR."""
    comments = run_gh_command(["gh", "api", f"/repos/{owner}/{repo}/pulls/{pr_number}/comments", "--paginate"])

    # Filter for unresolved comments only
    unresolved_comments = []
    for comment in comments:
        if not comment.get("resolved_at"):
            unresolved_comments.append(
                {
                    "id": comment.get("id"),
                    "pull_request_review_id": comment.get("pull_request_review_id"),
                    "diff_hunk": comment.get("diff_hunk"),
                    "path": comment.get("path"),
                    "position": comment.get("position"),
                    "original_position": comment.get("original_position"),
                    "body": comment.get("body"),
                }
            )

    return unresolved_comments


def print_jsonl(comments: List[Dict[str, Any]]) -> None:
    """Print comments as JSON lines to stdout."""
    for comment in comments:
        print(json.dumps(comment, ensure_ascii=False))


def main() -> None:
    """Main function."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Fetch GitHub review comments")
    parser.add_argument("--pr-number", type=int, help="Specific PR number to fetch comments from")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress informational messages")
    args = parser.parse_args()

    if not args.quiet:
        print("Fetching GitHub review comments...")

    # Get repository info
    repo_info = get_current_repo_info()
    owner, repo = repo_info["owner"], repo_info["repo"]
    if not args.quiet:
        print(f"Repository: {owner}/{repo}")

    # Determine PR number
    if args.pr_number:
        pr_number = args.pr_number
        if not args.quiet:
            print(f"Using specified PR: #{pr_number}")
    else:
        # Get current branch
        branch = get_current_branch()
        if not args.quiet:
            print(f"Current branch: {branch}")

        # Find PR for current branch
        pr_number = find_pr_for_branch(owner, repo, branch)
        if not args.quiet:
            print(f"Found PR: #{pr_number}")

    # Get unresolved review comments
    comments = get_unresolved_review_comments(owner, repo, pr_number)
    if not args.quiet:
        print(f"Found {len(comments)} unresolved review comments", file=sys.stderr)

    # Print comments as JSON lines to stdout
    print_jsonl(comments)


if __name__ == "__main__":
    main()
