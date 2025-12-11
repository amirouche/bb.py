#!/usr/bin/env python3
"""
GitHub Review Comments Helper (JSON output)

Fetches unresolved review comments for the current branch's PR and displays them
in JSON format.

Usage: ./bin/github-review-comments-json.py
"""

import json
import subprocess
import sys


def run_command(cmd, cwd=None):
    """Run a shell command and return stdout, or exit on error."""
    try:
        result = subprocess.run(
            cmd, shell=True, cwd=cwd, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {cmd}", file=sys.stderr)
        print(f"Error: {e.stderr.strip()}", file=sys.stderr)
        sys.exit(1)


def get_current_branch():
    """Get the current git branch name."""
    return run_command("git branch --show-current")


def get_repo_info():
    """Get repository owner and name."""
    remote_url = run_command("git remote get-url origin")
    # Extract owner/repo from git@github.com:owner/repo.git or https://github.com/owner/repo
    if remote_url.startswith("git@github.com:"):
        owner_repo = remote_url.replace("git@github.com:", "").replace(".git", "")
    elif remote_url.startswith("https://github.com/"):
        owner_repo = remote_url.replace("https://github.com/", "").replace(".git", "")
    else:
        print(f"Unsupported remote URL format: {remote_url}", file=sys.stderr)
        sys.exit(1)

    owner, repo = owner_repo.split("/")
    return owner, repo


def get_pr_number(branch):
    """Get the PR number for the current branch."""
    try:
        # Try to get PR info for current branch
        pr_info = run_command("gh pr view --json number")
        pr_data = json.loads(pr_info)
        return pr_data.get("number")
    except Exception:
        # If no PR is found, try to find it by branch name
        try:
            pr_list = run_command("gh pr list --head {branch} --json number --limit 1")
            pr_list_data = json.loads(pr_list)
            if pr_list_data:
                return pr_list_data[0].get("number")
        except Exception:
            pass

    print(f"No PR found for branch: {branch}", file=sys.stderr)
    sys.exit(1)


def get_review_comments(owner, repo, pr_number):
    """Get review comments for a PR using GitHub API."""
    api_url = f"repos/{owner}/{repo}/pulls/{pr_number}/comments"
    comments_json = run_command(f"gh api {api_url}")
    return json.loads(comments_json)


def main():
    """Main function to fetch and display review comments."""
    # Get current branch
    branch = get_current_branch()
    print(f"Current branch: {branch}", file=sys.stderr)

    # Get repository info
    owner, repo = get_repo_info()
    print(f"Repository: {owner}/{repo}", file=sys.stderr)

    # Get PR number
    pr_number = get_pr_number(branch)
    print(f"PR number: {pr_number}", file=sys.stderr)

    # Get review comments
    comments = get_review_comments(owner, repo, pr_number)

    # Filter unresolved comments and extract required fields
    unresolved_comments = []
    for comment in comments:
        if comment.get("resolved_at") is None:
            unresolved_comments.append(
                {
                    "id": comment.get("id"),
                    "url": comment.get("html_url", comment.get("url")),
                    "diff_hunk": comment.get("diff_hunk", ""),
                    "path": comment.get("path", ""),
                    "position": comment.get("position"),
                    "original_position": comment.get("original_position"),
                    "body": comment.get("body", ""),
                }
            )

    # Output as JSON
    print(json.dumps(unresolved_comments, indent=2))

    print(
        f"\nFound {len(unresolved_comments)} unresolved review comments.",
        file=sys.stderr,
    )

    # Exit with code 2 if there are unresolved comments
    if unresolved_comments:
        sys.exit(2)


if __name__ == "__main__":
    main()
