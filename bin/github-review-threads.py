#!/usr/bin/env python3
"""
GitHub Review Threads Helper (JSON output)

Fetches unresolved review threads for the current branch's PR and displays them
in JSON format using GitHub's GraphQL API.

This script checks for unresolved review threads (not individual comments) because
GitHub's review system works with threads that can contain multiple comments.
A thread is considered resolved when the code changes address the concerns.

Usage: ./bin/github-review-threads.py
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


def get_review_threads(owner, repo, pr_number):
    """Get review threads for a PR using GitHub GraphQL API."""
    query = """
    query FetchReviewComments($owner: String!, $repo: String!, $prNumber: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $prNumber) {
          reviewThreads(first: 100) {
            edges {
              node {
                isResolved
                isOutdated
                comments(first: 100) {
                  nodes {
                    id
                    body
                    diffHunk
                    path
                    position
                    originalPosition
                    url
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    # Use gh api graphql with variables
    result = run_command(
        f"gh api graphql --field owner={owner} --field repo={repo} --field prNumber={pr_number} -F query='{query}'"
    )
    data = json.loads(result)

    # Extract review threads and convert to comment-like format
    threads = data["data"]["repository"]["pullRequest"]["reviewThreads"]["edges"]

    comments = []
    for thread in threads:
        thread_node = thread["node"]
        for comment in thread_node["comments"]["nodes"]:
            # Add resolution status to comment
            comment["isResolved"] = thread_node["isResolved"]
            comment["isOutdated"] = thread_node["isOutdated"]
            comments.append(comment)

    return comments


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

    # Get review threads
    threads = get_review_threads(owner, repo, pr_number)

    # Filter unresolved threads and extract required fields
    unresolved_threads = []
    for thread in threads:
        # Use the isResolved field from GraphQL API
        if not thread.get("isResolved", False):
            unresolved_threads.append(
                {
                    "id": thread.get("id"),
                    "url": thread.get("url"),
                    "diff_hunk": thread.get("diffHunk", ""),
                    "path": thread.get("path", ""),
                    "position": thread.get("position"),
                    "original_position": thread.get("originalPosition"),
                    "body": thread.get("body", ""),
                    "is_outdated": thread.get("isOutdated", False),
                }
            )

    # Output as JSON
    print(json.dumps(unresolved_threads, indent=2))

    print(
        f"\nFound {len(unresolved_threads)} unresolved review threads.",
        file=sys.stderr,
    )

    # Exit with code 2 if there are unresolved threads
    if unresolved_threads:
        sys.exit(2)


if __name__ == "__main__":
    main()
