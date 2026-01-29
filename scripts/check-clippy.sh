#!/bin/bash

# Pre-push hook: check clippy for Rust files in commits being pushed
# Git provides: <local ref> <local sha> <remote ref> <remote sha> via stdin

CHANGED_FILES=""

while read local_ref local_sha remote_ref remote_sha; do
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
        # Branch being deleted, nothing to check
        continue
    fi

    if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
        # New branch - check files changed from merge-base with origin/master
        range="$(git merge-base origin/master "$local_sha")..$local_sha"
    else
        # Existing branch - check files changed since remote
        range="$remote_sha..$local_sha"
    fi

    CHANGED_FILES="$CHANGED_FILES $(git diff --name-only "$range" 2>/dev/null | grep -E '\.rs$')"
done

# Remove duplicates and whitespace
CHANGED_FILES=$(echo "$CHANGED_FILES" | tr ' ' '\n' | sort -u | grep -v '^$')

if [ -n "$CHANGED_FILES" ]; then
    echo "Checking clippy for changed Rust files:"
    echo "$CHANGED_FILES"
    cargo clippy -- -D warnings
else
    echo "No Rust files changed, skipping clippy check"
    exit 0
fi
