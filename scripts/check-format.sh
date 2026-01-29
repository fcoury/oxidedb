#!/bin/bash

# Get list of staged Rust files that need formatting check
CHANGED_FILES=$(git diff --cached --name-only | grep -E "\.rs$")

if [ -n "$CHANGED_FILES" ]; then
    echo "Checking format for changed Rust files:"
    echo "$CHANGED_FILES"
    cargo fmt -- --check
else
    echo "No Rust files changed, skipping format check"
    exit 0
fi


