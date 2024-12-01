#!/bin/bash

# Set `.githooks` as the directory for Git hooks
git config core.hooksPath .githooks

# Set all the hooks to be executable
chmod -R +x .githooks

echo "Pre-commit hooks are set up for directory:"
echo "$(git config --get core.hooksPath)"
exit 0
