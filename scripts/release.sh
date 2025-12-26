#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <version> [--push]" >&2
  exit 1
fi

version="$1"
shift || true

if ! echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "version must be like 0.2.0" >&2
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "working tree is dirty; commit or stash changes first" >&2
  exit 1
fi

sed -i -E "s/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"/version = \"$version\"/" Cargo.toml

git add Cargo.toml Cargo.lock

git commit -m "Release v$version"

git branch "v$version"

git tag "v$version"

if [ "${1:-}" = "--push" ]; then
  git push origin HEAD
  git push origin "v$version"
  git push origin "v$version" --tags
fi

cat <<MSG
Release prepared:
- branch: v$version
- tag: v$version

Next steps:
- push with: $0 $version --push
MSG
