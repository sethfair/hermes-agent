#!/usr/bin/env bash
# Point a Hermes install's main remote (origin) at the Seth fork, keeping
# Nous Research as the `upstream` side remote. Rerunnable; dry-run by default.
# Touches only git remote config: no fetch, checkout, pull, or service restart.
#
# Usage: scripts/switch-to-fork.sh [--apply | --revert] [--path DIR]
#   (default)  dry run: show current remotes and what would change
#   --apply    switch origin to the fork, add/keep upstream = Nous
#   --revert   origin back to Nous; remove upstream if it is the Nous URL
#   --path DIR install to operate on (default: $HERMES_INSTALL or ~/.hermes/hermes-agent)
set -euo pipefail

FORK_URL="https://github.com/sethfair/hermes-agent.git"
NOUS_URL="https://github.com/NousResearch/hermes-agent.git"
MODE=dry; DIR="${HERMES_INSTALL:-$HOME/.hermes/hermes-agent}"

while [ $# -gt 0 ]; do
  case "$1" in
    --apply) MODE=apply ;;
    --revert) MODE=revert ;;
    --path) DIR="${2:?--path needs a directory}"; shift ;;
    -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

# Compare URLs ignoring .git suffix, trailing slash, case, ssh/https form.
norm() { local u="${1%/}"; u="${u%.git}"; u="${u#git@github.com:}"; u="${u#ssh://git@github.com/}"
  u="${u#https://github.com/}"; u="${u#http://github.com/}"; printf '%s' "$u" | tr 'A-Z' 'a-z'; }
FORK_N=$(norm "$FORK_URL"); NOUS_N=$(norm "$NOUS_URL")

git -C "$DIR" rev-parse --git-dir >/dev/null 2>&1 || { echo "REFUSE: $DIR is not a git checkout" >&2; exit 1; }
echo "install: $DIR   mode: $MODE"
echo "remotes now:"; git -C "$DIR" remote -v | sed 's/^/  /'

if [ -n "$(git -C "$DIR" status --porcelain)" ]; then
  echo "REFUSE: install has uncommitted or untracked changes; clean it first:" >&2
  git -C "$DIR" status --short | sed 's/^/  /' >&2
  exit 1
fi

get() { git -C "$DIR" remote get-url "$1" 2>/dev/null || true; }
O=$(get origin); U=$(get upstream)
[ -n "$O" ] || { echo "REFUSE: no origin remote" >&2; exit 1; }
if [ -n "$U" ] && [ "$(norm "$U")" != "$NOUS_N" ]; then
  echo "REFUSE: existing 'upstream' remote points elsewhere ($U)" >&2; exit 1
fi

plan=()
if [ "$MODE" = revert ]; then
  [ "$(norm "$O")" = "$NOUS_N" ] || plan+=("set origin -> $NOUS_URL")
  [ -z "$U" ] || plan+=("remove remote upstream")
else
  case "$(norm "$O")" in
    "$FORK_N") ;;
    "$NOUS_N") plan+=("set origin -> $FORK_URL") ;;
    *) echo "REFUSE: origin is neither Nous nor the fork ($O)" >&2; exit 1 ;;
  esac
  [ -n "$U" ] || plan+=("add remote upstream -> $NOUS_URL")
fi

if [ ${#plan[@]} -eq 0 ]; then echo "nothing to change (already in target state)"; exit 0; fi
echo "planned changes:"; printf '  - %s\n' "${plan[@]}"
if [ "$MODE" = dry ]; then echo "DRY RUN: nothing changed. Re-run with --apply to switch (--revert to undo)."; exit 0; fi

if [ "$MODE" = revert ]; then
  git -C "$DIR" remote set-url origin "$NOUS_URL"
  [ -z "$U" ] || git -C "$DIR" remote remove upstream
else
  [ "$(norm "$O")" = "$FORK_N" ] || git -C "$DIR" remote set-url origin "$FORK_URL"
  [ -n "$U" ] || git -C "$DIR" remote add upstream "$NOUS_URL"
fi
echo "remotes after:"; git -C "$DIR" remote -v | sed 's/^/  /'
echo "done. No services were restarted."
