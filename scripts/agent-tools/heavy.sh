#!/usr/bin/env bash
# Run a heavy local command (tsc, eslint, test files) under one machine-wide lock, so the
# lanes of a redis-waves run never load this small box with several runs at once. Waiting
# for the lock is normal. Refuses to run, printing "### STOP", once the run that owns the
# current worktree is stopping.
set -u
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
paths=("$PWD"); for a in "$@"; do [[ "$a" == /* ]] && paths+=("$a"); done
python3 "$here/wave_mail.py" stopcheck -- "${paths[@]}" || exit 3
lock_dir="$(python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import wave_mail, pathlib; print(pathlib.Path(wave_mail.DEFAULT_ROOT).parent / ".tools")' "$here")"
mkdir -p "$lock_dir"
exec flock "$lock_dir/heavy.lock" "$@"
