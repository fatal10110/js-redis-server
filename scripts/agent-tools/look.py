#!/usr/bin/env python3
"""Several small code lookups in one call, with capped output.

Every tool call of a long-lived agent re-sends its whole context, so ten
one-line lookups cost ten times what one batched call costs. Give all the
lookups you need now as arguments:

  FILE:A-B                   lines A..B of FILE (numbered)
  ts:PATH:Name[,Name...]     TypeScript declarations by name in a file or directory
                             (function, const/let, class, interface, type, enum, and
                             Class.method; the leading doc comment included)
  rg:PATH:PATTERN            rg -n matches of PATTERN under PATH (at most --rg-max lines)
  rgl:PATH:PATTERN           files under PATH matching PATTERN, with match counts
  rgg:GLOB:PATH:PATTERN      rg -n restricted to GLOB

Real server sources, fetched once per tag through gh and cached (FLAVOR is redis or
valkey, TAG a release tag such as 7.2.4 or 9.0.0, or a branch such as unstable):
  FLAVOR@TAG:PATH:A-B        a line range, e.g. redis@7.2.4:src/t_stream.c:1800-1860
  FLAVOR@TAG:PATH:fn:a,b     C function definitions by name, e.g. valkey@9.0.0:src/networking.c:fn:processInlineBuffer
  FLAVOR@TAG:PATH:rg:PATTERN rg -n in that file

Relative paths resolve against --root (default: the current directory).
Prints "### STOP" first when the run that owns the worktree is stopping.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wave_mail  # noqa: E402

SRC_CACHE = Path(wave_mail.DEFAULT_ROOT).parent / ".cache" / "src"
SRC_REPOS = {"redis": "redis/redis", "valkey": "valkey-io/valkey"}
C_DECL = r"^(?:[A-Za-z_][\w\s\*]*?[\s\*])?{name}\s*\([^;]*$"

MODS = r"(?:export\s+)?(?:default\s+)?(?:declare\s+)?(?:abstract\s+)?(?:async\s+)?"
TOP_DECL = MODS + r"(?:function\*?\s+|const\s+|let\s+|var\s+|class\s+|interface\s+|type\s+|enum\s+){name}\b"
MEMBER_DECL = (r"^\s+(?:(?:public|private|protected|static|readonly|override|abstract|async|get|set)\s+)*"
               r"\*?{name}\s*(?:<[^>]*>)?\s*[(:=]")


def cap(lines, n, hint):
    if len(lines) <= n:
        return lines
    return lines[:n] + [f"... ({len(lines) - n} more lines cut; {hint})"]


def read_lines(path):
    return Path(path).read_text(encoding="utf-8", errors="replace").split("\n")


def do_range(path, a, b, maxl):
    try:
        text = read_lines(path)
    except OSError as e:
        return [f"error: {e}"]
    a = max(1, a)
    b = min(len(text), b)
    rows = [f"{i}\t{text[i - 1]}" for i in range(a, b + 1)]
    return cap(rows, maxl, f"read a narrower range than {a}-{b}")


STRIP = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:\\.|[^`\\])*`|//.*")


def block_end(text, i):
    """Last line of the declaration starting at line i, by bracket depth."""
    depth, seen = 0, False
    for j in range(i, len(text)):
        code = STRIP.sub("", text[j])
        for ch in code:
            if ch in "{([":
                depth += 1
                seen = True
            elif ch in "})]":
                depth -= 1
        if seen and depth <= 0:
            return j
        if not seen and j == i and not re.search(r"[=,(<:]\s*$", re.sub(r"//.*", "", text[j]).rstrip()):
            return j
    return len(text) - 1


def doc_start(text, i):
    start = i
    while start > 0 and text[start - 1].strip().startswith(("*", "/*", "*/", "//", "@")):
        start -= 1
    return start


def show(path, root):
    rel = os.path.relpath(path, root)
    return path if rel.startswith("..") else rel


def decls_in_file(path, name, root):
    try:
        text = read_lines(path)
    except OSError as e:
        return [f"error: {e}"]
    owner, _, member = name.rpartition(".")
    hits = []
    if owner:
        top = re.compile(r"^" + TOP_DECL.format(name=re.escape(owner)))
        mem = re.compile(MEMBER_DECL.format(name=re.escape(member)))
        for i, line in enumerate(text):
            if top.search(line):
                end = block_end(text, i)
                hits += [k for k in range(i + 1, end) if mem.search(text[k])]
    else:
        top = re.compile(r"^\s*" + TOP_DECL.format(name=re.escape(name)))
        hits = [i for i, line in enumerate(text) if top.search(line)]
    out = []
    for i in hits:
        start, end = doc_start(text, i), block_end(text, i)
        out.append(f"== {show(path, root)}:{start + 1}-{end + 1}")
        out += [f"{k + 1}\t{text[k]}" for k in range(start, end + 1)]
    return out


def do_ts(path, names, maxl, root):
    files = [path]
    if os.path.isdir(path):
        r = subprocess.run(["rg", "-l", "--color", "never", "-g", "*.ts", "-e",
                            "|".join(re.escape(n.rpartition(".")[0] or n) for n in names), path],
                           capture_output=True, text=True)
        files = sorted(l for l in r.stdout.split("\n") if l)
    out = []
    for name in names:
        rows = []
        for f in files:
            rows += decls_in_file(f, name, root)
        if not rows:
            out.append(f"== {name}: no declaration under {show(path, root)}")
            continue
        out += cap(rows, maxl, "ask for a range instead")
    return out


def fetch_src(flavor, tag, path):
    """Local cached copy of PATH at TAG of the real server's repository."""
    if not re.fullmatch(r"[\w.-]+", tag) or ".." in path or path.startswith("/"):
        raise ValueError(f"bad tag or path: {tag} {path}")
    dest = SRC_CACHE / flavor / tag / path
    if not dest.exists():
        r = subprocess.run(["gh", "api", f"repos/{SRC_REPOS[flavor]}/contents/{path}?ref={tag}",
                            "-H", "Accept: application/vnd.github.raw"], capture_output=True, timeout=120)
        if r.returncode:
            raise ValueError((r.stderr or r.stdout).decode("utf-8", "replace").strip()[-200:])
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".{dest.name}.{os.getpid()}")
        tmp.write_bytes(r.stdout)
        os.replace(tmp, dest)
    return dest


def do_c(path, names, maxl, label):
    text = read_lines(path)
    out = []
    for name in names:
        rx = re.compile(C_DECL.format(name=re.escape(name)))
        hits = [i for i, l in enumerate(text) if rx.search(l) and not l.startswith((" ", "\t", "#"))]
        if not hits:
            out.append(f"== {name}: no definition in {label}")
            continue
        for i in hits:
            start, end = doc_start(text, i), block_end(text, i)
            out.append(f"== {label}:{start + 1}-{end + 1}")
            out += cap([f"{k + 1}\t{text[k]}" for k in range(start, end + 1)], maxl,
                       f"read {label}:{start + 1 + maxl}-{end + 1}")
    return out


def do_src(spec, a, root):
    m = re.fullmatch(r"(redis|valkey)@([^:]+):([^:]+):(.+)", spec)
    if not m:
        return [f"error: bad source spec {spec!r} (see --help)"]
    flavor, tag, path, rest = m.groups()
    label = f"{flavor}@{tag}:{path}"
    try:
        local = str(fetch_src(flavor, tag, path))
    except (ValueError, OSError, subprocess.TimeoutExpired) as e:
        return [f"error: {label}: {e}"]
    r = re.fullmatch(r"(\d+)-(\d+)", rest)
    if r:
        return do_range(local, int(r.group(1)), int(r.group(2)), a.max)
    kind, _, arg = rest.partition(":")
    if kind == "fn":
        return do_c(local, [n for n in arg.split(",") if n], a.max, label)
    if kind == "rg":
        return [l.replace(local + ":", label + ":", 1) for l in do_rg(local, arg, None, False, a.rg_max, root)]
    return [f"error: bad source spec {spec!r} (see --help)"]


def do_rg(path, pattern, glob, files_only, maxl, root):
    args = ["rg", "--no-heading", "--color", "never"]
    args += ["-c"] if files_only else ["-n", "-m", "20"]
    if glob:
        args += ["-g", glob]
    args += ["-e", pattern, path]
    r = subprocess.run(args, capture_output=True, text=True)
    prefix = os.path.join(os.path.realpath(root), "")
    lines = [l.replace(prefix, "", 1)[:300] for l in r.stdout.rstrip("\n").split("\n") if l]
    if not lines:
        return [r.stderr.strip() or "(no matches)"]
    return cap(lines, maxl, "narrow PATH or PATTERN, or use rgl: first")


def resolve(root, p):
    return p if os.path.isabs(p) else str(Path(root) / p)


def run_spec(spec, root, a):
    m = re.fullmatch(r"(.+):(\d+)-(\d+)", spec)
    kind, _, rest = spec.partition(":")
    if re.match(r"(redis|valkey)@", spec):
        return do_src(spec, a, root)
    if kind == "ts" and rest:
        path, _, names = rest.rpartition(":")
        return do_ts(resolve(root, path), [n for n in names.split(",") if n], a.max, root)
    if kind in ("rg", "rgl") and rest:
        path, _, pattern = rest.partition(":")
        return do_rg(resolve(root, path), pattern, None, kind == "rgl", a.rg_max, root)
    if kind == "rgg" and rest:
        glob, _, rest2 = rest.partition(":")
        path, _, pattern = rest2.partition(":")
        return do_rg(resolve(root, path), pattern, glob, False, a.rg_max, root)
    if m:
        return do_range(resolve(root, m.group(1)), int(m.group(2)), int(m.group(3)), a.max)
    return [f"error: unknown spec {spec!r} (see --help)"]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("specs", nargs="+")
    p.add_argument("--root", default=os.getcwd())
    p.add_argument("--max", type=int, default=150, help="line cap per range or declaration")
    p.add_argument("--rg-max", type=int, default=40, help="line cap per rg spec")
    p.add_argument("--total", type=int, default=600, help="line cap for the whole output")
    a = p.parse_args(argv)
    lines = []
    stop = wave_mail.stop_banner([a.root] + re.findall(r"(?<![@\w])(/[^:\s]+)", " ".join(a.specs)))
    if stop:
        lines.append(stop)
    for spec in a.specs:
        lines.append(f"### {spec}")
        lines += run_spec(spec, a.root, a)
    print("\n".join(cap(lines, a.total, "split the lookups over two calls")))


if __name__ == "__main__":
    main()
