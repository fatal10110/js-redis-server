#!/usr/bin/env python3
"""Exact wire replies from real Redis / Valkey servers, one per compatibility profile.

Every claim about real behavior is checked byte for byte against a real server of the
profile's pinned version (src/core/compatibility/profile.ts). This starts (or reuses) one
throwaway docker container per profile and server-argument set, and keeps it running for
later calls and for other agents. Every use is recorded, so idle containers can be removed.

  probe.py run [-p PROFILES] [--resp 3] [--args '--requirepass pw'] [--raw] -- CMD...
      Sends each CMD on one connection and prints the reply bytes per profile.
      CMD is split like a shell line ('SET {u}k "a b"'); \\r \\n \\t \\xNN \\\\ escapes are
      decoded in every token (--no-escapes turns that off). {u} becomes a random id, the
      same for every profile, so keys never collide with another agent's probe.
      --raw sends each CMD as raw bytes instead (e.g. '*1\\r\\n$4\\r\\nPING\\r\\n').
      --mock WORKTREE also starts the mock from that checkout (src/cli.ts --compat <profile>)
      per profile, runs the same commands, and marks each reply "= mock" or "≠ mock: <bytes>".
  probe.py up [-p PROFILES] [--args ...]
      Starts the containers and prints {"<profile>": port}: run a standalone integration
      file against one with TEST_BACKEND=real REDIS_COMPAT=<profile> REDIS_STANDALONE_PORT=<port>.
  probe.py down [-p PROFILES] --args ARGS | -p PROFILE@TAG | --all
      Removes the one-off containers of an argument set or a custom image tag (the ones a
      run/up with --args or PROFILE@TAG started); remove yours when you are done with them.
      The shared default containers (pinned tag, no --args) stay unless --all is given.
  probe.py gc [--idle MINUTES]
      Removes every probe container, shared or not, unused for MINUTES (default 60).

PROFILES: comma-separated profile names (default: all), "redis", "valkey", or a profile
with an explicit image tag, e.g. redis-7.2@7.2.16 or valkey-9.0@9.0.6.
"""

import argparse
import calendar
import codecs
import contextlib
import fcntl
import hashlib
import json
import re
import secrets
import shlex
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wave_mail  # noqa: E402

# Pinned versions of src/core/compatibility/profile.ts, plus the extra servers issues cite.
PROFILES = {
    "redis-6.2": ("redis", "6.2.14"),
    "redis-7.0": ("redis", "7.0.15"),
    "redis-7.2": ("redis", "7.2.4"),
    "redis-7.4": ("redis", "7.4.4"),
    "redis-8.0": ("redis", "8.0.0"),
    "valkey-8.0": ("valkey/valkey", "8.0.0"),
    "valkey-9.0": ("valkey/valkey", "9.0.0"),
}
EXTRA = {"valkey-7.2": ("valkey/valkey", "7.2.4"), "valkey-8.1": ("valkey/valkey", "8.1.0")}
PREFIX = "wave-probe-"
LOCK = Path(wave_mail.DEFAULT_ROOT).parent / ".tools" / "probe.lock"
USED = LOCK.parent / "probe-used"  # one file per container; its mtime is the last use
IDLE = 0.25  # seconds of silence that end a raw read or the pushes after a reply


def docker(*args, check=True, timeout=300):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"docker {' '.join(args[:3])}: {(r.stderr or r.stdout).strip()[-300:]}")
    return r.stdout.strip()


def resolve_profiles(spec):
    if not spec or spec == "all":
        names = list(PROFILES)
    elif spec in ("redis", "valkey"):
        names = [p for p in PROFILES if p.startswith(spec)]
    else:
        names = [s.strip() for s in spec.split(",") if s.strip()]
    res = []
    for n in names:
        base, _, tag = n.partition("@")
        if base not in PROFILES and base not in EXTRA:
            raise SystemExit(f"unknown profile {base!r}; known: {', '.join(list(PROFILES) + list(EXTRA))}")
        image, pinned = PROFILES.get(base) or EXTRA[base]
        res.append((n, f"{image}:{tag or pinned}"))
    return res


def is_shared(image, server_args):
    """The default container of a profile: its pinned tag and no extra server arguments."""
    return not server_args.strip() and any(f"{i}:{t}" == image for i, t in [*PROFILES.values(), *EXTRA.values()])


def one_off_note(targets, server_args):
    """A reminder to remove the one-off containers a run or up just used."""
    mine = [n for n, image in targets if not is_shared(image, server_args)]
    if not mine:
        return None
    args = f" --args {shlex.quote(server_args)}" if server_args.strip() else ""
    return (f"(note: one-off containers for {', '.join(mine)}{args}; remove them when you no longer need them:"
            f" probe.py down -p {shlex.quote(','.join(mine))}{args})")


def idle_containers(started, used, now, idle_s):
    """Names whose last use (the later of start time and recorded use) is idle_s or more ago."""
    return sorted(n for n, t in started.items() if now - max(t, used.get(n, 0)) >= idle_s)


def mark_used(name):
    USED.mkdir(parents=True, exist_ok=True)
    (USED / name).touch()


def remove(names):
    for n in names:
        docker("rm", "-f", n, check=False)
        with contextlib.suppress(OSError):
            (USED / n).unlink()


def container_name(image, server_args):
    key = hashlib.sha1((image + " " + server_args).encode()).hexdigest()[:8]
    return PREFIX + re.sub(r"[^a-z0-9.-]", "-", image.split("/")[-1].lower()) + "-" + key


def published_port(name):
    out = docker("port", name, "6379/tcp", check=False)
    m = re.search(r":(\d+)\s*$", out.split("\n")[0]) if out else None
    return int(m.group(1)) if m else None


def ensure(image, server_args):
    name = container_name(image, server_args)
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        port = published_port(name)
        if port is None:
            docker("rm", "-f", name, check=False)
            docker("run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::6379", image,
                   *(["valkey-server"] if "valkey" in image else ["redis-server"]),
                   "--save", "", "--appendonly", "no", *shlex.split(server_args), timeout=900)
            for _ in range(100):
                port = published_port(name)
                if port:
                    break
                time.sleep(0.1)
        mark_used(name)
        fcntl.flock(lf, fcntl.LOCK_UN)
    deadline = time.time() + 30
    while time.time() < deadline:
        with contextlib.suppress(OSError):
            with socket.create_connection(("127.0.0.1", port), timeout=1) as s:
                s.sendall(b"PING\r\n")
                if s.recv(64)[:1] in (b"+", b"-"):
                    return name, port
        time.sleep(0.2)
    raise RuntimeError(f"{name} did not answer on port {port}")


def reply_end(buf, i=0):
    """Index just past one complete RESP2/RESP3 reply starting at i, or None."""
    if i >= len(buf):
        return None
    nl = buf.find(b"\r\n", i)
    if nl < 0:
        return None
    t, head = buf[i:i + 1], buf[i + 1:nl]
    j = nl + 2
    if t in (b"+", b"-", b":", b",", b"#", b"_", b"("):
        return j
    if t in (b"$", b"=", b"!"):
        n = int(head)
        return j if n < 0 else (j + n + 2 if len(buf) >= j + n + 2 else None)
    if t in (b"*", b"~", b">", b"%", b"|"):
        n = int(head)
        if n < 0:
            return j
        for _ in range(n * (2 if t in (b"%", b"|") else 1)):
            j = reply_end(buf, j)
            if j is None:
                return None
        return reply_end(buf, j) if t == b"|" else j
    return j  # an inline error or anything unexpected: the line is the reply


def show(b, limit=3000):
    s = repr(bytes(b))[2:-1]
    return s if len(s) <= limit else s[:limit] + f"... ({len(s) - limit} more chars)"


def decode(tok, escapes):
    return codecs.escape_decode(tok.encode("latin-1", "backslashreplace"))[0] if escapes else tok.encode("utf-8")


def frame(args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read_reply(s, raw):
    buf, closed = b"", False
    end = None
    deadline = time.time() + 10
    while time.time() < deadline:
        s.settimeout(IDLE if (raw and buf) or end is not None else max(0.05, deadline - time.time()))
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            if raw or end is not None:
                break
            continue
        if not chunk:
            closed = True
            break
        buf += chunk
        if not raw and end is None:
            end = reply_end(buf)
    return buf, closed


def exchange(port, cmds, a, uid):
    """[(cmd, reply text)] for each command sent on one connection."""
    rows = []
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        if a.resp == 3:
            s.sendall(frame([b"HELLO", b"3"]))
            read_reply(s, False)
        for cmd in cmds:
            cmd = cmd.replace("{u}", uid)
            payload = decode(cmd, a.escapes) if a.raw else frame([decode(t, a.escapes) for t in shlex.split(cmd)])
            try:
                s.sendall(payload)
                buf, closed = read_reply(s, a.raw)
            except OSError as e:
                rows.append((cmd, f"(error: {e})"))
                break
            rows.append((cmd, (show(buf) if buf else "(no reply)") + (" (connection closed by server)" if closed else "")))
            if closed:
                break
    return rows


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def mock_rows(worktree, profile, cmds, a, uid):
    port = free_port()
    proc = subprocess.Popen(["node", "--import", "tsx", "--no-warnings", "src/cli.ts", "--port", str(port),
                             "--compat", profile.partition("@")[0]], cwd=worktree,
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            if proc.poll() is not None:
                return None, "mock exited: " + proc.stderr.read().decode("utf-8", "replace").strip()[-300:]
            with contextlib.suppress(OSError):
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                return exchange(port, cmds, a, uid), None
            time.sleep(0.2)
        return None, "mock did not start within 60 s"
    finally:
        proc.kill()
        proc.wait()


def probe_one(profile, image, server_args, cmds, a, uid):
    try:
        name, port = ensure(image, server_args)
        rows = exchange(port, cmds, a, uid)
    except (RuntimeError, OSError, subprocess.TimeoutExpired) as e:
        return [f"## {profile} ({image}): error: {e}"]
    out = [f"## {profile} ({image}, port {port})"]
    mock, err = (mock_rows(a.mock, profile, cmds, a, uid) if a.mock else (None, None))
    if err:
        out.append(f"(mock: {err})")
    for i, (cmd, reply) in enumerate(rows):
        out += [f"> {cmd}", f"< {reply}"]
        if mock is not None:
            m = mock[i][1] if i < len(mock) else "(no reply: the mock closed the connection earlier)"
            out.append("= mock" if m == reply else f"≠ mock: {m}")
    if mock is not None and len(mock) > len(rows):
        out.append(f"≠ mock answered {len(mock) - len(rows)} more command(s) after the real server closed")
    return out


def cmd_run(a):
    banner = wave_mail.stop_banner([str(Path.cwd())])
    if banner:
        print(banner)
    targets = resolve_profiles(a.profiles)
    uid = secrets.token_hex(3)
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        results = list(ex.map(lambda t: probe_one(t[0], t[1], a.args, a.cmds, a, uid), targets))
    print(f"(probe id {{u}} = {uid})")
    for r in results:
        print("\n".join(r))
    note = one_off_note(targets, a.args)
    if note:
        print(note)


def cmd_up(a):
    targets = resolve_profiles(a.profiles)
    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        ports = list(ex.map(lambda t: ensure(t[1], a.args)[1], targets))
    print(json.dumps({t[0]: p for t, p in zip(targets, ports)}))
    note = one_off_note(targets, a.args)
    if note:
        print(note)


def running():
    return [n for n in docker("ps", "--format", "{{.Names}}").split("\n") if n.startswith(PREFIX)]


def cmd_down(a):
    if a.all:
        names = running()
        remove(names)
        print(json.dumps({"stopped": names}))
        return
    targets = resolve_profiles(a.profiles)
    mine = [container_name(image, a.args) for _, image in targets if not is_shared(image, a.args)]
    if not mine:
        raise SystemExit("down removes one-off containers only: pass the --args (and -p) of the run/up that "
                         "started them, or PROFILE@TAG; the shared defaults go with --all or gc")
    live = set(running())
    remove([n for n in mine if n in live])
    print(json.dumps({"stopped": [n for n in mine if n in live]}))


def cmd_gc(a):
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        names = running()
        started = {}
        if names:
            for line in docker("inspect", "-f", "{{.Name}} {{.State.StartedAt}}", *names).split("\n"):
                n, _, ts = line.strip().lstrip("/").partition(" ")
                started[n] = calendar.timegm(time.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S"))
        used = {f.name: f.stat().st_mtime for f in USED.glob(PREFIX + "*")} if USED.is_dir() else {}
        idle = idle_containers(started, used, time.time(), a.idle * 60)
        remove(idle)
        fcntl.flock(lf, fcntl.LOCK_UN)
    print(json.dumps({"stopped": idle, "kept": sorted(set(names) - set(idle))}))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("-p", "--profiles", default="all")
    r.add_argument("--resp", type=int, choices=(2, 3), default=2)
    r.add_argument("--args", default="", help="extra server arguments, e.g. '--requirepass pw'")
    r.add_argument("--raw", action="store_true")
    r.add_argument("--no-escapes", dest="escapes", action="store_false")
    r.add_argument("--mock", metavar="WORKTREE", help="also run the mock from this checkout and compare")
    r.add_argument("cmds", nargs="+")
    r.set_defaults(fn=cmd_run)
    u = sub.add_parser("up")
    u.add_argument("-p", "--profiles", default="all")
    u.add_argument("--args", default="")
    u.set_defaults(fn=cmd_up)
    d = sub.add_parser("down")
    d.add_argument("-p", "--profiles", default="all")
    d.add_argument("--args", default="")
    d.add_argument("--all", action="store_true", help="also the shared default containers")
    d.set_defaults(fn=cmd_down)
    g = sub.add_parser("gc")
    g.add_argument("--idle", type=float, default=60, help="minutes without a use (default 60)")
    g.set_defaults(fn=cmd_gc)
    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
