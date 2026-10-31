import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import probe  # noqa: E402

TS = """\
import { x } from './x'

/** Doc of the constant. */
export const LIMIT = 'a;b'

export function twice(n: number): number {
  return n * 2
}

export class Session {
  private items: string[] = []

  /** Closes it. */
  close(): void {
    this.items = []
  }

  async run(arg: {
    a: number
  }): Promise<void> {
    await Promise.resolve(arg)
  }
}

export type Pair = {
  a: string
  b: string
}
"""

C = """\
static int helper(int a) {
    return a;
}

/* Main entry. */
void xreadCommand(client *c,
                  int flags) {
    if (c) {
        helper(1);
    }
}
"""


class LookTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "src").mkdir()
        (self.root / "src" / "session.ts").write_text(TS)
        self.env = dict(os.environ, WAVES_ROOT=str(self.root / ".waves"))

    def tearDown(self):
        self.tmp.cleanup()

    def look(self, *specs):
        r = subprocess.run([sys.executable, str(HERE / "look.py"), "--root", str(self.root), *specs],
                           capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    def test_ts_declarations(self):
        out = self.look("ts:src:LIMIT,twice,Session.close,Session.run,Pair")
        self.assertIn("== src/session.ts:3-4", out)
        self.assertIn("== src/session.ts:6-8", out)
        self.assertIn("== src/session.ts:13-16", out)
        self.assertIn("== src/session.ts:18-22", out)
        self.assertIn("== src/session.ts:25-28", out)

    def test_missing_declaration_and_range(self):
        out = self.look("ts:src/session.ts:nope", "src/session.ts:6-7")
        self.assertIn("== nope: no declaration under src/session.ts", out)
        self.assertIn("6\texport function twice", out)
        self.assertIn("7\t  return n * 2", out)

    def test_rg_paths_are_relative(self):
        out = self.look("rg:src:items", "rgl:src:Session")
        self.assertIn("src/session.ts:11:", out)
        self.assertIn("src/session.ts:1", out)

    def test_c_functions(self):
        import look
        path = self.root / "t.c"
        path.write_text(C)
        out = "\n".join(look.do_c(str(path), ["xreadCommand", "helper"], 50, "redis@x:t.c"))
        self.assertIn("== redis@x:t.c:5-11", out)
        self.assertIn("== redis@x:t.c:1-3", out)


class ProbeTest(unittest.TestCase):
    def test_reply_end(self):
        cases = [
            (b"+OK\r\n", 5),
            (b"$3\r\nabc\r\n", 9),
            (b"$-1\r\n", 5),
            (b"*2\r\n$1\r\na\r\n:1\r\n", 15),
            (b"%1\r\n+k\r\n*1\r\n_\r\n", 15),
            (b"|1\r\n+a\r\n+b\r\n:1\r\n", 16),
            (b"*-1\r\n", 5),
        ]
        for buf, end in cases:
            self.assertEqual(probe.reply_end(buf + b"+next\r\n"), end, buf)
        for partial in (b"+OK", b"$3\r\nab", b"*2\r\n$1\r\na\r\n", b">3\r\n+a\r\n"):
            self.assertIsNone(probe.reply_end(partial), partial)

    def test_frame_and_escapes(self):
        self.assertEqual(probe.frame([probe.decode("ECHO", True), probe.decode("a\\r\\x00b", True)]),
                         b"*2\r\n$4\r\nECHO\r\n$4\r\na\r\x00b\r\n")
        self.assertEqual(probe.decode("a\\rb", False), b"a\\rb")

    def test_profiles(self):
        self.assertEqual(probe.resolve_profiles("redis-7.2@7.2.16,valkey-9.0"),
                         [("redis-7.2@7.2.16", "redis:7.2.16"), ("valkey-9.0", "valkey/valkey:9.0.0")])
        self.assertEqual(len(probe.resolve_profiles("all")), 7)
        with self.assertRaises(SystemExit):
            probe.resolve_profiles("redis-5.0")

    def test_one_off_containers(self):
        self.assertTrue(probe.is_shared("redis:7.2.4", ""))
        self.assertTrue(probe.is_shared("valkey/valkey:8.1.0", " "))
        self.assertFalse(probe.is_shared("redis:7.2.16", ""))
        self.assertFalse(probe.is_shared("redis:7.2.4", "--requirepass pw"))
        self.assertIsNone(probe.one_off_note(probe.resolve_profiles("redis-7.2,valkey-9.0"), "", "wf-a"))
        note = probe.one_off_note(probe.resolve_profiles("redis-7.2,redis-8.0"), "--cluster-enabled yes", "wf-a")
        self.assertIn("probe.py down -p redis-7.2,redis-8.0 --args '--cluster-enabled yes' --owner wf-a", note)
        self.assertIn("down -p redis-7.2@7.2.16 --owner wf-a)",
                      probe.one_off_note(probe.resolve_profiles("redis-7.2@7.2.16"), "", "wf-a"))

    def test_container_names(self):
        # The shared default keeps its owner-free name (running containers are found by it).
        self.assertEqual(probe.container_name("redis:7.2.4", "", "wf-a"), probe.container_name("redis:7.2.4", ""))
        self.assertEqual(probe.container_name("redis:7.2.4", " "), probe.container_name("redis:7.2.4", ""))
        a = probe.container_name("redis:7.2.4", "--requirepass pw", "wf-a")
        b = probe.container_name("redis:7.2.4", "--requirepass pw", "review-a")
        self.assertNotEqual(a, b)
        self.assertTrue(a.startswith("wave-probe-redis-7.2.4-wf-a-"), a)
        self.assertEqual(a, probe.container_name("redis:7.2.4", " --requirepass pw ", "wf-a"))

    def test_owner_token(self):
        self.assertEqual(probe.owner_token("WF_Lane/x"), "wf-lane-x")
        with tempfile.TemporaryDirectory() as d:
            sub = Path(d) / "My Tree"
            sub.mkdir()
            with mock.patch.dict(os.environ, {"WAVE_OWNER": ""}):
                self.assertEqual(probe.owner_token(None, cwd=str(sub)), "my-tree")
            with mock.patch.dict(os.environ, {"WAVE_OWNER": "research-x"}):
                self.assertEqual(probe.owner_token(None, cwd=str(sub)), "research-x")

    def test_idle_containers(self):
        started = {"a": 0, "b": 0, "c": 5000}
        self.assertEqual(probe.idle_containers(started, {"b": 4000}, 6000, 2500), ["a"])
        self.assertEqual(probe.idle_containers(started, {}, 6000, 1000), ["a", "b", "c"])

    def test_count_connections(self):
        text = """  sl  local_address rem_address   st tx_queue
   0: 00000000:18EB 00000000:0000 0A 00000000:00000000
   1: 020011AC:18EB 010011AC:D2F4 01 00000000:00000000
   2: 020011AC:18EB 010011AC:D2F6 06 00000000:00000000
   3: 020011AC:3C3B 010011AC:18EB 01 00000000:00000000
"""
        self.assertEqual(probe.count_connections(text), 1)
        self.assertEqual(probe.count_connections(""), 0)

if __name__ == "__main__":
    unittest.main()
