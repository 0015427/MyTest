import contextlib
import io
import json
import os
import shutil
import unittest
import uuid

from MySQLScriptV4 import cli

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXAMPLE = os.path.join(ROOT, "examples", "ecommerce.py")


class CliTest(unittest.TestCase):
    def setUp(self):
        self.dir = os.path.join(os.getcwd(), ".df_cli_" + uuid.uuid4().hex[:8])
        os.makedirs(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def call(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_plan_json(self):
        code, out, err = self.call(["plan", EXAMPLE, "--rows", "50", "--json"])
        self.assertEqual(code, 0, err + out)
        payload = json.loads(out)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["root"], "users")
        self.assertEqual(len(payload["tables"]), 3)
        self.assertEqual(payload["tables"][1]["parent"], "users")
        self.assertTrue(payload["samples"]["orders"])

    def test_plan_text(self):
        code, out, err = self.call(["plan", EXAMPLE, "--rows", "50", "--explain"])
        self.assertEqual(code, 0, err + out)
        self.assertIn("sample orders", out)
        self.assertIn("auto: users.id", out)

    def test_plan_defaults_to_plan_command(self):
        code, out, err = self.call([EXAMPLE, "--rows", "20"])
        self.assertEqual(code, 0, err + out)
        self.assertIn("root", out)

    def test_bad_spec_returns_2(self):
        path = os.path.join(self.dir, "bad.py")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(
                "from MySQLScriptV4 import Table\n"
                "TABLES = [Table('t', 'CREATE TABLE t (id INT NOT NULL, v TEXT, "
                "PRIMARY KEY (id))', rows=10, fields={'id': lambda r: r.i})]\n"
            )
        code, out, err = self.call(["plan", path, "--json"])
        self.assertEqual(code, 2)
        payload = json.loads(out)
        self.assertFalse(payload["ok"])
        self.assertTrue(any("缺少列" in p["message"] for p in payload["problems"]))

    def test_dry_run(self):
        code, out, err = self.call(["run", EXAMPLE, "--rows", "50", "--dry-run", "--no-progress"])
        self.assertEqual(code, 0, err + out)
        self.assertIn("result: ok", out)

    def test_run_requires_dsn(self):
        code, out, err = self.call(["run", EXAMPLE])
        self.assertEqual(code, 2)
        self.assertIn("--dsn", err)


if __name__ == "__main__":
    unittest.main()
