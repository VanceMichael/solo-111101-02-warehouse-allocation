import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from collector_02.run import execute_run

ROOT = Path(__file__).resolve().parents[1]


def write_fixture(root, routes):
    root = Path(root)
    (root / "routes.json").write_text(json.dumps(routes, ensure_ascii=False))
    return root


def write_csv(root, name, content):
    Path(root, name).write_text(content, encoding="utf-8")


def scenario(root, sources, aliases=None, partitions=None, run_id="run-x", name="x"):
    return {
        "name": name,
        "run_id": run_id,
        "sources": sources,
        "options": {
            "aliases": aliases or {},
            "expected_partitions": partitions or ["ES-1", "ES-2"],
        },
        "fixture_root": str(root),
    }


def run_once(manifest, database):
    return execute_run(manifest, str(database))


def run_in_subprocess(manifest_path, database, output):
    bootstrap = (
        "import sys; sys.path.insert(0, %r); "
        "from collector_02.cli import main; raise SystemExit(main())"
    ) % str(ROOT)
    return subprocess.run(
        [sys.executable, "-I", "-c", bootstrap, "--run", str(manifest_path),
         "--database", str(database), "--output", str(output)],
        cwd=ROOT, text=True, capture_output=True, timeout=10,
    )


WAREHOUSE = (
    "warehouse,sku,physical,reserved,watermark\n"
    "ES-1,A-01,10,3,105\n"
    "ES-2,A-01,5,1,105\n"
)


class IdempotencyTests(unittest.TestCase):
    def test_duplicate_import_and_restart_keep_single_projection(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {"/warehouse": [{"status": 200, "file": "w.csv"}]})
            write_csv(root, "w.csv", WAREHOUSE)
            manifest = scenario(root, ["/warehouse", "/warehouse"])
            first = run_once(manifest, work / "state.sqlite3")
            second = run_once(manifest, work / "state.sqlite3")
            self.assertEqual(first["inventory"], second["inventory"])
            self.assertEqual(first["inventory"][0]["available"], 11)
            self.assertEqual(second["notifications"], 0)
            # 重复抓取同一观测不会产生第二份快照行。
            import sqlite3
            with sqlite3.connect(work / "state.sqlite3") as connection:
                total = connection.execute("SELECT count(*) FROM snapshot_rows").fetchone()[0]
            self.assertEqual(total, 2)

    def test_delta_redelivery_and_restart_apply_once(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/delta": [{"status": 200, "file": "d.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "d.csv", (
                "kind,warehouse,sku,delta_physical,delta_reserved,base_watermark,event_watermark,event_id\n"
                "delta,ES-1,A-01,-2,0,105,106,ADJ-1\n"
            ))
            manifest = scenario(root, ["/warehouse", "/delta", "/delta"], run_id="run-d")
            first = run_once(manifest, work / "state.sqlite3")
            self.assertEqual(first["inventory"][0]["available"], 9)
            self.assertEqual(first["delta_watermark"], 106)
            # 独立进程重启后再次重投 ADJ-1，只能扣减一次。
            manifest_path = work / "scenario.json"
            manifest_path.write_text(json.dumps(manifest))
            output = work / "result.json"
            restarted = run_in_subprocess(manifest_path, work / "state.sqlite3", output)
            self.assertEqual(restarted.returncode, 0, restarted.stderr)
            second = json.loads(output.read_text())
            self.assertEqual(second["inventory"][0]["available"], 9)
            self.assertEqual(second["notifications"], 0)
            import sqlite3
            with sqlite3.connect(work / "state.sqlite3") as connection:
                applied = connection.execute(
                    "SELECT count(*) FROM delta_events WHERE state='applied'"
                ).fetchone()[0]
                stored = connection.execute("SELECT count(*) FROM delta_events").fetchone()[0]
            self.assertEqual(applied, 1)
            self.assertEqual(stored, 1)

    def test_refetch_base_snapshot_does_not_erase_applied_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/delta": [{"status": 200, "file": "d.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "d.csv", (
                "kind,warehouse,sku,delta_physical,delta_reserved,base_watermark,event_watermark,event_id\n"
                "delta,ES-1,A-01,-2,0,105,106,ADJ-1\n"
            ))
            manifest = scenario(root, ["/warehouse", "/delta"], run_id="run-r")
            run_once(manifest, work / "state.sqlite3")
            again = scenario(root, ["/warehouse", "/delta", "/warehouse"], run_id="run-r")
            result = run_once(again, work / "state.sqlite3")
            self.assertEqual(result["inventory"][0]["available"], 9)

    def test_same_event_id_different_content_is_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/d1": [{"status": 200, "file": "d1.csv"}],
                "/d2": [{"status": 200, "file": "d2.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            header = ("kind,warehouse,sku,delta_physical,delta_reserved,"
                      "base_watermark,event_watermark,event_id\n")
            write_csv(root, "d1.csv", header + "delta,ES-1,A-01,-2,0,105,106,ADJ-9\n")
            write_csv(root, "d2.csv", header + "delta,ES-1,A-01,-5,0,105,107,ADJ-9\n")
            manifest = scenario(root, ["/warehouse", "/d1", "/d2"], run_id="run-c")
            result = run_once(manifest, work / "state.sqlite3")
            reasons = {item["reason"] for item in result["quarantine"]}
            self.assertIn("event_conflict", reasons)
            # 只采用首个事件内容：10-2=8，另一份 -5 被隔离不叠加。
            self.assertEqual(result["inventory"][0]["available"], 9)

    def test_snapshot_quantity_conflict_at_same_watermark_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/w1": [{"status": 200, "file": "w1.csv"}],
                "/w2": [{"status": 200, "file": "w2.csv"}],
            })
            write_csv(root, "w1.csv", WAREHOUSE)
            write_csv(root, "w2.csv", (
                "warehouse,sku,physical,reserved,watermark\n"
                "ES-1,A-01,99,0,105\n"
                "ES-2,A-01,5,1,105\n"
            ))
            manifest = scenario(root, ["/w1", "/w2"], run_id="run-sc")
            result = run_once(manifest, work / "state.sqlite3")
            self.assertIn("snapshot_conflict",
                          {item["reason"] for item in result["quarantine"]})
            # 已确认行不被后到的不同数量替代。
            self.assertEqual(result["inventory"][0]["available"], 11)


if __name__ == "__main__":
    unittest.main()
