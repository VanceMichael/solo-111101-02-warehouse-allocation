import json
import tempfile
import unittest
from pathlib import Path

from collector_02.run import execute_run

from tests.test_run_idempotency import scenario, write_csv, write_fixture

WAREHOUSE = (
    "warehouse,sku,physical,reserved,watermark\n"
    "ES-1,A-01,10,3,105\n"
    "ES-2,A-01,5,1,105\n"
)
DELTA_HEADER = ("kind,warehouse,sku,delta_physical,delta_reserved,"
                "base_watermark,event_watermark,event_id\n")


class WatermarkAndCoverageTests(unittest.TestCase):
    def test_stale_lower_watermark_never_replaces_confirmed_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/stale": [{"status": 200, "file": "s.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "s.csv",
                      "warehouse,sku,physical,reserved,watermark\nES-1,A-01,99,0,104\n")
            manifest = scenario(root, ["/warehouse", "/stale"])
            result = execute_run(manifest, work / "state.sqlite3")
            self.assertEqual(result["watermark"], 105)
            self.assertTrue(result["published"])
            self.assertEqual(result["inventory"][0]["available"], 11)
            self.assertIn("stale_watermark",
                          {item["reason"] for item in result["ignored"]})

    def test_partial_watermark_cannot_be_stitched_and_keeps_no_zero_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
            })
            # 只有 ES-1 一个分区，预期两个分区。
            write_csv(root, "w.csv",
                      "warehouse,sku,physical,reserved,watermark\nES-1,A-01,10,3,105\n")
            manifest = scenario(root, ["/warehouse"], partitions=["ES-1", "ES-2"])
            result = execute_run(manifest, work / "state.sqlite3")
            self.assertEqual(result["coverage"], "partial")
            self.assertFalse(result["published"])
            self.assertIsNone(result["watermark"])
            self.assertEqual(result["inventory"], [])
            self.assertIn("incomplete_partitions",
                          {item["reason"] for item in result["ignored"]})

    def test_503_partition_does_not_clear_previously_published_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/missing": [{"status": 503}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            first_manifest = scenario(root, ["/warehouse"], run_id="run-keep")
            first = execute_run(first_manifest, work / "state.sqlite3")
            self.assertTrue(first["published"])
            second_manifest = scenario(root, ["/warehouse", "/missing"], run_id="run-keep")
            second = execute_run(second_manifest, work / "state.sqlite3")
            self.assertEqual(second["coverage"], "full")
            self.assertTrue(second["published"])
            self.assertEqual(second["inventory"][0]["available"], 11)
            self.assertIn("source_unavailable",
                          {item["reason"] for item in second["ignored"]})

    def test_backfilling_missing_partition_completes_reconciliation_without_manual_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/w1": [{"status": 200, "file": "w1.csv"}],
                "/missing": [{"status": 503}],
                "/w3": [{"status": 200, "file": "w3.csv"}],
            })
            write_csv(root, "w1.csv", (
                "warehouse,sku,physical,reserved,watermark\n"
                "ES-1,A-01,10,3,105\n"
                "ES-2,A-01,5,1,105\n"
            ))
            write_csv(root, "w3.csv",
                      "warehouse,sku,physical,reserved,watermark\nES-3,A-01,2,0,105\n")
            partial = scenario(root, ["/w1", "/missing"],
                               partitions=["ES-1", "ES-2", "ES-3"], run_id="run-bf")
            first = execute_run(partial, work / "state.sqlite3")
            self.assertFalse(first["published"])
            # 补采缺失分区后，同一状态库、同一 run 自动完成对账。
            backfilled = scenario(root, ["/w1", "/missing", "/w3"],
                                  partitions=["ES-1", "ES-2", "ES-3"], run_id="run-bf")
            second = execute_run(backfilled, work / "state.sqlite3")
            self.assertTrue(second["published"])
            self.assertEqual(second["coverage"], "full")
            self.assertEqual(second["watermark"], 105)
            self.assertEqual(second["inventory"][0]["available"], 13)
            self.assertEqual(second["notifications"], 0)

    def test_negative_quantity_and_reserved_exceeds_physical_are_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/bad": [{"status": 200, "file": "bad.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "bad.csv", (
                "warehouse,sku,physical,reserved,watermark\n"
                "ES-1,A-01,2,3,106\n"
                "ES-2,A-01,-1,0,106\n"
            ))
            result = execute_run(
                scenario(root, ["/warehouse", "/bad"], run_id="run-neg"),
                work / "state.sqlite3",
            )
            reasons = {item["reason"] for item in result["quarantine"]}
            self.assertIn("reserved_exceeds_physical", reasons)
            self.assertIn("negative_quantity", reasons)
            self.assertEqual(result["watermark"], 105)
            self.assertEqual(result["inventory"][0]["available"], 11)

    def test_negative_delta_is_legal_but_out_of_bounds_result_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/ok": [{"status": 200, "file": "ok.csv"}],
                "/bad": [{"status": 200, "file": "bad.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            # ES-1 physical=10：先 -2 合法到 8；再 -99 越界，隔离且不应用。
            write_csv(root, "ok.csv",
                      DELTA_HEADER + "delta,ES-1,A-01,-2,0,105,106,ADJ-OK\n")
            write_csv(root, "bad.csv",
                      DELTA_HEADER + "delta,ES-1,A-01,-99,0,105,107,ADJ-BAD\n")
            result = execute_run(
                scenario(root, ["/warehouse", "/ok", "/bad"], run_id="run-nd"),
                work / "state.sqlite3",
            )
            self.assertIn("delta_out_of_bounds",
                          {item["reason"] for item in result["quarantine"]})
            # ES-1: 8-3=5, ES-2: 5-1=4 => 9
            self.assertEqual(result["inventory"][0]["available"], 9)
            self.assertEqual(result["delta_watermark"], 106)

    def test_delta_without_complete_base_snapshot_is_held_for_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/w1": [{"status": 200, "file": "w1.csv"}],
                "/delta": [{"status": 200, "file": "d.csv"}],
                "/w3": [{"status": 200, "file": "w3.csv"}],
            })
            write_csv(root, "w1.csv", (
                "warehouse,sku,physical,reserved,watermark\n"
                "ES-1,A-01,10,3,105\n"
                "ES-2,A-01,5,1,105\n"
            ))
            write_csv(root, "w3.csv",
                      "warehouse,sku,physical,reserved,watermark\nES-3,A-01,2,0,105\n")
            write_csv(root, "d.csv",
                      DELTA_HEADER + "delta,ES-1,A-01,-2,0,105,106,ADJ-1\n")
            before = execute_run(
                scenario(root, ["/w1", "/delta"],
                         partitions=["ES-1", "ES-2", "ES-3"], run_id="run-base"),
                work / "state.sqlite3",
            )
            self.assertFalse(before["published"])
            self.assertIn("delta_base_missing",
                          {item["reason"] for item in before["quarantine"]})
            self.assertIsNone(before["delta_watermark"])
            after = execute_run(
                scenario(root, ["/w1", "/delta", "/w3"],
                         partitions=["ES-1", "ES-2", "ES-3"], run_id="run-base"),
                work / "state.sqlite3",
            )
            self.assertTrue(after["published"])
            # ES-1 8-3=5, ES-2 4, ES-3 2 => 11
            self.assertEqual(after["inventory"][0]["available"], 11)
            self.assertEqual(after["delta_watermark"], 106)


if __name__ == "__main__":
    unittest.main()
