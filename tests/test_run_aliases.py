import json
import tempfile
import unittest
from pathlib import Path

from collector_02.run import execute_run

from tests.test_run_idempotency import scenario, write_csv, write_fixture, WAREHOUSE


WAREHOUSE_TWO_SKU = (
    "warehouse,sku,physical,reserved,watermark\n"
    "ES-1,A-01,10,3,105\n"
    "ES-2,A-01,5,1,105\n"
    "ES-1,B-01,4,0,105\n"
    "ES-2,B-01,2,0,105\n"
)


class AliasMappingTests(unittest.TestCase):
    def test_explicit_alias_maps_claim_and_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "c.csv", (
                "channel,sku,quantity,watermark\n"
                "shop,S-A,4,105\n"
            ))
            manifest = scenario(root, ["/warehouse", "/channel"],
                                aliases={"S-A": "A-01"})
            result = execute_run(manifest, work / "state.sqlite3")
            item = result["inventory"][0]
            self.assertEqual(item["available"], 11)
            self.assertEqual(item["channel_claim"], 4)
            self.assertEqual(item["gap"], 7)

    def test_unknown_channel_sku_is_quarantined_without_fuzzy_match(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "c.csv", (
                "channel,sku,quantity,watermark\n"
                "shop,S-A,4,105\n"
                "shop,S-UNKNOWN,9,105\n"
            ))
            manifest = scenario(root, ["/warehouse", "/channel"],
                                aliases={"S-A": "A-01"})
            result = execute_run(manifest, work / "state.sqlite3")
            unknown = [item for item in result["quarantine"]
                       if item["reason"] == "unknown_sku"]
            self.assertEqual(len(unknown), 1)
            self.assertEqual(unknown[0]["sku"], "S-UNKNOWN")
            self.assertEqual(unknown[0]["detail"]["mapping"], "unconfirmed")
            # 未知 SKU 绝不制造可售量或额度：只有显式映射的 4 入账，9 被隔离。
            self.assertEqual(result["inventory"][0]["channel_claim"], 4)

    def test_two_channel_skus_explicitly_mapped_to_one_internal_sku_aggregate(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE_TWO_SKU)
            write_csv(root, "c.csv", (
                "channel,sku,quantity,watermark\n"
                "shop,S-A1,3,105\n"
                "shop,S-A2,2,105\n"
            ))
            # 两个规格编码显式映射到同一内部 SKU，额度相加，不会凭空多一批库存。
            manifest = scenario(root, ["/warehouse", "/channel"],
                                aliases={"S-A1": "A-01", "S-A2": "A-01"})
            result = execute_run(manifest, work / "state.sqlite3")
            a01 = next(item for item in result["inventory"] if item["sku"] == "A-01")
            self.assertEqual(a01["channel_claim"], 5)
            self.assertEqual(a01["available"], 11)
            self.assertEqual(a01["gap"], 6)

    def test_channel_claim_does_not_deduct_available_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "c.csv", "channel,sku,quantity,watermark\nshop,S-A,4,105\n")
            manifest = scenario(root, ["/warehouse", "/channel"], aliases={"S-A": "A-01"})
            result = execute_run(manifest, work / "state.sqlite3")
            # available 仍是物理差额 11，而不是 11-4。
            self.assertEqual(result["inventory"][0]["available"], 11)

    def test_duplicate_channel_delivery_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "c.csv", "channel,sku,quantity,watermark\nshop,S-A,4,105\n")
            manifest = scenario(root, ["/warehouse", "/channel", "/channel"],
                                aliases={"S-A": "A-01"})
            first = execute_run(manifest, work / "state.sqlite3")
            second = execute_run(manifest, work / "state.sqlite3")
            self.assertEqual(first["inventory"], second["inventory"])
            self.assertEqual(second["inventory"][0]["channel_claim"], 4)

    def test_mapped_sku_without_warehouse_row_is_flagged_not_zeroed(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            root = write_fixture(work, {
                "/warehouse": [{"status": 200, "file": "w.csv"}],
                "/channel": [{"status": 200, "file": "c.csv"}],
            })
            write_csv(root, "w.csv", WAREHOUSE)
            write_csv(root, "c.csv", "channel,sku,quantity,watermark\nshop,S-B,3,105\n")
            manifest = scenario(root, ["/warehouse", "/channel"], aliases={"S-B": "B-01"})
            result = execute_run(manifest, work / "state.sqlite3")
            reasons = {item["reason"] for item in result["quarantine"]}
            self.assertIn("inventory_missing", reasons)
            self.assertNotIn("B-01", {item["sku"] for item in result["inventory"]})


if __name__ == "__main__":
    unittest.main()
