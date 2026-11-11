import unittest
from collector_02.adapter import decode


class StockExportTests(unittest.TestCase):
    def test_bom_and_quoted_sku(self):
        rows = decode(('\ufeffsku,quantity\n"A,01",10\n').encode())
        self.assertEqual(rows, [{"sku": "A,01", "quantity": "10"}])

    def test_truncated_partition_row_is_rejected(self):
        with self.assertRaises(ValueError):
            decode(b"warehouse,sku,quantity\nES-1,A\n")

    def test_duplicate_header_rejected(self):
        with self.assertRaises(ValueError):
            decode(b"sku,sku\nA,B\n")
