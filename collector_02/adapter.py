import csv
import io


def decode(content):
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))
    if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
        raise ValueError("missing or duplicate column")
    rows = list(reader)
    if any(None in row or None in row.values() for row in rows):
        raise ValueError("incomplete row")
    return rows
