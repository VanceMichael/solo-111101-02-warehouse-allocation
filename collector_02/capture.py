import json
from .records import SourceRecord
from .store import RecordStore
from .adapter import decode
from .sources import FixtureTransport


def capture(root, route, database, observed_at):
    response = FixtureTransport(root).request(route)
    if response.status != 200:
        raise ValueError(f"source status {response.status}")
    values = decode(response.body)
    records = [SourceRecord.parse({"source": route, "observed_at": observed_at, "payload": value}) for value in values]
    store = RecordStore(database)
    store.initialize()
    with store.connect() as connection:
        before = connection.total_changes
        for record in records:
            connection.execute(
                "INSERT OR IGNORE INTO source_records(source,observed_at,fingerprint,payload) VALUES(?,?,?,?)",
                (record.source, record.observed_at, record.fingerprint(), json.dumps(record.payload, ensure_ascii=False, sort_keys=True)),
            )
        inserted = connection.total_changes - before
    return {"decoded": len(records), "inserted": inserted, "stored": store.count(), "route": route}
