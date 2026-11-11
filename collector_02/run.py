"""--run 工作流：按渠道水位采集快照与增量事件并输出可解释对账结果。

设计要点：
- 原始采集事实（快照行、渠道额度、增量事件、抓取证据）全部持久化、按业务键去重；
- 业务投影每次运行都从持久事实重建，因此重复投递、进程重启、重抓基准快照
  都不会重复扣加库存；
- 不完整水位、迟到旧快照、越界/冲突/未知 SKU 一律进入 ignored 或 quarantine，
  绝不静默制造可售量，也不把未知差异当作零。
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .adapter import decode
from .sources import FixtureTransport
from .store import RunStore

SNAPSHOT_COLUMNS = ("warehouse", "sku", "physical", "reserved", "watermark")
CHANNEL_COLUMNS = ("channel", "sku", "quantity", "watermark")
DELTA_COLUMNS = (
    "kind", "warehouse", "sku", "delta_physical", "delta_reserved",
    "base_watermark", "event_watermark", "event_id",
)


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sha256_hex(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_int(value):
    text = str(value).strip()
    if not text:
        raise ValueError("empty integer")
    return int(text)


def classify(fieldnames):
    names = set(fieldnames or [])
    if "kind" in names:
        return "delta"
    if "channel" in names:
        return "channel"
    if "warehouse" in names and "physical" in names:
        return "warehouse"
    return "unknown"


class RunError(ValueError):
    pass


def execute_run(manifest, database_path):
    if not isinstance(manifest, dict):
        raise RunError("场景文件必须是 JSON 对象")
    run_id = str(manifest.get("run_id", "")).strip()
    if not run_id:
        raise RunError("场景缺少 run_id")
    sources = manifest.get("sources")
    if not isinstance(sources, list) or not sources or not all(isinstance(s, str) for s in sources):
        raise RunError("场景 sources 必须是非空字符串数组")
    options = manifest.get("options") or {}
    aliases = options.get("aliases") or {}
    if not isinstance(aliases, dict):
        raise RunError("options.aliases 必须是对象")
    expected_partitions = options.get("expected_partitions") or []
    if not isinstance(expected_partitions, list):
        raise RunError("options.expected_partitions 必须是数组")
    fixture_root = manifest.get("fixture_root")
    if not fixture_root:
        raise RunError("场景缺少 fixture_root")
    root_path = Path(fixture_root)
    if not root_path.is_absolute():
        root_path = Path.cwd() / root_path

    store = RunStore(database_path)
    store.initialize()
    connection = store.connect()
    try:
        connection.execute(
            "INSERT OR IGNORE INTO runs(run_id, name, created_at) VALUES(?,?,?)",
            (run_id, manifest.get("name"), utc_now()),
        )
        before_notifications = connection.execute(
            "SELECT count(*) AS total FROM notifications WHERE run_id=?", (run_id,)
        ).fetchone()["total"]
        transport = FixtureTransport(root_path)
        for source in sources:
            _ingest_source(connection, run_id, transport, source, aliases)
        result = _reconcile(connection, run_id, expected_partitions)
        after_notifications = connection.execute(
            "SELECT count(*) AS total FROM notifications WHERE run_id=?", (run_id,)
        ).fetchone()["total"]
        result["notifications"] = after_notifications - before_notifications
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _next_attempt(connection, run_id, source):
    row = connection.execute(
        "SELECT coalesce(max(attempt), 0) + 1 AS attempt FROM fetches WHERE run_id=? AND source=?",
        (run_id, source),
    ).fetchone()
    return row["attempt"]


def _add_finding(connection, run_id, bucket, origin, reason, raw, *,
                 sku=None, source=None, watermark=None, detail=None, notify=False):
    detail = detail or {}
    raw_text = json.dumps(raw, ensure_ascii=False, sort_keys=True) if raw is not None else None
    identity = json.dumps(
        {"raw": raw_text, "detail": detail}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    dedupe_key = "|".join([
        run_id, bucket, origin, reason, str(sku or ""), str(source or ""),
        str(watermark if watermark is not None else ""), sha256_hex(identity),
    ])
    cursor = connection.execute(
        """INSERT OR IGNORE INTO findings
           (run_id, bucket, origin, reason, sku, source, watermark, raw, detail, dedupe_key, created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (run_id, bucket, origin, reason, sku, source,
         watermark, raw_text, json.dumps(detail, ensure_ascii=False, sort_keys=True),
         dedupe_key, utc_now()),
    )
    created = cursor.rowcount > 0
    if created and notify:
        # 通知按稳定业务键去重：重跑（attempt 递增、投影重建）不会重复通知。
        stable_key = "|".join([
            "stable", run_id, bucket, reason, str(sku or ""), str(source or ""),
            str(watermark if watermark is not None else ""),
        ])
        _notify(connection, run_id, "quarantine", reason, sku,
                {"source": source, "watermark": watermark, "detail": detail},
                stable_key)
    return created


def _notify(connection, run_id, kind, reason, sku, detail, dedupe_key):
    cursor = connection.execute(
        """INSERT OR IGNORE INTO notifications
           (run_id, kind, reason, sku, detail, dedupe_key, status, created_at)
           VALUES(?,?,?,?,?,?,'pending',?)""",
        (run_id, kind, reason, sku,
         json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
         dedupe_key, utc_now()),
    )
    return cursor.rowcount > 0


def _record_fetch(connection, run_id, source, attempt, response, decoded_rows, kind):
    body_sha = sha256_hex(response.body.decode("utf-8", errors="replace")) if response.body else None
    connection.execute(
        """INSERT OR IGNORE INTO fetches
           (run_id, source, attempt, status, etag, body_sha256, decoded_rows, created_at)
           VALUES(?,?,?,?,?,?,?,?)""",
        (run_id, source, attempt, response.status,
         response.headers.get("ETag"), body_sha, decoded_rows, utc_now()),
    )
    return body_sha


def _ingest_source(connection, run_id, transport, source, aliases):
    response = transport.request(source)
    attempt = _next_attempt(connection, run_id, source)
    if response.status != 200:
        _record_fetch(connection, run_id, source, attempt, response, 0, "unavailable")
        _add_finding(
            connection, run_id, "ignored", "ingestion", "source_unavailable", None,
            source=source, detail={"status": response.status, "attempt": attempt},
        )
        return
    try:
        rows = decode(response.body)
    except ValueError as error:
        _record_fetch(connection, run_id, source, attempt, response, 0, "malformed")
        _add_finding(
            connection, run_id, "quarantine", "ingestion", "malformed_source", None,
            source=source, detail={"error": str(error), "attempt": attempt}, notify=True,
        )
        return
    kind = classify(rows[0].keys() if rows else [])
    _record_fetch(connection, run_id, source, attempt, response, len(rows), kind)
    if kind == "warehouse":
        _ingest_snapshot(connection, run_id, source, rows)
    elif kind == "channel":
        _ingest_channel(connection, run_id, source, rows, aliases)
    elif kind == "delta":
        _ingest_delta(connection, run_id, source, rows)
    else:
        _add_finding(
            connection, run_id, "quarantine", "ingestion", "malformed_source", None,
            source=source, detail={"error": "unrecognized csv columns", "attempt": attempt},
            notify=True,
        )


def _missing_columns(row, required):
    return [column for column in required if column not in row]


def _ingest_snapshot(connection, run_id, source, rows):
    for raw in rows:
        missing = _missing_columns(raw, SNAPSHOT_COLUMNS)
        if missing:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         source=source, detail={"missing_columns": missing}, notify=True)
            continue
        sku = raw["sku"].strip()
        warehouse = raw["warehouse"].strip()
        try:
            physical = parse_int(raw["physical"])
            reserved = parse_int(raw["reserved"])
            watermark = parse_int(raw["watermark"])
        except ValueError as error:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         sku=sku, source=source, detail={"error": str(error)}, notify=True)
            continue
        if physical < 0 or reserved < 0:
            _add_finding(connection, run_id, "quarantine", "ingestion", "negative_quantity", raw,
                         sku=sku, source=source, watermark=watermark,
                         detail={"physical": physical, "reserved": reserved}, notify=True)
            continue
        if reserved > physical:
            _add_finding(connection, run_id, "quarantine", "ingestion", "reserved_exceeds_physical",
                         raw, sku=sku, source=source, watermark=watermark,
                         detail={"physical": physical, "reserved": reserved}, notify=True)
            continue
        content_sha = sha256_hex(json.dumps(
            {"physical": physical, "reserved": reserved}, sort_keys=True, separators=(",", ":")))
        try:
            connection.execute(
                """INSERT INTO snapshot_rows
                   (run_id, warehouse, sku, watermark, physical, reserved, source, content_sha)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (run_id, warehouse, sku, watermark, physical, reserved, source, content_sha),
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                """SELECT physical, reserved FROM snapshot_rows
                   WHERE run_id=? AND warehouse=? AND sku=? AND watermark=?""",
                (run_id, warehouse, sku, watermark),
            ).fetchone()
            if existing and (existing["physical"], existing["reserved"]) != (physical, reserved):
                _add_finding(
                    connection, run_id, "quarantine", "ingestion", "snapshot_conflict", raw,
                    sku=sku, source=source, watermark=watermark,
                    detail={
                        "warehouse": warehouse,
                        "accepted": {"physical": existing["physical"], "reserved": existing["reserved"]},
                        "rejected": {"physical": physical, "reserved": reserved},
                    },
                    notify=True,
                )


def _ingest_channel(connection, run_id, source, rows, aliases):
    for raw in rows:
        missing = _missing_columns(raw, CHANNEL_COLUMNS)
        if missing:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         source=source, detail={"missing_columns": missing}, notify=True)
            continue
        channel = raw["channel"].strip()
        channel_sku = raw["sku"].strip()
        try:
            quantity = parse_int(raw["quantity"])
            watermark = parse_int(raw["watermark"])
        except ValueError as error:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         sku=channel_sku, source=source, detail={"error": str(error)}, notify=True)
            continue
        if quantity < 0:
            _add_finding(connection, run_id, "quarantine", "ingestion", "negative_quantity", raw,
                         sku=channel_sku, source=source, watermark=watermark,
                         detail={"quantity": quantity}, notify=True)
            continue
        internal_sku = aliases.get(channel_sku)
        if not internal_sku:
            _add_finding(
                connection, run_id, "quarantine", "ingestion", "unknown_sku", raw,
                sku=channel_sku, source=source, watermark=watermark,
                detail={"channel": channel, "mapping": "unconfirmed", "quantity": quantity},
                notify=True,
            )
            continue
        content_sha = sha256_hex(json.dumps(
            {"internal_sku": internal_sku, "quantity": quantity},
            sort_keys=True, separators=(",", ":")))
        try:
            connection.execute(
                """INSERT INTO channel_rows
                   (run_id, channel, channel_sku, internal_sku, quantity, watermark, source, content_sha)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (run_id, channel, channel_sku, internal_sku, quantity,
                 watermark, source, content_sha),
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                """SELECT quantity, internal_sku FROM channel_rows
                   WHERE run_id=? AND channel=? AND channel_sku=? AND watermark=?""",
                (run_id, channel, channel_sku, watermark),
            ).fetchone()
            if existing and existing["quantity"] != quantity:
                _add_finding(
                    connection, run_id, "quarantine", "ingestion", "channel_conflict", raw,
                    sku=channel_sku, source=source, watermark=watermark,
                    detail={
                        "channel": channel,
                        "accepted_quantity": existing["quantity"],
                        "rejected_quantity": quantity,
                        "internal_sku": existing["internal_sku"],
                    },
                    notify=True,
                )


def _ingest_delta(connection, run_id, source, rows):
    for raw in rows:
        missing = _missing_columns(raw, DELTA_COLUMNS)
        if missing:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         source=source, detail={"missing_columns": missing}, notify=True)
            continue
        event_id = raw["event_id"].strip()
        sku = raw["sku"].strip()
        warehouse = raw["warehouse"].strip()
        if not event_id:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         sku=sku, source=source, detail={"error": "empty event_id"}, notify=True)
            continue
        try:
            delta_physical = parse_int(raw["delta_physical"])
            delta_reserved = parse_int(raw["delta_reserved"])
            base_watermark = parse_int(raw["base_watermark"])
            event_watermark = parse_int(raw["event_watermark"])
        except ValueError as error:
            _add_finding(connection, run_id, "quarantine", "ingestion", "invalid_row", raw,
                         sku=sku, source=source, detail={"error": str(error), "event_id": event_id},
                         notify=True)
            continue
        content_sha = sha256_hex(json.dumps({
            "warehouse": warehouse, "sku": sku,
            "delta_physical": delta_physical, "delta_reserved": delta_reserved,
            "base_watermark": base_watermark, "event_watermark": event_watermark,
        }, sort_keys=True, separators=(",", ":")))
        try:
            connection.execute(
                """INSERT INTO delta_events
                   (run_id, event_id, warehouse, sku, base_watermark, event_watermark,
                    delta_physical, delta_reserved, source, content_sha, state)
                   VALUES(?,?,?,?,?,?,?,?,?,?, 'pending')""",
                (run_id, event_id, warehouse, sku, base_watermark, event_watermark,
                 delta_physical, delta_reserved, source, content_sha),
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                "SELECT * FROM delta_events WHERE run_id=? AND event_id=?",
                (run_id, event_id),
            ).fetchone()
            same = existing and (
                existing["content_sha"] == content_sha
                or (
                    existing["warehouse"] == warehouse and existing["sku"] == sku
                    and existing["base_watermark"] == base_watermark
                    and existing["event_watermark"] == event_watermark
                    and existing["delta_physical"] == delta_physical
                    and existing["delta_reserved"] == delta_reserved
                )
            )
            if not same:
                _add_finding(
                    connection, run_id, "quarantine", "ingestion", "event_conflict", raw,
                    sku=sku, source=source, watermark=event_watermark,
                    detail={
                        "event_id": event_id,
                        "accepted": {
                            "warehouse": existing["warehouse"], "sku": existing["sku"],
                            "base_watermark": existing["base_watermark"],
                            "event_watermark": existing["event_watermark"],
                            "delta_physical": existing["delta_physical"],
                            "delta_reserved": existing["delta_reserved"],
                        },
                        "rejected": {
                            "warehouse": warehouse, "sku": sku,
                            "base_watermark": base_watermark,
                            "event_watermark": event_watermark,
                            "delta_physical": delta_physical,
                            "delta_reserved": delta_reserved,
                        },
                    },
                    notify=True,
                )
            # 相同 event_id 相同内容：重复投递，天然幂等，不重复应用。


def _reconcile(connection, run_id, expected_partitions):
    expected = set(expected_partitions)
    # 投影类裁定每次重建；采集期裁定（原始行问题）持久保留。
    connection.execute(
        "DELETE FROM findings WHERE run_id=? AND origin='projection'", (run_id,)
    )

    snapshot_rows = connection.execute(
        "SELECT * FROM snapshot_rows WHERE run_id=? ORDER BY watermark, warehouse, sku",
        (run_id,),
    ).fetchall()

    partitions_by_wm = {}
    rows_by_wm = {}
    for row in snapshot_rows:
        partitions_by_wm.setdefault(row["watermark"], set()).add(row["warehouse"])
        rows_by_wm.setdefault(row["watermark"], []).append(row)

    complete_watermarks = set()
    for watermark, partitions in partitions_by_wm.items():
        if expected and expected.issubset(partitions):
            complete_watermarks.add(watermark)
    published_wm = max(complete_watermarks) if complete_watermarks else None

    # 较低水位行不替代已确认行；不完整水位给出补采依据。
    if published_wm is not None:
        for row in snapshot_rows:
            if row["watermark"] < published_wm:
                _add_finding(
                    connection, run_id, "ignored", "projection", "stale_watermark",
                    _snapshot_raw(row), sku=row["sku"], source=row["source"],
                    watermark=row["watermark"],
                    detail={
                        "warehouse": row["warehouse"],
                        "selected_watermark": published_wm,
                        "basis": "lower watermark cannot replace confirmed snapshot",
                    },
                )
    for watermark, partitions in sorted(partitions_by_wm.items()):
        missing = sorted(expected - partitions)
        if missing:
            _add_finding(
                connection, run_id, "ignored", "projection", "incomplete_partitions", None,
                source=",".join(sorted(r["source"] for r in rows_by_wm[watermark])),
                watermark=watermark,
                detail={
                    "present_partitions": sorted(partitions),
                    "missing_partitions": missing,
                    "basis": "partitions of one watermark cannot be stitched into a full snapshot",
                },
            )

    applied, delta_watermark = _apply_deltas(
        connection, run_id, complete_watermarks, rows_by_wm
    )

    # 渠道已展示额度：显式别名映射后按内部 SKU 汇总，不从 available 再扣。
    claim_rows = connection.execute(
        "SELECT * FROM channel_rows WHERE run_id=?", (run_id,)
    ).fetchall()
    claims = {}
    for row in claim_rows:
        claims[row["internal_sku"]] = claims.get(row["internal_sku"], 0) + row["quantity"]

    inventory = []
    if published_wm is not None:
        warehouse_totals = {}
        for row in rows_by_wm[published_wm]:
            key = (row["warehouse"], row["sku"])
            if key in applied:
                physical, reserved = applied[key]
            else:
                physical, reserved = row["physical"], row["reserved"]
            entry = warehouse_totals.setdefault(
                row["sku"], {"physical": 0, "reserved": 0}
            )
            entry["physical"] += physical
            entry["reserved"] += reserved
        for sku in sorted(warehouse_totals):
            available = warehouse_totals[sku]["physical"] - warehouse_totals[sku]["reserved"]
            claim = claims.get(sku, 0)
            inventory.append({
                "sku": sku,
                "available": available,
                "channel_claim": claim,
                "gap": available - claim,
            })
        for sku in sorted(set(claims) - set(warehouse_totals)):
            _add_finding(
                connection, run_id, "quarantine", "projection", "inventory_missing", None,
                sku=sku, watermark=published_wm,
                detail={"channel_claim": claims[sku],
                        "basis": "mapped channel sku has no confirmed warehouse row"},
                notify=True,
            )

    coverage = "full" if published_wm is not None else "partial"
    published = published_wm is not None
    if not published:
        _notify(connection, run_id, "coverage", "partial_coverage", None,
                {"expected_partitions": sorted(expected),
                 "complete_watermarks": sorted(complete_watermarks)},
                f"{run_id}|coverage|partial")

    findings = connection.execute(
        "SELECT * FROM findings WHERE run_id=? ORDER BY bucket, reason, sku, source, watermark, id",
        (run_id,),
    ).fetchall()
    ignored, quarantine = [], []
    for finding in findings:
        item = {
            "reason": finding["reason"],
            "sku": finding["sku"],
            "source": finding["source"],
            "watermark": finding["watermark"],
            "origin": finding["origin"],
            "raw": json.loads(finding["raw"]) if finding["raw"] else None,
            "detail": json.loads(finding["detail"]),
        }
        (quarantine if finding["bucket"] == "quarantine" else ignored).append(item)

    fetches = connection.execute(
        "SELECT * FROM fetches WHERE run_id=? ORDER BY id", (run_id,)
    ).fetchall()
    inputs = [{
        "source": row["source"],
        "attempt": row["attempt"],
        "status": row["status"],
        "etag": row["etag"],
        "body_sha256": row["body_sha256"],
        "decoded_rows": row["decoded_rows"],
    } for row in fetches]

    return {
        "run_id": run_id,
        "inventory": inventory,
        "watermark": published_wm,
        "delta_watermark": delta_watermark,
        "coverage": coverage,
        "published": published,
        "ignored": ignored,
        "quarantine": quarantine,
        "notifications": 0,  # 由 execute_run 按 notifications 表行数差值回填
        "coverage_detail": {
            "expected_partitions": sorted(expected),
            "complete_watermarks": sorted(complete_watermarks),
            "selected_watermark": published_wm,
        },
        "inputs": inputs,
    }


def _snapshot_raw(row):
    return {
        "warehouse": row["warehouse"],
        "sku": row["sku"],
        "physical": str(row["physical"]),
        "reserved": str(row["reserved"]),
        "watermark": str(row["watermark"]),
    }


def _apply_deltas(connection, run_id, complete_watermarks, rows_by_wm):
    events = connection.execute(
        "SELECT * FROM delta_events WHERE run_id=? ORDER BY event_watermark, event_id, id",
        (run_id,),
    ).fetchall()
    snapshot_index = {}
    for watermark, rows in rows_by_wm.items():
        for row in rows:
            snapshot_index[(watermark, row["warehouse"], row["sku"])] = row

    # 每个仓库-SKU 键维护当前锚点水位与锚定后的数量，支持跨基准重定基。
    anchor_wm = {}
    physical = {}
    reserved = {}
    applied = {}
    delta_watermark = None

    for event in events:
        key = (event["warehouse"], event["sku"])
        base = event["base_watermark"]
        base_row = snapshot_index.get((base,) + key)
        if base not in complete_watermarks or base_row is None:
            connection.execute(
                "UPDATE delta_events SET state='quarantined' WHERE id=?", (event["id"],)
            )
            _add_finding(
                connection, run_id, "quarantine", "projection", "delta_base_missing",
                {
                    "event_id": event["event_id"],
                    "warehouse": event["warehouse"],
                    "sku": event["sku"],
                    "base_watermark": str(base),
                    "event_watermark": str(event["event_watermark"]),
                },
                sku=event["sku"], source=event["source"], watermark=event["event_watermark"],
                detail={"event_id": event["event_id"], "base_watermark": base,
                        "complete_watermarks": sorted(complete_watermarks)},
                notify=True,
            )
            anchor_wm.pop(key, None)
            continue
        if anchor_wm.get(key) != base:
            anchor_wm[key] = base
            physical[key] = base_row["physical"]
            reserved[key] = base_row["reserved"]
        next_physical = physical[key] + event["delta_physical"]
        next_reserved = reserved[key] + event["delta_reserved"]
        if next_physical < 0 or next_reserved < 0 or next_reserved > next_physical:
            connection.execute(
                "UPDATE delta_events SET state='quarantined' WHERE id=?", (event["id"],)
            )
            _add_finding(
                connection, run_id, "quarantine", "projection", "delta_out_of_bounds",
                {
                    "event_id": event["event_id"],
                    "warehouse": event["warehouse"],
                    "sku": event["sku"],
                    "delta_physical": str(event["delta_physical"]),
                    "delta_reserved": str(event["delta_reserved"]),
                    "base_watermark": str(base),
                    "event_watermark": str(event["event_watermark"]),
                },
                sku=event["sku"], source=event["source"], watermark=event["event_watermark"],
                detail={"event_id": event["event_id"],
                        "before": {"physical": physical[key], "reserved": reserved[key]},
                        "after": {"physical": next_physical, "reserved": next_reserved}},
                notify=True,
            )
            continue
        physical[key] = next_physical
        reserved[key] = next_reserved
        applied[key] = (next_physical, next_reserved)
        connection.execute(
            "UPDATE delta_events SET state='applied' WHERE id=?", (event["id"],)
        )
        if delta_watermark is None or event["event_watermark"] > delta_watermark:
            delta_watermark = event["event_watermark"]
    return applied, delta_watermark
