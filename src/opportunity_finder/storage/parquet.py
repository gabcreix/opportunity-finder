import json
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from opportunity_finder.models import RawItem

_SCHEMA = pa.schema([
    ("item_id",      pa.string()),
    ("source",       pa.string()),
    ("source_id",    pa.string()),
    ("kind",         pa.string()),
    ("author",       pa.string()),
    ("created_utc",  pa.timestamp("us", tz="UTC")),
    ("title",        pa.string()),
    ("text",         pa.string()),
    ("url",          pa.string()),
    ("lang",         pa.string()),
    ("score",        pa.int64()),
    ("parent_id",    pa.string()),
    ("raw",          pa.string()),   # JSON-serialized
    ("ingested_at",  pa.timestamp("us", tz="UTC")),
    ("version",      pa.string()),
])


def write_raw_items(items: Iterable[RawItem], base_path: Path) -> int:
    """Append-only write to Parquet partitioned by source and date.

    Returns the number of rows written (after deduplication).
    Partition layout: <base_path>/source=<src>/dt=YYYY-MM-DD/<ts>.parquet
    """
    rows = list(items)
    if not rows:
        return 0

    existing = _existing_item_ids(base_path)
    new_rows = [r for r in rows if r.item_id not in existing]

    if not new_rows:
        print("  [dedup] all items already present — nothing written")
        return 0

    skipped = len(rows) - len(new_rows)
    if skipped:
        print(f"  [dedup] skipped {skipped} already-seen item(s)")

    by_partition: dict[tuple[str, str], list[RawItem]] = defaultdict(list)
    for item in new_rows:
        date_str = item.created_utc.strftime("%Y-%m-%d")
        by_partition[(item.source, date_str)].append(item)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    total = 0
    for (source, date_str), batch in by_partition.items():
        out_dir = base_path / f"source={source}" / f"dt={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = _to_arrow(batch)
        pq.write_table(table, out_dir / f"{ts}.parquet")
        total += len(batch)

    return total


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _existing_item_ids(base_path: Path) -> set[str]:
    files = list(base_path.glob("**/*.parquet"))
    if not files:
        return set()
    glob = str(base_path / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT item_id FROM read_parquet('{glob}')").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()
    finally:
        con.close()


def _to_arrow(items: list[RawItem]) -> pa.Table:
    return pa.table(
        {
            "item_id":     [i.item_id for i in items],
            "source":      [i.source for i in items],
            "source_id":   [i.source_id for i in items],
            "kind":        [i.kind for i in items],
            "author":      [i.author for i in items],
            "created_utc": [i.created_utc for i in items],
            "title":       [i.title for i in items],
            "text":        [i.text for i in items],
            "url":         [i.url for i in items],
            "lang":        [i.lang for i in items],
            "score":       [i.score for i in items],
            "parent_id":   [i.parent_id for i in items],
            "raw":         [json.dumps(i.raw) for i in items],
            "ingested_at": [i.ingested_at for i in items],
            "version":     [i.version for i in items],
        },
        schema=_SCHEMA,
    )
