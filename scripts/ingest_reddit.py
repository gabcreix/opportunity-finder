#!/usr/bin/env python3
"""
Fetch ~50 Reddit posts, persist to Parquet, validate with DuckDB.

Usage:
    python scripts/ingest_reddit.py           # real Reddit fetch
    python scripts/ingest_reddit.py --mock    # synthetic data (no network needed)
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the package importable without `pip install -e .`
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb

from opportunity_finder.connectors.reddit.json_api import RedditJsonConnector
from opportunity_finder.models import RawItem
from opportunity_finder.storage.parquet import write_raw_items

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
SUBREDDITS = ["SomebodyMakeThis", "AppIdeas"]
POSTS_PER_SUBREDDIT = 25   # 25 × 2 = 50 total


# ------------------------------------------------------------------
# Mock data for offline / CI validation
# ------------------------------------------------------------------

def _mock_items() -> list[RawItem]:
    """Return a small set of synthetic RawItems that exercise the full schema."""
    now = datetime.now(timezone.utc)
    posts = [
        ("abc123", "SomebodyMakeThis", "App that recognizes plants from photos",
         "I always want to know what plant I'm looking at. Ojalá existiera una app que reconozca plantas.", "en", 142),
        ("def456", "SomebodyMakeThis", "Automatic grocery list from recipes",
         "Would love a tool that reads a recipe URL and adds all ingredients to my shopping list.", "en", 87),
        ("ghi789", "AppIdeas", "Seguimiento de gastos para autónomos",
         "Necesito algo simple que separe gastos personales de profesionales y genere el modelo 130.", "es", 54),
        ("jkl012", "AppIdeas", "Noise-canceling for home office backgrounds",
         "Real-time background noise removal that works in any video call app, not just Zoom.", "en", 210),
        ("mno345", "SomebodyMakeThis", "Price tracker for local farmers markets",
         "I wish there was a way to compare prices across different stalls at my local farmers market.", "en", 33),
        ("pqr678", "AppIdeas", "Habit tracker without streaks",
         "All habit apps punish you for missing a day with broken streaks. I want one that just shows progress.", "en", 178),
        ("stu901", "SomebodyMakeThis", "App para aprender a cocinar paso a paso",
         "Una app que te enseñe a cocinar de cero, con vídeos cortos para cada técnica.", "es", 65),
        ("vwx234", "AppIdeas", "Shared expense tracker for roommates",
         "Something simpler than Splitwise that just tracks rent, utilities, and groceries.", "en", 99),
    ]
    items = []
    for i, (sid, sub, title, text, lang, score) in enumerate(posts):
        from datetime import timedelta
        created = now - timedelta(hours=i * 3)
        raw_post = {
            "id": sid, "subreddit": sub, "title": title, "selftext": text,
            "author": f"user_{i}", "created_utc": created.timestamp(),
            "permalink": f"/r/{sub}/comments/{sid}/", "score": score,
            "url": f"https://www.reddit.com/r/{sub}/comments/{sid}/",
        }
        items.append(RawItem(
            item_id=f"reddit:{sid}",
            source="reddit",
            source_id=sid,
            kind="post",
            author=f"user_{i}",
            created_utc=created,
            title=title,
            text=text,
            url=f"https://www.reddit.com/r/{sub}/comments/{sid}/",
            lang=lang,
            score=score,
            parent_id=None,
            raw=raw_post,
        ))
    return items


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main(mock: bool = False) -> None:
    if mock:
        print("Running in MOCK mode — no network calls.\n")
        all_items = _mock_items()
        print(f"Generated {len(all_items)} synthetic items")
    else:
        connector = RedditJsonConnector()
        all_items = []
        for sub in SUBREDDITS:
            print(f"\nFetching r/{sub} (up to {POSTS_PER_SUBREDDIT} posts)…")
            batch = list(connector.fetch(sub, limit=POSTS_PER_SUBREDDIT))
            print(f"  fetched: {len(batch)}")
            all_items.extend(batch)
        print(f"\nTotal fetched: {len(all_items)}")

    print(f"\nWriting to {DATA_DIR} …")
    written = write_raw_items(all_items, DATA_DIR)
    print(f"Written: {written} rows")

    # ----------------------------------------------------------------
    # DuckDB validation
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("DuckDB VALIDATION")
    print("=" * 60)

    glob = str(DATA_DIR / "**" / "*.parquet")
    con = duckdb.connect()

    print("\n[counts by source]")
    df_counts = con.execute(f"""
        SELECT
            source,
            COUNT(*)                                AS total,
            COUNT(*) FILTER (WHERE text != '')      AS with_body,
            COUNT(DISTINCT lang)                    AS distinct_langs,
            MIN(created_utc)                        AS oldest,
            MAX(created_utc)                        AS newest
        FROM read_parquet('{glob}')
        GROUP BY source
    """).fetchdf()
    print(df_counts.to_string(index=False))

    print("\n[language breakdown]")
    df_langs = con.execute(f"""
        SELECT lang, COUNT(*) AS n
        FROM read_parquet('{glob}')
        GROUP BY lang
        ORDER BY n DESC
    """).fetchdf()
    print(df_langs.to_string(index=False))

    print("\n[top 10 by score — item_id | lang | score | title (60 chars)]")
    df_sample = con.execute(f"""
        SELECT
            item_id,
            lang,
            score,
            LEFT(COALESCE(title, '(no title)'), 60) AS title_preview
        FROM read_parquet('{glob}')
        ORDER BY score DESC
        LIMIT 10
    """).fetchdf()
    print(df_sample.to_string(index=False))

    print("\n[schema]")
    df_schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{glob}') LIMIT 0"
    ).fetchdf()
    print(df_schema[["column_name", "column_type"]].to_string(index=False))

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    mock_mode = "--mock" in sys.argv
    main(mock=mock_mode)
