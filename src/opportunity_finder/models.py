from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RawItem:
    item_id: str          # "{source}:{source_id}" — deterministic global PK
    source: str
    source_id: str
    kind: str             # "post" | "comment" | "article"
    author: str | None
    created_utc: datetime
    title: str | None
    text: str
    url: str
    lang: str             # "en" / "es" / "und" (undetermined, < 20 chars)
    score: int
    parent_id: str | None
    raw: dict             # complete original object
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: str = "1.0"
