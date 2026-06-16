from abc import ABC, abstractmethod
from collections.abc import Iterator

from opportunity_finder.models import RawItem


class ConnectorBase(ABC):
    """Common interface for all data source connectors.

    Callers interact only with `fetch()`; the transport layer is an
    implementation detail (public JSON, OAuth/PRAW, scraper, etc.).
    """

    source: str  # e.g. "reddit"

    @abstractmethod
    def fetch(self, subreddit: str, limit: int = 100) -> Iterator[RawItem]:
        """Yield up to `limit` RawItems normalized to the canonical schema."""
        ...
