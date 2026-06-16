import time
from collections.abc import Iterator
from datetime import datetime, timezone

import requests
from lingua import IsoCode639_1, Language, LanguageDetectorBuilder

from opportunity_finder.connectors.base import ConnectorBase
from opportunity_finder.models import RawItem

_BASE_URL = "https://www.reddit.com/r/{subreddit}/new.json"
_MIN_LANG_CHARS = 20
_REQUEST_INTERVAL = 6.5   # ~9 req/min — stays comfortably under the ~10 limit
_MAX_RETRIES = 3

# Only load models for the two languages we care about (ES/EN) — faster init
_DETECTOR = (
    LanguageDetectorBuilder.from_languages(Language.ENGLISH, Language.SPANISH)
    .with_low_accuracy_mode()
    .build()
)


class RedditJsonConnector(ConnectorBase):
    """Fetch Reddit posts via public unauthenticated JSON endpoints.

    Swap this for a PRAW-backed implementation by creating
    `connectors/reddit/praw_api.py` with the same interface; nothing
    outside this module needs to change.
    """

    source = "reddit"

    def __init__(self, user_agent: str = "opportunity-finder/0.1 (personal research)"):
        self._session = requests.Session()
        self._session.headers["User-Agent"] = user_agent
        self._last_request_ts: float = 0.0

    def fetch(self, subreddit: str, limit: int = 100) -> Iterator[RawItem]:
        fetched = 0
        after: str | None = None

        while fetched < limit:
            batch_size = min(100, limit - fetched)
            params: dict = {"limit": batch_size}
            if after:
                params["after"] = after

            data = self._get(_BASE_URL.format(subreddit=subreddit), params)
            if data is None:
                break

            children = data["data"]["children"]
            if not children:
                break

            for child in children:
                yield self._normalize(child["data"])
                fetched += 1
                if fetched >= limit:
                    return

            after = data["data"].get("after")
            if not after:
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize(self, post: dict) -> RawItem:
        source_id = post["id"]

        selftext = post.get("selftext") or ""
        if selftext in ("[deleted]", "[removed]"):
            selftext = ""

        title = post.get("title") or ""
        author = post.get("author")
        if author in ("[deleted]", "[removed]", None):
            author = None

        return RawItem(
            item_id=f"reddit:{source_id}",
            source="reddit",
            source_id=source_id,
            kind="post",
            author=author,
            created_utc=datetime.fromtimestamp(post["created_utc"], tz=timezone.utc),
            title=title or None,
            text=selftext,
            url=f"https://www.reddit.com{post['permalink']}",
            lang=self._detect_lang(f"{title} {selftext}"),
            score=post.get("score", 0),
            parent_id=None,
            raw=post,
        )

    def _detect_lang(self, text: str) -> str:
        clean = text.strip()
        if len(clean) < _MIN_LANG_CHARS:
            return "und"
        lang = _DETECTOR.detect_language_of(clean)
        if lang is None:
            return "und"
        return lang.iso_code_639_1.name.lower()

    def _get(self, url: str, params: dict) -> dict | None:
        for attempt in range(_MAX_RETRIES):
            self._throttle()
            try:
                resp = self._session.get(url, params=params, timeout=15)
                self._last_request_ts = time.monotonic()

                if resp.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    print(f"  [rate-limited] waiting {wait}s …")
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    print(f"  [403 body] {resp.text[:300]}")

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as exc:
                if attempt == _MAX_RETRIES - 1:
                    raise
                wait = 2 ** attempt
                print(f"  [retry {attempt + 1}] {exc} — waiting {wait}s …")
                time.sleep(wait)

        return None

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        gap = _REQUEST_INTERVAL - elapsed
        if gap > 0:
            time.sleep(gap)
