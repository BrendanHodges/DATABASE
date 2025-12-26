"""
Minimal Google Sheets wrapper (gspread + retries/backoff).

Usage:
    from sheets_wrapper import Sheets

    sheets = Sheets(credentials_file=r"C:\path\to\Credentials.json")  # or set env GOOGLE_CREDENTIALS_FILE
    ws_list = sheets.worksheets("YOUR_SHEET_ID")
    print([ws.title for ws in ws_list])

    # Find a worksheet by partial title and batch-get some ranges:
    ws = sheets.find_worksheet("YOUR_SHEET_ID", "Baltimore")
    if ws:
        rows = [24, 25, 122, 123, 124]
        ranges = [f"B{r}:D{r}" for r in rows]
        data = sheets.batch_get(ws, ranges)  # list of ranges
"""

from __future__ import annotations
import os, time, random
from functools import lru_cache
from typing import Iterable, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound

# Least-privilege scope for Sheets access
DEFAULT_SCOPES: List[str] = ["https://www.googleapis.com/auth/spreadsheets"]


def _sleep_time(base: float, attempt: int) -> float:
    """Exponential backoff + jitter."""
    return base * (2 ** attempt) + random.uniform(0, 0.2)


def _retry(fn, *, retries: int = 5, base_delay: float = 0.6):
    """
    Retry a callable on transient errors (quota 429, 5xx, network).
    Raises the final exception if all attempts fail.
    """
    last = None
    for attempt in range(retries):
        try:
            return fn()
        except (APIError, ConnectionError, TimeoutError, OSError) as e:
            last = e
            if attempt == retries - 1:
                raise
            time.sleep(_sleep_time(base_delay, attempt))
    if last:
        raise last


@lru_cache(maxsize=1)
def _client(credentials_file: Optional[str] = None,
            scopes_key: Optional[Tuple[str, ...]] = None) -> gspread.Client:
    """
    Create and cache a single authorized gspread client.
    Cached across calls so you don't re-auth every time.
    """
    creds_path = credentials_file or os.getenv("GOOGLE_CREDENTIALS_FILE")
    if not creds_path:
        raise ValueError(
            "Set credentials_file or env var GOOGLE_CREDENTIALS_FILE "
            "pointing to your service account JSON."
        )
    scopes = list(scopes_key) if scopes_key else DEFAULT_SCOPES
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


class Sheets:
    """
    Tiny façade over gspread with:
    - open_by_key(sheet_id)           -> Spreadsheet
    - worksheets(sheet_id)            -> list[Worksheet]
    - batch_get(worksheet, ranges)    -> list[list[list[str]]]
    - worksheets_excluding(sheet_id, exclude_titles)
    - find_worksheet(sheet_id, title_contains)

    Built-in retries/backoff for API limits.
    """

    def __init__(self, *,
                 credentials_file: Optional[str] = None,
                 scopes: Optional[List[str]] = None,
                 retries: int = 5,
                 base_delay: float = 0.6) -> None:
        self._credentials_file = credentials_file
        self._scopes = scopes or DEFAULT_SCOPES
        self._retries = retries
        self._base_delay = base_delay
        # cache the client instance (shared via lru_cache)
        self.client = _client(self._credentials_file, tuple(self._scopes))

    # ---- High-level helpers ----
    def open_by_key(self, sheet_id: str) -> gspread.Spreadsheet:
        """Open a spreadsheet by its id (with retries)."""
        try:
            return _retry(
                lambda: self.client.open_by_key(sheet_id),
                retries=self._retries, base_delay=self._base_delay
            )
        except SpreadsheetNotFound as e:
            raise SpreadsheetNotFound(
                f"Spreadsheet not found or no access: {sheet_id}. "
                f"Did you share it with the service account email in your JSON?"
            ) from e

    def worksheets(self, sheet_id: str):
        """Return list of Worksheet objects for the spreadsheet id."""
        sheet = self.open_by_key(sheet_id)
        return _retry(
            lambda: sheet.worksheets(),
            retries=self._retries, base_delay=self._base_delay
        )

    def batch_get(self, ws: gspread.Worksheet, ranges: List[str]):
        """
        Batch-get multiple ranges in one API call (quota friendly).
        Returns a list (one entry per range), each being the 2D values.
        """
        return _retry(
            lambda: ws.batch_get(ranges),
            retries=self._retries, base_delay=self._base_delay
        )

    # ---- Convenience filters ----
    def worksheets_excluding(self, sheet_id: str, exclude_titles: Iterable[str] = ()):
        """All worksheets except those named in exclude_titles."""
        excl = set(exclude_titles or ())
        return [ws for ws in self.worksheets(sheet_id) if ws.title not in excl]

    def find_worksheet(self, sheet_id: str, title_contains: str) -> Optional[gspread.Worksheet]:
        """Return the first worksheet whose title contains the substring (case-insensitive)."""
        needle = (title_contains or "").lower()
        for ws in self.worksheets(sheet_id):
            if needle in ws.title.lower():
                return ws
        return None
