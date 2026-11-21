"""Read members of a remote zip without downloading the whole archive.

A zip's table of contents (central directory) sits at the end of the file and every member
is located by byte offset, so with HTTP Range requests (`Accept-Ranges: bytes`) zipfile can
open the archive and extract single members. Used for the 500-image KITTI subset: the
12.6 GB image archive is never copied in full.
"""
from __future__ import annotations

import io
from typing import Callable

RangeFetcher = Callable[[int, int], bytes]  # (start, end_inclusive) -> bytes


class RangeFile(io.RawIOBase):
    """Seekable read-only file over a byte-range fetch function."""

    def __init__(self, size: int, fetch: RangeFetcher) -> None:
        self.size = size
        self._fetch = fetch
        self._pos = 0
        self.requests = 0
        self.bytes_fetched = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self.size}[whence]
        self._pos = max(0, base + offset)
        return self._pos

    def readinto(self, b) -> int:  # type: ignore[override]
        if self._pos >= self.size or len(b) == 0:
            return 0
        end = min(self._pos + len(b), self.size) - 1
        data = self._fetch(self._pos, end)
        self.requests += 1
        self.bytes_fetched += len(data)
        n = len(data)
        b[:n] = data
        self._pos += n
        return n


def http_range_file(session, url: str, buffer_size: int = 1 << 20) -> tuple[io.BufferedReader, RangeFile]:
    head = session.head(url, allow_redirects=True, timeout=60)
    head.raise_for_status()
    if head.headers.get("Accept-Ranges", "").lower() != "bytes":
        raise IOError("server does not support byte ranges for the image archive")  # url kept out of logs
    size = int(head.headers["Content-Length"])

    def fetch(start: int, end: int) -> bytes:
        r = session.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=120)
        if r.status_code != 206:
            raise IOError(f"expected HTTP 206 for range {start}-{end}, got {r.status_code}")
        return r.content

    raw = RangeFile(size, fetch)
    return io.BufferedReader(raw, buffer_size=buffer_size), raw
