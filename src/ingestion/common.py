"""Shared helpers for the M1 fetchers (spec T01: preserve raw data, ingestion
timestamp, origin, version and integrity information; re-ingest reliably).

Each fetcher lands files in a local landing zone:

    data/local/landing/<source>/ingest_date=YYYY-MM-DD/<files>
    data/local/landing/<source>/ingest_date=YYYY-MM-DD/_manifest.json

and keeps a per-source state file (data/local/landing/<source>/_state.json)
mapping each URL to the last checksum/ETag seen. A re-run skips files whose
upstream ETag/size are unchanged and whose local copy still matches its
checksum; --force re-downloads and reports whether the checksum is identical.
scripts/ingest_m1.sh then copies the landing directory into HDFS Bronze.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from src.common.config import PROJECT_ROOT

FETCHER_VERSION = "m1.0"
CHUNK = 1 << 20
log = logging.getLogger("ingestion")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def today_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class FileRecord:
    """One landed file. `status` is downloaded | unchanged | redownloaded_identical |
    redownloaded_changed | failed."""

    name: str
    url: str
    status: str
    sha256: str | None = None
    bytes: int | None = None
    source_version: str | None = None
    http_etag: str | None = None
    http_last_modified: str | None = None
    downloaded_at: str | None = None
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Manifest:
    source: str
    ingest_date: str
    started_at: str
    fetcher_version: str = FETCHER_VERSION
    finished_at: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    files: list[FileRecord] = field(default_factory=list)

    @property
    def failed(self) -> list[FileRecord]:
        return [f for f in self.files if f.status == "failed"]

    def write(self, landing_dir: Path) -> Path:
        self.finished_at = utc_now()
        path = landing_dir / "_manifest.json"
        path.write_text(json.dumps(asdict(self), indent=2, default=str) + "\n", encoding="utf-8")
        return path


def landing_dir(cfg: dict[str, Any], source: str, ingest_date: str) -> Path:
    root = PROJECT_ROOT / cfg["local"]["data_dir"] / "landing" / source
    d = root / f"ingest_date={ingest_date}"
    d.mkdir(parents=True, exist_ok=True)
    return d


class State:
    """Per-source memory of what was fetched before (URL -> last FileRecord fields)."""

    def __init__(self, cfg: dict[str, Any], source: str) -> None:
        self.path = PROJECT_ROOT / cfg["local"]["data_dir"] / "landing" / source / "_state.json"
        self.data: dict[str, dict[str, Any]] = (
            json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}
        )

    def get(self, url: str) -> dict[str, Any] | None:
        return self.data.get(url)

    def put(self, rec: FileRecord, local_path: Path) -> None:
        self.data[rec.url] = {
            "sha256": rec.sha256,
            "bytes": rec.bytes,
            "http_etag": rec.http_etag,
            "http_last_modified": rec.http_last_modified,
            "local_path": str(local_path.relative_to(PROJECT_ROOT)),
            "downloaded_at": rec.downloaded_at,
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2) + "\n", encoding="utf-8")


def http_session(user_agent: str = "bdfp-course-project/1.0"):  # -> requests.Session
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET", "HEAD"))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = user_agent
    return s


def fetch_file(
    session: Any,
    url: str,
    dest: Path,
    state: State,
    force: bool = False,
    version_from_headers: Callable[[dict[str, str]], str] | None = None,
) -> FileRecord:
    """Download url -> dest unless the upstream ETag/size and the local checksum are unchanged."""
    name = dest.name
    try:
        head = session.head(url, allow_redirects=True, timeout=60)
        head.raise_for_status()
        etag = head.headers.get("ETag")
        last_mod = head.headers.get("Last-Modified")
        size = int(head.headers["Content-Length"]) if "Content-Length" in head.headers else None
        prev = state.get(url)
        version = version_from_headers(dict(head.headers)) if version_from_headers else (last_mod or etag)

        if prev and not force:
            prev_path = PROJECT_ROOT / prev["local_path"]
            same_upstream = prev.get("http_etag") == etag and (size is None or prev.get("bytes") == size)
            if same_upstream and prev_path.exists() and sha256_file(prev_path) == prev["sha256"]:
                if prev_path != dest:
                    dest.write_bytes(prev_path.read_bytes())
                log.info("unchanged, not re-downloaded: %s", url)
                return FileRecord(name, url, "unchanged", prev["sha256"], prev["bytes"], version,
                                  etag, last_mod, prev.get("downloaded_at"))

        t0 = time.perf_counter()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with session.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with tmp.open("wb") as fh:
                for block in r.iter_content(CHUNK):
                    fh.write(block)
        tmp.replace(dest)
        digest = sha256_file(dest)
        nbytes = dest.stat().st_size
        if size is not None and nbytes != size:
            raise IOError(f"size mismatch: got {nbytes} bytes, Content-Length {size}")
        status = "downloaded"
        if prev:
            status = "redownloaded_identical" if prev["sha256"] == digest else "redownloaded_changed"
        rec = FileRecord(name, url, status, digest, nbytes, version, etag, last_mod, utc_now(),
                         extra={"download_seconds": round(time.perf_counter() - t0, 2)})
        state.put(rec, dest)
        log.info("%s: %s (%d bytes, sha256 %s…)", status, url, nbytes, digest[:12])
        return rec
    except Exception as exc:  # recorded, not hidden (CLAUDE.md: don't hide failures)
        log.error("failed: %s: %s", url, exc)
        return FileRecord(name, url, "failed", error=f"{type(exc).__name__}: {exc}")


def base_argparser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--ingest-date", default=today_utc(), help="partition date (default: today, UTC)")
    p.add_argument("--force", action="store_true", help="re-download even if unchanged upstream")
    return p


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
