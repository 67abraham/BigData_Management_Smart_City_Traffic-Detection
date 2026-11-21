import io
import json
import zipfile
from pathlib import Path

import pytest

import src.ingestion.common as common
from src.ingestion.fetch_kitti import redact, sample_ids
from src.ingestion.fetch_open_meteo import year_chunks
from src.ingestion.locate_atr301 import _matches, _mentions_i94
from src.ingestion.range_zip import RangeFile


# ---------- fake HTTP ----------
class FakeResp:
    def __init__(self, body: bytes, headers: dict[str, str], status: int = 200) -> None:
        self.content, self.headers, self.status_code = body, headers, status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise IOError(f"HTTP {self.status_code}")

    def iter_content(self, n: int):
        for i in range(0, len(self.content), n):
            yield self.content[i:i + n]

    def __enter__(self):
        return self

    def __exit__(self, *a) -> None:
        pass


class FakeSession:
    def __init__(self, body: bytes, etag: str = '"v1"') -> None:
        self.body, self.etag, self.gets = body, etag, 0

    def _h(self) -> dict[str, str]:
        return {"ETag": self.etag, "Content-Length": str(len(self.body)), "Last-Modified": "Mon, 01 May 2023"}

    def head(self, url, **kw):
        return FakeResp(b"", self._h())

    def get(self, url, **kw):
        self.gets += 1
        return FakeResp(self.body, self._h())


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(common, "PROJECT_ROOT", tmp_path)
    return {"local": {"data_dir": "data/local"}}


def test_fetch_then_skip_unchanged_then_force(cfg) -> None:
    s = FakeSession(b"hello world")
    d = common.landing_dir(cfg, "demo", "2026-11-21")
    state = common.State(cfg, "demo")
    r1 = common.fetch_file(s, "https://x/f.bin", d / "f.bin", state)
    assert r1.status == "downloaded" and r1.sha256 == common.sha256_bytes(b"hello world")
    state.save()

    state2 = common.State(cfg, "demo")          # fresh process, state from disk
    r2 = common.fetch_file(s, "https://x/f.bin", d / "f.bin", state2)
    assert r2.status == "unchanged" and s.gets == 1

    r3 = common.fetch_file(s, "https://x/f.bin", d / "f.bin", state2, force=True)
    assert r3.status == "redownloaded_identical" and s.gets == 2


def test_upstream_change_and_local_corruption_trigger_download(cfg) -> None:
    d = common.landing_dir(cfg, "demo", "2026-11-21")
    state = common.State(cfg, "demo")
    common.fetch_file(FakeSession(b"v1 data"), "https://x/f", d / "f", state)
    (d / "f").write_bytes(b"tampered")              # local copy no longer matches its checksum
    r = common.fetch_file(FakeSession(b"v1 data"), "https://x/f", d / "f", state)
    assert r.status == "redownloaded_identical"
    r = common.fetch_file(FakeSession(b"v2 data", etag='"v2"'), "https://x/f", d / "f", state)
    assert r.status == "redownloaded_changed"


def test_fetch_failure_is_recorded_not_raised(cfg) -> None:
    class Broken(FakeSession):
        def head(self, url, **kw):
            return FakeResp(b"", {}, status=503)
    d = common.landing_dir(cfg, "demo", "2026-11-21")
    r = common.fetch_file(Broken(b""), "https://x/f", d / "f", common.State(cfg, "demo"))
    assert r.status == "failed" and "503" in r.error


def test_manifest_written(cfg) -> None:
    d = common.landing_dir(cfg, "demo", "2026-11-21")
    m = common.Manifest("demo", "2026-11-21", common.utc_now())
    m.files.append(common.FileRecord("a", "u", "failed", error="x"))
    data = json.loads(m.write(d).read_text())
    assert data["files"][0]["status"] == "failed" and data["finished_at"] and m.failed


# ---------- range zip ----------
def test_range_file_reads_zip_member() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("training/image_2/000001.png", b"\x89PNG" + b"x" * 5000)
        zf.writestr("training/image_2/000002.png", b"\x89PNG" + b"y" * 5000)
    data = buf.getvalue()
    raw = RangeFile(len(data), lambda a, b: data[a:b + 1])
    with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1024)) as zf:
        assert zf.read("training/image_2/000002.png") == b"\x89PNG" + b"y" * 5000
    assert raw.requests > 0 and raw.bytes_fetched <= len(data) * 2


# ---------- small pure helpers ----------
def test_year_chunks() -> None:
    assert year_chunks("2012-10-01", "2014-02-01") == [
        ("2012-10-01", "2012-12-31"), ("2013-01-01", "2013-12-31"), ("2014-01-01", "2014-02-01")]
    with pytest.raises(ValueError):
        year_chunks("2014-01-02", "2014-01-01")


def test_sample_ids_deterministic() -> None:
    ids = [f"{i:06d}" for i in range(7481)]
    a, b = sample_ids(ids, 500, 241091038), sample_ids(list(reversed(ids)), 500, 241091038)
    assert a == b and len(set(a)) == 500 and a == sorted(a)
    with pytest.raises(ValueError):
        sample_ids(ids[:10], 11, 1)


def test_redact_hides_account_link() -> None:
    assert redact("https://host/private/token123/data_object_image_2.zip?sig=abc") == "kitti://data_object_image_2.zip"


def test_atr_matching() -> None:
    assert _matches({"SEQ": "301", "x": 1}, "301") and _matches({"id": 301}, "301") and _matches({"id": "0301"}, "301")
    assert not _matches({"id": "3011"}, "301") and not _matches({"id": "1301"}, "301")
    assert _mentions_i94({"loc": "I-94 E of Snelling Ave"}) and not _mentions_i94({"loc": "I-394"})
