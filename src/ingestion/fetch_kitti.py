"""T01 ingestion: KITTI 2D Object Detection — full label set + a deterministic image subset.

- data_object_label_2.zip is downloaded in full (small) and kept byte-identical.
- From data_object_image_2.zip only `subset_size` training images are extracted, via HTTP
  range reads (src.ingestion.range_zip); the archive itself is never stored.
- The subset is a seeded random sample of the training IDs that have labels, so it is
  reproducible. Every extracted image gets a SHA-256 in _manifest.json.
Download URLs come from the user's KITTI account and are read from the environment
(KITTI_LABEL_URL, KITTI_IMAGE_URL in .env); they are never written to git. Only the URL's
file name is recorded in the manifest. Images must not be committed or redistributed.
Run: python -m src.ingestion.fetch_kitti [--force]
"""
from __future__ import annotations

import os
import random
import re
import sys
import zipfile
from pathlib import Path

import yaml

from src.common.config import PROJECT_ROOT, load_config
from src.ingestion.common import (FileRecord, Manifest, State, base_argparser, fetch_file,
                                  http_session, landing_dir, log, setup_logging, sha256_file, utc_now)
from src.ingestion.range_zip import http_range_file

SOURCE = "kitti"
LABEL_RE = re.compile(r"^training/label_2/(\d{6})\.txt$")
IMAGE_FMT = "training/image_2/{id}.png"


def redact(url: str) -> str:
    """Keep only the file name: account download links must not end up in git."""
    return "kitti://" + url.split("?")[0].rsplit("/", 1)[-1]


def sample_ids(ids: list[str], k: int, seed: int) -> list[str]:
    ids = sorted(ids)
    if k > len(ids):
        raise ValueError(f"subset_size {k} > available ids {len(ids)}")
    return sorted(random.Random(seed).sample(ids, k))


def main() -> int:
    setup_logging()
    args = base_argparser(__doc__).parse_args()
    cfg = load_config()
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())[SOURCE]
    label_url = os.environ.get(src["label_url_env"])
    image_url = os.environ.get(src["image_url_env"])
    if not label_url or not image_url:
        raise SystemExit(f"set {src['label_url_env']} and {src['image_url_env']} in .env (see .env.example)")

    out = landing_dir(cfg, SOURCE, args.ingest_date)
    state = State(cfg, SOURCE)
    s = http_session()
    man = Manifest(SOURCE, args.ingest_date, utc_now(), params={
        "label_archive": redact(label_url), "image_archive": redact(image_url), "split": src["split"],
        "subset_size": src["subset_size"], "subset_seed": src["subset_seed"]})

    # 1) labels, full archive
    lab = fetch_file(s, label_url, out / "data_object_label_2.zip", state, args.force,
                     log_name=redact(label_url))
    lab.url = redact(label_url)
    lab.source_version = f"kitti object label_2 last-modified:{lab.http_last_modified}"
    man.files.append(lab)
    if lab.status == "failed":
        state.save(); print(man.write(out)); return 1
    with zipfile.ZipFile(out / "data_object_label_2.zip") as zf:
        label_ids = [m.group(1) for n in zf.namelist() if (m := LABEL_RE.match(n))]
    man.params["label_files_in_archive"] = len(label_ids)
    chosen = sample_ids(label_ids, int(src["subset_size"]), int(src["subset_seed"]))
    (out / "subset_ids.txt").write_text("\n".join(chosen) + "\n", encoding="utf-8")

    # 2) image subset via range reads
    img_dir = out / "training" / "image_2"
    img_dir.mkdir(parents=True, exist_ok=True)
    buf, raw = http_range_file(s, image_url)
    with zipfile.ZipFile(buf) as zf:
        members = set(zf.namelist())
        man.params["image_archive_bytes"] = raw.size
        man.params["image_members_in_archive"] = sum(1 for n in members if n.endswith(".png"))
        for img_id in chosen:
            name = IMAGE_FMT.format(id=img_id)
            dest = img_dir / f"{img_id}.png"
            key = f"{redact(image_url)}#{name}"
            prev = state.get(key)
            try:
                if prev and not args.force and dest.exists() and sha256_file(dest) == prev["sha256"]:
                    man.files.append(FileRecord(name, key, "unchanged", prev["sha256"], prev["bytes"],
                                                "kitti object image_2", downloaded_at=prev.get("downloaded_at")))
                    continue
                if name not in members:
                    raise KeyError(f"{name} not in image archive")
                info = zf.getinfo(name)
                with zf.open(info) as src_fh, dest.open("wb") as fh:  # zipfile verifies CRC-32
                    while block := src_fh.read(1 << 20):
                        fh.write(block)
                rec = FileRecord(name, key, "downloaded", sha256_file(dest), dest.stat().st_size,
                                 "kitti object image_2", downloaded_at=utc_now(),
                                 extra={"zip_crc32": info.CRC, "zip_file_size": info.file_size})
                state.put(rec, dest)
            except Exception as exc:
                log.error("failed %s: %s", name, exc)
                rec = FileRecord(name, key, "failed", error=f"{type(exc).__name__}: {exc}")
            man.files.append(rec)
    man.params["range_requests"] = raw.requests
    man.params["range_bytes_fetched"] = raw.bytes_fetched
    log.info("image subset: %d files, %d range requests, %d bytes fetched",
             len(chosen), raw.requests, raw.bytes_fetched)

    state.save()
    print(man.write(out))
    return 1 if man.failed else 0


if __name__ == "__main__":
    sys.exit(main())
