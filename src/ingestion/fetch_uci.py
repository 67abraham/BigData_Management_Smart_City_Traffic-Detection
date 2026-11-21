"""T01 file-batch ingestion: UCI Metro Interstate Traffic Volume (dataset 492).

Lands the original zip (byte-identical) plus the CSV.gz member extracted from it, each
with a SHA-256 in _manifest.json.   Run: python -m src.ingestion.fetch_uci [--force]
"""
from __future__ import annotations

import sys
import zipfile

import yaml

from src.common.config import PROJECT_ROOT, load_config
from src.ingestion.common import (FileRecord, Manifest, State, base_argparser, fetch_file,
                                  http_session, landing_dir, setup_logging, sha256_file, utc_now)

SOURCE = "uci_metro"


def main() -> int:
    setup_logging()
    args = base_argparser(__doc__).parse_args()
    cfg = load_config()
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())[SOURCE]
    out = landing_dir(cfg, SOURCE, args.ingest_date)
    state = State(cfg, SOURCE)
    man = Manifest(SOURCE, args.ingest_date, utc_now(), params={"url": src["url"], "doi": src["doi"]})

    zip_rec = fetch_file(http_session(), src["url"], out / src["landing_name"], state, args.force)
    zip_rec.source_version = f"uci-492 doi:{src['doi']} last-modified:{zip_rec.http_last_modified}"
    man.files.append(zip_rec)

    if zip_rec.status != "failed":
        try:
            with zipfile.ZipFile(out / src["landing_name"]) as zf:
                names = zf.namelist()
                if src["member"] not in names:
                    raise KeyError(f"{src['member']} not in zip; found {names}")
                info = zf.getinfo(src["member"])
                zf.extract(src["member"], out)
            member = out / src["member"]
            man.files.append(FileRecord(
                src["member"], f"{src['url']}#{src['member']}", "extracted", sha256_file(member),
                member.stat().st_size, zip_rec.source_version, downloaded_at=utc_now(),
                extra={"extracted_from": src["landing_name"], "zip_member_date": list(info.date_time),
                       "zip_members": names}))
        except Exception as exc:
            man.files.append(FileRecord(src["member"], src["url"], "failed", error=repr(exc)))

    state.save()
    print(man.write(out))
    return 1 if man.failed else 0


if __name__ == "__main__":
    sys.exit(main())
