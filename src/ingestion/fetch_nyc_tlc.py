"""T01 file-batch ingestion: NYC TLC Yellow Taxi trip records (monthly Parquet) + zone lookup.

NYC TLC is a separate city and dataset; it is never joined to the UCI traffic data (spec §4).
Run: python -m src.ingestion.fetch_nyc_tlc [--force]
"""
from __future__ import annotations

import sys

import yaml

from src.common.config import PROJECT_ROOT, load_config
from src.ingestion.common import (Manifest, State, base_argparser, fetch_file, http_session,
                                  landing_dir, setup_logging, utc_now)

SOURCE = "nyc_tlc"


def main() -> int:
    setup_logging()
    args = base_argparser(__doc__).parse_args()
    cfg = load_config()
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())[SOURCE]
    out = landing_dir(cfg, SOURCE, args.ingest_date)
    state = State(cfg, SOURCE)
    s = http_session()
    man = Manifest(SOURCE, args.ingest_date, utc_now(),
                   params={"dataset": src["dataset"], "months": src["months"]})

    for month in src["months"]:
        name = f"{src['dataset']}_tripdata_{month}.parquet"
        rec = fetch_file(s, f"{src['base_url']}/{name}", out / name, state, args.force)
        rec.source_version = f"tlc {src['dataset']} {month} last-modified:{rec.http_last_modified} etag:{rec.http_etag}"
        rec.extra["month"] = month
        man.files.append(rec)

    rec = fetch_file(s, src["zone_lookup_url"], out / "taxi_zone_lookup.csv", state, args.force)
    rec.source_version = f"tlc zone lookup last-modified:{rec.http_last_modified}"
    man.files.append(rec)

    state.save()
    print(man.write(out))
    return 1 if man.failed else 0


if __name__ == "__main__":
    sys.exit(main())
