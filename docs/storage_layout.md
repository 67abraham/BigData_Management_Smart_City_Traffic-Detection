# HDFS storage layout (spec T02) — Bronze part, M1

| Path | Content | Format | Written by |
|---|---|---|---|
| `/datalake/bronze/<source>/raw/ingest_date=YYYY-MM-DD/` | landed files, byte-identical to upstream, plus `_manifest.json` | as delivered (zip, csv.gz, json, parquet, png) | `scripts/ingest_m1.sh` (`hdfs dfs -put`) |
| `/datalake/bronze/<source>/records/ingest_date=YYYY-MM-DD/` | one row per source record, values as delivered + provenance columns | Parquet | `src/ingestion/bronze_load.py` |
| `/datalake/bronze/nyc_tlc/records/ingest_date=…/source_month=YYYY-MM/` | NYC trips, one sub-partition per monthly file (schemas can differ per month) | Parquet | same |
| `/datalake/bronze/nyc_tlc/zone_lookup/ingest_date=…/` | TLC taxi zone lookup | Parquet | same |
| `/datalake/bronze/kitti/image_inventory/ingest_date=…/` | every training image ID with label path, availability, size, PNG dimensions, SHA-256 | Parquet | same |

`<source>` ∈ `uci_metro`, `open_meteo`, `nyc_tlc`, `kitti`. Raw and records are separate trees so that
Spark partition discovery on `records/` sees only `ingest_date=` partitions.

Re-running on the same UTC day replaces that day's partitions (idempotent); a run on a new day adds a
new `ingest_date` partition, so earlier deliveries stay available for lineage. Silver (M2) selects the
latest `ingest_date` per source. Replication is 2 (see `configs/hadoop/hdfs-site.xml`).
Access rules and the full Bronze/Silver/Gold catalogue are completed in M2.
