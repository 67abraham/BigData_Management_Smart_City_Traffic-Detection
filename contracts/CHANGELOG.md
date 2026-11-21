# Data product versions (spec T07 / AC07 "version descriptions")

Rules: the schema version is in the product path (`…/<name>/v1/`) and the contract file name
(`<name>_v1.yaml`). A change that breaks readers (renamed/removed field, changed type, unit or meaning,
changed key or grain) creates `v2` next to `v1`. Additive, backward-compatible changes (a new nullable
field) stay in `v1` and are listed here. Re-building the same version from new upstream data is a new
**release** of that version: the contract's generated `release` block records its run, row count, size
and read-back result.

Release levels (spec §7): **R0** = baseline snapshot supplied or approved by the course; **R1** = this
platform's verified output. No R0 was supplied for these datasets, so every release below is R1.

## v1 — first release (M4)
| Product | Grain / key | Built from |
|---|---|---|
| `traffic_hourly_v1` | (station_id, hour_start_utc) | Gold `uci_traffic_weather_hourly` v1 |
| `traffic_features_v1` | (station_id, hour_start_utc) | `traffic_hourly_v1` |
| `road_image_manifest_v1` | image_id | Bronze KITTI image inventory + labels |
| `traffic_window_metrics_v1` | (station_id, window_start_utc) | Flink window results of an M3 replay run |

The run that produced the current release of each product, and its read-back result, are in the
`release` block at the end of each contract.
