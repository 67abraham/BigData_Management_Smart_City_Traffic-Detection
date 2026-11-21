# SQL analyses (spec T06/AC06)

Five Spark SQL analyses over the published products, run `m4_20261121T103306Z` (Spark app
`app-20261121103309-0025`). Code and SQL text: `src/analytics/sql_analyses.py`; run with
`bash scripts/run_m4.sh analytics`. Results (CSV + JSON with the SQL) and physical plans:
`outputs/reports/analytics/m4_20261121T103306Z/`. NYC is analysed on its own and never joined to UCI.
Figures below are copied from those result files.

## A1 — Hourly pattern at ATR 301 (`traffic_hourly_v1`)
Mean hourly volume (vehicles/hour) by local hour:

| | 02:00 | 05:00 | 07:00 | 12:00 | 16:00 | 20:00 | 23:00 |
|---|---:|---:|---:|---:|---:|---:|---:|
| weekday | 303 | 2,699 | 6,061 | 4,874 | **6,241** | 2,877 | 1,404 |
| weekend | 620 | 643 | 1,602 | 4,403 | 4,375 | 2,851 | 1,711 |

Weekdays have two peaks, 07:00 (6,061) and 16:00 (6,241, the highest), and a minimum at 02:00 (303).
Weekends have no morning peak; volume rises to a plateau of about 4,400 from 12:00 to 16:00 (maximum
4,408 at 13:00). All 24 hours × 2 day types: `A1_hourly_pattern.csv`.

## A2 — Working day vs weekend vs holiday (`traffic_hourly_v1`)
Daily totals, only days with all 24 hours present (so gaps and DST days do not bias the means):

| Day type | Full days | Mean daily volume | Min | Max |
|---|---:|---:|---:|---:|
| working_day | 821 | 86,583 | 46,632 | 97,332 |
| weekend | 351 | 63,503 | 22,271 | 82,409 |
| holiday | 36 | 61,710 | 39,604 | 93,417 |

Holidays look like weekends (−29% vs working days), not like working days. The spread is wide (a holiday
maximum of 93,417 shows some "holidays" in UCI's list, e.g. regional events, carry working-day traffic).

## A3 — Weather (ERA5 reanalysis) and volume (`traffic_hourly_v1`)
Weekday, non-holiday hours 07:00–18:00 local. Each hour is compared with the mean of **dry** hours at
the **same local hour**, so the different time-of-day mix of rainy hours does not distort the result.

| ERA5 class | Hours | Mean volume | Volume vs dry, same hour |
|---|---:|---:|---:|
| rain < 1 mm | 1,025 | 5,306.0 | 1.0009 |
| dry | 11,747 | 5,320.6 | 1.0000 |
| rain ≥ 1 mm | 366 | 5,229.3 | 0.9795 |
| snow | 636 | 4,689.9 | **0.8813** |

Light rain shows no difference; rain of 1 mm or more about −2%; hours with ERA5 snowfall about **−12%**.
This is an association in reanalysis data on a ~25 km grid, not a measured cause; season and incidents
are not controlled.

## A4 — NYC pickups by borough (`nyc_zone_hourly_demand` v1, 2023-01..03)

| Borough | Trips | % | Mean distance (mi) | Mean total (USD) |
|---|---:|---:|---:|---:|
| Manhattan | 8,267,291 | 88.90 | 2.41 | 23.18 |
| Queens | 848,263 | 9.12 | 12.42 | 69.82 |
| Unknown (zone 264) | 104,034 | 1.12 | 4.49 | 34.39 |
| Brooklyn | 57,577 | 0.62 | 4.90 | 33.35 |
| Bronx | 13,843 | 0.15 | 5.19 | 34.77 |
| N/A (zone 265, outside NYC) | 6,994 | 0.08 | 5.38 | 88.31 |
| EWR | 1,187 | 0.01 | 1.33 | 107.72 |
| Staten Island | 869 | 0.01 | 10.97 | 64.99 |

Yellow-taxi demand is overwhelmingly Manhattan; Queens trips are five times longer on average because
its pickups are dominated by the airports (A5).

## A5 — Ten busiest NYC pickup zones

| Zone (LocationID) | Borough | Trips | Peak local hour |
|---|---|---:|---:|
| JFK Airport (132) | Queens | 454,035 | 22 |
| Upper East Side South (237) | Manhattan | 433,962 | 14 |
| Midtown Center (161) | Manhattan | 428,424 | 18 |
| Upper East Side North (236) | Manhattan | 401,705 | 15 |
| Midtown East (162) | Manhattan | 330,008 | 18 |
| Penn Station/Madison Sq West (186) | Manhattan | 326,799 | 18 |
| Times Sq/Theatre District (230) | Manhattan | 310,666 | 22 |
| Lincoln Square East (142) | Manhattan | 301,212 | 21 |
| LaGuardia Airport (138) | Queens | 286,286 | 14 |
| Murray Hill (170) | Manhattan | 273,218 | 18 |

Business districts peak at the 18:00 evening commute; JFK and Times Square at 22:00 (late arrivals,
theatre). Taxi pickups are demand, not road-counter traffic volume (spec §4).

Performance comparisons (the second half of T06) are in `docs/performance.md`.
