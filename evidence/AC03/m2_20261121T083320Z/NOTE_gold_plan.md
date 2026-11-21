# Note on the Gold NYC step in this run

The Gold aggregation here PASSED (sum of trips 9,300,058 = accepted Silver rows), but its plan used
SortAggregate, because `first()` on the string columns pu_borough/pu_zone forced a sort-based
aggregate. The job was changed to group by those columns (they depend only on pu_location_id, so no
groups are added) and re-run as `m2_20261121T084148Z`: HashAggregate -> Exchange -> HashAggregate,
same 212,041 rows. The Silver NYC results of this run remain the current ones.
