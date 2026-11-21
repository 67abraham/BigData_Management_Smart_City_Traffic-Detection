# Note on this run

All four products built and passed the read-back test, and the contract release blocks were written.
Defect found afterwards in the CSV copy of road_image_manifest_v1 (data/manifests/): values were joined
with plain commas, and `origin` contains a comma ("KITTI 2D object detection benchmark, training split"),
so CSV readers see shifted columns. The Parquet product was not affected. Fix: the CSV is written with
Python's csv module (quoting), with a round-trip unit test; products re-built in the next run.
