# Original plan

The original plan for this talk was to do a bake-off between Dask, Rapids and
Spark. For reasons that should maybe have been obvious to me, that's actually a
huge amount of work because it requires:
1. Spinning up a bunch of infrastructure
2. Making sure parquet data is readable by Spark, Dask and RAPIDS, all of whom
   have differently implemented parquet readers

1. Clean 4 billion rows of download data and store it as parquet
2. 

## Lessons learned

* Parquet is hard
    * fastparquet
    * pyarrow
    * RAPIDS
    * Spark

* Infrastructure is a pain to spin up for this kind of bake-off
