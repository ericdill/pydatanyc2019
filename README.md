# pydatanyc2019

Spark, Dask and Rapids talk repository

## Data source(s)
The bulk of the analysis is going to be done on the public anaconda-package-data dataset.
This dataset is approximately 300 million rows and is available for download from s3, hosted by Anaconda.
There is also a [github repository](https://github.com/ContinuumIO/anaconda-package-data) that lets you spawn a binder notebook link so you can play around with the dataset.
I am going to be starting from the internal dataset that the anaconda-package-data is derived from since it is nearly 4 billion rows and many folks in the business world find billion-row data sets more interesting than million-row datasets.
That means that some of the work that I have in this repository will not be reproducible by you (unless you work at Anaconda!), but most of the work in this repo will be reproducible by you since my first task will be to reproduce the anaconda-package-data download. 

## Techniques and Infrastructure

* RAPIDS GPU conda environment spec can be found int eh `rapids-env.yml` file somewhere in this repository.
  Materialize the env with `conda env create -f rapids-env.yml`.
  Create a kernel with `conda activate rapidsai`, followed by `python -m ipykernel install --user --name rapidsai`
* Spark environment is likely going to be Spark 2.4 and it will definitely be running on AWS EMR
* Dask environment spec can be found in the `dask-env.yml` file somewhere in this repo.
  Materialize the env with `conda env create -f dask-env.yml`
  Create a kernel with `conda activate dask`, followed by `python -m ipykernel install --user --name dask`

### Local development machine

* ryzen 5 2600 (6 core)
* 32GB ram
* nvidia RTX 2070

### AWS EMR cluster

#### Copy data from s3 to hdfs

1. try hadoop distcp. FAIL
2. try s3-dist-cp. FAIL
3. resize emr master node
4. aws s3 cp to emr master node
5. hdfs dfs -put to hdfs cluster

```
# Make the directories on hdfs where we will put our files
$ find anaconda-parsed-logs/ -type d | awk '{ print length(), $0 | "sort -n" }' | cut -d " " -f2 | xargs -I{} hdfs dfs -mkdir hdfs:///user/edill/{}

# Upload the parquet files individually
$ find anaconda-parsed-logs/ -name "*.parquet" | xargs -L1 -I{} hdfs dfs -put {} hdfs:///user/edill/{}
```

### AWS GPU ec2 instance