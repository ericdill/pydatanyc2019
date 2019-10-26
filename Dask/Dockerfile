FROM daskgateway/dask-gateway:0.5.0
#FROM continuumio/miniconda:latest

RUN conda install -y -c conda-forge \
    gcsfs=0.3.0 \
    pyarrow=0.14.1 \
    datashader=0.7.0 \
    holoviews=1.12 \
    datashader=0.7.0 \
    dask-ml=1.0.0 \
    dask=2.4.0 \
    distributed=2.4.0 \
    python=3.6 \
    panel=0.6.3 \
    python-snappy \
    git \
    pip \
    tpot \
    pytorch-cpu \
    skorch \
    dask-gateway=0.5.0 && \
    pip install git+https://github.com/dask/gcsfs -U && \
    conda clean -afy

