* jupyterlab extension: https://github.com/rapidsai/jupyterlab-nvdashboard
    * Requires:
        * distributed
        * dask_cuda
        * dask_cudf
        
# Create env

```
$ conda env create -f rapids-env.yml
$ conda activate rapids-nightly
$ python -m ipykernel install --user --name rapids-nightly
Installed kernelspec rapids-nightly in /home/ericdill/.local/share/jupyter/kernels/rapids-nightly

# Use kernda to make sure conda activates properly when you start your ipython kernel. Copy the
# path from the previous command and add "kernel.json"
$ kernda /home/ericdill/.local/share/jupyter/kernels/rapids-nightly/kernel.json -o --display-name "Rapids Nightly [py37]"
```
