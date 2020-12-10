# Cluster Utilities

## `run-mapreduce.sh`

* Executes the mapper.py and reduce.py scripts on the cluster.
* Collects useful logs from yarn.
* Prints out and saves any Python errors.

## `appid.py`

* Script for grabbing the application id from yarn logs.

## `get-pyerrors.py`

* Fetch Python stack errors from the yarn log file.
