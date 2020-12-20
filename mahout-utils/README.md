# Mahout Utilities

## Usage

* Make a copy of `kmeans-config-template.ini` and edit / add configuration values for your project.
* Run `run-kmeans.py <config.ini>` to produce kmeans clusters and evaluation output.

## Module Structure

### `kmeans-config-template.ini`

* Example configuration file for the kmeans programme.

### `run-kmeans.py`

```
usage: run-kmeans.py [-h] [--force-upload] [--force-centroids] [--force-kmeans] [--force-eval] [-i] conf

Automatically run and manage mahout kmeans with simple user configuration files.

positional arguments:
  conf               config file for choosing kmeans parameters

optional arguments:
optional arguments:
  -h, --help           show this help message and exit
  --force-upload       rerun and overwrite raw files that already exist
  --force-centroids    rerun and overwrite centroid files that already exist
  --force-canopy-eval  rerun and overwrite canopy cluster evaluation files that already
                       exist
  --force-kmeans       rerun and overwrite cluster files that already exist
  --force-eval         rerun and overwrite evaluation files that already exist
  -i, --interactive    user prompt on task failure
  ```
