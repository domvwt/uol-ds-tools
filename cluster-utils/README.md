# Cluster Utilities

## Usage

* Open the `run-mapreduce.sh` script and edit the inputs at the top to point to the correct locations.
  + `INPUT_DIR` should point to your data directory.
  + `INPUT_FILE` is the name of the file to upload.
  + `OUTPUT_DIR` is the directory where output should be saved - this is overwritten on each run.
  + `MAPPER_SCRIPT` points to your mapper.
  + `REDUCER_SCRIPT` points to your reducer.
* Save and close `run-mapreduce.sh`.
* Run the script on the cluster - you may need to assign execution privileges by running `chmod +x run-mapreduce.sh`.
* Errors will be printed to the terminal.
* Ouput can be found in the newly created `output` directory.

## Module Structure

### `run-mapreduce.sh`

* Executes the mapper and reducer scripts on the cluster.
* Copies output to the local filesystem.
* Collects useful logs from yarn.
* Prints out and saves any Python errors.

### `get-app-id.py`

* Script for grabbing the application id from yarn logs.

### `get-pyerrors.py`

* Fetch Python stack errors from the yarn log file.
