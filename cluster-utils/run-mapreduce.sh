#!/usr/bin/env bash
INPUT_DIR="../data/raw/200704/"
INPUT_FILE="200704hourly.txt"

echo "Executing mapreduce script..."
echo "Started: `date -u +"%Y-%m-%d--%H%M"`"
echo

hdfs dfs -copyFromLocal "${INPUT_DIR}${INPUT_FILE}"
hdfs dfs -rm -r output
rm -rf output

hadoop jar /opt/hadoop/current/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar \
  -file mapper.py -mapper mapper.py \
  -file reducer.py -reducer reducer.py \
  -input ${INPUT_FILE} \
  -output output \
2>&1 | tee logs.txt \
&& hdfs dfs -copyToLocal output

app_id=`python get-app-id.py`

# Give yarn a chance to collect the logs
sleep 5

# Get the executor logs from yarn
yarn logs -applicationId "${app_id}" > nice_logs.txt

# Print any errors from nice_logs.txt
pyerrors=`python get-pyerrors.py`
if [ ! -z "$pyerrors" ]; then
    echo
    echo -e "\e[31mPython errors found:\e[0m"
    echo -e "$pyerrors"
    echo
    echo -e "\e[31mEnd of Python errors.\e[0m"
    echo
fi

echo "Finished: `date -u +"%Y-%m-%d--%H%M"`"
