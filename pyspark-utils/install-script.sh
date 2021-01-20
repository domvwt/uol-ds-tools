#! /usr/bin/bash

# ----------------------------------------------------------------------------
# PySpark Setup Script
# ----------------------------------------------------------------------------
# Download, install, and configure PySpark on Ubuntu.
# ----------------------------------------------------------------------------

sudo apt-get update -y && \
sudo apt-get install openjdk-8-jre -y && \
wget https://apache.mirrors.nublue.co.uk/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz && \
tar -xzf spark-3.0.1-bin-hadoop3.2.tgz && \
sudo mv spark-3.0.1-bin-hadoop3.2 /opt/spark && \
sudo apt-get install ipython3 pip -y && \
pip install pyspark==3.0 && \
echo 'export SPARK_HOME=/opt/spark' >> ~/.bash_profile && \
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bash_profile && \
echo 'export PYSPARK_DRIVER_PYTHON=/usr/bin/ipython3' >> ~/.bash_profile && \
echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> ~/.bash_profile && \
source ~/.bash_profile && \
echo "Type 'pyspark' on the command line to start session"
