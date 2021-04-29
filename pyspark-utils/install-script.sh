#! /usr/bin/bash

# ----------------------------------------------------------------------------
# PySpark Setup Script
# ----------------------------------------------------------------------------
# Download, install, and configure PySpark on Ubuntu.
# ----------------------------------------------------------------------------

SPARK_VERSION="3.1.1"
HADOOP_VERSION="3.2"

echo "Preparing Spark requirements..."
if ! test -d "/opt/spark"; then
  echo "Updating system..." && \
  sudo apt-get update -y && \
  sudo apt-get install openjdk-8-jre -y && \
  echo "Downloading Spark..." && \
  wget --quiet https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  tar -xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  echo "Installing Python..." && \
  sudo apt-get install ipython3 python3-pip -y && \
  echo "Installing Python libs..." && \
  pip install -Uqq pyspark==${SPARK_VERSION} && \
  sudo mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark  && \
  rm -rf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  echo 'export SPARK_HOME=/opt/spark' >> ~/.bash_profile && \
  echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bash_profile && \
  echo 'export PYSPARK_DRIVER_PYTHON=/usr/bin/ipython3' >> ~/.bash_profile && \
  echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> ~/.bash_profile && \
  source ~/.bash_profile && \
  echo "Complete."
  echo "Enter 'pyspark' on the command line to start session"
else
  echo "Requirements already satisfied."
fi
