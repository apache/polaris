#!/bin/bash

# Idempotent setup for regression tests. Run manually or let run.sh auto-run.
#
# Warning - first time setup may download large amounts of files
# Warning - may clobber conf/spark-defaults.conf

set -x

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=$(realpath ~/spark-3.5.1-bin-hadoop3-scala2.13)
fi
SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
export PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Ensure binaries are downloaded locally
echo 'Verifying Spark binaries...'
if ! [ -f ${SPARK_HOME}/bin/spark-sql ]; then
  echo 'Setting up Spark...'
  if ! [ -f ~/spark-3.5.1-bin-hadoop3-scala2.13.tgz ]; then
    echo 'Downloading spark distro...'
    wget -O ~/spark-3.5.1-bin-hadoop3-scala2.13.tgz https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3-scala2.13.tgz
    if ! [ -f ~/spark-3.5.1-bin-hadoop3-scala2.13.tgz ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ~/spark-3.5.1-bin-hadoop3-scala2.13.tgz https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3-scala2.13.tgz
      fi
    fi
  else
    echo 'Found existing Spark tarball'
  fi
  tar xzvf ~/spark-3.5.1-bin-hadoop3-scala2.13.tgz -C ~
  echo 'Done!'
  SPARK_HOME=$(realpath ~/spark-3.5.1-bin-hadoop3-scala2.13)
  SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
else
  echo 'Verified Spark distro already installed.'
fi

# Download the iceberg cloud provider bundles needed
echo 'Verified bundle jars installed.'
if ! [ -f ${SPARK_HOME}/jars/iceberg-azure-bundle-1.5.2.jar  ]; then
    echo 'Download azure bundle jar...'
    wget -O ${SPARK_HOME}/jars/iceberg-azure-bundle-1.5.2.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-azure-bundle/1.5.2/iceberg-azure-bundle-1.5.2.jar
    if ! [ -f ${SPARK_HOME}/jars/iceberg-azure-bundle-1.5.2.jar  ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ${SPARK_HOME}/jars/iceberg-azure-bundle-1.5.2.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-azure-bundle/1.5.2/iceberg-azure-bundle-1.5.2.jar
      fi
    fi
else
  echo 'Verified azure bundle jar already installed'
fi
if ! [ -f ${SPARK_HOME}/jars/iceberg-gcp-bundle-1.5.2.jar  ]; then
    echo 'Download azure bundle jar...'
    wget -O ${SPARK_HOME}/jars/iceberg-gcp-bundle-1.5.2.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-gcp-bundle/1.5.2/iceberg-gcp-bundle-1.5.2.jar
    if ! [ -f ${SPARK_HOME}/jars/iceberg-gcp-bundle-1.5.2.jar  ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ${SPARK_HOME}/jars/iceberg-gcp-bundle-1.5.2.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-gcp-bundle/1.5.2/iceberg-gcp-bundle-1.5.2.jar
      fi
    fi
else
  echo 'Verified gcp bundle jar already installed'
fi

# Ensure Spark boilerplate conf is set
echo 'Verifying Spark conf...'
if grep 'POLARIS_TESTCONF_V5' ${SPARK_CONF} 2>/dev/null; then
  echo 'Verified spark conf'
else
  echo 'Setting spark conf...'
  # Instead of clobbering existing spark conf, just comment it all out in case it was customized carefully.
  sed -i 's/^/# /' ${SPARK_CONF}
cat << EOF >> ${SPARK_CONF}

# POLARIS_TESTCONF_V5
spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.AbstractFileSystem.s3.impl org.apache.hadoop.fs.s3a.S3A
spark.sql.variable.substitute true

spark.driver.extraJavaOptions -Dderby.system.home=/tmp/derby

spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type=rest
spark.sql.catalog.polaris.uri=http://${POLARIS_HOST:-localhost}:8181/api/catalog
spark.sql.catalog.polaris.warehouse=snowflake
spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=true
spark.sql.catalog.polaris.client.region=us-west-2
EOF
  echo 'Success!'
fi

# setup python venv and install polaris client library and test dependencies
pushd $SCRIPT_DIR && ./pyspark-setup.sh && popd

# bootstrap dependencies so that future queries don't need to wait for the downloads.
# this is mostly useful for building the Docker image with all needed dependencies
${SPARK_HOME}/bin/spark-sql -e "SELECT 1"
