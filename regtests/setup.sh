#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Idempotent setup for regression tests. Run manually or let run.sh auto-run.
#
# Warning - first time setup may download large amounts of files
# Warning - may clobber conf/spark-defaults.conf

set -x

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi
SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
DERBY_HOME="/tmp/derby"
ICEBERG_VERSION="1.7.0"
export PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Ensure binaries are downloaded locally
echo 'Verifying Spark binaries...'
if ! [ -f ${SPARK_HOME}/bin/spark-sql ]; then
  echo 'Setting up Spark...'
  if [ -z "${SPARK_VERSION}" ] || [ -z "${SPARK_DISTRIBUTION}" ]; then
    echo 'SPARK_VERSION or SPARK_DISTRIBUTION not set. Please set SPARK_VERSION and SPARK_DISTRIBUTION to the desired version.'
    exit 1
  fi
  if ! [ -f ~/${SPARK_DISTRIBUTION}.tgz ]; then
    echo 'Downloading spark distro...'
    wget -O ~/${SPARK_DISTRIBUTION}.tgz https://dlcdn.apache.org/spark/${SPARK_VERSION}/${SPARK_DISTRIBUTION}.tgz
    if ! [ -f ~/${SPARK_DISTRIBUTION}.tgz ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ~/${SPARK_DISTRIBUTION}.tgz https://dlcdn.apache.org/spark/${SPARK_VERSION}/${SPARK_DISTRIBUTION}.tgz
      fi
    fi
  else
    echo 'Found existing Spark tarball'
  fi
  tar xzvf ~/${SPARK_DISTRIBUTION}.tgz -C ~
  echo 'Done!'
  SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
  SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
else
  echo 'Verified Spark distro already installed.'
fi

# Download the iceberg cloud provider bundles needed
echo 'Verified bundle jars installed.'
if ! [ -f ${SPARK_HOME}/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar  ]; then
    echo 'Download azure bundle jar...'
    wget -O ${SPARK_HOME}/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-azure-bundle/${ICEBERG_VERSION}/iceberg-azure-bundle-${ICEBERG_VERSION}.jar
    if ! [ -f ${SPARK_HOME}/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar  ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ${SPARK_HOME}/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-azure-bundle/${ICEBERG_VERSION}/iceberg-azure-bundle-${ICEBERG_VERSION}.jar
      fi
    fi
else
  echo 'Verified azure bundle jar already installed'
fi
if ! [ -f ${SPARK_HOME}/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar  ]; then
    echo 'Download gcp bundle jar...'
    wget -O ${SPARK_HOME}/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar
    if ! [ -f ${SPARK_HOME}/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar  ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ${SPARK_HOME}/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar
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
spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.AbstractFileSystem.s3.impl org.apache.hadoop.fs.s3a.S3A
spark.sql.variable.substitute true

spark.driver.extraJavaOptions -Dderby.system.home=${DERBY_HOME}

spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type=rest
spark.sql.catalog.polaris.uri=http://${POLARIS_HOST:-localhost}:8181/api/catalog
spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials
spark.sql.catalog.polaris.client.region=us-west-2
EOF
  echo 'Success!'
fi

# cleanup derby home if existed
if [ -d "${DERBY_HOME}" ]; then
  echo "Directory ${DERBY_HOME} exists. Deleting it..."
  rm -rf "${DERBY_HOME}"
fi
# setup python venv and install polaris client library and test dependencies
pushd $SCRIPT_DIR && ./pyspark-setup.sh && popd

# bootstrap dependencies so that future queries don't need to wait for the downloads.
# this is mostly useful for building the Docker image with all needed dependencies
${SPARK_HOME}/bin/spark-sql -e "SELECT 1"
