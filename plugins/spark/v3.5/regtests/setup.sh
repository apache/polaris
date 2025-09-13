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
###################################
# Idempotent setup for spark regression tests. Run manually or let run.sh auto-run.
#
# Warning - first time setup may download large amounts of files
# Warning - may clobber conf/spark-defaults.conf
# Warning - it will set the SPARK_HOME environment variable with the spark setup
#
# The script can be called independently like following
#   ./setup.sh --sparkVersion ${SPARK_VERSION} --scalaVersion ${SCALA_VERSION} --jar ${JAR_PATH}
# Required Parameters:
#   --sparkVersion   : the spark version to setup
#   --scalaVersion   : the scala version of spark to setup
#   --jar            : path to the local Polaris Spark client jar
#

set -x

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

SPARK_VERSION=3.5.6
SCALA_VERSION=2.12
POLARIS_CLIENT_JAR=""
POLARIS_VERSION=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sparkVersion)
      SPARK_VERSION="$2"
      shift # past argument
      shift # past value
      ;;
    --scalaVersion)
      SCALA_VERSION="$2"
      shift # past argument
      shift # past value
      ;;
    --polarisVersion)
      POLARIS_VERSION="$2"
      shift # past argument
      shift # past value
      ;;
    --jar)
      POLARIS_CLIENT_JAR="$2"
      shift # past argument
      shift # past value
      ;;
    --) shift;
      break
      ;;
  esac
done

echo "SET UP FOR SPARK_VERSION=${SPARK_VERSION} SCALA_VERSION=${SCALA_VERSION} POLARIS_VERSION=${POLARIS_VERSION} POLARIS_CLIENT_JAR=${POLARIS_CLIENT_JAR}"

if [ "$SCALA_VERSION" == "2.12" ]; then
  SPARK_DISTRIBUTION=spark-${SPARK_VERSION}-bin-hadoop3
else
  SPARK_DISTRIBUTION=spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}
fi

echo "Getting spark distribution ${SPARK_DISTRIBUTION}"

if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi
SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
DERBY_HOME="/tmp/derby"

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
    wget -O ~/${SPARK_DISTRIBUTION}.tgz https://www.apache.org/dyn/closer.lua/spark/spark-${SPARK_VERSION}/${SPARK_DISTRIBUTION}.tgz?action=download
    if ! [ -f ~/${SPARK_DISTRIBUTION}.tgz ]; then
      if [[ "${OSTYPE}" == "darwin"* ]]; then
        echo "Detected OS: mac. Running 'brew install wget' to try again."
        brew install wget
        wget -O ~/${SPARK_DISTRIBUTION}.tgz https://www.apache.org/dyn/closer.lua/spark/spark-${SPARK_VERSION}/${SPARK_DISTRIBUTION}.tgz?action=download
      fi
    fi
  else
    echo 'Found existing Spark tarball'
  fi
  # check if the download was successful
  if ! [ -f ~/${SPARK_DISTRIBUTION}.tgz ]; then
    echo 'Failed to download Spark distribution. Please check the logs.'
    exit 1
  fi
  tar xzvf ~/${SPARK_DISTRIBUTION}.tgz -C ~/${TEST_ROOT_DIR}
  if [ $? -ne 0 ]; then
    echo 'Failed to extract Spark distribution. Please check the logs.'
    exit 1
  else
    echo 'Extracted Spark distribution.'
    rm ~/${SPARK_DISTRIBUTION}.tgz
  fi
  SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
  SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
else
  echo 'Verified Spark distro already installed.'
fi

echo "SPARK_HOME=${SPARK_HOME}"
echo "SPARK_CONF=${SPARK_CONF}"

# Ensure Spark boilerplate conf is set
echo 'Verifying Spark conf...'
if grep 'POLARIS_TESTCONF_V5' ${SPARK_CONF} 2>/dev/null; then
  echo 'Verified spark conf'
else
  echo 'Setting spark conf...'
  # Instead of clobbering existing spark conf, just comment it all out in case it was customized carefully.
  sed -i 's/^/# /' ${SPARK_CONF}

# If POLARIS_CLIENT_JAR is provided, set the spark conf to use the jars configuration.
# Otherwise use the packages setting
if [[ -z "$POLARIS_CLIENT_JAR" ]]; then
  cat << EOF >> ${SPARK_CONF}
# POLARIS Spark client test conf
spark.jars.packages org.apache.polaris:polaris-spark-3.5_$SCALA_VERSION:$POLARIS_VERSION,io.delta:delta-spark_${SCALA_VERSION}:3.2.1
EOF
else
  cat << EOF >> ${SPARK_CONF}
# POLARIS Spark client test conf
spark.jars $POLARIS_CLIENT_JAR
spark.jars.packages io.delta:delta-spark_${SCALA_VERSION}:3.2.1
EOF
fi

cat << EOF >> ${SPARK_CONF}

spark.sql.variable.substitute true

spark.driver.extraJavaOptions -Dderby.system.home=${DERBY_HOME}

spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension
# this configuration is needed for delta table
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.catalog.polaris=org.apache.polaris.spark.SparkCatalog
spark.sql.catalog.polaris.uri=http://${POLARIS_HOST:-localhost}:8181/api/catalog
# this configuration is currently only used for iceberg tables, generic tables currently
# don't support credential vending
spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials
spark.sql.catalog.polaris.client.region=us-west-2
# configuration required to ensure DataSourceV2 load works correctly for
# different table formats
spark.sql.sources.useV1SourceList=''
EOF
  echo 'Success!'
fi

# cleanup derby home if existed
if [ -d "${DERBY_HOME}" ]; then
  echo "Directory ${DERBY_HOME} exists. Deleting it..."
  rm -rf "${DERBY_HOME}"
fi

echo "Launch spark-sql at ${SPARK_HOME}/bin/spark-sql"
# bootstrap dependencies so that future queries don't need to wait for the downloads.
# this is mostly useful for building the Docker image with all needed dependencies
${SPARK_HOME}/bin/spark-sql -e "SELECT 1"

# ensure SPARK_HOME is setup for later tests
export SPARK_HOME=$SPARK_HOME
