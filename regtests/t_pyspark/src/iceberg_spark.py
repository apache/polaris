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

"""Spark connector with different catalog types."""
from typing import Any, Dict, List, Optional, Union

from pyspark.errors import PySparkRuntimeError
from pyspark.sql import SparkSession


class IcebergSparkSession:
  """Create a Spark session that connects to Polaris.

  The session is expected to be used within a with statement, as in:

  with IcebergSparkSession(
          credentials=f"{client_id}:{client_secret}",
          aws_region='us-west-2',
          polaris_url="http://polaris:8181/api/catalog",
          catalog_name="catalog_name"
  ) as spark:
      spark.sql(f"USE catalog.{hybrid_executor.database}.{hybrid_executor.schema}")
      table_list = spark.sql("SHOW TABLES").collect()
  """

  def __init__(
      self,
      bearer_token: str = None,
      credentials: str = None,
      aws_region: str = "us-west-2",
      catalog_name: str = None,
      polaris_url: str = None,
      realm: str = 'default-realm'
  ):
    """Constructor for Iceberg Spark session. Sets the member variables."""
    self.bearer_token = bearer_token
    self.credentials = credentials
    self.aws_region = aws_region
    self.catalog_name = catalog_name
    self.polaris_url = polaris_url
    self.realm = realm

  def get_catalog_name(self):
    """Get the catalog name of this spark session based on catalog_type."""
    return self.catalog_name

  def get_session(self):
    """Get the real spark session."""
    return self.spark_session

  def sql(self, query: str, args: Optional[Union[Dict[str, Any], List]] = None, **kwargs):
    """Wrapper for the sql function of SparkSession."""
    return self.spark_session.sql(query, args, **kwargs)

  def __enter__(self):
    """Initial method for Iceberg Spark session. Creates a Spark session with specified configs.
    """
    packages = [
      "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0",
      "org.apache.hadoop:hadoop-aws:3.4.0",
      "software.amazon.awssdk:bundle:2.23.19",
      "software.amazon.awssdk:url-connection-client:2.23.19",
    ]
    excludes = ["org.checkerframework:checker-qual", "com.google.errorprone:error_prone_annotations"]

    packages_string = ",".join(packages)
    excludes_string = ",".join(excludes)
    catalog_name = self.get_catalog_name()

    creds = self.credentials
    credConfig = f"spark.sql.catalog.{catalog_name}.credential"
    if self.bearer_token is not None:
      creds = self.bearer_token
      credConfig = f"spark.sql.catalog.{catalog_name}.token"
    spark_session_builder = (
      SparkSession.builder.config("spark.jars.packages", packages_string)
      .config("spark.jars.excludes", excludes_string)
      .config("spark.sql.iceberg.vectorization.enabled", "false")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
      )
      .config(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(f"spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation", "vended-credentials")
      .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
      .config(f"spark.sql.catalog.{catalog_name}.uri", self.polaris_url)
      .config(f"spark.sql.catalog.{catalog_name}.warehouse", self.catalog_name)
      .config(f"spark.sql.catalog.{catalog_name}.scope", 'PRINCIPAL_ROLE:ALL')
      .config(f"spark.sql.catalog.{catalog_name}.header.realm", self.realm)
      .config(f"spark.sql.catalog.{catalog_name}.client.region", self.aws_region)
      .config(credConfig, creds)
      .config("spark.ui.showConsoleProgress", False)
    )

    self.spark_session = spark_session_builder.getOrCreate()
    self.quiet_logs(self.spark_session.sparkContext)
    return self

  def quiet_logs(self, sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Destructor for Iceberg Spark session. Stops the Spark session."""
    self.spark_session.stop()
