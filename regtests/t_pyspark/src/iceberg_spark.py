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

from typing import Any, Dict, List, Optional, Union
from pyspark.sql import SparkSession


class IcebergSparkSession:
  """Create a Spark session that connects to Polaris.

  The session is expected to be used within a with statement, as in:

  with IcebergSparkSession(
          credentials=f"{client_id}:{client_secret}",
          aws_region='us-west-2',
          polaris_url="http://localhost:8181/api/catalog",
          catalog_name="catalog_name"
  ) as spark:
      spark.sql(f"USE catalog.{hybrid_executor.database}.{hybrid_executor.schema}")
      table_list = spark.sql("SHOW TABLES").collect()
  """

  def __init__(
      self,
      bearer_token: str = None,
      credentials: str = None,
      aws_region: str = 'us-west-2',
      catalog_name: str = None,
      polaris_url: str = None,
      realm: str = 'default-realm',
      iceberg_version: str = '1.7.1'
  ):
    """
    Constructor for Iceberg Spark session. Initializes session parameters and settings.

    Args:
        bearer_token (str, optional): Bearer token for authentication.
        credentials (str, optional): Credentials for accessing the catalog.
        aws_region (str, optional): AWS region for S3. Defaults to 'us-west-2'.
        catalog_name (str, optional): The name of the catalog to use.
        polaris_url (str, optional): The URL for the Polaris service.
        realm (str, optional): The realm used for authentication. Defaults to 'default-realm'.
        iceberg_version (str, optional): Version of Iceberg to use. Defaults to '1.7.1'.
    """
    self.bearer_token = bearer_token
    self.credentials = credentials
    self.aws_region = aws_region
    self.catalog_name = catalog_name
    self.polaris_url = polaris_url
    self.realm = realm
    self.iceberg_version = iceberg_version

  def get_catalog_name(self):
    """
    Get the catalog name.

    Returns:
        str: The catalog name used in the session.
    """
    return self.catalog_name

  def get_session(self):
    """
    Get the Spark session.

    Returns:
        SparkSession: The Spark session instance.
    """
    return self.spark_session

  def sql(self, query: str, args: Optional[Union[Dict[str, Any], List]] = None, **kwargs):
    """
    Wrapper for the sql function of SparkSession.

    Args:
        query (str): The SQL query to execute.
        args (optional): Arguments for the SQL query, either as a dictionary or a list.
        **kwargs: Additional keyword arguments to pass to the Spark SQL function.

    Returns:
        DataFrame: The result of the SQL query as a Spark DataFrame.
    """
    return self.spark_session.sql(query, args, **kwargs)

  def __enter__(self):
    """
    Initializes the Iceberg Spark session and configures it with necessary parameters.

    This method is called when the session is used in a 'with' statement.

    Returns:
        IcebergSparkSession: The initialized IcebergSparkSession object.
    """
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{self.iceberg_version}",
        "org.apache.hadoop:hadoop-aws:3.4.0",
        "software.amazon.awssdk:bundle:2.23.19",
        "software.amazon.awssdk:url-connection-client:2.23.19",
        f"org.apache.iceberg:iceberg-azure-bundle:{self.iceberg_version}",
        f"org.apache.iceberg:iceberg-gcp-bundle:{self.iceberg_version}"
    ]
    excludes = ["org.checkerframework:checker-qual", "com.google.errorprone:error_prone_annotations"]

    packages_string = ",".join(packages)
    excludes_string = ",".join(excludes)
    catalog_name = self.get_catalog_name()

    creds = self.credentials
    cred_config = f"spark.sql.catalog.{catalog_name}.credential"
    if self.bearer_token is not None:
      creds = self.bearer_token
      cred_config = f"spark.sql.catalog.{catalog_name}.token"
    spark_session_builder = (
        SparkSession.builder.config("spark.jars.packages", packages_string)
        .config("spark.jars.excludes", excludes_string)
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
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
        .config(cred_config, creds)
        .config("spark.ui.showConsoleProgress", False)
    )

    self.spark_session = spark_session_builder.getOrCreate()
    self.quiet_logs(self.spark_session.sparkContext)
    return self

  def quiet_logs(self, sc):
    """
    Reduce the verbosity of Spark logs by setting the log level to ERROR.

    Args:
        sc: The Spark context to configure logging for.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """
    Destructor for Iceberg Spark session. Stops the Spark session when exiting the 'with' block.

    Args:
        exc_type: The exception type (if any) raised in the 'with' block.
        exc_val: The exception value (if any) raised in the 'with' block.
        exc_tb: The traceback (if any) raised in the 'with' block.
    """
    self.spark_session.stop()
