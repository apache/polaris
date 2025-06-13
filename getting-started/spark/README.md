<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Getting Started with Apache Spark and Apache Polaris

This getting started guide provides a `docker-compose` file to set up [Apache Spark](https://spark.apache.org/) with Apache Polaris. Apache Polaris is configured as an Iceberg REST Catalog in Spark. 
A Jupyter notebook is used to run PySpark.

## Build the Polaris image

If a Polaris image is not already present locally, build one with the following command:

```shell
./gradlew \
  :polaris-quarkus-server:assemble \
  :polaris-quarkus-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

## Run the `docker-compose` file

To start the `docker-compose` file with the necessary dependencies, run this command from the repo's root directory:
```shell
sh getting-started/spark/launch-docker.sh
```

This will spin up 2 container services
* The `polaris` service for running Apache Polaris using an in-memory metastore
* The `jupyter` service for running Jupyter notebook with PySpark

## Access the Jupyter notebook interface
In the Jupyter notebook container log, look for the URL to access the Jupyter notebook. The url should be in the format, `http://127.0.0.1:8888/lab?token=<token>`.

Open the Jupyter notebook in a browser.
Navigate to [`notebooks/SparkPolaris.ipynb`](http://127.0.0.1:8888/lab/tree/notebooks/SparkPolaris.ipynb) <!-- markdown-link-check-disable-line -->

## Run the Jupyter notebook
You can now run all cells in the notebook or write your own code!
