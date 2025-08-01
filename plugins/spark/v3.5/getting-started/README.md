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

# Getting Started with Apache Spark and Apache Polaris With Delta and Iceberg

This getting started guide provides a `docker-compose` file to set up [Apache Spark](https://spark.apache.org/) with Apache Polaris using
the new Polaris Spark Client. 

The Polaris Spark Client enables manage of both Delta and Iceberg tables using Apache Polaris.

A Jupyter notebook is started to run PySpark, and Polaris Python client is also installed to call Polaris APIs
directly through Python Client.

## Build the Spark Client Jar and Polaris image
If Spark Client Jar is not presented locally under plugins/spark/v3.5/build/<scala_version>/libs, please build the jar
using
- `./gradlew assemble` -- build the Polaris project and skip the tests.

If a Polaris image is not already present locally, build one with the following command:

```shell
./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true
```

## Run the `docker-compose` file

To start the `docker-compose` file, run this command from the repo's root directory:
```shell
docker-compose -f plugins/spark/v3.5/getting-started/docker-compose.yml up
```

This will spin up 2 container services
* The `polaris` service for running Apache Polaris using an in-memory metastore
* The `jupyter` service for running Jupyter notebook with PySpark

NOTE: Starting the container first time may take a couple of minutes, because it will need to download the Spark 3.5.6.
When working with Delta, the Polaris Spark Client requires delta-io >= 3.2.1, and it requires at least Spark 3.5.3, 
but the current jupyter Spark image only support Spark 3.5.0.

### Run with AWS access setup
If you want to interact with S3 bucket, make sure you have the following environment variables setup correctly in
your local env before running the `docker-compose` file.
```
AWS_ACCESS_KEY_ID=<your_access_key>
AWS_SECRET_ACCESS_KEY=<your_secret_key>
```

## Access the Jupyter notebook interface
In the Jupyter notebook container log, look for the URL to access the Jupyter notebook. The url should be in the 
format, `http://127.0.0.1:8888/lab?token=<token>`.

Open the Jupyter notebook in a browser.
Navigate to [`notebooks/SparkPolaris.ipynb`](http://127.0.0.1:8888/lab/tree/notebooks/SparkPolaris.ipynb) <!-- markdown-link-check-disable-line -->

If the above url doesn't work, try to replace `127.0.0.1` with `localhost`, for example:
`http://localhost:8888/lab?token=<token>`.

## Run the Jupyter notebook
You can now run all cells in the notebook or write your own code!
