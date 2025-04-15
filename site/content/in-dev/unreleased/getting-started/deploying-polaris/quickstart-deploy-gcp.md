---
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
Title: Deploying Polaris on Google Cloud Platform (GCP)
type: docs
weight: 330
---

Build and launch Polaris using the AWS Startup Script at the location provided in the command below. This script will start a [Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres) instance, which will be used as the backend Postgres instance holding all Polaris data.
Additionally, Polaris will be bootstrapped to use this database and Docker containers will be spun up for Spark SQL and Trino.

The requirements to run the script below are:
* Install the `gcloud` CLI, if it is not already installed on the GCP VM. Instructions to download the `gcloud` CLI can be found [here](https://cloud.google.com/sdk/docs/install).
* Ensure the `Cloud SQL Admin API` has been enabled in your project and that your VM's Principal has access to the correct role: `roles/cloudsql.admin`.

```shell
chmod +x getting-started/assets/cloud_providers/deploy-gcp.sh
./getting-started/assets/cloud_providers/deploy-gcp.sh
```

Also, set the following static credentials for interacting with the Polaris server in the following exercises: 

```shell
export CLIENT_ID=root
export CLIENT_SECRET=s3cr3t
```

To shut down the Polaris server, run the following commands:

```shell
docker compose -f getting-started/eclipselink/docker-compose.yml down
```

To deploy Polaris in a production setting, please review further recommendations at the [Configuring Polaris for Production]({{% relref "../../configuring-polaris-for-production" %}}) page.