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
Title: Deploying Polaris on Azure
type: docs
weight: 320
---

Build and launch Polaris using the AWS Startup Script at the location provided in the command below. This script will start an [Azure Database for PostgreSQL - Flexible Server](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/overview) instance, which will be used as the backend Postgres instance holding all Polaris data.
Additionally, Polaris will be bootstrapped to use this database and Docker containers will be spun up for Spark SQL and Trino.

The requirements to run the script below are:
* Install the AZ CLI, if it is not already installed on the Azure VM. Instructions to download the AZ CLI can be found [here](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).
* You must be logged into the AZ CLI. Please run `az account show` to ensure that you are logged in prior to running this script.
* Assign a System-Assigned Managed Identity to the Azure VM.

```shell
chmod +x getting-started/assets/cloud_providers/deploy-azure.sh
export ASSETS_PATH=$(pwd)/getting-started/assets/
export CLIENT_ID=root
export CLIENT_SECRET=s3cr3t
./getting-started/assets/cloud_providers/deploy-azure.sh
```

## Next Steps
Congrats, you now have a running instance of Polaris! For further information regarding how to use Polaris, check out the [Using Polaris]({{% relref "../using-polaris" %}}) page.

## Cleanup Instructions
To shut down the Polaris server, run the following commands:

```shell
export ASSETS_PATH=$(pwd)/getting-started/assets/
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml down
```

To deploy Polaris in a production setting, please review further recommendations at the [Configuring Polaris for Production]({{% relref "../../configuring-polaris-for-production" %}}) page.
