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
Title: Deploying Polaris on Amazon Web Services (AWS)
type: docs
weight: 310
---

Build and launch Polaris using the AWS Startup Script at the location provided in the command below. This script will start an [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/) instance, which will be used as the backend Postgres instance holding all Polaris data.
Additionally, Polaris will be bootstrapped to use this database and Docker containers will be spun up for Spark SQL and Trino.

The requirements to run the script below are:
* There must be at least two subnets created in the VPC and region in which your EC2 instance reside. The span of subnets MUST include at least 2 availability zones (AZs) within the same region.
* Your EC2 instance must be enabled with [IMDSv1 or IMDSv2 with 2+ hop limit](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-IMDS-new-instances.html#configure-IMDS-new-instances-instance-settings).
* The AWS identity that you will use to run this script must have the following AWS permissions:
  * "ec2:DescribeInstances"
  * "rds:CreateDBInstance"
  * "rds:DescribeDBInstances"
  * "rds:CreateDBSubnetGroup"
  * "sts:AssumeRole" on the same role as the Instance Profile role of the EC2 instance on which you are running this script. Additionally, you should ensure that the Instance Profile contains a trust policy that allows the role to trust itself to be assumed.

```shell
chmod +x getting-started/assets/cloud_providers/deploy-aws.sh
export ASSETS_PATH=$(pwd)/getting-started/assets/
export CLIENT_ID=root
export CLIENT_SECRET=s3cr3t
./getting-started/assets/cloud_providers/deploy-aws.sh
```

## Next Steps
Congrats, you now have a running instance of1 Polaris! For details on how to use Polaris, check out the [Using Polaris]({{% relref "../using-polaris" %}}) page.

## Cleanup Instructions
To shut down the Polaris server, run the following commands:

```shell
export ASSETS_PATH=$(pwd)/getting-started/assets/
docker compose -p polaris -f getting-started/eclipselink/docker-compose.yml down
```

To deploy Polaris in a production setting, please review further recommendations at the [Configuring Polaris for Production]({{% relref "../../configuring-polaris-for-production" %}}) page.
