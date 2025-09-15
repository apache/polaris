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
title: Doris X Polaris: Building Unified Data Lakehouse with Iceberg REST Catalog - A Practical Guide
date: 2025-09-15
author: zy-kkk
---

With the continuous evolution of data lake technologies, efficiently and securely managing massive datasets stored on object storage (such as AWS S3) while providing unified access endpoints for upstream analytics engines (like [Apache Doris](https://doris.apache.org)) has become a core challenge in modern data architectures. [Apache Polaris](https://polaris.apache.org/), as an open and standardized REST Catalog service for Iceberg, provides an ideal solution to this challenge. It not only handles centralized metadata management but also significantly enhances data lake security and manageability through fine-grained access control and flexible credential management mechanisms.

This document will provide a detailed guide on integrating Apache Doris with Polaris to achieve efficient querying and management of Iceberg data on S3. We'll guide you through the complete process from environment preparation to final data querying step by step

**Through this documentation, you will quickly learn:**

* **AWS Environment Setup**: How to create and configure S3 buckets in AWS, and prepare the necessary IAM roles and policies for both Polaris and Doris, enabling Polaris to access S3 and vend temporary credentials for Doris.

* **Polaris Deployment and Configuration**: How to download and start the Polaris service, and create Iceberg Catalog, Namespace, and corresponding Principal/Role/permissions in Polaris to provide secure metadata access endpoints for Doris.

* **Doris-Polaris Integration**: Explains how Doris obtains metadata access tokens from Polaris via OAuth2, and demonstrates two core underlying storage access methods:

  1. Temporary AK/SK distribution by Polaris (Credential Vending mechanism)

  2. Doris directly using static AK/SK to access S3

## About Apache Doris

[Apache Doris](https://doris.apache.org) is the fastest analytical and search database for the AI era.

It provides high-performance hybrid search capabilities across structured data, semi-structured data (such as JSON), and vector data. It excels at delivering high-concurrency, low-latency queries, while also offering advanced optimization for complex join operations. In addition, Doris can serve as a unified query engine, delivering high-performance analytical services not only on its self-managed internal table format but also on open lakehouse formats such as Iceberg.

With Doris, users can easily build a real-time lakehouse data platform.

## About Apache Polaris

Apache Polaris (Incubating) is a catalog implementation for Apache Iceberg™ tables and is built on the open source Apache Iceberg™ REST protocol.

With Polaris, you can provide centralized, secure read and write access to your Iceberg tables across different REST-compatible query engines.

## Hands-on Guide

### 1. AWS Environment Setup

Before we begin, we need to prepare S3 buckets and corresponding IAM roles on AWS, which form the foundation for Polaris to manage data and Doris to access data.

#### 1.1 Create S3 Bucket

First, we create an S3 bucket named `polaris-doris-test` to store the Iceberg table data that will be created later.

```bash
# Create an S3 bucket
aws s3 mb s3://polaris-doris-test --region us-west-2
# Verify that the bucket was created successfully
aws s3 ls | grep polaris-doris-test
```

#### 1.2 Create IAM Role for Object Storage Access

To implement secure credential management, we need to create an IAM role for Polaris to use through the STS AssumeRole mechanism. This design follows the security best practices of the least privileged principle and separation of duties.

1. Create a trust policy file

	Create the `polaris-trust-policy.json` file:
	
	> Note: Replace YOUR\_ACCOUNT\_ID with your actual AWS account ID, which can be obtained using `aws sts get-caller-identity --query Account --output text`.
	
	```bash
	cat > polaris-trust-policy.json <<EOF
	{
	  "Version": "2012-10-17",
	  "Statement": [
	    {
	      "Effect": "Allow",
	      "Principal": {
	        "AWS": "arn:aws:iam::YOUR_ACCOUNT_ID:root"
	      },
	      "Action": "sts:AssumeRole",
	      "Condition": {
	        "StringEquals": {
	          "sts:ExternalId": "polaris-doris-demo"
	        }
	      }
	    }
	  ]
	}
	EOF
	```

2. Create an IAM Role

	```bash
	aws iam create-role \
	      --role-name polaris-doris-demo \
	      --assume-role-policy-document file:///path/to/polaris-trust-policy.json \
	      --description "IAM Role for Polaris to access S3 storage"
	```

3. Attach S3 access permission policy

	```bash
	# Attach the AmazonS3FullAccess managed policy (for testing only, use fine-grained permissions for production environments)
	aws iam attach-role-policy \
	    --role-name polaris-doris-demo \
	    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
	```

#### 1.3 Bind IAM Role to EC2 Instance (Optional)

> If you do not perform this step, you need to export `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` before starting polaris

If your Polaris service will run on an EC2 instance, it is best to bind an IAM role to the EC2 instance instead of using access keys. This avoids hard-coding credentials in the code and improves security.

1. Create a trust policy for the EC2 instance role

	First, create the trust policy file that allows the EC2 service to assume this role:
	
	```json
	cat > ec2-trust-policy.json <<EOF
	{
	  "Version": "2012-10-17",
	  "Statement": [
	    {
	      "Effect": "Allow",
	      "Principal": {
	        "Service": "ec2.amazonaws.com"
	      },
	      "Action": "sts:AssumeRole"
	    }
	  ]
	}
	EOF
	```

2. Create EC2 Instance Role

	```bash
	aws iam create-role \
	    --role-name polaris-ec2-role \
	    --assume-role-policy-document file:///path/to/ec2-trust-policy.json \
	    --description "IAM Role for EC2 instance running Polaris service"
	```

3. Attach S3 access permission policy

	```bash
	# Attach the AmazonS3FullAccess managed policy
	aws iam attach-role-policy \
	    --role-name polaris-ec2-role \
	    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
	```

4. Create an instance configuration file

	```bash
	# Create an instance profile
	aws iam create-instance-profile \
	    --instance-profile-name polaris-ec2-instance-profile
	
	# Add a role to an instance profile
	aws iam add-role-to-instance-profile \
	    --instance-profile-name polaris-ec2-instance-profile \
	    --role-name polaris-ec2-role
	```

5. Attach the instance profile to the EC2 instance

	```bash
	# If it is a newly created EC2 instance, specify it at startup
	aws ec2 run-instances \
	    --image-id ami-xxxxxxxxx \
	    --instance-type t3.medium \
	    --iam-instance-profile Name=polaris-ec2-instance-profile \
	    --other-parameters...
	
	# If it is an existing EC2 instance, you need to associate the instance profile
	aws ec2 associate-iam-instance-profile \
	    --instance-id i-xxxxxxxxx \
	    --iam-instance-profile Name=polaris-ec2-instance-profile
	```

### 2. Polaris Deployment and Catalog Creation

With the environment ready, we'll now deploy the Polaris service and configure the Iceberg Catalog.

> This document uses the source code quick start method. For more deployment methods, please refer to: https://polaris.apache.org/releases/1.0.1/getting-started/deploying-polaris/

#### 2.1 Clone Source Code and Start Polaris

1. Configure AWS Credentials(Optional)

	If you're not running Polaris on EC2, or if the EC2 instance doesn't have the appropriate IAM Role attached, you need to provide Polaris with AK/SK that has permission to assume the `polaris-doris-demo` role through environment variables.

   ```bash
   export AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
   export AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
   ```

2. Clone Polaris Repository and Switch to Specific Version

	```bash
	git clone https://github.com/apache/polaris.git
	cd polaris
	# Recommend using a released stable version
	git checkout apache-polaris-1.0.1-incubating
	```

3. Run Polaris

   Ensure you have Java 21+ and Docker 27+ installed.

   ```bash
   ./gradlew run -Dpolaris.bootstrap.credentials=POLARIS,root,secret
   ```

   * `POLARIS` is the realm

   * `root` is the `CLIENT_ID`

   * `secret` is the `CLIENT_SECRET`

   * If credentials are not set, it will use preset credentials `POLARIS,root,s3cr3t`

   This command will compile and start the Polaris service, which listens on port 8181 by default.
   
   > You can also use binary distribution, see: https://github.com/apache/polaris/tree/main/runtime/distribution


#### 2.2 Create Catalog and Namespace in Polaris

1. Export ROOT Credentials

   > The `CLIENT_ID` and `CLIENT_SECRET` here are the same as those we set when we started Polaris

   ```bash
   export CLIENT_ID=root
   export CLIENT_SECRET=secret
   ```

2. Create Catalog (Pointing to S3 Storage)

   ```bash
   ./polaris catalogs create \
     --storage-type s3 \
     --default-base-location s3://polaris-doris-test/polaris1 \
     --role-arn arn:aws:iam::<account_id>:role/polaris-doris-demo \
     --external-id polaris-doris-demo \
     doris_catalog
   ```

   * `--storage-type`: Specifies the underlying storage as S3.

   * `--default-base-location`: Default root path for Iceberg table data.

   * `--role-arn`: IAM Role that Polaris service uses to assume for S3 access.

   * `--external-id`: External ID used when assuming the role, must match the configuration in the IAM Role trust policy.

3. Create Namespace

	```bash
	./polaris namespaces create --catalog doris_catalog doris_demo
	```
	
	This creates a namespace (database) named `doris_demo` under `doris_catalog`.

#### 2.3 Polaris Security Roles and Permission Configuration

To allow Doris to access as a `non-root` user, we need to create a new user and role with appropriate permissions.

1. Create Principal Role and Catalog Role

   ```bash
   # Create a Principal Role for aggregating permissions
   ./polaris principal-roles create doris_pr_role

   # Create a Catalog Role under doris_catalog
   ./polaris catalog-roles create --catalog doris_catalog doris_catalog_role
   ```

2. Grant Permissions to Catalog Role

   ```bash
   # Grant doris_catalog_role permission to manage content within the Catalog
   ./polaris privileges catalog grant \
       --catalog doris_catalog \
       --catalog-role doris_catalog_role \
       CATALOG_MANAGE_CONTENT
   ```

3. Associate Principal Role and Catalog Role

   ```bash
   # Assign doris_catalog_role to doris_pr_role
   ./polaris catalog-roles grant \
     --catalog doris_catalog \
     --principal-role doris_pr_role \
     doris_catalog_role
   ```

4. Create New Principal (User) and Bind Role

   ```bash
   # Create a new user (Principal) named doris_user
   ./polaris principals create doris_user
   # Example output: {"clientId": "6e155b128dc06c13", "clientSecret": "ce9fbb4cc91c43ff2955f2c6545239d7"}
   # Please note down this new client_id and client_secret pair, as Doris will use them for connection.

   # Bind doris_user to doris_pr_role
   ./polaris principal-roles grant \
     doris_pr_role \
     --principal doris_user
   ```

   With this, all Polaris-side configuration is complete. We've created a user named `doris_user` that obtains permission to manage `doris_catalog` through `doris_pr_role`.

### 3. Doris-Polaris Integration

Now, we'll create an Iceberg Catalog in Doris that connects to the newly configured Polaris service. Doris supports multiple flexible authentication combinations.

> Note: In this example, we use OAuth2 authentication credential to connect to the Polaris rest service. In addition, Doris also supports using `iceberg.rest.oauth2.token `to directly provide a pre-obtained Bearer Token

#### Method 1: OAuth2 + Temporary Storage Credentials (Credential Vending)

This is the **most recommended** approach. Doris uses OAuth2 credentials to authenticate with Polaris and obtain metadata. When needing to read/write data files on S3, Doris requests a temporary S3 access credential with minimal privileges from Polaris.

**Doris Catalog Creation Statement:**

Use the `clientId` and `clientSecret` generated for `doris_user`.

```sql
CREATE CATALOG polaris_vended PROPERTIES (
    'type' = 'iceberg',
    -- Catalog name in Polaris
    'warehouse' = 'doris_catalog',
    'iceberg.catalog.type' = 'rest',
    -- Polaris service address
    'iceberg.rest.uri' = 'http://YOUR_POLARIS_HOST:8181/api/catalog',
    -- Metadata authentication method
    'iceberg.rest.security.type' = 'oauth2',
    -- Replace with doris_user's client_id:client_secret
    'iceberg.rest.oauth2.credential' = 'client_id:client_secret',
    'iceberg.rest.oauth2.server-uri' = 'http://YOUR_POLARIS_HOST:8181/api/catalog/v1/oauth/tokens',
    'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:doris_pr_role',
    -- Enable credential vending
    'iceberg.rest.vended-credentials-enabled' = 'true',
    -- S3 basic configuration (no keys required)
    's3.endpoint' = 'https://s3.us-west-2.amazonaws.com',
    's3.region' = 'us-west-2'
);
```

#### Method 2: OAuth2 + Static Storage Credentials (AK/SK)

In this approach, Doris still uses OAuth2 to access Polaris metadata, but when accessing S3 data, it uses static AK/SK hardcoded in the Doris Catalog configuration. This method is simple to configure and suitable for quick testing, but has lower security.

**Doris Catalog Creation Statement:**

```sql
CREATE CATALOG polaris_aksk PROPERTIES (
    'type' = 'iceberg',
    'warehouse' = 'doris_catalog',
    'iceberg.catalog.type' = 'rest',
    'iceberg.rest.uri' = 'http://YOUR_POLARIS_HOST:8181/api/catalog',
    'iceberg.rest.security.type' = 'oauth2',
    'iceberg.rest.oauth2.credential' = 'client_id:client_secret',
    'iceberg.rest.oauth2.server-uri' = 'http://YOUR_POLARIS_HOST:8181/api/catalog/v1/oauth/tokens',
    'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:doris_pr_role',
    -- Directly provide S3 access keys
    's3.access_key' = 'YOUR_S3_ACCESS_KEY',
    's3.secret_key' = 'YOUR_S3_SECRET_KEY',
    's3.endpoint' = 'https://s3.us-west-2.amazonaws.com',
    's3.region' = 'us-west-2'
);
```

### 4. Managing Iceberg Table in Doris with Polaris

Regardless of which method you use to create the Catalog, you can manage the Iceberg table with following SQL statements.

```sql
-- Switch to the Catalog you created and the Namespace configured in Polaris
USE polaris_vended.doris_demo;

-- Create an Iceberg table
CREATE TABLE my_iceberg_table (
  id INT,
  name STRING
)
PROPERTIES (
  'write-format'='parquet'
);

-- Insert data
INSERT INTO my_iceberg_table VALUES (1, 'Doris'), (2, 'Polaris');

-- Query data
SELECT * FROM my_iceberg_table;
-- Expected result:
-- +------+---------+
-- | id   | name    |
-- +------+---------+
-- | 1    | Doris   |
-- | 2    | Polaris |
-- +------+---------+
```

If all the above operations succeed, congratulations! You have successfully established the complete data lake pipeline from Doris -> Polaris -> Iceberg on S3.

For more information about managing Iceberg table with Doris, please visit:

https://doris.apache.org/docs/lakehouse/catalogs/iceberg-catalog

