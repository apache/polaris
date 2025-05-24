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

EC2_INSTANCE_ID=$(cat /var/lib/cloud/data/instance-id)

DESCRIBE_INSTANCE=$(aws ec2 describe-instances \
    --instance-ids $EC2_INSTANCE_ID \
    --query 'Reservations[*].Instances[*].{Instance:InstanceId,VPC:VpcId,AZ:Placement.AvailabilityZone,RoleArn:IamInstanceProfile.Arn}' \
    --output json)

CURRENT_VPC=$(echo $DESCRIBE_INSTANCE | jq -r .[0].[0]."VPC")
CURRENT_REGION=$(echo $DESCRIBE_INSTANCE | jq -r .[0].[0]."AZ" | sed 's/.$//')
RAW_ROLE_ARN=$(echo $DESCRIBE_INSTANCE | jq -r .[0].[0]."RoleArn")
export AWS_ROLE_ARN="${RAW_ROLE_ARN/instance-profile/role}"


ALL_SUBNETS=$(aws ec2 describe-subnets \
  --region $CURRENT_REGION \
  --query 'Subnets[*].{SubnetId:SubnetId}' \
  --output json \
  | jq -r '[.[]["SubnetId"]] | join(" ")')

RANDOM_SUFFIX=$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 8)
SUBNET_GROUP_NAME="polaris-db-subnet-group-$RANDOM_SUFFIX"
INSTANCE_NAME="polaris-backend-test-$RANDOM_SUFFIX"

aws rds create-db-subnet-group \
  --db-subnet-group-name $SUBNET_GROUP_NAME \
  --db-subnet-group-description "Apache Polaris Quickstart DB Subnet Group" \
  --subnet-ids $ALL_SUBNETS

DB_INSTANCE_INFO=$(aws rds create-db-instance \
  --db-instance-identifier $INSTANCE_NAME \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username postgres \
  --master-user-password postgres \
  --db-name POLARIS \
  --db-subnet-group-name $SUBNET_GROUP_NAME \
  --allocated-storage 10)

DB_ARN=$(echo $DB_INSTANCE_INFO | jq -r '.["DBInstance"]["DBInstanceArn"]')

DESCRIBE_DB=$(aws rds describe-db-instances --db-instance-identifier $DB_ARN)

until echo $DESCRIBE_DB | jq -e '.["DBInstances"][0] | has("Endpoint")';
do
  echo "sleeping 10s to wait for Postgres DB provisioning..."
  sleep 10
  DESCRIBE_DB=$(aws rds describe-db-instances --db-instance-identifier $DB_ARN)
done

POSTGRES_ADDR=$(echo $DESCRIBE_DB | jq -r '.["DBInstances"][0]["Endpoint"]' | jq -r '"\(.Address):\(.Port)"')
export QUARKUS_DATASOURCE_JDBC_URL=$(printf '%s' "jdbc:postgresql://$POSTGRES_ADDR/POLARIS")
export QUARKUS_DATASOURCE_USERNAME=postgres
export QUARKUS_DATASOURCE_PASSWORD=postgres
echo $QUARKUS_DATASOURCE_JDBC_URL

S3_BUCKET_NAME="polaris-quickstart-s3-$RANDOM_SUFFIX"
echo "S3 Bucket Name: $S3_BUCKET_NAME"

aws s3api create-bucket --bucket $S3_BUCKET_NAME --region $CURRENT_REGION --create-bucket-configuration LocationConstraint=$CURRENT_REGION

export STORAGE_LOCATION="s3://$S3_BUCKET_NAME/quickstart_catalog/"

./gradlew clean :polaris-quarkus-server:assemble :polaris-quarkus-admin:assemble \
       -Dquarkus.container-image.tag=postgres-latest \
       -Dquarkus.container-image.build=true \
       --no-build-cache

docker compose -p polaris -f getting-started/jdbc/docker-compose-bootstrap-db.yml -f getting-started/jdbc/docker-compose.yml up -d
