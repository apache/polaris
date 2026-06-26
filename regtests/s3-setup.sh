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

set -e

# This script initializes an S3-compatible backend for testing
# It creates the test bucket and configures access

AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-http://s3.local:9000}
S3_BUCKET=${AWS_STORAGE_BUCKET:-polaris-test-bucket}
S3_REGION=${AWS_REGION:-us-west-2}

# Helper function to run aws command with correct endpoint and region
function aws_cli() {
    aws --endpoint-url "${AWS_ENDPOINT_URL}" --region "${S3_REGION}" "$@"
}

echo "Setting up S3-compatible storage at ${AWS_ENDPOINT_URL}"

# Wait for the S3-compatible backend to become reachable before issue any API calls.
MAX_WAIT=60
WAIT_INTERVAL=3
elapsed=0
until aws_cli s3api list-buckets > /dev/null 2>&1; do
    if [ "$elapsed" -ge "${MAX_WAIT}" ]; then
        echo "ERROR: S3 backend at ${AWS_ENDPOINT_URL} did not become ready within ${MAX_WAIT}s"
        exit 1
    fi
    echo "Waiting for S3-compatible backend to be ready... (${elapsed} elapsed)"
    sleep "${WAIT_INTERVAL}"
    elapsed=$((elapsed + WAIT_INTERVAL))
done

# Create bucket if it doesn't exist
if ! aws_cli s3api head-bucket --bucket "${S3_BUCKET}" > /dev/null 2>&1; then
    echo "Creating bucket: ${S3_BUCKET}"
    aws_cli s3 mb "s3://${S3_BUCKET}"
else
    echo "Bucket already exists: ${S3_BUCKET}"
fi

echo "S3 setup complete"
