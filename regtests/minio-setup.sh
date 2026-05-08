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

# This script initializes MinIO for testing
# It creates the test bucket and configures access

MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}
MINIO_BUCKET=${AWS_STORAGE_BUCKET:-polaris-test-bucket}

echo "Setting up MinIO at ${MINIO_ENDPOINT}"

# Configure mc (MinIO client)
mc alias set minio-test ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}

# Create bucket if it doesn't exist
if ! mc ls minio-test/${MINIO_BUCKET} > /dev/null 2>&1; then
    echo "Creating bucket: ${MINIO_BUCKET}"
    mc mb minio-test/${MINIO_BUCKET}
else
    echo "Bucket already exists: ${MINIO_BUCKET}"
fi

# Set anonymous read policy (optional, for easier debugging)
# mc anonymous set download minio-test/${MINIO_BUCKET}

echo "MinIO setup complete"
