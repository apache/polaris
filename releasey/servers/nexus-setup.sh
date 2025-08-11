#!/bin/sh
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

# Minimal Nexus setup script for docker-compose testing environment
set -e

# Install required packages
echo "Installing required packages..."
apk add --no-cache curl docker-cli

NEXUS_URL=${NEXUS_URL:-http://localhost:8081}
TEST_USERNAME=${TEST_USERNAME:-testuser}
TEST_PASSWORD=${TEST_PASSWORD:-testpass123}

echo "Configuring Nexus with test user: ${TEST_USERNAME}"

# Wait for Nexus to be ready (docker-compose healthcheck handles most of this)
for i in $(seq 1 10); do
  if curl -f -s "${NEXUS_URL}/service/rest/v1/status" >/dev/null 2>&1; then
    break
  fi
  echo "Waiting for Nexus... ($i/10)"
  sleep 5
done

# Get admin password (try file first, fallback to default)
if command -v docker >/dev/null && docker exec nexus test -f /nexus-data/admin.password 2>/dev/null; then
  ADMIN_PASSWORD=$(docker exec nexus cat /nexus-data/admin.password 2>/dev/null)
else
  ADMIN_PASSWORD="admin123"
fi

curl -u "admin:${ADMIN_PASSWORD}" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"disclaimer": "Use of Sonatype Nexus Repository - Community Edition is governed by the End User License Agreement at https://links.sonatype.com/products/nxrm/ce-eula. By returning the value from ‘accepted:false’ to ‘accepted:true’, you acknowledge that you have read and agree to the End User License Agreement at https://links.sonatype.com/products/nxrm/ce-eula.", "accepted": true}' \
  'http://localhost:8082/service/rest/v1/system/eula'

# Create test user (ignore if already exists)
curl -u "admin:${ADMIN_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{
        \"userId\": \"${TEST_USERNAME}\",
        \"firstName\": \"Test\",
        \"lastName\": \"User\",
        \"emailAddress\": \"test@example.com\",
        \"password\": \"${TEST_PASSWORD}\",
        \"status\": \"active\",
        \"roles\": [\"nx-admin\"]
      }" \
  "${NEXUS_URL}/service/rest/v1/security/users" 2>/dev/null || true