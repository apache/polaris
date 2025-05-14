#!/bin/sh
set -e

POLARIS_SERVICE_URL="http://polaris:8181"
POLARIS_MGMT_API_URL="${POLARIS_SERVICE_URL}/api/management/v1/catalogs"
POLARIS_TOKEN_URL="${POLARIS_SERVICE_URL}/api/catalog/v1/oauth/tokens"
POLARIS_ADMIN_USER="root"
POLARIS_ADMIN_PASS="s3cr3t"
POLARIS_REALM="POLARIS_MINIO_REALM"

echo "Waiting for Polaris service to be healthy..."
attempt_counter=0
max_attempts=20
until curl -s -f "${POLARIS_SERVICE_URL}/q/health/live" > /dev/null; do
  if [ ${attempt_counter} -eq ${max_attempts} ]; then
    echo "Max attempts reached. Failed to connect to Polaris health check."
    exit 1
  fi
  echo "Attempting to connect to Polaris (${attempt_counter}/${max_attempts})..."
  attempt_counter=$((attempt_counter+1))
  sleep 5
done
echo "Polaris service is live."

echo "Attempting to get Polaris admin token..."
ADMIN_TOKEN_RESPONSE=$(curl -s -w "%{http_code}" -X POST "${POLARIS_TOKEN_URL}" \
  --user "${POLARIS_ADMIN_USER}:${POLARIS_ADMIN_PASS}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" -d "scope=PRINCIPAL_ROLE:ALL" -d "realmName=${POLARIS_REALM}")

HTTP_CODE=$(echo "$ADMIN_TOKEN_RESPONSE" | tail -n1)
TOKEN_BODY=$(echo "$ADMIN_TOKEN_RESPONSE" | sed '$d')

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "Failed to get Polaris admin token. HTTP Code: $HTTP_CODE. Response:"
  echo "$TOKEN_BODY"
  exit 1
fi
ADMIN_TOKEN=$(echo "$TOKEN_BODY" | jq -r .access_token)
if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then echo "Failed to parse admin token"; exit 1; fi
echo "Polaris admin token obtained."

CATALOG_NAME="minio_catalog"
BUCKET_NAME="polaris-bucket"
CATALOG_WAREHOUSE_PATH="s3a://${BUCKET_NAME}/iceberg_warehouse/${CATALOG_NAME}"

S3_ACCESS_KEY="${POLARIS_S3_USER}" # Polaris service's S3 user
S3_SECRET_KEY="${POLARIS_S3_PASSWORD}"
S3_ENDPOINT="http://minio:9000"

CREATE_CATALOG_PAYLOAD=$(cat <<EOF
{
  "name": "${CATALOG_NAME}",
  "type": "INTERNAL",
  "properties": {
    "warehouse": "${CATALOG_WAREHOUSE_PATH}",
    "storage.type": "s3",
    "s3.endpoint": "${S3_ENDPOINT}",
    "s3.access-key-id": "${S3_ACCESS_KEY}",
    "s3.secret-access-key": "${S3_SECRET_KEY}",
    "s3.path-style-access": "true",
    "client.region": "us-east-1"
  },
  "readOnly": false
}
EOF
)

echo "Attempting to create/verify catalog '${CATALOG_NAME}'..."
# ... (Rest of the catalog creation/verification logic from previous response, ensuring it uses $ADMIN_TOKEN)
STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${ADMIN_TOKEN}" "${POLARIS_MGMT_API_URL}/${CATALOG_NAME}")

if [ "$STATUS_CODE" -eq 200 ]; then
  echo "Catalog '${CATALOG_NAME}' already exists. Skipping creation."
elif [ "$STATUS_CODE" -eq 404 ]; then
  echo "Catalog '${CATALOG_NAME}' not found. Attempting to create..."
  CREATE_RESPONSE_CODE=$(curl -s -o /tmp/create_catalog_response.txt -w "%{http_code}" -X POST "${POLARIS_MGMT_API_URL}" \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "${CREATE_CATALOG_PAYLOAD}")

  if [ "$CREATE_RESPONSE_CODE" -eq 201 ] || [ "$CREATE_RESPONSE_CODE" -eq 200 ]; then
    echo "Catalog '${CATALOG_NAME}' creation request successful (HTTP ${CREATE_RESPONSE_CODE})."
  else
    echo "Failed to create catalog '${CATALOG_NAME}'. HTTP Status: ${CREATE_RESPONSE_CODE}. Response:"
    cat /tmp/create_catalog_response.txt
    exit 1
  fi
else
  echo "Unexpected status code ${STATUS_CODE} when checking for catalog '${CATALOG_NAME}'."
  curl -I -s -H "Authorization: Bearer ${ADMIN_TOKEN}" "${POLARIS_MGMT_API_URL}/${CATALOG_NAME}"
  exit 1
fi
echo "Catalog '${CATALOG_NAME}' setup script completed."
