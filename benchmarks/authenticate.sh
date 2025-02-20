#!/usr/bin/env bash

if [ $# -eq 1 ]; then
  export CLIENT_ID=$(echo "$1" | cut -d: -f1) CLIENT_SECRET=$(echo "$1" | cut -d: -f2)
elif [ $# -eq 2 ]; then
  export CLIENT_ID=$1 CLIENT_SECRET=$2
else
  export CLIENT_ID=root CLIENT_SECRET=s3cr3t
fi

# eval $(grep -aEo 'realm: [^:]*:[^:]*:[^:]*' /tmp/polaris.log | perl -pe 's/realm: [^:]*: ([^:]*):([^:]*)/export CLIENT_ID=$1 CLIENT_SECRET=$2/')

echo export CLIENT_ID=$CLIENT_ID
echo export CLIENT_SECRET=$CLIENT_SECRET
echo export TOKEN=$(
  curl \
    -s \
    -X POST \
    "http://${POLARIS_HOST:-localhost}:8181/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials" \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" \
    -d "scope=PRINCIPAL_ROLE:ALL" |
    jq -r .access_token
)
