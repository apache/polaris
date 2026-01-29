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

echo "Remember to change applications.properties!"

read -p "Did you change the applications.properties? (y to proceed) " answer
if ![[ "$answer" == "y" ]]; then
  echo "Aborted."
  exit 1
fi

echo 'IN POLARIS SHELL'
echo 'export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXXX/XXXX/XXXX"'

read -p "Did you export the Slack Webhook URL? (y to proceed) " answer
if ![[ "$answer" == "y" ]]; then
  echo "Aborted."
  exit 1
fi

echo 'export CATALOG_NAME=catalog1'
export CATALOG_NAME=catalog1

echo 'export RESTRICTED_CATALOG_NAME=restricted-catalog1'
export RESTRICTED_CATALOG_NAME=restricted-catalog1

echo 'Creating unrestricted and restricted catalogs...'
echo './polaris --client-id root --client-secret s3cr3t catalogs create $CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$CATALOG_NAME/"'
./polaris --client-id root --client-secret s3cr3t catalogs create $CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$CATALOG_NAME/"
echo './polaris --client-id root --client-secret s3cr3t catalogs create $RESTRICTED_CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$RESTRICTED_CATALOG_NAME/"'
./polaris --client-id root --client-secret s3cr3t catalogs create $RESTRICTED_CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$RESTRICTED_CATALOG_NAME/"

while true; do
  read -p "Ready to delete unrestricted catalog? " answer
  [[ "$answer" == "y" ]] && break
done
echo './polaris --client-id root --client-secret s3cr3t catalogs delete $CATALOG_NAME'
./polaris --client-id root --client-secret s3cr3t catalogs delete $CATALOG_NAME

while true; do
  read -p "Ready to delete RESTRICTED catalog? " answer
  [[ "$answer" == "y" ]] && break
done
echo './polaris --client-id root --client-secret s3cr3t catalogs delete $RESTRICTED_CATALOG_NAME'
./polaris --client-id root --client-secret s3cr3t catalogs delete $RESTRICTED_CATALOG_NAME