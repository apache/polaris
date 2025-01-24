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

PRINCIPAL_TOKEN="principal:root;realm:default-realm"

# Use local filesystem by default
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://polaris:8181/api/management/v1/catalogs \
  -d '{
        "catalog": {
          "name": "polaris",
          "type": "INTERNAL",
          "readOnly": false,
          "properties": {
            "default-base-location": "file:///tmp/polaris/"
          },
          "storageConfigInfo": {
            "storageType": "FILE",
            "allowedLocations": [
              "file:///tmp"
            ]
          }
        }
      }'

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://polaris:8181/api/management/v1/catalogs/polaris/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}'
