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
title: smallrye-polaris_storage_cloudflare_r2
build:
  list: never
  render: never
---

Cloudflare R2 parent API tokens. The `R2_LOCAL_SIGNING` credential vending mechanism signs  temporary credentials for S3 catalogs from one of these tokens.   

The realm feature `RESOLVE_CREDENTIALS_BY_STORAGE_NAME` selects the token. When it is  false (the default), every R2 catalog uses the default `access-key` and `secret-key`,  and the catalog's `storageName` is ignored. When it is true, a catalog without a `storageName` uses the default token, and a catalog with a `storageName` uses `polaris.storage.cloudflare-r2.<storage>.*` . A name without a configured token is refused.   

A named token needs both `access-key` and `secret-key`, so a name with only one of  them fails the server start.   

A named token set through environment variables gets its storage name in lower case: `POLARIS_STORAGE_CLOUDFLARE_R2_PROD_ACCESS_KEY` sets the access key for the storage name `prod` . Use environment variables only for a storage name of lower-case letters and digits with no  separator. The server silently ignores a variable such as `POLARIS_STORAGE_CLOUDFLARE_R2_TEAM_A_ACCESS_KEY` , so no storage name gets that token. Set the  token for any other name in `application.properties` or as a `-D` system property.  Quote a name that holds a dot: `polaris.storage.cloudflare-r2."prod.eu".access-key`.  Without the quotes, the server start fails.   

There is no ambient credential chain for R2, so a catalog without a configured token cannot  vend credentials. The secret keys exist only in this server configuration. Catalog storage  configs carry no secret. An R2 catalog sets an S3 endpoint, so the realm feature `ALLOW_SETTING_S3_ENDPOINTS` must stay true (the default).   

A vended credential has these limits:   

 * A read-write credential can also delete and list objects within its prefixes.    
 * A table at the bucket root gets a credential for the whole bucket.    
 * A credential stays valid until it expires, and only revoking the parent token ends it        early.    
 * The tokens are server-wide: every realm that allows `R2_LOCAL_SIGNING` uses them.    
 * Whoever can create or update a catalog (`CATALOG_CREATE` or `CATALOG_WRITE_PROPERTIES` ) chooses the buckets, the endpoint, and the storage name within        the parent token's reach, so the parent token should cover only the buckets the realm        needs.  

While `quarkus.config.mapping.validate-unknown` is true (the default), a server build  without this extension aborts at startup with `SRCFG00050` on a `polaris.storage.cloudflare-r2.*` system property.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.storage.cloudflare-r2.access-key` |  | `string` | Access key id of the default Cloudflare R2 parent API token. The parent token must carry Object  Read and Write permission on every bucket the catalogs use; a temporary credential cannot  exceed it. The default token counts as set only when both `access-key` and `secret-key` are set.  |
| `polaris.storage.cloudflare-r2.secret-key` |  | `string` | Secret access key of the default Cloudflare R2 parent API token.  |
| `polaris.storage.cloudflare-r2.`_`<storage>`_`.access-key` |  | `string` | Access key id of the Cloudflare R2 parent API token for catalogs with this `storageName` . The parent token must carry Object Read and Write permission on every bucket  those catalogs use; a temporary credential cannot exceed it.  |
| `polaris.storage.cloudflare-r2.`_`<storage>`_`.secret-key` |  | `string` | Secret access key of the Cloudflare R2 parent API token for catalogs with this `storageName` .  |
