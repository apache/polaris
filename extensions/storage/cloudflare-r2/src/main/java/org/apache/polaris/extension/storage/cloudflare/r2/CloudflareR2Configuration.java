/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.extension.storage.cloudflare.r2;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.docs.ConfigDocs;

/**
 * Cloudflare R2 parent API tokens. The {@code R2_LOCAL_SIGNING} credential vending mechanism signs
 * temporary credentials for S3 catalogs from one of these tokens.
 *
 * <p>The realm feature {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME} selects the token. When it is
 * false (the default), every R2 catalog uses the default {@code access-key} and {@code secret-key},
 * and the catalog's {@code storageName} is ignored. When it is true, a catalog without a {@code
 * storageName} uses the default token, and a catalog with a {@code storageName} uses {@code
 * polaris.storage.cloudflare-r2.<storage>.*}. A name without a configured token is refused.
 *
 * <p>A named token needs both {@code access-key} and {@code secret-key}, so a name with only one of
 * them fails the server start.
 *
 * <p>A named token set through environment variables gets its storage name in lower case: {@code
 * POLARIS_STORAGE_CLOUDFLARE_R2_PROD_ACCESS_KEY} sets the access key for the storage name {@code
 * prod}. Use environment variables only for a storage name of lower-case letters and digits with no
 * separator. The server silently ignores a variable such as {@code
 * POLARIS_STORAGE_CLOUDFLARE_R2_TEAM_A_ACCESS_KEY}, so no storage name gets that token. Set the
 * token for any other name in {@code application.properties} or as a {@code -D} system property.
 * Quote a name that holds a dot: {@code polaris.storage.cloudflare-r2."prod.eu".access-key}.
 * Without the quotes, the server start fails.
 *
 * <p>There is no ambient credential chain for R2, so a catalog without a configured token cannot
 * vend credentials. The secret keys exist only in this server configuration. Catalog storage
 * configs carry no secret. An R2 catalog sets an S3 endpoint, so the realm feature {@code
 * ALLOW_SETTING_S3_ENDPOINTS} must stay true (the default).
 *
 * <p>A vended credential has these limits:
 *
 * <ul>
 *   <li>A read-write credential can also delete and list objects within its prefixes.
 *   <li>A table at the bucket root gets a credential for the whole bucket.
 *   <li>A credential stays valid until it expires, and only revoking the parent token ends it
 *       early.
 *   <li>The tokens are server-wide: every realm that allows {@code R2_LOCAL_SIGNING} uses them.
 *   <li>Whoever can create or update a catalog ({@code CATALOG_CREATE} or {@code
 *       CATALOG_WRITE_PROPERTIES}) chooses the buckets, the endpoint, and the storage name within
 *       the parent token's reach, so the parent token should cover only the buckets the realm
 *       needs.
 * </ul>
 *
 * <p>While {@code quarkus.config.mapping.validate-unknown} is true (the default), a server build
 * without this extension aborts at startup with {@code SRCFG00050} on a {@code
 * polaris.storage.cloudflare-r2.*} system property.
 */
@ConfigMapping(prefix = "polaris.storage.cloudflare-r2")
public interface CloudflareR2Configuration {

  /**
   * Access key id of the default Cloudflare R2 parent API token. The parent token must carry Object
   * Read and Write permission on every bucket the catalogs use; a temporary credential cannot
   * exceed it. The default token counts as set only when both {@code access-key} and {@code
   * secret-key} are set.
   */
  @WithName("access-key")
  Optional<String> accessKey();

  /** Secret access key of the default Cloudflare R2 parent API token. */
  @WithName("secret-key")
  Optional<String> secretKey();

  /**
   * Named Cloudflare R2 parent API tokens, keyed by storage name. A catalog uses a named token only
   * when the realm feature {@code RESOLVE_CREDENTIALS_BY_STORAGE_NAME} is true. Both {@code
   * access-key} and {@code secret-key} are required for every name, so a name with one of them
   * fails the server start.
   */
  @WithParentName
  @ConfigDocs.ConfigPropertyName("storage")
  Map<String, NamedToken> storages();

  /** One named Cloudflare R2 parent API token. */
  interface NamedToken {
    /**
     * Access key id of the Cloudflare R2 parent API token for catalogs with this {@code
     * storageName}. The parent token must carry Object Read and Write permission on every bucket
     * those catalogs use; a temporary credential cannot exceed it.
     */
    @WithName("access-key")
    String accessKey();

    /**
     * Secret access key of the Cloudflare R2 parent API token for catalogs with this {@code
     * storageName}.
     */
    @WithName("secret-key")
    String secretKey();
  }
}
