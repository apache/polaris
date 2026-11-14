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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;

/**
 * Production readiness checks for the Cloudflare R2 extension. The checks name property names only
 * and never print a configured value.
 */
@ApplicationScoped
public class CloudflareR2ProductionReadinessChecks {

  private static final String ACCESS_KEY_PROPERTY = "polaris.storage.cloudflare-r2.access-key";
  private static final String SECRET_KEY_PROPERTY = "polaris.storage.cloudflare-r2.secret-key";

  /**
   * Warns when exactly one half of the default parent token is set. The resolver ignores a half-set
   * default token, so R2_LOCAL_SIGNING catalogs that use it cannot vend credentials.
   */
  @Produces
  public ProductionReadinessCheck checkCloudflareR2ParentToken(
      CloudflareR2Configuration configuration) {
    boolean hasAccessKey = configuration.accessKey().isPresent();
    boolean hasSecretKey = configuration.secretKey().isPresent();
    if (hasAccessKey == hasSecretKey) {
      return ProductionReadinessCheck.OK;
    }
    String missingProperty = hasAccessKey ? SECRET_KEY_PROPERTY : ACCESS_KEY_PROPERTY;
    return ProductionReadinessCheck.of(
        Error.of(
            "The default Cloudflare R2 parent token is ignored because "
                + missingProperty
                + " is not set. R2_LOCAL_SIGNING catalogs that use the default token cannot vend"
                + " credentials until both "
                + ACCESS_KEY_PROPERTY
                + " and "
                + SECRET_KEY_PROPERTY
                + " are set.",
            missingProperty));
  }
}
