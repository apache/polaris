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
package org.apache.polaris.service.quarkus.auth.external;

import io.smallrye.config.WithDefault;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/** Polaris-specific, per-tenant configuration for OIDC authentication. */
public interface OidcTenantConfiguration {

  /**
   * Identity mapping configuration for this OIDC tenant. This configuration is used to map claims
   * in the JWT token to Polaris principals.
   */
  PrincipalMapper principalMapper();

  /**
   * Roles mapping configuration for this OIDC tenant. This configuration is used to map claims in
   * the JWT token to Polaris principal roles.
   */
  PrincipalRolesMapper principalRolesMapper();

  interface PrincipalMapper {

    /**
     * The path to the claim that contains the principal ID. Nested paths can be expressed using "/"
     * as a separator, e.g. {@code "resource_access/client1/roles"} would look for the "roles" field
     * inside the "client1" object inside the "resource_access" object in the token claims.
     *
     * <p>Optional. Either this option or {@link #nameClaimPath()} must be provided.
     */
    Optional<String> idClaimPath();

    /**
     * The claim that contains the principal name. Nested paths can be expressed using "/" as a
     * separator, e.g. {@code "resource_access/client1/roles"} would look for the "roles" field
     * inside the "client1" object inside the "resource_access" object in the token claims.
     *
     * <p>Optional. Either this option or {@link #idClaimPath()} must be provided.
     */
    Optional<String> nameClaimPath();

    /**
     * The type of the principal mapper. Must be a registered {@link
     * org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalMapper} identifier.
     */
    @WithDefault("default")
    String type();
  }

  interface PrincipalRolesMapper {

    /**
     * The type of the principal roles mapper. Must be a registered {@link
     * org.apache.polaris.service.quarkus.auth.external.mapping.PrincipalRolesMapper} identifier.
     */
    @WithDefault("default")
    String type();

    /**
     * A regular expression that matches the role names in the identity. Only roles that match this
     * regex will be included in the Polaris-specific roles.
     */
    Optional<String> filter();

    /** A list of regex mappings that will be applied to each role name in the identity. */
    List<RegexMapping> mappings();

    default Predicate<String> filterPredicate() {
      return role -> filter().isEmpty() || role.matches(filter().get());
    }

    default Function<String, String> mapperFunction() {
      return role -> {
        for (RegexMapping mapping : mappings()) {
          role = mapping.replace(role);
        }
        return role;
      };
    }

    interface RegexMapping {

      /**
       * A regular expression that will be applied to each role name in the identity. Along with
       * {@link #replacement()}, this regex is used to transform the role names in the identity into
       * Polaris-specific roles.
       */
      String regex();

      /**
       * The replacement string for the role names in the identity. This is used along with {@link
       * #regex()} to transform the role names in the identity into Polaris-specific roles.
       */
      String replacement();

      default String replace(String role) {
        return role.replaceAll(regex(), replacement());
      }
    }
  }
}
