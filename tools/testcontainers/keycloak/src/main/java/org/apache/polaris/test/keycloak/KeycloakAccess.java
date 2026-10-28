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

package org.apache.polaris.test.keycloak;

import java.net.URI;
import java.util.Map;

/** A facade interface for accessing Keycloak server functionalities. */
public interface KeycloakAccess {

  /** The claim name used to identify the principal in Keycloak tokens. */
  String PRINCIPAL_NAME_CLAIM = "preferred_username";

  /**
   * Returns the URL of the Keycloak issuer. This is typically {@code
   * https://<keycloak-server>/realms/<realm-name>}.
   */
  URI getIssuerUrl();

  /**
   * Returns the URL of the Keycloak token endpoint. This is typically {@code
   * https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token}.
   */
  URI getTokenEndpoint();

  /**
   * Returns the path of the Keycloak token endpoint. This is typically {@code
   * /realms/<realm-name>/protocol/openid-connect/token}.
   */
  default String getTokenPath() {
    return getIssuerUrl().relativize(getTokenEndpoint()).getPath();
  }

  /** Creates a new role in Keycloak with the specified name. */
  void createRole(String name);

  /** Creates a new user in Keycloak. */
  void createUser(String name, String password);

  /** Assigns a role to a user in Keycloak. Both the role and the user must exist. */
  void assignRoleToUser(String role, String user);

  /** Creates a new service account in Keycloak with the specified client ID and client secret. */
  void createServiceAccount(String clientId, String clientSecret);

  /** Deletes a role in Keycloak with the specified name. */
  void deleteRole(String name);

  /** Deletes a user in Keycloak with the specified name. */
  void deleteUser(String name);

  /** Deletes a service account in Keycloak with the specified client ID. */
  void deleteServiceAccount(String clientId);

  /**
   * Obtains an access token from the Keycloak server. The passed map becomes the POST request body
   * and should contain the grant type and all required parameters.
   */
  String getToken(Map<String, String> params);
}
