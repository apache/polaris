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

import com.google.common.base.Splitter;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

public class KeycloakTestResource implements QuarkusTestResourceLifecycleManager {

  /**
   * Initialization argument key for specifying roles to be created in the Keycloak server.
   *
   * <p>The value associated with this key should be a comma-separated list of role names.
   */
  public static final String ROLES_ARG = "roles";

  /**
   * Initialization argument key for specifying users to be created in the Keycloak server.
   *
   * <p>The value associated with this key should be a comma-separated list of user entries, where
   * each entry is represented as a key-value pair in the format "username=password".
   */
  public static final String USERS_ARG = "users";

  /**
   * Initialization argument key for specifying role grants to users in the Keycloak server.
   *
   * <p>The value associated with this key should be a comma-separated list of grant entries, where
   * each entry is represented as a key-value pair in the format "username=role".
   */
  public static final String GRANTS_ARG = "grants";

  /**
   * Initialization argument key for specifying service accounts to be created in the Keycloak
   * server.
   *
   * <p>The value associated with this key should be a comma-separated list of client entries, where
   * each entry is represented as a key-value pair in the format "client_id=client_secret".
   */
  public static final String CLIENTS_ARG = "clients";

  private Map<String, String> initArgs = Map.of();
  private KeycloakContainer keycloak;

  @Override
  public void init(Map<String, String> initArgs) {
    this.initArgs = Map.copyOf(initArgs);
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        keycloak, new TestInjector.AnnotatedAndMatchesType(Keycloak.class, KeycloakAccess.class));
  }

  @Override
  public Map<String, String> start() {
    keycloak = new KeycloakContainer();
    keycloak.start();

    var roles = Splitter.on(",").trimResults().omitEmptyStrings().split(initArgs.get(ROLES_ARG));
    var users =
        Splitter.on(",")
            .trimResults()
            .omitEmptyStrings()
            .withKeyValueSeparator("=")
            .split(initArgs.get(USERS_ARG));
    var grants =
        Splitter.on(",")
            .trimResults()
            .omitEmptyStrings()
            .withKeyValueSeparator("=")
            .split(initArgs.get(GRANTS_ARG));
    var clients =
        Splitter.on(",")
            .trimResults()
            .omitEmptyStrings()
            .withKeyValueSeparator("=")
            .split(initArgs.get(CLIENTS_ARG));

    roles.forEach(role -> keycloak.createRole(role));
    users.forEach((user, password) -> keycloak.createUser(user, password));
    grants.forEach((user, role) -> keycloak.assignRoleToUser(role, user));
    clients.forEach((id, secret) -> keycloak.createServiceAccount(id, secret));

    return Map.of("quarkus.oidc.auth-server-url", keycloak.getIssuerUrl().toString());
  }

  @Override
  public void stop() {
    if (keycloak != null) {
      try {
        keycloak.stop();
      } finally {
        keycloak = null;
      }
    }
  }
}
