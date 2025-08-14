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
package org.apache.polaris.test.commons.keycloak;

import com.google.common.base.Preconditions;
import dasniko.testcontainers.keycloak.ExtendableKeycloakContainer;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UserResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class KeycloakContainer extends ExtendableKeycloakContainer<KeycloakContainer>
    implements KeycloakAccess {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeycloakContainer.class);

  private static final String CONTEXT_PATH = "/realms/master/";

  private URI issuerUrl;
  private URI tokenEndpoint;
  private Keycloak keycloakAdminClient;

  @SuppressWarnings("resource")
  public KeycloakContainer() {
    super(
        ContainerSpecHelper.containerSpecHelper("keycloak", KeycloakContainer.class)
            .dockerImageName(null)
            .asCanonicalNameString());
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
    withEnv("KC_LOG_LEVEL", getRootLoggerLevel() + ",org.keycloak:" + getKeycloakLoggerLevel());
  }

  @Override
  public void start() {
    super.start();
    keycloakAdminClient = getKeycloakAdminClient();
    URI rootUrl = URI.create(getAuthServerUrl());
    issuerUrl = rootUrl.resolve(CONTEXT_PATH);
    tokenEndpoint = issuerUrl.resolve("protocol/openid-connect/token");
    createRole("ALL");
    createUser("root");
    assignRoleToUser("ALL", "root");
    RootCredentialsSet.fromEnvironment()
        .credentials()
        .values()
        .forEach(creds -> createServiceAccount(creds.clientId(), creds.clientSecret()));
  }

  @Override
  public void stop() {
    super.stop();
    if (keycloakAdminClient != null) {
      keycloakAdminClient.close();
      keycloakAdminClient = null;
    }
  }

  @Override
  public URI getIssuerUrl() {
    return issuerUrl;
  }

  @Override
  public URI getTokenEndpoint() {
    return tokenEndpoint;
  }

  @Override
  public void createRole(String roleName) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    RoleRepresentation role = new RoleRepresentation();
    role.setName("PRINCIPAL_ROLE:" + roleName);
    master.roles().create(role);
  }

  @Override
  public void createUser(String name) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    UserRepresentation user = new UserRepresentation();
    user.setEnabled(true);
    user.setUsername(name);
    user.setFirstName(name);
    user.setLastName(name);
    user.setEmail(name + "@polaris.local");
    user.setEmailVerified(true);
    user.setRequiredActions(List.of());
    CredentialRepresentation credential = new CredentialRepresentation();
    credential.setType(CredentialRepresentation.PASSWORD);
    credential.setValue(KeycloakAccess.USER_PASSWORD);
    credential.setTemporary(false);
    user.setCredentials(List.of(credential));
    try (Response response = master.users().create(user)) {
      Preconditions.checkState(
          response.getStatus() == 201, "Failed to create user, status: " + response.getStatus());
    }
  }

  @Override
  public void assignRoleToUser(String role, String user) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    List<UserRepresentation> users = master.users().search(user);
    UserResource userResource = master.users().get(users.getFirst().getId());
    RoleRepresentation roleRepresentation =
        master.roles().get("PRINCIPAL_ROLE:" + role).toRepresentation();
    userResource.roles().realmLevel().add(List.of(roleRepresentation));
  }

  @Override
  public void createServiceAccount(String clientId, String clientSecret) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    ClientRepresentation client = new ClientRepresentation();
    client.setClientId(clientId);
    client.setSecret(clientSecret);
    client.setPublicClient(false);
    client.setServiceAccountsEnabled(true);
    client.setDirectAccessGrantsEnabled(true); // required for password grant
    try (Response response = master.clients().create(client)) {
      Preconditions.checkState(
          response.getStatus() == 201, "Failed to create client, status: " + response.getStatus());
    }
  }

  @Override
  public void deleteRole(String name) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    master.roles().deleteRole("PRINCIPAL_ROLE:" + name);
  }

  @Override
  public void deleteUser(String name) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    List<UserRepresentation> users = master.users().search(name);
    for (UserRepresentation user : users) {
      master.users().get(user.getId()).remove();
    }
  }

  @Override
  public void deleteServiceAccount(String clientId) {
    RealmResource master = keycloakAdminClient.realms().realm("master");
    List<ClientRepresentation> clients = master.clients().findByClientId(clientId);
    for (ClientRepresentation client : clients) {
      master.clients().get(client.getId()).remove();
    }
  }

  private static String getRootLoggerLevel() {
    return LOGGER.isInfoEnabled() ? "INFO" : LOGGER.isWarnEnabled() ? "WARN" : "ERROR";
  }

  private static String getKeycloakLoggerLevel() {
    return LOGGER.isDebugEnabled() ? "DEBUG" : getRootLoggerLevel();
  }
}
