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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class KeycloakContainer extends GenericContainer<KeycloakContainer>
    implements KeycloakAccess {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeycloakContainer.class);

  private static final int KEYCLOAK_PORT = 8080;
  private static final String REALM = "master";
  private static final String ADMIN_USERNAME = "admin";
  private static final String ADMIN_PASSWORD = "admin";

  private URI issuerUrl;
  private URI tokenEndpoint;
  private HttpClient httpClient;

  @SuppressWarnings("resource")
  public KeycloakContainer() {
    super(
        ContainerSpecHelper.containerSpecHelper("keycloak", KeycloakContainer.class)
            .dockerImageName(null)
            .asCanonicalNameString());
    withExposedPorts(KEYCLOAK_PORT);
    withEnv("KEYCLOAK_ADMIN", ADMIN_USERNAME);
    withEnv("KEYCLOAK_ADMIN_PASSWORD", ADMIN_PASSWORD);
    withEnv("KC_LOG_LEVEL", getRootLoggerLevel() + ",org.keycloak:" + getKeycloakLoggerLevel());
    withCommand("start-dev");
    waitingFor(
        Wait.forHttp("/realms/" + REALM)
            .forStatusCode(200)
            .withStartupTimeout(Duration.ofMinutes(2)));
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override
  public void start() {
    super.start();
    httpClient = HttpClient.newHttpClient();
    String baseUrl = "http://" + getHost() + ":" + getMappedPort(KEYCLOAK_PORT);
    issuerUrl = URI.create(baseUrl + "/realms/" + REALM + "/");
    tokenEndpoint = issuerUrl.resolve("protocol/openid-connect/token");
  }

  @Override
  public void stop() {
    super.stop();
    httpClient = null;
  }

  @Override
  public URI getIssuerUrl() {
    return issuerUrl;
  }

  @Override
  public URI getTokenEndpoint() {
    return tokenEndpoint;
  }

  /*
   * The methods below were taken from org.keycloak:keycloak-admin-client. We don't depend on that
   * artifact directly to avoid bringing in RESTEasy Classic transitively.
   */

  @Override
  public void createRole(String roleName) {
    int status = adminPost(realmAdminUrl("/roles"), "{\"name\":\"" + roleName + "\"}");
    Preconditions.checkState(
        status == 201, "Failed to create role '%s', status: %s", roleName, status);
  }

  @Override
  public void createUser(String name, String password) {
    String body =
        """
        {
          "enabled": true,
          "username": "%s",
          "firstName": "%s",
          "lastName": "%s",
          "email": "%s@polaris.local",
          "emailVerified": true,
          "requiredActions": [],
          "credentials": [
            {
              "type": "password",
              "value": "%s",
              "temporary": false
            }
          ]
        }"""
            .formatted(name, name, name, name, password);
    int status = adminPost(realmAdminUrl("/users"), body);
    Preconditions.checkState(status == 201, "Failed to create user '%s', status: %s", name, status);
  }

  @Override
  public void assignRoleToUser(String role, String user) {
    String userId = findUserId(user);
    JsonNode roleRep = adminGet(realmAdminUrl("/roles/" + encode(role)));
    String body = "[" + JsonMapper.shared().writeValueAsString(roleRep) + "]";
    int status = adminPost(realmAdminUrl("/users/" + userId + "/role-mappings/realm"), body);
    Preconditions.checkState(
        status == 204, "Failed to assign role '%s' to user '%s', status: %s", role, user, status);
  }

  @Override
  public void createServiceAccount(String clientId, String clientSecret) {
    String body =
        """
        {
          "clientId": "%s",
          "secret": "%s",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "directAccessGrantsEnabled": true
        }"""
            .formatted(clientId, clientSecret);
    int status = adminPost(realmAdminUrl("/clients"), body);
    Preconditions.checkState(
        status == 201, "Failed to create client '%s', status: %s", clientId, status);
  }

  @Override
  public void deleteRole(String name) {
    int status = adminDelete(realmAdminUrl("/roles/" + encode(name)));
    Preconditions.checkState(status == 204, "Failed to delete role '%s', status: %s", name, status);
  }

  @Override
  public void deleteUser(String name) {
    JsonNode users = adminGet(realmAdminUrl("/users?username=" + encode(name)));
    for (JsonNode user : users) {
      if (name.equals(user.get("username").asString())) {
        String id = user.get("id").asString();
        int status = adminDelete(realmAdminUrl("/users/" + id));
        Preconditions.checkState(
            status == 204, "Failed to delete user '%s', status: %s", id, status);
      }
    }
  }

  @Override
  public void deleteServiceAccount(String clientId) {
    JsonNode clients = adminGet(realmAdminUrl("/clients?clientId=" + encode(clientId)));
    for (JsonNode client : clients) {
      String id = client.get("id").asString();
      int status = adminDelete(realmAdminUrl("/clients/" + id));
      Preconditions.checkState(
          status == 204, "Failed to delete client '%s', status: %s", id, status);
    }
  }

  @Override
  public String getToken(Map<String, String> params) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(tokenEndpoint)
              .header("Content-Type", "application/x-www-form-urlencoded")
              .POST(HttpRequest.BodyPublishers.ofString(encode(params)))
              .build();
      HttpResponse<InputStream> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
      Preconditions.checkState(
          response.statusCode() == 200,
          "Failed to invoke token endpoint, status: %s",
          response.statusCode());
      return JsonMapper.shared().readTree(response.body()).get("access_token").asString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to invoke token endpoint", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Failed to invoke token endpoint", e);
    }
  }

  private String getAdminToken() {
    return getToken(
        Map.of(
            "grant_type",
            "password",
            "client_id",
            "admin-cli",
            "username",
            ADMIN_USERNAME,
            "password",
            ADMIN_PASSWORD));
  }

  private String findUserId(String username) {
    JsonNode users = adminGet(realmAdminUrl("/users?username=" + encode(username)));
    for (JsonNode user : users) {
      if (username.equals(user.get("username").asString())) {
        return user.get("id").asString();
      }
    }
    throw new IllegalStateException("User not found: " + username);
  }

  private JsonNode adminGet(URI uri) {
    String token = getAdminToken();
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(uri)
              .header("Authorization", "Bearer " + token)
              .GET()
              .build();
      HttpResponse<InputStream> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
      Preconditions.checkState(
          response.statusCode() == 200,
          "Keycloak admin GET failed (%s): %s",
          response.statusCode(),
          uri);
      return JsonMapper.shared().readTree(response.body());
    } catch (IOException e) {
      throw new UncheckedIOException("Keycloak admin GET failed: " + uri, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Keycloak admin GET failed: " + uri, e);
    }
  }

  private int adminPost(URI uri, String body) {
    String token = getAdminToken();
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(uri)
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    } catch (IOException e) {
      throw new UncheckedIOException("Keycloak admin POST failed: " + uri, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Keycloak admin POST failed: " + uri, e);
    }
  }

  private int adminDelete(URI uri) {
    String token = getAdminToken();
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(uri)
              .header("Authorization", "Bearer " + token)
              .DELETE()
              .build();
      return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    } catch (IOException e) {
      throw new UncheckedIOException("Keycloak admin DELETE failed: " + uri, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Keycloak admin DELETE failed: " + uri, e);
    }
  }

  private URI realmAdminUrl(String subPath) {
    return URI.create(
        "http://"
            + getHost()
            + ":"
            + getMappedPort(KEYCLOAK_PORT)
            + "/admin/realms/"
            + REALM
            + subPath);
  }

  private static String encode(Map<String, String> params) {
    return params.entrySet().stream()
        .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
        .collect(Collectors.joining("&"));
  }

  private static String encode(String value) {
    // Note: URLEncoder is for POSTing form data, not for URL path segments;
    // Here we use it for both out of convenience since the encoding is
    // mostly the same.
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String getRootLoggerLevel() {
    return LOGGER.isInfoEnabled() ? "INFO" : LOGGER.isWarnEnabled() ? "WARN" : "ERROR";
  }

  private static String getKeycloakLoggerLevel() {
    return LOGGER.isDebugEnabled() ? "DEBUG" : getRootLoggerLevel();
  }
}
