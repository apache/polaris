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

package org.apache.polaris.tools.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

final class ClientCredentialsAuthorizationProvider implements AuthorizationProvider {
  private final URI tokenEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final Optional<String> scope;
  private final HttpClient httpClient;
  private final ObjectMapper mapper;
  private volatile Token tokenCache;

  ClientCredentialsAuthorizationProvider(
      URI tokenEndpoint,
      String clientId,
      String clientSecret,
      Optional<String> scope,
      HttpClient httpClient,
      ObjectMapper mapper) {
    this.tokenEndpoint = Objects.requireNonNull(tokenEndpoint, "tokenEndpoint must not be null");
    this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
    this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret must not be null");
    this.scope = Objects.requireNonNull(scope, "scope must not be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
  }

  @Override
  public Optional<String> authorizationHeader() {
    try {
      Token current = tokenCache;
      Instant now = Instant.now();
      if (current == null || current.expiresAt.minusSeconds(60).isBefore(now)) {
        synchronized (this) {
          current = tokenCache;
          if (current == null || current.expiresAt.minusSeconds(60).isBefore(now)) {
            tokenCache = current = fetchToken();
          }
        }
      }
      return Optional.of("Bearer " + current.accessToken);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Failed to obtain OAuth token", e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain OAuth token", e);
    }
  }

  private Token fetchToken() throws IOException, InterruptedException {
    StringBuilder body = new StringBuilder("grant_type=client_credentials");
    body.append("&client_id=").append(encode(clientId));
    body.append("&client_secret=").append(encode(clientSecret));
    scope.ifPresent(value -> body.append("&scope=").append(encode(value)));

    HttpRequest request =
        HttpRequest.newBuilder(tokenEndpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IOException(
          "OAuth token endpoint returned " + response.statusCode() + ": " + response.body());
    }

    JsonNode root = mapper.readTree(response.body());
    JsonNode tokenNode = root.get("access_token");
    if (tokenNode == null || tokenNode.asText().isEmpty()) {
      throw new IOException("OAuth token response missing access_token");
    }
    long expiresIn = root.path("expires_in").asLong(3600);
    Instant expiresAt = Instant.now().plusSeconds(Math.max(expiresIn, 60));
    return new Token(tokenNode.asText(), expiresAt);
  }

  private String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static final class Token {
    private final String accessToken;
    private final Instant expiresAt;

    private Token(String accessToken, Instant expiresAt) {
      this.accessToken = accessToken;
      this.expiresAt = expiresAt;
    }
  }
}
