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
package org.apache.polaris.service.quarkus.auth.external.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.json.Json;
import jakarta.json.JsonValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ClaimsLocatorTest {

  private ClaimsLocator claimsLocator;
  private JsonWebToken token;

  @BeforeEach
  public void setup() {
    claimsLocator = new ClaimsLocator();
    token = mock(JsonWebToken.class);
  }

  @Test
  public void nullToken() {
    assertThatThrownBy(() -> claimsLocator.locateClaim("sub", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Claim path cannot be empty");
  }

  @Test
  public void nullPath() {
    assertThatThrownBy(() -> claimsLocator.locateClaim(null, token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Claim path cannot be empty");
  }

  @Test
  public void emptyPath() {
    assertThatThrownBy(() -> claimsLocator.locateClaim("", token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Claim path cannot be empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/foo", "foo//bar", "foo/"})
  public void emtpySegment(String path) {
    assertThatThrownBy(() -> claimsLocator.locateClaim(path, token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Empty segment in claim path: " + path);
  }

  @Test
  public void simpleClaim() {
    when(token.getClaim("sub")).thenReturn("user123");
    Object result = claimsLocator.locateClaim("sub", token);
    assertThat(result).isEqualTo("user123");
  }

  @Test
  public void nestedClaim() {
    Map<String, Object> resourceAccess =
        Map.of("client1", Map.<String, Object>of("roles", List.of("admin", "user")));
    when(token.getClaim("resource_access")).thenReturn(resourceAccess);
    Object result = claimsLocator.locateClaim("resource_access/client1/roles", token);
    assertThat(result).isEqualTo(List.of("admin", "user"));
  }

  @Test
  public void nonExistentClaim() {
    when(token.getClaim("non_existent")).thenReturn(null);
    Object result = claimsLocator.locateClaim("non_existent", token);
    assertThat(result).isNull();
  }

  @Test
  public void nonExistentNestedClaim() {
    when(token.getClaim("resource_access")).thenReturn(Map.of());
    Object result = claimsLocator.locateClaim("resource_access/non_existent/roles", token);
    assertThat(result).isNull();
  }

  @Test
  public void nonMapIntermediateValue() {
    when(token.getClaim("sub")).thenReturn("user123");
    Object result = claimsLocator.locateClaim("sub/roles", token);
    assertThat(result).isNull();
  }

  @Test
  public void namespacedClaim() {
    String namespacedClaimName = "https://any.namespace/roles";
    when(token.getClaim(namespacedClaimName)).thenReturn(List.of("admin", "user"));
    Object result = claimsLocator.locateClaim("\"" + namespacedClaimName + "\"", token);
    assertThat(result).isEqualTo(List.of("admin", "user"));
  }

  @Test
  public void nestedNamespacedClaim() {
    String namespacedClaimName = "https://any.namespace/roles";
    Map<String, Object> resourceMap = Map.of(namespacedClaimName, List.of("admin", "user"));
    when(token.getClaim("resource")).thenReturn(resourceMap);
    Object result = claimsLocator.locateClaim("resource/\"" + namespacedClaimName + "\"", token);
    assertThat(result).isEqualTo(List.of("admin", "user"));
  }

  @Test
  public void namespacedClaimWithNestedPath() {
    String namespacedClaimName = "https://any.namespace/resource_access";
    Map<String, Object> nestedMap = Map.of("roles", List.of("admin", "user"));
    when(token.getClaim(namespacedClaimName)).thenReturn(nestedMap);
    Object result = claimsLocator.locateClaim("\"" + namespacedClaimName + "\"/roles", token);
    assertThat(result).isEqualTo(List.of("admin", "user"));
  }

  @Test
  public void escapedQuotes() {
    when(token.getClaim("claim\"with\"quotes")).thenReturn("value");
    Object result = claimsLocator.locateClaim("\"claim\\\"with\\\"quotes\"", token);
    assertThat(result).isEqualTo("value");
  }

  @Test
  public void unclosedQuotes() {
    assertThatThrownBy(() -> claimsLocator.locateClaim("\"unclosed", token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unclosed quotes in claim path");
  }

  @ParameterizedTest
  @MethodSource
  public void jsonValues(JsonValue jsonValue, Object expected) {
    when(token.getClaim("claim")).thenReturn(jsonValue);
    Object result = claimsLocator.locateClaim("claim", token);
    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> jsonValues() {
    return Stream.of(
        Arguments.of(JsonValue.TRUE, true),
        Arguments.of(JsonValue.FALSE, false),
        Arguments.of(JsonValue.NULL, null),
        Arguments.of(
            Json.createObjectBuilder().add("firstName", "Duke").add("lastName", "Java").build(),
            Map.of("firstName", "Duke", "lastName", "Java")),
        Arguments.of(
            Json.createArrayBuilder().add("Java").add("Python").add("JavaScript").build(),
            List.of("Java", "Python", "JavaScript")),
        Arguments.of(Json.createValue("Hello, World!"), "Hello, World!"),
        Arguments.of(Json.createValue(123), 123L),
        Arguments.of(Json.createValue(123.45), 123.45d));
  }
}
