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

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonNumber;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.jwt.JsonWebToken;

/** A utility class for locating claims in a JWT token by path. */
@ApplicationScoped
public class ClaimsLocator {

  /**
   * Locates a claim, possibly a nested one, in a JWT token by path.
   *
   * <p>The path is a string with segments separated by '/' characters. For example,
   * "resource_access/client1/roles" would look for the "roles" field inside the "client1" object
   * inside the "resource_access" object in the token claims.
   *
   * @param claimPath the path to the claim, with segments separated by '/' characters
   * @param token the JWT token
   * @return the claim value, or null if the claim is not found or any segment in the path is null
   */
  @Nullable
  public Object locateClaim(String claimPath, JsonWebToken token) {
    if (claimPath == null || claimPath.isEmpty() || token == null) {
      throw new IllegalArgumentException("Claim path cannot be empty");
    }
    String[] segments = claimPath.split("/");
    if (segments.length == 0) {
      throw new IllegalArgumentException("Claim path cannot be empty");
    }
    Object currentValue = token.getClaim(segments[0]);
    for (int i = 1; i < segments.length; i++) {
      if (currentValue instanceof Map<?, ?> map) {
        currentValue = map.get(segments[i]);
      } else {
        // If the current value is null or isn't a map, we can't navigate further
        return null;
      }
    }
    return currentValue instanceof JsonValue jsonValue ? convert(jsonValue) : currentValue;
  }

  private static Object convert(JsonValue jsonValue) {
    return switch (jsonValue.getValueType()) {
      case TRUE -> true;
      case FALSE -> false;
      case NULL -> null;
      case STRING -> ((JsonString) jsonValue).getString();
      case NUMBER -> {
        JsonNumber jsonNumber = (JsonNumber) jsonValue;
        if (jsonNumber.isIntegral()) yield jsonNumber.longValue();
        yield jsonNumber.doubleValue();
      }
      case ARRAY -> jsonValue.asJsonArray().stream().map(ClaimsLocator::convert).toList();
      case OBJECT ->
          jsonValue.asJsonObject().entrySet().stream()
              .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), convert(e.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    };
  }
}
