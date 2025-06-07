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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.jwt.JsonWebToken;

/** A utility class for locating claims in a JWT token by path. */
@ApplicationScoped
class ClaimsLocator {

  /**
   * Locates a claim, possibly a nested one, in a JWT token by path.
   *
   * <p>The path is a string with segments separated by '/' characters. For example,
   * "resource_access/client1/roles" would look for the "roles" field inside the "client1" object
   * inside the "resource_access" object in the token claims.
   *
   * <p>Paths must not start or end with '/' characters, and segments must not be empty. If a
   * segment is empty, an {@link IllegalArgumentException} will be thrown.
   *
   * <p>Path segments containing '/' characters (such as namespaced claims like {@code
   * "https://namespace/claim"}) should be enclosed in double quotes. For example, {@code
   * "foo/\"https://namespace/claim\"/bar"} would navigate through three segments: {@code "foo"},
   * {@code "https://namespace/claim"}, and {@code "bar"}.
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

    String[] segments = parseClaimPath(claimPath);
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

  /**
   * Parses a claim path into segments, properly handling quoted segments that may contain '/'
   * characters, such as namespaced claims.
   *
   * @param claimPath the claim path to parse
   * @return an array of path segments
   */
  private static String[] parseClaimPath(String claimPath) {
    List<String> segments = new ArrayList<>();
    StringBuilder currentSegment = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < claimPath.length(); i++) {
      char c = claimPath.charAt(i);

      if (c == '"') {
        // Check if this quote is escaped
        if (i > 0 && claimPath.charAt(i - 1) == '\\') {
          // Remove the escape character and add the quote
          currentSegment.setLength(currentSegment.length() - 1);
          currentSegment.append(c);
        } else {
          // Toggle quote mode
          inQuotes = !inQuotes;
        }
      } else if (c == '/' && !inQuotes) {
        // End of segment
        if (currentSegment.isEmpty()) {
          throw new IllegalArgumentException("Empty segment in claim path: " + claimPath);
        }
        segments.add(currentSegment.toString());
        currentSegment.setLength(0);
      } else {
        // Regular character, add to current segment
        currentSegment.append(c);
      }
    }

    // Add the last segment if it's not empty
    if (currentSegment.isEmpty()) {
      throw new IllegalArgumentException("Empty segment in claim path: " + claimPath);
    }
    segments.add(currentSegment.toString());

    if (inQuotes) {
      throw new IllegalArgumentException("Unclosed quotes in claim path: " + claimPath);
    }

    return segments.toArray(new String[0]);
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
