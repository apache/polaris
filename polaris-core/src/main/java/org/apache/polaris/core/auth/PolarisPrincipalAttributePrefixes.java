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
package org.apache.polaris.core.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.collection.AttributeMap.AttributeKey;

/**
 * Provenance prefixes for authorizer-facing attributes on {@link PolarisPrincipal}.
 *
 * <p>Authenticators set primary attributes such as {@link
 * PolarisPrincipalAttributes#PRINCIPAL_ENTITY_ATTRIBUTE_KEY}. A post-auth augmentor then projects
 * selected values onto these prefixed keys. External authorizers should consume the derived keys
 * rather than walking {@code PrincipalEntity}.
 */
public final class PolarisPrincipalAttributePrefixes {

  /** Prefix for attributes asserted by an authenticator or identity provider. */
  public static final String AUTH_PREFIX = "polaris.auth.";

  /** Prefix for selected Polaris-managed / internal principal facts. */
  public static final String SYSTEM_PREFIX = "polaris.system.";

  /** Prefix for free-form user-defined principal properties. */
  public static final String USER_PREFIX = "polaris.user.";

  /** Derived client id from {@code PrincipalEntity} internal properties. */
  public static final String SYSTEM_CLIENT_ID = SYSTEM_PREFIX + "client_id";

  /** Derived credential-rotation flag from {@code PrincipalEntity} internal properties. */
  public static final String SYSTEM_CREDENTIAL_ROTATION_REQUIRED =
      SYSTEM_PREFIX + "credential-rotation-required";

  private PolarisPrincipalAttributePrefixes() {}

  /** Returns a typed attribute key for a prefixed string attribute. */
  public static AttributeKey<String> stringKey(String key) {
    return new AttributeKey<>(key);
  }

  /**
   * Returns whether {@code key} is a derived, prefixed principal attribute. A bare prefix with no
   * attribute name after it (for example {@code "polaris.user."}) is not a derived key.
   */
  public static boolean isDerivedAttributeKey(String key) {
    return hasPrefixedName(key, AUTH_PREFIX)
        || hasPrefixedName(key, SYSTEM_PREFIX)
        || hasPrefixedName(key, USER_PREFIX);
  }

  private static boolean hasPrefixedName(String key, String prefix) {
    return key != null && key.startsWith(prefix) && key.length() > prefix.length();
  }

  /**
   * Returns the derived string attributes from {@code principal} for external authorizers.
   *
   * <p>Non-string values and keys outside the provenance namespaces (including {@link
   * PolarisPrincipalAttributes#PRINCIPAL_ENTITY_ATTRIBUTE_KEY}) are omitted.
   */
  public static Map<String, String> derivedStringAttributes(PolarisPrincipal principal) {
    return derivedStringAttributes(principal.getAttributes());
  }

  private static Map<String, String> derivedStringAttributes(AttributeMap attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return Map.of();
    }
    Map<String, String> derived = new LinkedHashMap<>();
    attributes.forEach(attribute -> {
      String key = attribute.key().key();
      Object value = attribute.value();
      if (isDerivedAttributeKey(key) && value instanceof String stringValue) {
        derived.put(key, stringValue);
      }
    });
    return Map.copyOf(derived);
  }
}
