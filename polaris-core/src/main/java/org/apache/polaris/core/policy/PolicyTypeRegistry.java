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
package org.apache.polaris.core.policy;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for custom (non-predefined) policy types.
 *
 * <p>This registry serves as a fallback when resolving policy types that are not part of {@link
 * PredefinedPolicyTypes}. It is primarily intended for testing purposes, allowing tests to register
 * custom policy types without polluting the production enum.
 *
 * <p>The resolution order is:
 *
 * <ol>
 *   <li>{@link PredefinedPolicyTypes} - checked first
 *   <li>{@link PolicyTypeRegistry} - fallback for custom types
 * </ol>
 *
 * <p>Usage in tests:
 *
 * <pre>{@code
 * @BeforeEach
 * void setUp() {
 *   PolicyTypeRegistry.register(TestNonInheritablePolicyType.INSTANCE);
 * }
 *
 * @AfterEach
 * void tearDown() {
 *   PolicyTypeRegistry.unregister(TestNonInheritablePolicyType.INSTANCE);
 * }
 * }</pre>
 */
public final class PolicyTypeRegistry {

  private static final Map<Integer, PolicyType> REGISTRY_BY_CODE = new ConcurrentHashMap<>();
  private static final Map<String, PolicyType> REGISTRY_BY_NAME = new ConcurrentHashMap<>();

  private PolicyTypeRegistry() {
    // Utility class - prevent instantiation
  }

  /**
   * Registers a custom policy type. The policy type will be resolvable via {@link #fromCode(int)}
   * and {@link #fromName(String)}.
   *
   * @param policyType the custom policy type to register
   * @throws IllegalArgumentException if a policy type with the same code or name is already
   *     registered
   */
  @VisibleForTesting
  public static void register(PolicyType policyType) {
    if (REGISTRY_BY_CODE.containsKey(policyType.getCode())) {
      throw new IllegalArgumentException(
          "Policy type with code " + policyType.getCode() + " is already registered");
    }
    if (REGISTRY_BY_NAME.containsKey(policyType.getName())) {
      throw new IllegalArgumentException(
          "Policy type with name '" + policyType.getName() + "' is already registered");
    }
    REGISTRY_BY_CODE.put(policyType.getCode(), policyType);
    REGISTRY_BY_NAME.put(policyType.getName(), policyType);
  }

  /**
   * Unregisters a custom policy type.
   *
   * @param policyType the custom policy type to unregister
   */
  @VisibleForTesting
  public static void unregister(PolicyType policyType) {
    REGISTRY_BY_CODE.remove(policyType.getCode());
    REGISTRY_BY_NAME.remove(policyType.getName());
  }

  /** Clears all registered custom policy types. Intended for test cleanup. */
  @VisibleForTesting
  public static void clear() {
    REGISTRY_BY_CODE.clear();
    REGISTRY_BY_NAME.clear();
  }

  /**
   * Retrieves a custom policy type by its code.
   *
   * @param code the type code
   * @return the policy type, or {@code null} if not found
   */
  public static @Nullable PolicyType fromCode(int code) {
    return REGISTRY_BY_CODE.get(code);
  }

  /**
   * Retrieves a custom policy type by its name.
   *
   * @param name the policy type name
   * @return the policy type, or {@code null} if not found
   */
  public static @Nullable PolicyType fromName(String name) {
    return REGISTRY_BY_NAME.get(name);
  }
}
