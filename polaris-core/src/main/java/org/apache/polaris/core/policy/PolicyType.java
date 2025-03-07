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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.annotation.Nullable;

/**
 * Represents a policy type in Polaris. A policy type defines a category of policies that may be
 * either predefined or custom (user-defined).
 *
 * <p>A policy type can be either inheritable or non-inheritable. Inheritable policies are passed
 * down to lower-level entities (e.g., from a namespace to a table).
 */
public interface PolicyType {

  /**
   * Retrieves the unique type code associated with this policy type.
   *
   * @return the type code of the policy type
   */
  @JsonValue
  int getCode();

  /**
   * Retrieves the human-readable name of this policy type.
   *
   * @return the name of the policy type
   */
  String getName();

  /**
   * Determines whether this policy type is inheritable.
   *
   * @return {@code true} if the policy type is inheritable, otherwise {@code false}
   */
  boolean isInheritable();

  /**
   * Retrieves a {@link PolicyType} instance corresponding to the given type code.
   *
   * <p>This method searches for the policy type in predefined policy types. If a custom policy type
   * storage mechanism is implemented in the future, it may also check registered custom policy
   * types.
   *
   * @param code the type code of the policy type
   * @return the corresponding {@link PolicyType}, or {@code null} if no matching type is found
   */
  @JsonCreator
  static @Nullable PolicyType fromCode(int code) {
    return PredefinedPolicyType.fromCode(code);
  }

  /**
   * Retrieves a {@link PolicyType} instance corresponding to the given policy name.
   *
   * <p>This method searches for the policy type in predefined policy types. If a custom policy type
   * storage mechanism is implemented in the future, it may also check registered custom policy
   * types.
   *
   * @param name the name of the policy type
   * @return the corresponding {@link PolicyType}, or {@code null} if no matching type is found
   */
  static @Nullable PolicyType fromName(String name) {
    return PredefinedPolicyType.fromName(name);
  }
}
