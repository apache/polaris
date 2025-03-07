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
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;

/* Represents all predefined policy types in Polaris */
public enum PredefinedPolicyType implements PolicyType {
  DATA_COMPACTION(0, "system.data-compaction", true),
  METADATA_COMPACTION(1, "system.metadata-compaction", true),
  ORPHAN_FILE_REMOVAL(2, "system.orphan-file-removal", true),
  SNAPSHOT_RETENTION(3, "system.snapshot-retention", true);

  private final int code;
  private final String name;
  private final boolean isInheritable;
  private static final PredefinedPolicyType[] REVERSE_CODE_MAPPING_ARRAY;
  private static final ImmutableMap<String, PredefinedPolicyType> REVERSE_NAME_MAPPING_ARRAY;

  static {
    int maxId = 0;
    for (PredefinedPolicyType policyType : PredefinedPolicyType.values()) {
      if (maxId < policyType.code) {
        maxId = policyType.code;
      }
    }

    REVERSE_CODE_MAPPING_ARRAY = new PredefinedPolicyType[maxId + 1];
    ImmutableMap.Builder<String, PredefinedPolicyType> builder = ImmutableMap.builder();
    // populate both
    for (PredefinedPolicyType policyType : PredefinedPolicyType.values()) {
      REVERSE_CODE_MAPPING_ARRAY[policyType.code] = policyType;
      builder.put(policyType.name, policyType);
    }
    REVERSE_NAME_MAPPING_ARRAY = builder.build();
  }

  PredefinedPolicyType(int code, String name, boolean isInheritable) {
    this.code = code;
    this.name = name;
    this.isInheritable = isInheritable;
  }

  /** {@inheritDoc} */
  @Override
  @JsonValue
  public int getCode() {
    return code;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isInheritable() {
    return isInheritable;
  }

  /**
   * Retrieves a {@link PredefinedPolicyType} instance corresponding to the given type code.
   *
   * @param code the type code of the predefined policy type
   * @return the corresponding {@link PredefinedPolicyType}, or {@code null} if no matching type is
   *     found
   */
  @JsonCreator
  public static @Nullable PredefinedPolicyType fromCode(int code) {
    if (code >= REVERSE_CODE_MAPPING_ARRAY.length) {
      return null;
    }

    return REVERSE_CODE_MAPPING_ARRAY[code];
  }

  /**
   * Retrieves a {@link PredefinedPolicyType} instance corresponding to the given policy name.
   *
   * @param name the name of the predefined policy type
   * @return the corresponding {@link PredefinedPolicyType}, or {@code null} if no matching type is
   *     found
   */
  public static @Nullable PredefinedPolicyType fromName(String name) {
    return REVERSE_NAME_MAPPING_ARRAY.get(name);
  }
}
