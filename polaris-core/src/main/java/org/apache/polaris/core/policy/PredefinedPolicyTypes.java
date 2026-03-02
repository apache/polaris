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
public enum PredefinedPolicyTypes implements PolicyType {
  DATA_COMPACTION(0, "system.data-compaction", true),
  METADATA_COMPACTION(1, "system.metadata-compaction", true),
  ORPHAN_FILE_REMOVAL(2, "system.orphan-file-removal", true),
  SNAPSHOT_EXPIRY(3, "system.snapshot-expiry", true);

  private final int code;
  private final String name;
  private final boolean isInheritable;
  private static final PredefinedPolicyTypes[] REVERSE_CODE_MAPPING_ARRAY;
  private static final ImmutableMap<String, PredefinedPolicyTypes> REVERSE_NAME_MAPPING_ARRAY;

  static {
    int maxId = 0;
    for (PredefinedPolicyTypes policyType : PredefinedPolicyTypes.values()) {
      if (maxId < policyType.code) {
        maxId = policyType.code;
      }
    }

    REVERSE_CODE_MAPPING_ARRAY = new PredefinedPolicyTypes[maxId + 1];
    ImmutableMap.Builder<String, PredefinedPolicyTypes> builder = ImmutableMap.builder();
    // populate both
    for (PredefinedPolicyTypes policyType : PredefinedPolicyTypes.values()) {
      REVERSE_CODE_MAPPING_ARRAY[policyType.code] = policyType;
      builder.put(policyType.name, policyType);
    }
    REVERSE_NAME_MAPPING_ARRAY = builder.build();
  }

  PredefinedPolicyTypes(int code, String name, boolean isInheritable) {
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
   * Retrieves a {@link PredefinedPolicyTypes} instance corresponding to the given type code.
   *
   * @param code the type code of the predefined policy type
   * @return the corresponding {@link PredefinedPolicyTypes}, or {@code null} if no matching type is
   *     found
   */
  @JsonCreator
  public static @Nullable PredefinedPolicyTypes fromCode(int code) {
    if (code < 0 || code >= REVERSE_CODE_MAPPING_ARRAY.length) {
      return null;
    }

    return REVERSE_CODE_MAPPING_ARRAY[code];
  }

  /**
   * Retrieves a {@link PredefinedPolicyTypes} instance corresponding to the given policy name.
   *
   * @param name the name of the predefined policy type
   * @return the corresponding {@link PredefinedPolicyTypes}, or {@code null} if no matching type is
   *     found
   */
  public static @Nullable PredefinedPolicyTypes fromName(String name) {
    return REVERSE_NAME_MAPPING_ARRAY.get(name);
  }
}
