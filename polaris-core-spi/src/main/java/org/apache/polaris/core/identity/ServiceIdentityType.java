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
package org.apache.polaris.core.identity;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;

/**
 * The internal persistence-object counterpart to ServiceIdentityTypeInfo.ServiceIdentityTypeEnum
 * defined in the API model. We define integer type codes in this enum for better compatibility
 * within persisted data in case the names of enum types are ever changed in place.
 *
 * <p>Important: Codes must be kept in-sync with JsonSubTypes annotated within {@link
 * ServiceIdentityInfoDpo}.
 */
public enum ServiceIdentityType {
  NULL_TYPE(0),
  AWS_IAM(1),
  ;

  private static final ServiceIdentityType[] REVERSE_MAPPING_ARRAY;

  static {
    // find max array size
    int maxCode =
        Arrays.stream(ServiceIdentityType.values())
            .mapToInt(ServiceIdentityType::getCode)
            .max()
            .orElse(0);

    // allocate mapping array
    REVERSE_MAPPING_ARRAY = new ServiceIdentityType[maxCode + 1];

    // populate mapping array
    for (ServiceIdentityType authType : ServiceIdentityType.values()) {
      REVERSE_MAPPING_ARRAY[authType.code] = authType;
    }
  }

  private final int code;

  ServiceIdentityType(int code) {
    this.code = code;
  }

  /**
   * Given the code associated with the type, return the associated ServiceIdentityType. Return
   * NULL_TYPE if not found
   *
   * @param identityTypeCode code associated to the service identity type
   * @return ServiceIdentityType corresponding to that code or null if mapping not found
   */
  public static @Nonnull ServiceIdentityType fromCode(int identityTypeCode) {
    // ensure it is within bounds
    if (identityTypeCode < 0 || identityTypeCode >= REVERSE_MAPPING_ARRAY.length) {
      return NULL_TYPE;
    }

    // get value
    return REVERSE_MAPPING_ARRAY[identityTypeCode];
  }

  public int getCode() {
    return this.code;
  }
}
