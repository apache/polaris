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
package org.apache.polaris.core.persistence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Base result class for any call to the persistence layer */
public class BaseResult {
  // return code, indicates success or failure
  private final int returnStatusCode;

  // additional information for some error return code
  private final String extraInformation;

  public BaseResult() {
    this.returnStatusCode = ReturnStatus.SUCCESS.getCode();
    this.extraInformation = null;
  }

  public BaseResult(@NotNull BaseResult.ReturnStatus returnStatus) {
    this.returnStatusCode = returnStatus.getCode();
    this.extraInformation = null;
  }

  @JsonCreator
  public BaseResult(
      @JsonProperty("returnStatus") @NotNull BaseResult.ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation) {
    this.returnStatusCode = returnStatus.getCode();
    this.extraInformation = extraInformation;
  }

  public ReturnStatus getReturnStatus() {
    return ReturnStatus.getStatus(this.returnStatusCode);
  }

  public String getExtraInformation() {
    return extraInformation;
  }

  public boolean isSuccess() {
    return this.returnStatusCode == ReturnStatus.SUCCESS.getCode();
  }

  public boolean alreadyExists() {
    return this.returnStatusCode == ReturnStatus.ENTITY_ALREADY_EXISTS.getCode();
  }

  /** Possible return code for the various API calls. */
  public enum ReturnStatus {
    // all good
    SUCCESS(1),

    // an unexpected error was thrown, should result in a 500 error to the client
    UNEXPECTED_ERROR_SIGNALED(2),

    // the specified catalog path cannot be resolved. There is a possibility that by the time a call
    // is made by the client to the persistent storage, something has changed due to concurrent
    // modification(s). The client should retry in that case.
    CATALOG_PATH_CANNOT_BE_RESOLVED(3),

    // the specified entity (and its path) cannot be resolved. There is a possibility that by the
    // time a call is made by the client to the persistent storage, something has changed due to
    // concurrent modification(s). The client should retry in that case.
    ENTITY_CANNOT_BE_RESOLVED(4),

    // entity not found
    ENTITY_NOT_FOUND(5),

    // grant not found
    GRANT_NOT_FOUND(6),

    // entity already exists
    ENTITY_ALREADY_EXISTS(7),

    // entity cannot be dropped, it is one of the bootstrap object like a catalog admin role or the
    // service admin principal role
    ENTITY_UNDROPPABLE(8),

    // Namespace is not empty and cannot be dropped
    NAMESPACE_NOT_EMPTY(9),

    // Catalog is not empty and cannot be dropped. All catalog roles (except the admin catalog
    // role) and all namespaces in the catalog must be dropped before the namespace can be dropped
    CATALOG_NOT_EMPTY(10),

    // The target entity was concurrently modified
    TARGET_ENTITY_CONCURRENTLY_MODIFIED(11),

    // entity cannot be renamed
    ENTITY_CANNOT_BE_RENAMED(12),

    // error caught while sub-scoping credentials. Error message will be returned
    SUBSCOPE_CREDS_ERROR(13),
    ;

    // code for the enum
    private final int code;

    /** constructor */
    ReturnStatus(int code) {
      this.code = code;
    }

    int getCode() {
      return this.code;
    }

    // to efficiently map a code to its corresponding return status
    private static final ReturnStatus[] REVERSE_MAPPING_ARRAY;

    static {
      // find max array size
      int maxCode = 0;
      for (ReturnStatus returnStatus : ReturnStatus.values()) {
        if (maxCode < returnStatus.code) {
          maxCode = returnStatus.code;
        }
      }

      // allocate mapping array
      REVERSE_MAPPING_ARRAY = new ReturnStatus[maxCode + 1];

      // populate mapping array
      for (ReturnStatus returnStatus : ReturnStatus.values()) {
        REVERSE_MAPPING_ARRAY[returnStatus.code] = returnStatus;
      }
    }

    static ReturnStatus getStatus(int code) {
      return code >= REVERSE_MAPPING_ARRAY.length ? null : REVERSE_MAPPING_ARRAY[code];
    }
  }
}
