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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;

/**
 * Abstract of Polaris Storage Integration. It holds the reference to an object that having the
 * service principle information
 *
 * @param <T> the concrete type of {@link PolarisStorageConfigurationInfo} this integration supports
 */
public abstract class PolarisStorageIntegration<T extends PolarisStorageConfigurationInfo> {

  private final String integrationIdentifierOrId;
  private final T config;

  public PolarisStorageIntegration(T config, String identifierOrId) {
    this.config = config;
    this.integrationIdentifierOrId = identifierOrId;
  }

  protected T config() {
    return config;
  }

  public String getStorageIdentifierOrId() {
    return integrationIdentifierOrId;
  }

  /**
   * Subscope the creds against the allowed read and write locations.
   *
   * @param realmConfig the call context
   * @param allowListOperation whether to allow LIST on all the provided allowed read/write
   *     locations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @param refreshCredentialsEndpoint an optional endpoint to use for refreshing credentials. If
   *     supported by the storage type it will be returned to the client in the appropriate
   *     properties. The endpoint may be relative to the base URI and the client is responsible for
   *     handling the relative path
   * @return An enum map including the scoped credentials
   */
  public abstract StorageAccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint);

  /**
   * Validate access for the provided operation actions and locations.
   *
   * @param actions a set of operation actions to validate, like LIST/READ/DELETE/WRITE/ALL
   * @param locations a set of locations to get access to
   * @return A Map of string, representing the result of validation, the key value is {@code
   *     <location, validate result>}. A validate result looks like this
   *     <pre>
   * {
   *   "status" : "failure",
   *   "actions" : {
   *     "READ" : {
   *       "message" : "The specified file was not found",
   *       "status" : "failure"
   *     },
   *     "DELETE" : {
   *       "message" : "One or more objects could not be deleted (Status Code: 200; Error Code: null)",
   *       "status" : "failure"
   *     },
   *     "LIST" : {
   *       "status" : "success"
   *     },
   *     "WRITE" : {
   *       "message" : "Access Denied (Status Code: 403; Error Code: AccessDenied)",
   *       "status" : "failure"
   *     }
   *   },
   *   "message" : "Some of the integration checks failed. Check the Polaris documentation for more information."
   * }
   * </pre>
   */
  @Nonnull
  public abstract Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateAccessToLocations(
          @Nonnull RealmConfig realmConfig,
          @Nonnull T storageConfig,
          @Nonnull Set<PolarisStorageActions> actions,
          @Nonnull Set<String> locations);

  /**
   * Result of calling {@link #validateAccessToLocations(RealmConfig,
   * PolarisStorageConfigurationInfo, Set, Set)}
   */
  public static final class ValidationResult {
    private final boolean success;
    private final String message;

    public ValidationResult(boolean success, String message) {
      this.success = success;
      this.message = message;
    }

    public boolean isSuccess() {
      return success;
    }

    public String getMessage() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ValidationResult)) return false;
      ValidationResult that = (ValidationResult) o;
      return success == that.success;
    }

    @Override
    public int hashCode() {
      return Boolean.hashCode(success);
    }

    @Override
    public String toString() {
      return "ValidationResult{" + "success=" + success + ", message='" + message + '\'' + '}';
    }
  }
}
