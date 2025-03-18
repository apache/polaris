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
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;
import org.apache.polaris.core.persistence.dao.entity.ValidateAccessResult;

/** Manage credentials for storage locations. */
public interface PolarisCredentialVendor {
  /**
   * Get a sub-scoped credentials for an entity against the provided allowed read and write
   * locations.
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @return an enum map containing the scoped credentials
   */
  @Nonnull
  ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations);

  /**
   * Validate whether the entity has access to the locations with the provided target operations
   *
   * @param callCtx the polaris call context
   * @param catalogId the catalog id
   * @param entityId the entity id
   * @param actions a set of operation actions: READ/WRITE/LIST/DELETE/ALL
   * @param locations a set of locations to verify
   * @return a Map of {@code <location, validate result>}, a validate result value looks like this
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
  ValidateAccessResult validateAccessToLocations(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long entityId,
      PolarisEntityType entityType,
      @Nonnull Set<PolarisStorageActions> actions,
      @Nonnull Set<String> locations);
}
