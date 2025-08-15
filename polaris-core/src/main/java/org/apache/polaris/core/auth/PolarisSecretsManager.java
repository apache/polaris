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

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;

/** Manages secrets for Polaris principals. */
public interface PolarisSecretsManager {
  /**
   * Load the principal secrets given the client_id.
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @return the secrets associated to that principal, including the entity id of the principal
   */
  @Nonnull
  PrincipalSecretsResult loadPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId);

  /**
   * Rotate secrets
   *
   * @param callCtx call context
   * @param clientId principal client id
   * @param principalId id of the principal
   * @param reset true if the principal's secrets should be disabled and replaced with a one-time
   *     password. if the principal's secret is already a one-time password, this flag is
   *     automatically true
   * @param oldSecretHash main secret hash for the principal
   * @return the secrets associated to that principal amd the id of the principal
   */
  @Nonnull
  PrincipalSecretsResult rotatePrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash);

  @Nonnull
  PrincipalSecretsResult resetPrincipalSecrets(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      @Nonnull String oldSecretHash,
      String customClientId,
      String customClientSecret);
}
