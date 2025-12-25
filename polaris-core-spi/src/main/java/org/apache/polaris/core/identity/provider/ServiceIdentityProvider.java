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

package org.apache.polaris.core.identity.provider;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;

/**
 * A provider interface for managing and resolving service identities in Polaris.
 *
 * <p>In a multi-tenant Polaris deployment, each catalog or tenant may be associated with a distinct
 * service identity that represents the Polaris service itself when accessing external systems
 * (e.g., cloud services like AWS or GCP). This provider offers a central mechanism to allocate
 * service identities to catalog entities and resolve them at runtime.
 *
 * <p>The provider helps abstract the configuration and retrieval of service-managed credentials
 * from the logic that uses them. It ensures a consistent and secure way to handle identity
 * resolution across different deployment models, including SaaS and self-managed environments.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li><b>Allocation</b>: Assign a service identity to a catalog entity during creation. The
 *       actual credentials are not stored in the entity; only a reference is persisted.
 *   <li><b>Resolution (with credentials)</b>: Retrieve the full identity including credentials for
 *       authentication purposes (e.g., signing requests with SigV4).
 *   <li><b>Resolution (without credentials)</b>: Retrieve the identity information for display in
 *       API responses without exposing sensitive credentials.
 * </ul>
 */
public interface ServiceIdentityProvider {
  /**
   * Allocates a {@link ServiceIdentityInfoDpo} for the given connection configuration. This method
   * is typically invoked during catalog entity creation to associate a service identity with the
   * catalog for accessing external services.
   *
   * <p>The allocation strategy is implementation-specific:
   *
   * <ul>
   *   <li>A vendor may choose to use the same service identity across all entities in an account.
   *   <li>Alternatively, different service identities can be assigned per catalog entity.
   *   <li>The associated DPO stores only a reference to the service identity, not the credentials
   *       themselves.
   * </ul>
   *
   * @param connectionConfig The connection configuration for which a service identity should be
   *     allocated.
   * @return An {@link Optional} containing the allocated {@link ServiceIdentityInfoDpo}, or empty
   *     if no service identity is available or applicable for this connection.
   */
  Optional<ServiceIdentityInfoDpo> allocateServiceIdentity(
      @Nonnull ConnectionConfigInfo connectionConfig);

  /**
   * Retrieves the user-facing {@link ServiceIdentityInfo} model for the given service identity
   * reference, without exposing sensitive credentials.
   *
   * <p>This method is used when generating API responses (e.g., {@code getCatalog}) to return
   * identity details such as the AWS IAM user ARN, but not the actual credentials.
   *
   * @param serviceIdentityInfo The service identity metadata to resolve.
   * @return An {@link Optional} containing the {@link ServiceIdentityInfo} model for API responses,
   *     or empty if the identity cannot be resolved.
   */
  Optional<ServiceIdentityInfo> getServiceIdentityInfo(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo);

  /**
   * Retrieves the service identity credential by resolving the actual credential or secret
   * referenced by the given service identity info, typically from a secret manager or internal
   * credential store.
   *
   * <p>This method is used when Polaris needs to authenticate to external systems using the service
   * identity, such as when signing requests with SigV4 authentication.
   *
   * @param serviceIdentityInfo The service identity metadata to resolve.
   * @return An {@link Optional} containing a {@link ServiceIdentityCredential} with credentials, or
   *     empty if the identity cannot be resolved.
   */
  Optional<ServiceIdentityCredential> getServiceIdentityCredential(
      @Nonnull ServiceIdentityInfoDpo serviceIdentityInfo);
}
