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

package org.apache.polaris.core.identity.registry;

import java.util.Optional;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.resolved.ResolvedServiceIdentity;

/**
 * A registry interface for managing and resolving service identities in Polaris.
 *
 * <p>In a multi-tenant Polaris deployment, each catalog or tenant may be associated with a distinct
 * service identity that represents the Polaris service itself when accessing external systems
 * (e.g., cloud services like AWS or GCP). This registry provides a central mechanism to manage
 * those identities and resolve them at runtime.
 *
 * <p>The registry helps abstract the configuration and retrieval of service-managed credentials
 * from the logic that uses them. It ensures a consistent and secure way to handle identity
 * resolution across different deployment models, including SaaS and self-managed environments.
 */
public interface ServiceIdentityRegistry {
  /**
   * Discover a new {@link ServiceIdentityInfoDpo} for the given service identity type. Typically
   * used during entity creation to associate a default or generated identity.
   *
   * @param serviceIdentityType The type of service identity (e.g., AWS_IAM).
   * @return A new {@link ServiceIdentityInfoDpo} representing the discovered service identity.
   */
  ServiceIdentityInfoDpo discoverServiceIdentity(ServiceIdentityType serviceIdentityType);

  /**
   * Resolves the given service identity by retrieving the actual credential or secret referenced by
   * it, typically from a secret manager or internal credential store.
   *
   * @param serviceIdentityInfo The service identity metadata to resolve.
   * @return A {@link ResolvedServiceIdentity} including credentials and other resolved data.
   */
  Optional<ResolvedServiceIdentity> resolveServiceIdentity(
      ServiceIdentityInfoDpo serviceIdentityInfo);
}
