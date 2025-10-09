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
package org.apache.polaris.core.credentials.connection;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;

/**
 * Vendor for generating connection credentials for remote catalog or storage access.
 *
 * <p>Implementations combine Polaris-managed service identity credentials with user-provided
 * authentication parameters to produce the final credentials needed to connect to external systems.
 *
 * <p>For CDI-based implementations (e.g., auth-type-specific vendors), use the {@code AuthType}
 * annotation to indicate which authentication type(s) they support. The credential manager uses CDI
 * to automatically select the appropriate vendor at runtime.
 *
 * <p><b>Multiple Implementations:</b> If multiple vendors support the same authentication type, use
 * {@code @Priority} to specify precedence. Higher priority values take precedence. Without
 * {@code @Priority}, multiple vendors will cause an {@code IllegalStateException} at runtime.
 *
 * <p>Example:
 *
 * <pre>
 * &#64;ApplicationScoped
 * &#64;AuthType(AuthenticationType.SIGV4)
 * &#64;Priority(200)  // Overrides default implementation
 * public class CustomSigV4Vendor implements ConnectionCredentialVendor { ... }
 * </pre>
 */
public interface ConnectionCredentialVendor {

  /**
   * Generate connection credentials by combining service identity with authentication parameters.
   *
   * <p>The connection configuration contains both the Polaris-managed service identity (e.g., an
   * IAM user) and user-configured authentication settings (e.g., which role to assume, signing
   * region).
   *
   * <p>Implementations should validate that the service identity and authentication parameters are
   * of the expected types using preconditions.
   *
   * @param connectionConfig The connection configuration containing service identity and
   *     authentication parameters
   * @return Connection credentials object containing credentials, properties, and optional
   *     expiration
   */
  @Nonnull
  ConnectionCredentials getConnectionCredentials(@Nonnull ConnectionConfigInfoDpo connectionConfig);
}
