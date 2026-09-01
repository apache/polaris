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
package org.apache.polaris.service.auth;

import io.quarkus.security.credential.Credential;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * A Quarkus Security {@link Credential} exposing Polaris-specific attributes.
 *
 * <p>Unless a credential implements {@link
 * org.apache.polaris.service.auth.external.ExternalPolarisCredential}, it is treated as internal by
 * the authenticator and requires a backing principal entity in the Polaris metastore.
 *
 * @see org.apache.polaris.service.auth.internal.InternalPolarisCredential
 * @see org.apache.polaris.service.auth.external.ExternalPolarisCredential
 */
public interface PolarisCredential extends Credential {

  /**
   * The principal name, or null if the credential does not carry one. A name is not guaranteed to
   * be present here; it is the authenticator's responsibility to validate it and reject credentials
   * that lack a required name.
   */
  @Nullable String getPrincipalName();

  /** The principal roles, or empty if the principal has no roles. */
  Set<String> getPrincipalRoles();
}
