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
package org.apache.polaris.core.identity.credential;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.SecretReference;
import software.amazon.awssdk.annotations.NotNull;

/**
 * Represents a service identity credential used by Polaris to authenticate to external systems.
 *
 * <p>This class encapsulates both the service identity metadata (e.g., AWS IAM ARN) and the
 * associated credentials (e.g., AWS access keys) needed to authenticate as the Polaris service when
 * accessing external catalog services.
 *
 * <p>The credential contains:
 *
 * <ul>
 *   <li>Identity type (e.g., AWS_IAM)
 *   <li>A {@link SecretReference} pointing to where the credential configuration is stored
 *   <li>The actual authentication credentials (implementation-specific, e.g.,
 *       AwsCredentialsProvider)
 * </ul>
 */
public abstract class ServiceIdentityCredential {
  private final ServiceIdentityType identityType;
  private SecretReference identityInfoReference;

  public ServiceIdentityCredential(@Nonnull ServiceIdentityType identityType) {
    this(identityType, null);
  }

  public ServiceIdentityCredential(
      @Nonnull ServiceIdentityType identityType, @Nullable SecretReference identityInfoReference) {
    this.identityType = identityType;
    this.identityInfoReference = identityInfoReference;
  }

  public @NotNull ServiceIdentityType getIdentityType() {
    return identityType;
  }

  public @Nonnull SecretReference getIdentityInfoReference() {
    return identityInfoReference;
  }

  public void setIdentityInfoReference(@NotNull SecretReference identityInfoReference) {
    this.identityInfoReference = identityInfoReference;
  }

  /**
   * Converts this service identity credential into its corresponding persisted form (DPO).
   *
   * <p>The DPO contains only a reference to the credential, not the credential itself, as the
   * actual secrets are managed externally.
   *
   * @return The persistence object representation
   */
  public abstract @Nonnull ServiceIdentityInfoDpo asServiceIdentityInfoDpo();

  /**
   * Converts this service identity credential into its API model representation.
   *
   * <p>The model contains identity information (e.g., IAM ARN) but excludes sensitive credentials
   * such as access keys or session tokens.
   *
   * @return The API model representation for client responses
   */
  public abstract @Nonnull ServiceIdentityInfo asServiceIdentityInfoModel();
}
