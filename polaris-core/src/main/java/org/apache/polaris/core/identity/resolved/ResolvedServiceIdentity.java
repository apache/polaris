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
package org.apache.polaris.core.identity.resolved;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.secrets.ServiceSecretReference;
import software.amazon.awssdk.annotations.NotNull;

/**
 * Represents a resolved service identity.
 *
 * <p>This class is used to represent the identity of a service after it has been resolved. It
 * contains the type of the identity and any additional information for the service identity. E.g.,
 * The credential of the service identity.
 */
public abstract class ResolvedServiceIdentity {
  private final ServiceIdentityType identityType;
  private ServiceSecretReference identityInfoReference;

  public ResolvedServiceIdentity(ServiceIdentityType identityType) {
    this(identityType, null);
  }

  public ResolvedServiceIdentity(
      ServiceIdentityType identityType, ServiceSecretReference identityInfoReference) {
    this.identityType = identityType;
    this.identityInfoReference = identityInfoReference;
  }

  public @NotNull ServiceIdentityType getIdentityType() {
    return identityType;
  }

  public @Nonnull ServiceSecretReference getIdentityInfoReference() {
    return identityInfoReference;
  }

  public void setIdentityInfoReference(@NotNull ServiceSecretReference identityInfoReference) {
    this.identityInfoReference = identityInfoReference;
  }

  /** Converts this resolved identity into its corresponding persisted form (DPO). */
  public abstract @Nonnull ServiceIdentityInfoDpo asServiceIdentityInfoDpo();

  public abstract @Nonnull ServiceIdentityInfo asServiceIdentityInfoModel();
}
