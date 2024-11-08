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
package org.apache.polaris.core.persistence.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

/**
 * PrincipalSecrets model representing the secrets used to authenticate a catalog principal. This is
 * used to exchange the information with PRINCIPAL_SECRETS table
 */
@Entity
@Table(name = "PRINCIPAL_SECRETS")
public class ModelPrincipalSecrets {
  // the id of the principal
  private long principalId;

  // the client id for that principal
  @Id private String principalClientId;

  // the main secret for that principal
  private String mainSecret;

  // the secondary secret for that principal
  private String secondarySecret;

  // Hash of mainSecret
  private String mainSecretHash;

  // Hash of secondarySecret
  private String secondarySecretHash;

  private String secretSalt;

  // Used for Optimistic Locking to handle concurrent reads and updates
  @Version private long version;

  public long getPrincipalId() {
    return principalId;
  }

  public String getPrincipalClientId() {
    return principalClientId;
  }

  public String getMainSecret() {
    return mainSecret;
  }

  public String getSecondarySecret() {
    return secondarySecret;
  }

  public String getSecretSalt() {
    return secretSalt;
  }

  public String getMainSecretHash() {
    return mainSecretHash;
  }

  public String getSecondarySecretHash() {
    return secondarySecretHash;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final ModelPrincipalSecrets principalSecrets;

    private Builder() {
      principalSecrets = new ModelPrincipalSecrets();
    }

    public Builder principalId(long principalId) {
      principalSecrets.principalId = principalId;
      return this;
    }

    public Builder principalClientId(String principalClientId) {
      principalSecrets.principalClientId = principalClientId;
      return this;
    }

    public Builder mainSecret(String mainSecret) {
      principalSecrets.mainSecret = mainSecret;
      return this;
    }

    public Builder secondarySecret(String secondarySecret) {
      principalSecrets.secondarySecret = secondarySecret;
      return this;
    }

    public Builder secretSalt(String secretSalt) {
      principalSecrets.secretSalt = secretSalt;
      return this;
    }

    public Builder mainSecretHash(String mainSecretHash) {
      principalSecrets.mainSecretHash = mainSecretHash;
      return this;
    }

    public Builder secondarySecretHash(String secondarySecretHash) {
      principalSecrets.secondarySecretHash = secondarySecretHash;
      return this;
    }

    public ModelPrincipalSecrets build() {
      return principalSecrets;
    }
  }

  public static ModelPrincipalSecrets fromPrincipalSecrets(PolarisPrincipalSecrets record) {
    if (record == null) return null;

    return ModelPrincipalSecrets.builder()
        .principalId(record.getPrincipalId())
        .principalClientId(record.getPrincipalClientId())
        .secretSalt(record.getSecretSalt())
        .mainSecretHash(record.getMainSecretHash())
        .secondarySecretHash(record.getSecondarySecretHash())
        .build();
  }

  public static PolarisPrincipalSecrets toPrincipalSecrets(ModelPrincipalSecrets model) {
    if (model == null) return null;

    return new PolarisPrincipalSecrets(
        model.getPrincipalId(),
        model.getPrincipalClientId(),
        model.getMainSecret(),
        model.getSecondarySecret(),
        model.getSecretSalt(),
        model.getMainSecretHash(),
        model.getSecondarySecretHash());
  }

  public void update(PolarisPrincipalSecrets principalSecrets) {
    if (principalSecrets == null) return;

    this.principalId = principalSecrets.getPrincipalId();
    this.principalClientId = principalSecrets.getPrincipalClientId();
    this.secretSalt = principalSecrets.getSecretSalt();
    this.mainSecret = principalSecrets.getMainSecret();
    this.secondarySecret = principalSecrets.getSecondarySecret();
    this.mainSecretHash = principalSecrets.getMainSecretHash();
    this.secondarySecretHash = principalSecrets.getSecondarySecretHash();

    // Once a hash is stored, do not keep the original secret
    if (this.mainSecretHash != null) {
      this.mainSecret = null;
    }
    if (this.secondarySecretHash != null) {
      this.secondarySecret = null;
    }
  }
}
