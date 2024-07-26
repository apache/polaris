/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package io.polaris.core.persistence.models;

import io.polaris.core.entity.PolarisPrincipalSecrets;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

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

    public ModelPrincipalSecrets build() {
      return principalSecrets;
    }
  }

  public static ModelPrincipalSecrets fromPrincipalSecrets(PolarisPrincipalSecrets record) {
    if (record == null) return null;

    return ModelPrincipalSecrets.builder()
        .principalId(record.getPrincipalId())
        .principalClientId(record.getPrincipalClientId())
        .mainSecret(record.getMainSecret())
        .secondarySecret(record.getSecondarySecret())
        .build();
  }

  public static PolarisPrincipalSecrets toPrincipalSecrets(ModelPrincipalSecrets model) {
    if (model == null) return null;

    return new PolarisPrincipalSecrets(
        model.getPrincipalId(),
        model.getPrincipalClientId(),
        model.getMainSecret(),
        model.getSecondarySecret());
  }

  public void update(PolarisPrincipalSecrets principalSecrets) {
    if (principalSecrets == null) return;

    this.principalId = principalSecrets.getPrincipalId();
    this.principalClientId = principalSecrets.getPrincipalClientId();
    this.mainSecret = principalSecrets.getMainSecret();
    this.secondarySecret = principalSecrets.getSecondarySecret();
  }
}
