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
package org.apache.polaris.core.secrets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a "wrapped reference" to a user-owned secret that holds an identifier to retrieve
 * possibly remotely-stored secret material, along with an open-ended "referencePayload" that is
 * specific to an implementation of the secret storage and which is needed "unwrap" the actual
 * secret in combination with whatever is stored in the remote secrets storage.
 *
 * <p>Example scenarios:
 *
 * <p>If an implementation simply stores secrets directly in the secrets manager, the
 * referencePayload may be empty and "unwrapping" would be a simple identity/no-op transformation.
 *
 * <p>If tampering or corruption of secrets in the secrets manager presents a unique threat, an
 * implementation may use the referencePayload to ensure data integrity of the secret by storing a
 * checksum or hash of the stored secret.
 *
 * <p>If the system must protect against independent exfiltration/attacks on a dedicated secrets
 * manager and the core persistence database, the referencePayload may be used to coordinate
 * secondary encryption keys such that the original secret can only be fully "unwrapped" given both
 * the stored "secret material" as well as the referencePayload and any associated keys used for
 * encryption.
 */
public class UserSecretReference {
  @JsonProperty(value = "urn")
  private final String urn;

  @JsonProperty(value = "referencePayload")
  private final Map<String, String> referencePayload;

  /**
   * @param urn A string which should be self-sufficient to retrieve whatever secret material that
   *     is stored in the remote secret store and also to identify an implementation of the
   *     UserSecretsManager which is capable of interpreting this concrete UserSecretReference.
   *     Should be of the form:
   *     'urn:polaris-secret:&lt;secret-manager-type&gt;:&lt;type-specific-identifier&gt;
   * @param referencePayload Optionally, any additional information that is necessary to fully
   *     reconstitute the original secret based on what is retrieved by the {@code urn}; this
   *     payload may include hashes/checksums, encryption key ids, OTP encryption keys, additional
   *     protocol/version specifiers, etc., which are implementation-specific.
   */
  public UserSecretReference(
      @JsonProperty(value = "urn", required = true) @Nonnull String urn,
      @JsonProperty(value = "referencePayload") @Nullable Map<String, String> referencePayload) {
    // TODO: Add better/standardized parsing and validation of URN syntax
    Preconditions.checkArgument(
        urn.startsWith("urn:polaris-secret:") && urn.split(":").length >= 4,
        "Invalid secret URN '%s'; must be of the form "
            + "'urn:polaris-secret:<secret-manager-type>:<type-specific-identifier>'",
        urn);
    this.urn = urn;
    this.referencePayload = Objects.requireNonNullElse(referencePayload, new HashMap<>());
  }

  /**
   * Since UserSecretReference objects are specific to UserSecretManager implementations, the
   * "secret-manager-type" portion of the URN should be used to validate that a URN is valid for a
   * given implementation and to dispatch to the correct implementation at runtime if multiple
   * concurrent implementations are possible in a given runtime environment.
   */
  @JsonIgnore
  public String getUserSecretManagerTypeFromUrn() {
    // TODO: Add better/standardized parsing and validation of URN syntax
    return urn.split(":")[2];
  }

  public @Nonnull String getUrn() {
    return urn;
  }

  public @Nonnull Map<String, String> getReferencePayload() {
    return referencePayload;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUrn(), getReferencePayload());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof UserSecretReference)) {
      return false;
    }
    UserSecretReference that = (UserSecretReference) obj;
    return Objects.equals(this.getUrn(), that.getUrn())
        && Objects.equals(this.getReferencePayload(), that.getReferencePayload());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("urn", getUrn())
        .add("referencePayload", String.format("<num entries: %d>", getReferencePayload().size()))
        .toString();
  }
}
