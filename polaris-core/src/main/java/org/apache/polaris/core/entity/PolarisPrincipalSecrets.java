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
package org.apache.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import org.apache.polaris.core.DigestUtils;
import org.jspecify.annotations.Nullable;

/**
 * Simple class to represent the secrets used to authenticate a catalog principal, These secrets are
 * managed separately.
 */
public class PolarisPrincipalSecrets {

  // secure random number generator
  private static final SecureRandom secureRandom = new SecureRandom();

  // the id of the principal
  private final long principalId;

  // the client id for that principal
  private final String principalClientId;

  // the main secret hash for that principal
  private String mainSecret;

  // the secondary secret for that principal
  private String secondarySecret;

  // Hash of mainSecret
  private String mainSecretHash;

  // Hash of secondarySecret
  private String secondarySecretHash;

  private String secretSalt;

  /**
   * Generate a secure random string
   *
   * @return the secure random string we generated
   */
  private String generateRandomHexString(int stringLength) {

    // generate random byte array
    byte[] randomBytes =
        new byte[stringLength / 2]; // Each byte will be represented by two hex characters
    secureRandom.nextBytes(randomBytes);

    // build string
    StringBuilder sb = new StringBuilder();
    for (byte randomByte : randomBytes) {
      sb.append(String.format("%02x", randomByte));
    }

    return sb.toString();
  }

  private String hashSecret(String secret) {
    return DigestUtils.sha256Hex(secret + ":" + secretSalt);
  }

  @JsonCreator
  public PolarisPrincipalSecrets(
      @JsonProperty("principalId") long principalId,
      @JsonProperty("principalClientId") String principalClientId,
      @JsonProperty("mainSecret") String mainSecret,
      @JsonProperty("secondarySecret") String secondarySecret,
      @JsonProperty("secretSalt") String secretSalt,
      @JsonProperty("mainSecretHash") String mainSecretHash,
      @JsonProperty("secondarySecretHash") String secondarySecretHash) {
    this.principalId = principalId;
    this.principalClientId = principalClientId;
    this.mainSecret = mainSecret;
    this.secondarySecret = secondarySecret;

    this.secretSalt = secretSalt;
    if (this.secretSalt == null) {
      this.secretSalt = generateRandomHexString(16);
    }
    this.mainSecretHash = mainSecretHash;
    if (this.mainSecretHash == null) {
      this.mainSecretHash = hashSecret(mainSecret);
    }
    this.secondarySecretHash = secondarySecretHash;
    if (this.secondarySecretHash == null) {
      this.secondarySecretHash = hashSecret(secondarySecret);
    }
  }

  public PolarisPrincipalSecrets(
      long principalId, String principalClientId, String mainSecret, String secondarySecret) {
    this.principalId = principalId;
    this.principalClientId = principalClientId;
    this.mainSecret = mainSecret;
    this.secondarySecret = secondarySecret;

    this.secretSalt = generateRandomHexString(16);
    this.mainSecretHash = hashSecret(mainSecret);
    this.secondarySecretHash = hashSecret(secondarySecret);
  }

  public PolarisPrincipalSecrets(PolarisPrincipalSecrets principalSecrets) {
    this.principalId = principalSecrets.getPrincipalId();
    this.principalClientId = principalSecrets.getPrincipalClientId();
    this.mainSecret = principalSecrets.getMainSecret();
    this.secondarySecret = principalSecrets.getSecondarySecret();
    this.secretSalt = principalSecrets.getSecretSalt();
    this.mainSecretHash = principalSecrets.getMainSecretHash();
    this.secondarySecretHash = principalSecrets.getSecondarySecretHash();
  }

  public PolarisPrincipalSecrets(long principalId) {
    this.principalId = principalId;
    this.principalClientId = this.generateRandomHexString(16);
    this.mainSecret = this.generateRandomHexString(32);
    this.secondarySecret = this.generateRandomHexString(32);

    this.secretSalt = this.generateRandomHexString(16);
    this.mainSecretHash = hashSecret(mainSecret);
    this.secondarySecretHash = hashSecret(secondarySecret);
  }

  public PolarisPrincipalSecrets(long principalId, String newClientId, @Nullable String newSecret) {
    this.principalId = principalId;
    this.principalClientId = newClientId;
    this.mainSecret = (newSecret != null) ? newSecret : this.generateRandomHexString(32);
    this.secondarySecret = this.generateRandomHexString(32);

    this.secretSalt = this.generateRandomHexString(16);
    this.mainSecretHash = hashSecret(mainSecret);
    this.secondarySecretHash = hashSecret(secondarySecret);
  }

  /** Rotate the main secrets */
  public void rotateSecrets(String newSecondaryHash) {
    this.secondarySecret = null;
    this.secondarySecretHash = newSecondaryHash;

    this.mainSecret = this.generateRandomHexString(32);
    this.mainSecretHash = hashSecret(mainSecret);
  }

  public long getPrincipalId() {
    return principalId;
  }

  public String getPrincipalClientId() {
    return principalClientId;
  }

  public boolean matchesSecret(String potentialSecret) {
    String potentialSecretHash = hashSecret(potentialSecret);
    return potentialSecretHash.equals(this.mainSecretHash)
        || potentialSecretHash.equals(this.secondarySecretHash);
  }

  public String getMainSecret() {
    return mainSecret;
  }

  public String getSecondarySecret() {
    return secondarySecret;
  }

  public String getMainSecretHash() {
    return mainSecretHash;
  }

  /**
   * Credentials-generation fingerprint for tokens minted against the <em>current</em> main secret.
   * Derived from existing fields with the principal's secret salt, so no schema change is required.
   * The value is not itself a credential and is safe to embed in signed (but not encrypted) JWTs.
   *
   * @return the main credentials version, or {@code null} when no main secret hash is set
   */
  @Nullable
  public String getCredentialsVersion() {
    return credentialsVersionForHash(mainSecretHash);
  }

  /**
   * Returns true when {@code credentialsVersion} matches the salted fingerprint of the current main
   * secret hash <em>or</em> the secondary secret hash. After a single rotate, the previous main
   * hash is kept as secondary so client secrets and bound JWTs both remain valid for that
   * generation; a further rotate/reset advances secondary and invalidates the older fingerprint.
   *
   * <p>Comparisons are constant-time against each candidate fingerprint.
   */
  public boolean matchesCredentialsVersion(@Nullable String credentialsVersion) {
    if (credentialsVersion == null || credentialsVersion.isEmpty()) {
      return false;
    }
    boolean matches = false;
    String mainVersion = credentialsVersionForHash(mainSecretHash);
    if (mainVersion != null) {
      matches |= constantTimeEquals(credentialsVersion, mainVersion);
    }
    String secondaryVersion = credentialsVersionForHash(secondarySecretHash);
    if (secondaryVersion != null) {
      matches |= constantTimeEquals(credentialsVersion, secondaryVersion);
    }
    return matches;
  }

  /**
   * Salted, non-reversible fingerprint of a secret-verification hash for embedding in JWTs. Uses
   * the same per-principal {@link #secretSalt} already stored for secret hashing.
   */
  @Nullable
  private String credentialsVersionForHash(@Nullable String secretHash) {
    if (secretHash == null || secretHash.isEmpty()) {
      return null;
    }
    String salt = secretSalt == null ? "" : secretSalt;
    return DigestUtils.sha256Hex(secretHash + ":" + salt);
  }

  private static boolean constantTimeEquals(String a, String b) {
    return MessageDigest.isEqual(
        a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
  }

  public String getSecondarySecretHash() {
    return secondarySecretHash;
  }

  public String getSecretSalt() {
    return secretSalt;
  }
}
