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
import java.security.SecureRandom;
import org.apache.commons.codec.digest.DigestUtils;

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

  public String getSecondarySecretHash() {
    return secondarySecretHash;
  }

  public String getSecretSalt() {
    return secretSalt;
  }
}
