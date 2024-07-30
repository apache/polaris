/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.security.SecureRandom;

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

  // the main secret for that principal
  private String mainSecret;

  // the secondary secret for that principal
  private String secondarySecret;

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

  @JsonCreator
  public PolarisPrincipalSecrets(
      @JsonProperty("principalId") long principalId,
      @JsonProperty("principalClientId") String principalClientId,
      @JsonProperty("mainSecret") String mainSecret,
      @JsonProperty("secondarySecret") String secondarySecret) {
    this.principalId = principalId;
    this.principalClientId = principalClientId;
    this.mainSecret = mainSecret;
    this.secondarySecret = secondarySecret;
  }

  public PolarisPrincipalSecrets(PolarisPrincipalSecrets principalSecrets) {
    this.principalId = principalSecrets.getPrincipalId();
    this.principalClientId = principalSecrets.getPrincipalClientId();
    this.mainSecret = principalSecrets.getMainSecret();
    this.secondarySecret = principalSecrets.getSecondarySecret();
  }

  public PolarisPrincipalSecrets(long principalId) {
    this.principalId = principalId;
    this.principalClientId = this.generateRandomHexString(16);
    this.mainSecret = this.generateRandomHexString(32);
    this.secondarySecret = this.generateRandomHexString(32);
  }

  /**
   * Rotate the main secrets
   *
   * @param mainSecretToRotate the main secrets to rotate
   */
  public void rotateSecrets(String mainSecretToRotate) {
    this.secondarySecret = mainSecretToRotate;
    this.mainSecret = this.generateRandomHexString(32);
  }

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
}
