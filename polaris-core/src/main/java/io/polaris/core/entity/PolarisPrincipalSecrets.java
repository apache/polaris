/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
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
