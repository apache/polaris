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
package org.apache.polaris.service.storage.sign;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEDecrypter;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.crypto.DirectDecrypter;
import com.nimbusds.jose.crypto.DirectEncrypter;
import com.nimbusds.jwt.EncryptedJWT;
import com.nimbusds.jwt.JWTClaimsSet;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.storage.StorageLocation;

/**
 * Service for creating and validating remote signing tokens using JWE (JSON Web Encryption).
 *
 * <p>This service encrypts {@link RemoteSigningToken} instances into a JWE token using direct
 * encryption. The token includes an expiration time and can be validated to ensure it hasn't been
 * tampered with or expired.
 *
 * <p>Multiple keys can be configured to enable smooth key rotation. The current key (specified by
 * {@link RemoteSigningConfiguration#currentKeyId()}) is used for encryption, while all configured
 * keys can be used for decryption. This allows existing tokens to remain valid during key rotation.
 *
 * <p>The encryption method can be configured via {@link
 * RemoteSigningConfiguration#encryptionMethod()}. Supported methods include AES-CBC with HMAC,
 * AES-GCM, and XChaCha20-Poly1305.
 */
@ApplicationScoped
public class RemoteSigningTokenService {

  private static final String CLAIM_PRINCIPAL_NAME = "p";
  private static final String CLAIM_CATALOG_NAME = "c";
  private static final String CLAIM_TABLE_IDENTIFIER = "t";
  private static final String CLAIM_ALLOWED_LOCATIONS = "l";
  private static final String CLAIM_READ_WRITE = "w";

  private final RemoteSigningConfiguration configuration;
  private final Clock clock;
  private final Map<String, SecretKey> encryptionKeys;
  private final String currentKeyId;
  private final EncryptionMethod encryptionMethod;

  @Inject
  public RemoteSigningTokenService(RemoteSigningConfiguration configuration, Clock clock) {
    this.configuration = configuration;
    this.clock = clock;
    this.encryptionMethod = parseEncryptionMethod(configuration.encryptionMethod());
    this.encryptionKeys = initializeKeys(configuration, this.encryptionMethod);
    this.currentKeyId = configuration.currentKeyId();
  }

  /**
   * Creates an encrypted JWE token containing the remote signing token.
   *
   * <p>The token is encrypted using the current key (specified by {@link
   * RemoteSigningConfiguration#currentKeyId()}).
   *
   * @param token the token to encrypt
   * @return the encrypted JWE token string
   * @throws IllegalArgumentException if encryption fails
   */
  public String encrypt(RemoteSigningToken token) {
    try {
      Instant now = clock.instant();

      JWTClaimsSet.Builder claimsBuilder =
          new JWTClaimsSet.Builder()
              .issueTime(Date.from(now))
              .claim(CLAIM_PRINCIPAL_NAME, token.principalName())
              .claim(CLAIM_CATALOG_NAME, token.catalogName())
              .claim(
                  CLAIM_TABLE_IDENTIFIER,
                  PolarisCatalogHelpers.tableIdentifierToList(token.tableIdentifier()))
              .claim(
                  CLAIM_ALLOWED_LOCATIONS,
                  token.allowedLocations().stream()
                      .map(StorageLocation::toString)
                      .collect(Collectors.toList()))
              .claim(CLAIM_READ_WRITE, token.readWrite());

      // Only set expiration time if tokens should expire
      if (configuration.tokenLifespan().isPositive()) {
        claimsBuilder.expirationTime(Date.from(now.plus(configuration.tokenLifespan())));
      }

      JWTClaimsSet claimsSet = claimsBuilder.build();

      JWEHeader header =
          new JWEHeader.Builder(JWEAlgorithm.DIR, encryptionMethod).keyID(currentKeyId).build();

      SecretKey currentKey = encryptionKeys.get(currentKeyId);
      EncryptedJWT encryptedJWT = new EncryptedJWT(header, claimsSet);
      encryptedJWT.encrypt(new DirectEncrypter(currentKey));

      return encryptedJWT.serialize();
    } catch (JOSEException e) {
      throw new IllegalArgumentException("Failed to create remote signing token", e);
    }
  }

  /**
   * Validates and decrypts a JWE token to extract the remote signing token.
   *
   * <p>The key ID from the token header is validated against the configured keys. This ensures that
   * tokens encrypted with unknown keys are rejected.
   *
   * <p>If unexpirable tokens are enabled, tokens without an expiration time are accepted.
   * Otherwise, tokens must have a valid, non-expired expiration time.
   *
   * @param token the JWE token string
   * @return the decrypted parameters
   * @throws IllegalArgumentException if decryption or validation fails, or if the key ID is unknown
   */
  public RemoteSigningToken decrypt(String token) {
    try {
      // Parse the JWE token without decrypting
      EncryptedJWT jwe = EncryptedJWT.parse(token);

      String keyId = jwe.getHeader().getKeyID();
      if (keyId == null) {
        throw new IllegalArgumentException("Token is missing key ID header");
      }

      SecretKey decryptionKey = encryptionKeys.get(keyId);
      if (decryptionKey == null) {
        throw new IllegalArgumentException("Unknown key ID: " + keyId);
      }

      JWEDecrypter decrypter = new DirectDecrypter(decryptionKey);
      jwe.decrypt(decrypter);

      JWTClaimsSet claims = jwe.getJWTClaimsSet();

      Date expirationTime = claims.getExpirationTime();
      if (expirationTime == null) {
        if (configuration.tokenLifespan().isPositive()) {
          throw new IllegalArgumentException("Token is missing expiration time");
        }
      } else if (expirationTime.toInstant().isBefore(clock.instant())) {
        throw new IllegalArgumentException("Invalid or expired remote signing token");
      }

      String principalName = claims.getStringClaim(CLAIM_PRINCIPAL_NAME);
      String catalogName = claims.getStringClaim(CLAIM_CATALOG_NAME);
      List<String> tableIdentifier = claims.getStringListClaim(CLAIM_TABLE_IDENTIFIER);
      List<String> locations = claims.getStringListClaim(CLAIM_ALLOWED_LOCATIONS);
      Boolean readWrite = claims.getBooleanClaim(CLAIM_READ_WRITE);

      if (principalName == null
          || catalogName == null
          || tableIdentifier == null
          || locations == null
          || readWrite == null) {
        throw new IllegalArgumentException("Token is missing required claims");
      }

      return RemoteSigningToken.builder()
          .principalName(principalName)
          .catalogName(catalogName)
          .tableIdentifier(TableIdentifier.of(tableIdentifier.toArray(new String[0])))
          .allowedLocations(locations.stream().map(StorageLocation::of).collect(Collectors.toSet()))
          .readWrite(readWrite)
          .build();

    } catch (ParseException | JOSEException e) {
      throw new IllegalArgumentException("Failed to parse remote signing token", e);
    }
  }

  private static EncryptionMethod parseEncryptionMethod(String methodName) {
    EncryptionMethod method = EncryptionMethod.parse(methodName);
    if (method.cekBitLength() == 0) {
      throw new IllegalStateException(
          "Unsupported encryption method: "
              + methodName
              + ". Supported methods are: A128CBC-HS256, A192CBC-HS384, A256CBC-HS512, "
              + "A128GCM, A192GCM, A256GCM");
    }
    return method;
  }

  private static Map<String, SecretKey> initializeKeys(
      RemoteSigningConfiguration configuration, EncryptionMethod encryptionMethod) {
    Map<String, String> inlineKeys = configuration.keys();
    Map<String, Path> keyFiles = configuration.keyFiles();
    Optional<Path> keyDirectory = configuration.keyDirectory();

    int requiredLength = encryptionMethod.cekBitLength() / 8;

    if (inlineKeys.isEmpty() && keyFiles.isEmpty() && keyDirectory.isEmpty()) {
      // Generate random secret key
      // (this should not happen in production, see ProductionReadinessChecks)
      byte[] keyBytes = new byte[requiredLength];
      new SecureRandom().nextBytes(keyBytes);
      return Map.of("default", new SecretKeySpec(keyBytes, "AES"));
    }

    Map<String, SecretKey> keys = new HashMap<>();

    // Load inline keys
    for (Map.Entry<String, String> entry : inlineKeys.entrySet()) {
      String keyId = entry.getKey();
      String base64Key = entry.getValue();
      keys.put(keyId, decodeKey(keyId, base64Key, encryptionMethod));
    }

    // Load keys from files (these override inline keys if same key ID)
    for (Map.Entry<String, Path> entry : keyFiles.entrySet()) {
      String keyId = entry.getKey();
      Path keyFile = entry.getValue();
      String base64Key = readKeyFromFile(keyId, keyFile);
      keys.put(keyId, decodeKey(keyId, base64Key, encryptionMethod));
    }

    // Load keys from directory (these override inline and file-based keys if same key ID)
    if (keyDirectory.isPresent()) {
      Path dir = keyDirectory.get();
      if (!Files.isDirectory(dir)) {
        throw new IllegalStateException(
            "Key directory does not exist or is not a directory: " + dir);
      }
      try (var stream = Files.list(dir)) {
        stream
            .filter(Files::isRegularFile)
            .forEach(
                keyFile -> {
                  String keyId = keyFile.getFileName().toString();
                  String base64Key = readKeyFromFile(keyId, keyFile);
                  keys.put(keyId, decodeKey(keyId, base64Key, encryptionMethod));
                });
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read keys from directory: " + dir, e);
      }
    }

    String currentId = configuration.currentKeyId();
    if (!keys.containsKey(currentId)) {
      throw new IllegalStateException(
          "Current key ID '"
              + currentId
              + "' not found in configured keys. "
              + "Available key IDs: "
              + keys.keySet().stream().sorted().collect(Collectors.joining(", ")));
    }

    return Map.copyOf(keys);
  }

  private static String readKeyFromFile(String keyId, Path keyFile) {
    try {
      return Files.readString(keyFile).trim();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read signing key '" + keyId + "' from file: " + keyFile, e);
    }
  }

  private static SecretKey decodeKey(
      String keyId, String base64Key, EncryptionMethod encryptionMethod) {
    byte[] keyBytes = Base64.getDecoder().decode(base64Key);
    int requiredLength = encryptionMethod.cekBitLength() / 8;
    if (keyBytes.length != requiredLength) {
      throw new IllegalStateException(
          "Secret key '%s' must be exactly %d bytes for %s encryption, but was %d bytes."
              .formatted(keyId, requiredLength, encryptionMethod.getName(), keyBytes.length));
    }
    return new SecretKeySpec(keyBytes, "AES");
  }
}
