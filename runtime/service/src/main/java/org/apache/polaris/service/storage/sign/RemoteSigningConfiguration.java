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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Configuration for remote sign. */
@ConfigMapping(prefix = "polaris.remote-sign")
public interface RemoteSigningConfiguration {

  /**
   * Map of key IDs to their base64-encoded secret keys. These keys are used for encrypting and
   * decrypting JWEs.
   *
   * <p>Each key must have the correct length for the selected encryption method. Multiple keys can
   * be configured to enable smooth key rotation - old keys can still decrypt existing tokens while
   * new tokens are encrypted with the current key.
   *
   * <p>Multiple keys can be configured to enable smooth key rotation. The {@link #currentKeyId()}
   * specifies which key is used for encryption, while all configured keys can be used for
   * decryption.
   *
   * <p>For production use, consider using {@link #keyFiles()} instead to avoid exposing secrets in
   * configuration.
   *
   * <p>Example of configuration:
   *
   * <pre>
   * polaris.remote-signing.keys.key1=base64EncodedKey1
   * polaris.remote-signing.keys.key2=base64EncodedKey2
   * </pre>
   *
   * @return map of key IDs to base64-encoded secret keys
   */
  Map<String, String> keys();

  /**
   * Map of key IDs to file paths containing base64-encoded secret keys. These keys are used for
   * encrypting and decrypting JWEs.
   *
   * <p>Each file should contain a single base64-encoded key that is exactly 32 bytes (256 bits)
   * when decoded. The file content is read at startup and trimmed of leading/trailing whitespace.
   *
   * <p>Multiple keys can be configured to enable smooth key rotation. The {@link #currentKeyId()}
   * specifies which key is used for encryption, while all configured keys can be used for
   * decryption.
   *
   * <p>This is the preferred method for production deployments as it avoids exposing secrets in
   * configuration files or environment variables.
   *
   * <p>Example of configuration:
   *
   * <pre>
   * polaris.remote-signing.key-files.key1=/etc/polaris/secrets/signing-key1
   * polaris.remote-signing.key-files.key2=/etc/polaris/secrets/signing-key2
   * </pre>
   *
   * @return map of key IDs to file paths containing base64-encoded secret keys
   */
  Map<String, Path> keyFiles();

  /**
   * Path to a directory containing key files. Each file in the directory represents a signing key
   * where the filename is used as the key ID and the file content is the base64-encoded secret key.
   *
   * <p>Each file should contain a single base64-encoded key that is exactly 32 bytes (256 bits)
   * when decoded. The file content is read at startup and trimmed of leading/trailing whitespace.
   *
   * <p>This is particularly useful for Kubernetes deployments where secrets can be mounted as files
   * in a volume directory.
   *
   * <p>Keys loaded from this directory will override keys with the same ID from {@link #keys()} or
   * {@link #keyFiles()}.
   *
   * <p>Example of configuration:
   *
   * <pre>
   * polaris.remote-signing.key-directory=/etc/polaris/secrets/signing-keys/
   * </pre>
   *
   * <p>With files:
   *
   * <pre>
   * /etc/polaris/secrets/signing-keys/key1  (content: base64 encoded key)
   * /etc/polaris/secrets/signing-keys/key2  (content: base64 encoded key)
   * </pre>
   *
   * @return optional path to the key directory
   */
  Optional<Path> keyDirectory();

  /**
   * The key ID to use for encrypting new tokens.
   *
   * <p>This key ID must exist in either the {@link #keys()} or {@link #keyFiles()} map. During key
   * rotation, update this value to the new key ID after adding the new key.
   *
   * @return the current key ID for encryption
   */
  @WithDefault("default")
  String currentKeyId();

  /**
   * The duration for which signed parameters are valid. The default is 7 hours.
   *
   * <p>After this duration, the signed parameters will be considered expired and the signing
   * request will be rejected.
   *
   * <p>If this value is set to zero or negative, tokens will be created without an expiration time.
   *
   * @return the token lifespan duration
   */
  @WithDefault("PT7H")
  Duration tokenLifespan();

  /**
   * The JWE encryption method to use for encrypting tokens. The default is A256CBC-HS512.
   *
   * <p>Supported encryption methods are:
   *
   * <ul>
   *   <li>{@code A128CBC-HS256} - AES-128-CBC with HMAC-SHA-256 (requires 32-byte key)
   *   <li>{@code A192CBC-HS384} - AES-192-CBC with HMAC-SHA-384 (requires 48-byte key)
   *   <li>{@code A256CBC-HS512} - AES-256-CBC with HMAC-SHA-512 (requires 64-byte key)
   *   <li>{@code A128GCM} - AES-128-GCM (requires 16-byte key)
   *   <li>{@code A192GCM} - AES-192-GCM (requires 24-byte key)
   *   <li>{@code A256GCM} - AES-256-GCM (requires 32-byte key)
   * </ul>
   *
   * <p>All configured keys must have the correct length for the selected encryption method.
   *
   * @return the encryption method name
   */
  @WithDefault("A256CBC-HS512")
  String encryptionMethod();
}
