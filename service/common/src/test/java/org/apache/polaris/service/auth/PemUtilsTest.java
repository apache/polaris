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
package org.apache.polaris.service.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PemUtilsTest {
  private static final String RSA_ALGORITHM = "RSA";

  private static final String PUBLIC_KEY_HEADER = "-----BEGIN PUBLIC KEY-----";

  private static final String PUBLIC_KEY_FOOTER = "-----END PUBLIC KEY-----";

  private static final String PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----";

  private static final String PRIVATE_KEY_FOOTER = "-----END PRIVATE KEY-----";

  private static final String LINE_SEPARATOR = System.lineSeparator();

  private static final String RSA_PUBLIC_KEY_FILE = "rsa-public-key.pem";

  private static final String RSA_PRIVATE_KEY_FILE = "rsa-private-key.pem";

  private static final String RSA_PUBLIC_KEY_AND_PRIVATE_KEY_FILE = "rsa-public-key-pair.pem";

  private static final String EMPTY_FILE = "empty.pem";

  private static final Base64.Encoder encoder = Base64.getMimeEncoder();

  @TempDir private static Path tempDir;

  private static Path rsaRublicKeyPath;

  private static PublicKey rsaPublicKey;

  private static Path rsaPrivateKeyPath;

  private static PrivateKey rsaPrivateKey;

  private static Path rsaPublicKeyAndPrivateKeyPath;

  private static Path emptyFilePath;

  @BeforeAll
  public static void setKeyPair() throws NoSuchAlgorithmException, IOException {
    final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
    final KeyPair keyPair = keyPairGenerator.generateKeyPair();
    rsaPublicKey = keyPair.getPublic();
    rsaPrivateKey = keyPair.getPrivate();

    final String publicKeyEncoded = getPublicKeyEncoded(rsaPublicKey);
    final String privateKeyEncoded = getPrivateKeyEncoded(rsaPrivateKey);

    rsaRublicKeyPath = tempDir.resolve(RSA_PUBLIC_KEY_FILE);
    Files.writeString(rsaRublicKeyPath, publicKeyEncoded);

    rsaPrivateKeyPath = tempDir.resolve(RSA_PRIVATE_KEY_FILE);
    Files.writeString(rsaPrivateKeyPath, privateKeyEncoded);

    rsaPublicKeyAndPrivateKeyPath = tempDir.resolve(RSA_PUBLIC_KEY_AND_PRIVATE_KEY_FILE);
    final String rsaPublicKeyAndPrivateKey = publicKeyEncoded + LINE_SEPARATOR + privateKeyEncoded;
    Files.writeString(rsaPublicKeyAndPrivateKeyPath, rsaPublicKeyAndPrivateKey);

    emptyFilePath = tempDir.resolve(EMPTY_FILE);
    Files.write(emptyFilePath, new byte[0]);
  }

  @Test
  public void testReadPublicKeyFromFileRSA() throws IOException {
    final PublicKey publicKeyRead = PemUtils.readPublicKeyFromFile(rsaRublicKeyPath, RSA_ALGORITHM);

    assertEquals(rsaPublicKey, publicKeyRead);
  }

  @Test
  public void testReadPrivateKeyFromFileRSA() throws IOException {
    final PrivateKey privateKeyRead =
        PemUtils.readPrivateKeyFromFile(rsaPrivateKeyPath, RSA_ALGORITHM);

    assertEquals(rsaPrivateKey, privateKeyRead);
  }

  @Test
  public void testReadPublicKeyFromFileRSAWithPrivateKeyIgnored() throws IOException {
    final PublicKey publicKeyRead =
        PemUtils.readPublicKeyFromFile(rsaPublicKeyAndPrivateKeyPath, RSA_ALGORITHM);

    assertEquals(rsaPublicKey, publicKeyRead);
  }

  @Test
  public void testReadEmptyFIle() {
    assertThrows(
        IOException.class, () -> PemUtils.readPublicKeyFromFile(emptyFilePath, RSA_ALGORITHM));
  }

  private static String getPublicKeyEncoded(final PublicKey publicKey) {
    final StringBuilder builder = new StringBuilder();

    builder.append(PUBLIC_KEY_HEADER);
    builder.append(LINE_SEPARATOR);

    final byte[] publicKeyEncoded = publicKey.getEncoded();
    final String encoded = encoder.encodeToString(publicKeyEncoded);
    builder.append(encoded);
    builder.append(LINE_SEPARATOR);

    builder.append(PUBLIC_KEY_FOOTER);
    builder.append(LINE_SEPARATOR);

    return builder.toString();
  }

  private static String getPrivateKeyEncoded(final PrivateKey privateKey) {
    final StringBuilder builder = new StringBuilder();

    builder.append(PRIVATE_KEY_HEADER);
    builder.append(LINE_SEPARATOR);

    final byte[] privateKeyEncoded = privateKey.getEncoded();
    final String encoded = encoder.encodeToString(privateKeyEncoded);
    builder.append(encoded);
    builder.append(LINE_SEPARATOR);

    builder.append(PRIVATE_KEY_FOOTER);
    builder.append(LINE_SEPARATOR);

    return builder.toString();
  }
}
